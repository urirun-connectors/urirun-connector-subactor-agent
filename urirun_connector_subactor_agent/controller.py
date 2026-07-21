"""Deterministic control loop over task packages owned by ``todo-agent``.

The connector never accepts a command or a working directory from an actor.  The
operator configures one todo-agent repository through ``SUBACTOR_TODO_ROOT`` and
all execution is delegated to todo-agent's validated runner.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

STATE_SCHEMA = "subactor-agent.state.v1"
TRIGGER_KINDS = frozenset({"manual", "schedule", "ticket", "repository", "webhook", "retry"})
IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$")
TASK_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
CONTEXT_FIELDS = frozenset({"source", "repository", "ticket_id", "ref", "reason"})
_THREAD_CYCLE_LOCK = threading.Lock()
_THREAD_STATE_LOCK = threading.Lock()


class ControllerError(RuntimeError):
    """Base error translated to a urirun failure envelope by the URI layer."""


class ConfigurationError(ControllerError):
    """Deployment configuration is missing or invalid."""


class CycleBusyError(ControllerError):
    """Another control cycle already owns this connector's execution lease."""


@dataclass(frozen=True)
class Settings:
    root: Path
    state_dir: Path
    enabled: bool
    allow_apply: bool
    max_tasks: int
    max_cycles: int
    retry_base_seconds: int
    retry_max_seconds: int


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name)
    try:
        value = int(raw) if raw is not None else default
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if value < minimum or value > maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def load_settings(*, require_root: bool = True) -> Settings:
    raw_root = os.environ.get("SUBACTOR_TODO_ROOT", "").strip()
    if not raw_root:
        if require_root:
            raise ConfigurationError("SUBACTOR_TODO_ROOT is not configured")
        root = Path(".").resolve()
    else:
        root = Path(raw_root).expanduser().resolve()
    if require_root and (not root.is_dir() or not (root / "TODO").is_dir()):
        raise ConfigurationError("SUBACTOR_TODO_ROOT must contain a TODO directory")

    raw_state = os.environ.get("SUBACTOR_AGENT_STATE_DIR", "").strip()
    state_dir = (
        Path(raw_state).expanduser().resolve()
        if raw_state
        else Path("~/.local/state/urirun/subactor-agent").expanduser().resolve()
    )
    return Settings(
        root=root,
        state_dir=state_dir,
        enabled=_env_bool("SUBACTOR_AGENT_ENABLED"),
        allow_apply=_env_bool("SUBACTOR_AGENT_ALLOW_APPLY"),
        max_tasks=_env_int("SUBACTOR_AGENT_MAX_TASKS", 10, 1, 100),
        max_cycles=_env_int("SUBACTOR_AGENT_MAX_CYCLES", 100, 1, 1000),
        retry_base_seconds=_env_int("SUBACTOR_AGENT_RETRY_BASE_SECONDS", 60, 1, 86400),
        retry_max_seconds=_env_int("SUBACTOR_AGENT_RETRY_MAX_SECONDS", 3600, 1, 604800),
    )


def _empty_state() -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        "generation": 0,
        "pending_events": [],
        "processed_event_ids": [],
        "receipts": {},
        "task_health": {},
        "history": [],
    }


class StateStore:
    """Small atomic JSON state store shared by cron, webhook and manual calls."""

    def __init__(self, directory: Path):
        self.directory = directory
        self.path = directory / "state.json"
        self.lock_path = directory / "state.lock"

    def _ensure(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _lock(self) -> Iterator[None]:
        self._ensure()
        with self.lock_path.open("a+", encoding="utf-8") as stream:
            try:
                import fcntl
            except ImportError:  # pragma: no cover - Linux is the deployment target
                with _THREAD_STATE_LOCK:
                    yield
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def _read_unlocked(self) -> dict[str, Any]:
        if not self.path.exists():
            return _empty_state()
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ControllerError("subactor-agent state is unreadable") from exc
        if not isinstance(value, dict) or value.get("schema") != STATE_SCHEMA:
            raise ControllerError("subactor-agent state has an unsupported schema")
        return value

    def _write_unlocked(self, state: dict[str, Any]) -> None:
        temporary = self.path.with_name(f"state.{os.getpid()}.{threading.get_ident()}.tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, self.path)

    def read(self) -> dict[str, Any]:
        with self._lock():
            return self._read_unlocked()

    def update(self, transform: Callable[[dict[str, Any]], Any]) -> Any:
        with self._lock():
            state = self._read_unlocked()
            result = transform(state)
            self._write_unlocked(state)
            return result


@contextmanager
def _cycle_lease(settings: Settings) -> Iterator[None]:
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    lease_path = settings.state_dir / "cycle.lock"
    acquired_thread = _THREAD_CYCLE_LOCK.acquire(blocking=False)
    if not acquired_thread:
        raise CycleBusyError("another subactor-agent cycle is running")
    try:
        with lease_path.open("a+", encoding="utf-8") as stream:
            try:
                import fcntl
            except ImportError:  # pragma: no cover - Linux is the deployment target
                yield
            else:
                try:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise CycleBusyError("another subactor-agent cycle is running") from exc
                try:
                    yield
                finally:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
    finally:
        _THREAD_CYCLE_LOCK.release()


def _todo_api() -> tuple[Callable[..., list[dict[str, Any]]], Callable[..., tuple[dict[str, Any], Path]]]:
    """Lazy dependency boundary: connector discovery works without importing todo-agent."""
    try:
        from todo_agent.discovery import discover_tasks
        from todo_agent.runner import run_task
    except ImportError as exc:
        raise ConfigurationError(
            "ifuri-todo-agent is not installed; install urirun-connector-subactor-agent[todo]"
        ) from exc
    return discover_tasks, run_task


def _parse_now(value: str = "") -> datetime:
    if not value:
        return datetime.now(UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ControllerError("now must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _valid_task_id(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized and not TASK_ID.fullmatch(normalized):
        raise ControllerError("invalid task_id")
    return normalized


def _valid_trigger(kind: str) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized not in TRIGGER_KINDS:
        raise ControllerError(f"trigger must be one of {sorted(TRIGGER_KINDS)}")
    return normalized


def discover(*, event: str = "manual", task_id: str = "", now: str = "") -> dict[str, Any]:
    settings = load_settings()
    normalized_event = _valid_trigger(event)
    selected_task = _valid_task_id(task_id)
    discovery_event = "schedule" if normalized_event == "schedule" else "manual"
    discover_tasks, _ = _todo_api()
    tasks = discover_tasks(
        settings.root,
        event=discovery_event,
        task_id=selected_task or None,
        now_utc=_parse_now(now),
    )
    return {
        "event": normalized_event,
        "discovery_event": discovery_event,
        "tasks": tasks,
        "count": len(tasks),
    }


def _normalized_context(value: dict[str, Any] | None) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ControllerError("context must be an object")
    unknown = set(value) - CONTEXT_FIELDS
    if unknown:
        raise ControllerError(f"unsupported context fields: {sorted(unknown)}")
    result: dict[str, str] = {}
    for key, item in value.items():
        if isinstance(item, (dict, list)):
            raise ControllerError("trigger context values must be scalar")
        text = str(item)
        if len(text) > 500:
            raise ControllerError("trigger context values are limited to 500 characters")
        result[key] = text
    return result


def emit_trigger(
    *,
    kind: str,
    event_id: str = "",
    task_id: str = "",
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = load_settings()
    normalized_kind = _valid_trigger(kind)
    if normalized_kind == "schedule":
        raise ControllerError("schedule is generated by cron cycles and cannot be enqueued")
    normalized_task = _valid_task_id(task_id)
    normalized_id = str(event_id or f"evt-{uuid.uuid4().hex}").strip()
    if not IDENTIFIER.fullmatch(normalized_id):
        raise ControllerError("invalid event_id")
    event = {
        "id": normalized_id,
        "kind": normalized_kind,
        "task_id": normalized_task or None,
        "context": _normalized_context(context),
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    store = StateStore(settings.state_dir)

    def append(state: dict[str, Any]) -> tuple[bool, int]:
        pending = state.setdefault("pending_events", [])
        processed = state.setdefault("processed_event_ids", [])
        if normalized_id in processed or any(item.get("id") == normalized_id for item in pending):
            return True, len(pending)
        if len(pending) >= 1000:
            raise ControllerError("trigger queue limit reached")
        pending.append(event)
        return False, len(pending)

    deduplicated, depth = store.update(append)
    return {"event": event, "deduplicated": deduplicated, "queue_depth": depth}


def _receipt_key(trigger: str, trigger_id: str, task: dict[str, Any], generation: int) -> str:
    task_id = str(task["task_id"])
    if trigger == "schedule":
        slot = str(task.get("schedule_slot") or "unknown")
        return f"schedule:{slot}:{task_id}"
    if trigger_id:
        return f"trigger:{trigger_id}:{task_id}"
    return f"manual:{generation}:{task_id}"


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _claim(store: StateStore, key: str, task_id: str, now_utc: datetime) -> tuple[bool, str, int]:
    def claim(state: dict[str, Any]) -> tuple[bool, str, int]:
        receipts = state.setdefault("receipts", {})
        previous = receipts.get(key, {})
        status = str(previous.get("status", ""))
        attempts = int(previous.get("attempts", 0))
        if status in {"running", "succeeded", "warning"}:
            return False, f"already_{status}", attempts
        retry_at = str(previous.get("retry_at") or "")
        if status == "failed" and retry_at and _parse_iso(retry_at) > now_utc:
            return False, "retry_backoff", attempts
        attempts += 1
        receipts[key] = {
            "task_id": task_id,
            "status": "running",
            "attempts": attempts,
            "started_at": now_utc.isoformat().replace("+00:00", "Z"),
        }
        return True, "claimed", attempts

    return store.update(claim)


def _append_history(state: dict[str, Any], item: dict[str, Any]) -> None:
    history = state.setdefault("history", [])
    history.append(item)
    del history[:-200]


def _finish(
    store: StateStore,
    settings: Settings,
    *,
    key: str,
    task_id: str,
    attempts: int,
    status: str,
    run_dir: str = "",
    error: str = "",
) -> dict[str, Any]:
    now_utc = datetime.now(UTC)

    def finish(state: dict[str, Any]) -> dict[str, Any]:
        health = state.setdefault("task_health", {}).setdefault(task_id, {"failures": 0})
        receipt: dict[str, Any] = {
            "task_id": task_id,
            "status": status,
            "attempts": attempts,
            "finished_at": now_utc.isoformat().replace("+00:00", "Z"),
        }
        if run_dir:
            receipt["run_dir"] = run_dir
        if error:
            receipt["error"] = error[:500]
        if status in {"succeeded", "warning"}:
            health.update({"failures": 0, "last_status": status, "last_success_at": receipt["finished_at"]})
        else:
            failures = int(health.get("failures", 0)) + 1
            delay = min(settings.retry_base_seconds * (2 ** (failures - 1)), settings.retry_max_seconds)
            retry_at = now_utc + timedelta(seconds=delay)
            receipt["retry_at"] = retry_at.isoformat().replace("+00:00", "Z")
            health.update({"failures": failures, "last_status": "failed", "retry_at": receipt["retry_at"]})
        state.setdefault("receipts", {})[key] = receipt
        _append_history(state, {"receipt": key, **receipt})
        return receipt

    return store.update(finish)


def _complete_event(store: StateStore, event_id: str) -> None:
    if not event_id:
        return

    def complete(state: dict[str, Any]) -> None:
        pending = state.setdefault("pending_events", [])
        state["pending_events"] = [item for item in pending if item.get("id") != event_id]
        processed = state.setdefault("processed_event_ids", [])
        if event_id not in processed:
            processed.append(event_id)
            del processed[:-1000]

    store.update(complete)


def run_cycle(
    *,
    trigger: str = "schedule",
    trigger_id: str = "",
    task_id: str = "",
    max_tasks: int = 1,
    execute: bool = False,
    apply_changes: bool = False,
    now: str = "",
) -> dict[str, Any]:
    settings = load_settings()
    normalized_trigger = _valid_trigger(trigger)
    normalized_task = _valid_task_id(task_id)
    normalized_trigger_id = str(trigger_id or "").strip()
    if normalized_trigger_id and not IDENTIFIER.fullmatch(normalized_trigger_id):
        raise ControllerError("invalid trigger_id")
    if isinstance(max_tasks, bool) or not isinstance(max_tasks, int) or not 1 <= max_tasks <= settings.max_tasks:
        raise ControllerError(f"max_tasks must be between 1 and {settings.max_tasks}")
    if execute and not settings.enabled:
        raise ConfigurationError("execution is disabled; set SUBACTOR_AGENT_ENABLED=true")
    if apply_changes and (not execute or not settings.allow_apply):
        raise ConfigurationError("apply_changes requires execute=true and SUBACTOR_AGENT_ALLOW_APPLY=true")

    store = StateStore(settings.state_dir)
    now_utc = _parse_now(now)
    with _cycle_lease(settings):
        generation = store.update(lambda state: _next_generation(state, normalized_trigger, normalized_trigger_id))
        discovery = discover(event=normalized_trigger, task_id=normalized_task, now=now_utc.isoformat())
        discovered_tasks = discovery["tasks"]
        selected = discovered_tasks[:max_tasks] if not execute else discovered_tasks
        results: list[dict[str, Any]] = []
        _, run_task = _todo_api()
        attempted = 0

        for task in selected:
            current_task = _valid_task_id(str(task["task_id"]))
            key = _receipt_key(normalized_trigger, normalized_trigger_id, task, generation)
            if not execute:
                results.append({"task_id": current_task, "status": "planned", "receipt": key})
                continue
            if attempted >= max_tasks:
                results.append({"task_id": current_task, "status": "deferred", "receipt": key})
                continue
            claimed, reason, attempts = _claim(store, key, current_task, now_utc)
            if not claimed:
                results.append({"task_id": current_task, "status": "skipped", "reason": reason, "receipt": key})
                continue
            attempted += 1
            try:
                manifest, run_dir = run_task(settings.root, current_task, apply_changes=apply_changes)
                status = str(manifest.get("status", "failed"))
                if status not in {"succeeded", "warning"}:
                    status = "failed"
                receipt = _finish(
                    store,
                    settings,
                    key=key,
                    task_id=current_task,
                    attempts=attempts,
                    status=status,
                    run_dir=str(run_dir),
                )
                results.append(
                    {
                        "task_id": current_task,
                        "status": status,
                        "receipt": key,
                        "run_dir": str(run_dir),
                        "correlation_id": manifest.get("correlation_id"),
                        "retry_at": receipt.get("retry_at"),
                    }
                )
            except Exception as exc:  # todo-agent errors become stateful retry feedback
                receipt = _finish(
                    store,
                    settings,
                    key=key,
                    task_id=current_task,
                    attempts=attempts,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                )
                results.append(
                    {
                        "task_id": current_task,
                        "status": "failed",
                        "receipt": key,
                        "error": str(exc)[:500],
                        "retry_at": receipt.get("retry_at"),
                    }
                )

        completed = execute and _tasks_completed(store, normalized_trigger, normalized_trigger_id, discovered_tasks, generation)
        if completed:
            _complete_event(store, normalized_trigger_id)
        return {
            "generation": generation,
            "trigger": normalized_trigger,
            "trigger_id": normalized_trigger_id or None,
            "execute": execute,
            "apply_changes": apply_changes,
            "discovered": discovery["count"],
            "selected": len(selected) if not execute else attempted,
            "completed": completed,
            "results": results,
        }


def _tasks_completed(
    store: StateStore,
    trigger: str,
    trigger_id: str,
    tasks: list[dict[str, Any]],
    generation: int,
) -> bool:
    state = store.read()
    receipts = state.get("receipts", {})
    return all(
        str(receipts.get(_receipt_key(trigger, trigger_id, task, generation), {}).get("status"))
        in {"succeeded", "warning"}
        for task in tasks
    )


def _next_generation(state: dict[str, Any], trigger: str, trigger_id: str) -> int:
    generation = int(state.get("generation", 0)) + 1
    state["generation"] = generation
    state["last_cycle"] = {"generation": generation, "trigger": trigger, "trigger_id": trigger_id or None, "at": _iso_now()}
    return generation


def _next_pending(store: StateStore) -> dict[str, Any] | None:
    state = store.read()
    pending = state.get("pending_events", [])
    return dict(pending[0]) if pending else None


def run_loop(
    *,
    max_cycles: int = 1,
    interval_seconds: float = 60.0,
    max_tasks: int = 1,
    execute: bool = False,
    apply_changes: bool = False,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    settings = load_settings()
    if isinstance(max_cycles, bool) or not isinstance(max_cycles, int) or not 1 <= max_cycles <= settings.max_cycles:
        raise ControllerError(f"max_cycles must be between 1 and {settings.max_cycles}")
    try:
        interval = float(interval_seconds)
    except (TypeError, ValueError) as exc:
        raise ControllerError("interval_seconds must be a number") from exc
    if interval < 0 or interval > 3600:
        raise ControllerError("interval_seconds must be between 0 and 3600")

    store = StateStore(settings.state_dir)
    cycles: list[dict[str, Any]] = []
    for index in range(max_cycles):
        event = _next_pending(store)
        if event:
            cycle = run_cycle(
                trigger=str(event["kind"]),
                trigger_id=str(event["id"]),
                task_id=str(event.get("task_id") or ""),
                max_tasks=max_tasks,
                execute=execute,
                apply_changes=apply_changes,
            )
        else:
            cycle = run_cycle(
                trigger="schedule",
                max_tasks=max_tasks,
                execute=execute,
                apply_changes=apply_changes,
            )
        cycles.append(cycle)
        if index + 1 < max_cycles:
            sleep(interval)
    return {"cycles": cycles, "count": len(cycles), "execute": execute}


def status() -> dict[str, Any]:
    settings = load_settings()
    state = StateStore(settings.state_dir).read()
    return {
        "schema": state["schema"],
        "generation": int(state.get("generation", 0)),
        "pending_events": state.get("pending_events", []),
        "queue_depth": len(state.get("pending_events", [])),
        "task_health": state.get("task_health", {}),
        "last_cycle": state.get("last_cycle"),
        "enabled": settings.enabled,
        "allow_apply": settings.allow_apply,
    }


def runs(*, limit: int = 20) -> dict[str, Any]:
    settings = load_settings()
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ControllerError("limit must be between 1 and 200")
    history = StateStore(settings.state_dir).read().get("history", [])
    selected = list(reversed(history[-limit:]))
    return {"runs": selected, "count": len(selected)}


def doctor() -> dict[str, Any]:
    raw_root = os.environ.get("SUBACTOR_TODO_ROOT", "").strip()
    configured = bool(raw_root)
    root_ready = configured and Path(raw_root).expanduser().resolve().joinpath("TODO").is_dir()
    dependency_ready = True
    dependency_error = ""
    try:
        _todo_api()
    except ControllerError as exc:
        dependency_ready = False
        dependency_error = str(exc)
    return {
        "status": "ready" if root_ready and dependency_ready else "not_ready",
        "configured": configured,
        "root_ready": root_ready,
        "todo_agent_ready": dependency_ready,
        "execution_enabled": _env_bool("SUBACTOR_AGENT_ENABLED"),
        "apply_enabled": _env_bool("SUBACTOR_AGENT_ALLOW_APPLY"),
        "reason": dependency_error or ("" if root_ready else "SUBACTOR_TODO_ROOT is missing or invalid"),
    }
