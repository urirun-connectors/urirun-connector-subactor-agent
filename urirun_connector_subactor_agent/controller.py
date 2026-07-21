"""Deterministic control loop over task packages owned by ``todo-agent``.

The connector never accepts a command or a working directory from an actor.  The
operator configures one todo-agent repository through ``SUBACTOR_TODO_ROOT`` and
all execution is delegated to todo-agent's validated runner.
"""

from __future__ import annotations

import hashlib
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
NON_RETRYABLE_ERROR_TYPES = frozenset(
    {"BackendContractError", "ConfigError", "SecurityError", "TargetScopeError"}
)
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
    planfile_backend: str
    planfile_onedev_url: str
    planfile_onedev_project: str
    planfile_onedev_user: str
    planfile_onedev_password_file: str


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
        planfile_backend=os.environ.get("SUBACTOR_PLANFILE_BACKEND", "").strip().lower(),
        planfile_onedev_url=os.environ.get("SUBACTOR_PLANFILE_ONEDEV_URL", "").strip(),
        planfile_onedev_project=os.environ.get("SUBACTOR_PLANFILE_ONEDEV_PROJECT", "").strip(),
        planfile_onedev_user=os.environ.get("SUBACTOR_PLANFILE_ONEDEV_USER", "").strip(),
        planfile_onedev_password_file=os.environ.get("SUBACTOR_PLANFILE_ONEDEV_PASSWORD_FILE", "").strip(),
    )


def _empty_state() -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        "generation": 0,
        "pending_events": [],
        "processed_event_ids": [],
        "receipts": {},
        "task_health": {},
        "resolutions": {},
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


def _todo_planfile_api() -> tuple[Callable[..., Any], Callable[..., Any], Callable[..., Any]]:
    """Lazy Planfile registry boundary owned by todo-agent."""
    try:
        from todo_agent.planfile_backend import (
            next_backend_task,
            set_backend_task_status,
            sync_tasks_to_backend,
        )
    except ImportError as exc:
        raise ConfigurationError("todo-agent Planfile backend integration is unavailable") from exc
    return sync_tasks_to_backend, next_backend_task, set_backend_task_status


def _planfile_registry(settings: Settings) -> Any:
    if settings.planfile_backend != "onedev":
        raise ConfigurationError("SUBACTOR_PLANFILE_BACKEND must be 'onedev' when configured")
    required = {
        "SUBACTOR_PLANFILE_ONEDEV_URL": settings.planfile_onedev_url,
        "SUBACTOR_PLANFILE_ONEDEV_PROJECT": settings.planfile_onedev_project,
        "SUBACTOR_PLANFILE_ONEDEV_USER": settings.planfile_onedev_user,
        "SUBACTOR_PLANFILE_ONEDEV_PASSWORD_FILE": settings.planfile_onedev_password_file,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ConfigurationError(f"missing Planfile OneDev settings: {', '.join(missing)}")
    password_file = Path(settings.planfile_onedev_password_file).expanduser().resolve()
    if not password_file.is_file():
        raise ConfigurationError("SUBACTOR_PLANFILE_ONEDEV_PASSWORD_FILE is not a file")
    try:
        from todo_agent.planfile_backend import open_onedev_backend
    except ImportError as exc:
        raise ConfigurationError("todo-agent Planfile OneDev integration is unavailable") from exc
    try:
        return open_onedev_backend(
            url=settings.planfile_onedev_url,
            project=settings.planfile_onedev_project,
            username=settings.planfile_onedev_user,
            password_file=str(password_file),
        )
    except Exception as exc:
        raise ConfigurationError(f"Planfile OneDev backend is unavailable: {type(exc).__name__}: {exc}") from exc


def _planfile_event_id(ticket: dict[str, Any]) -> str:
    identity = f"{ticket['ticket_id']}|{ticket.get('updated_at', '')}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"planfile:{ticket['ticket_id']}:{digest}"


def _pending_planfile_ticket_ids(store: StateStore) -> set[str]:
    return {
        str((event.get("context") or {}).get("ticket_id") or "")
        for event in store.read().get("pending_events", [])
        if isinstance(event, dict)
        and (event.get("context") or {}).get("source") == "planfile"
        and (event.get("context") or {}).get("ticket_id")
    }


def _poll_planfile(settings: Settings, store: StateStore) -> dict[str, Any] | None:
    if not settings.planfile_backend:
        return None
    backend = _planfile_registry(settings)
    sync_tasks, next_task, _ = _todo_planfile_api()
    synced = sync_tasks(settings.root, backend)
    pending_ticket_ids = _pending_planfile_ticket_ids(store)
    ticket = next_task(settings.root, backend, exclude_ticket_ids=pending_ticket_ids)
    if not ticket:
        return {
            "synced": len(synced),
            "ticket": None,
            "excluded_pending": sorted(pending_ticket_ids),
        }
    ticket_id = str(ticket["ticket_id"])
    event_id = _planfile_event_id(ticket)
    emitted = emit_trigger(
        kind="ticket",
        event_id=event_id,
        task_id=str(ticket["task_id"]),
        context={"source": "planfile", "ticket_id": ticket_id},
    )
    return {
        "synced": len(synced),
        "ticket": ticket_id,
        "task_id": str(ticket["task_id"]),
        "event_id": event_id,
        "action": "deduplicated" if emitted["deduplicated"] else "enqueued",
        "excluded_pending": sorted(pending_ticket_ids),
    }


def _set_planfile_event_status(
    settings: Settings,
    event: dict[str, Any],
    status: str,
) -> dict[str, Any] | None:
    context = event.get("context") if isinstance(event.get("context"), dict) else {}
    if context.get("source") != "planfile" or not context.get("ticket_id"):
        return None
    backend = _planfile_registry(settings)
    _, _, set_status = _todo_planfile_api()
    ticket_id = str(context["ticket_id"])
    set_status(backend, ticket_id, status)
    return {"ticket": ticket_id, "status": status}


def _finish_planfile_event(settings: Settings, event: dict[str, Any], cycle: dict[str, Any]) -> dict[str, Any] | None:
    context = event.get("context") if isinstance(event.get("context"), dict) else {}
    if context.get("source") != "planfile" or not context.get("ticket_id"):
        return None
    results = cycle.get("results") if isinstance(cycle.get("results"), list) else []
    statuses = {str(item.get("status")) for item in results if isinstance(item, dict)}
    non_retryable = any(
        item.get("status") == "failed" and item.get("retryable") is False
        for item in results
        if isinstance(item, dict)
    )
    if non_retryable:
        target = "review"
    elif "failed" in statuses:
        target = "open"
    elif "warning" in statuses:
        target = "review"
    elif cycle.get("completed") and "succeeded" in statuses:
        target = "done"
    elif cycle.get("completed"):
        target = "review"
    else:
        return None
    transition = _set_planfile_event_status(settings, event, target)
    if transition and target in {"open", "review"}:
        failed = next(
            (item for item in results if isinstance(item, dict) and item.get("status") == "failed"),
            {},
        )
        reason_code = (
            "contract_or_policy_failure"
            if non_retryable
            else "verification_required"
            if "warning" in statuses
            else "execution_failure"
        )
        resolution = {
            "schema": "subactor.resolution.blocker/v1",
            "task_id": str(event.get("task_id") or "unknown"),
            "reason_code": reason_code,
            "required_capabilities": [],
            "attempts": [str(failed.get("receipt"))] if failed.get("receipt") else [],
            "evidence": {
                "ticket_id": str(context["ticket_id"]),
                "status": str(failed.get("status") or ("warning" if "warning" in statuses else "unknown")),
                "error": str(failed.get("error") or "")[:500],
            },
            "task_state": "verifying" if "warning" in statuses else "resolving",
            "platform_mode": "degraded" if target == "review" else "continuity",
            "continue_unblocked": True,
            "disposition": "human_review" if target == "review" else "retry_after_backoff",
            "retry_at": failed.get("retry_at") if target == "open" else None,
            "recorded_at": _iso_now(),
        }
        _record_resolution(StateStore(settings.state_dir), resolution)
        cycle["resolution"] = resolution
    elif transition and target == "done":
        _clear_resolution(StateStore(settings.state_dir), str(event.get("task_id") or ""))
    if non_retryable and transition:
        _complete_event(StateStore(settings.state_dir), str(event.get("id") or ""))
    return transition


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


def _retryable_error(exc: Exception) -> bool:
    """Classify policy/contract causes without importing todo-agent eagerly."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if type(current).__name__ in NON_RETRYABLE_ERROR_TYPES:
            return False
        current = current.__cause__ or current.__context__
    return True


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


def _record_resolution(store: StateStore, resolution: dict[str, Any]) -> None:
    task_id = str(resolution.get("task_id") or "unknown")

    def record(state: dict[str, Any]) -> None:
        state.setdefault("resolutions", {})[task_id] = resolution
        _append_history(state, {"resolution": resolution})

    store.update(record)


def _clear_resolution(store: StateStore, task_id: str) -> None:
    if not task_id:
        return

    def clear(state: dict[str, Any]) -> None:
        state.setdefault("resolutions", {}).pop(task_id, None)

    store.update(clear)


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
                retryable = _retryable_error(exc)
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
                        "retryable": retryable,
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


def _event_retry_at(state: dict[str, Any], event: dict[str, Any]) -> datetime | None:
    task_id = str(event.get("task_id") or "")
    event_id = str(event.get("id") or "")
    if not task_id or not event_id:
        return None
    receipt = state.get("receipts", {}).get(f"trigger:{event_id}:{task_id}", {})
    retry_at = str(receipt.get("retry_at") or "") if receipt.get("status") == "failed" else ""
    return _parse_iso(retry_at) if retry_at else None


def _next_pending(store: StateStore, *, now_utc: datetime | None = None) -> dict[str, Any] | None:
    state = store.read()
    pending = state.get("pending_events", [])
    current = now_utc or datetime.now(UTC)
    for event in pending:
        retry_at = _event_retry_at(state, event)
        if retry_at is None or retry_at <= current:
            return dict(event)
    return None


def _first_pending(store: StateStore) -> dict[str, Any] | None:
    pending = store.read().get("pending_events", [])
    return dict(pending[0]) if pending else None


def _planfile_idle_cycle(store: StateStore, *, execute: bool, apply_changes: bool) -> dict[str, Any]:
    state = store.read()
    deferred = []
    for event in state.get("pending_events", []):
        retry_at = _event_retry_at(state, event)
        if retry_at:
            deferred.append(
                {
                    "event_id": event.get("id"),
                    "task_id": event.get("task_id"),
                    "retry_at": retry_at.isoformat().replace("+00:00", "Z"),
                }
            )
    return {
        "generation": int(state.get("generation", 0)),
        "trigger": "ticket",
        "trigger_id": None,
        "execute": execute,
        "apply_changes": apply_changes,
        "discovered": 0,
        "selected": 0,
        "completed": True,
        "results": [],
        "idle_reason": "retry_backoff" if deferred else "no_open_ticket",
        "deferred": deferred,
    }


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
    if execute and not settings.enabled:
        raise ConfigurationError("execution is disabled; set SUBACTOR_AGENT_ENABLED=true")
    if apply_changes and (not execute or not settings.allow_apply):
        raise ConfigurationError("apply_changes requires execute=true and SUBACTOR_AGENT_ALLOW_APPLY=true")

    store = StateStore(settings.state_dir)
    cycles: list[dict[str, Any]] = []
    for index in range(max_cycles):
        event = _next_pending(store)
        registry = None
        if event is None:
            registry = _poll_planfile(settings, store)
            event = _next_pending(store)
            if event is None and not settings.planfile_backend:
                event = _first_pending(store)
        if event:
            started = _set_planfile_event_status(settings, event, "in_progress") if execute else None
            try:
                cycle = run_cycle(
                    trigger=str(event["kind"]),
                    trigger_id=str(event["id"]),
                    task_id=str(event.get("task_id") or ""),
                    max_tasks=max_tasks,
                    execute=execute,
                    apply_changes=apply_changes,
                )
            except Exception:
                if started:
                    _set_planfile_event_status(settings, event, "open")
                raise
            if started:
                cycle["planfile_started"] = started
            transition = _finish_planfile_event(settings, event, cycle)
            if transition:
                cycle["planfile_transition"] = transition
        elif settings.planfile_backend:
            cycle = _planfile_idle_cycle(store, execute=execute, apply_changes=apply_changes)
        else:
            cycle = run_cycle(
                trigger="schedule",
                max_tasks=max_tasks,
                execute=execute,
                apply_changes=apply_changes,
            )
        if registry:
            cycle["planfile_registry"] = registry
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
        "resolutions": state.get("resolutions", {}),
        "last_cycle": state.get("last_cycle"),
        "enabled": settings.enabled,
        "allow_apply": settings.allow_apply,
        "planfile_backend": settings.planfile_backend or None,
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
    planfile_backend = os.environ.get("SUBACTOR_PLANFILE_BACKEND", "").strip().lower()
    planfile_ready = not planfile_backend
    planfile_error = ""
    if planfile_backend:
        try:
            _planfile_registry(load_settings(require_root=False))
            _todo_planfile_api()
            planfile_ready = True
        except ControllerError as exc:
            planfile_error = str(exc)
    return {
        "status": "ready" if root_ready and dependency_ready and planfile_ready else "not_ready",
        "configured": configured,
        "root_ready": root_ready,
        "todo_agent_ready": dependency_ready,
        "execution_enabled": _env_bool("SUBACTOR_AGENT_ENABLED"),
        "apply_enabled": _env_bool("SUBACTOR_AGENT_ALLOW_APPLY"),
        "planfile_backend": planfile_backend or None,
        "planfile_ready": planfile_ready,
        "reason": planfile_error or dependency_error or ("" if root_ready else "SUBACTOR_TODO_ROOT is missing or invalid"),
    }
