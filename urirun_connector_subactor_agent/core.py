"""URI-native control plane for Subactor's todo-agent execution fleet."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import urirun

from . import controller

CONNECTOR_ID = "subactor-agent"
conn = urirun.connector(CONNECTOR_ID, scheme="subactor-agent")


def _ok(action: str, operation: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        result = operation()
        return urirun.ok(connector=CONNECTOR_ID, action=action, **result)
    except Exception as exc:  # every handler must degrade to a portable envelope
        return urirun.fail(str(exc), connector=CONNECTOR_ID, action=action, error_type=type(exc).__name__)


@conn.handler(
    "tasks/query/discover",
    isolated=False,
    meta={"label": "Discover runnable todo-agent tasks for a manual, cron or external trigger"},
)
def tasks_discover(event: str = "manual", task_id: str = "", now: str = "") -> dict[str, Any]:
    return _ok("tasks-discover", lambda: controller.discover(event=event, task_id=task_id, now=now))


@conn.handler(
    "state/query/status",
    isolated=False,
    meta={"label": "Read control-loop generation, trigger queue and retry health"},
)
def state_status() -> dict[str, Any]:
    return _ok("state-status", controller.status)


@conn.handler(
    "runs/query/list",
    isolated=False,
    meta={"label": "List bounded execution receipts produced by todo-agent cycles"},
)
def runs_list(limit: int = 20) -> dict[str, Any]:
    return _ok("runs-list", lambda: controller.runs(limit=limit))


@conn.handler(
    "doctor/query/report",
    isolated=False,
    meta={"label": "Report todo-agent dependency, repository and mutation-gate readiness"},
)
def doctor_report() -> dict[str, Any]:
    return _ok("doctor-report", controller.doctor)


@conn.handler(
    "trigger/event/emit",
    isolated=True,
    meta={"label": "Append and deduplicate a bounded external trigger for the control loop"},
)
def trigger_emit(
    kind: str,
    event_id: str = "",
    task_id: str = "",
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _ok(
        "trigger-emit",
        lambda: controller.emit_trigger(kind=kind, event_id=event_id, task_id=task_id, context=context),
    )


@conn.handler(
    "cycle/command/run",
    isolated=True,
    meta={"label": "Run one deduplicated todo-agent control cycle with deterministic retry feedback"},
)
def cycle_run(
    trigger: str = "schedule",
    trigger_id: str = "",
    task_id: str = "",
    max_tasks: int = 1,
    execute: bool = False,
    apply_changes: bool = False,
    now: str = "",
) -> dict[str, Any]:
    return _ok(
        "cycle-run",
        lambda: controller.run_cycle(
            trigger=trigger,
            trigger_id=trigger_id,
            task_id=task_id,
            max_tasks=max_tasks,
            execute=execute,
            apply_changes=apply_changes,
            now=now,
        ),
    )


@conn.handler(
    "loop/session/run",
    isolated=True,
    meta={"label": "Run a bounded trigger-first loop, falling back to cron discovery"},
)
def loop_run(
    max_cycles: int = 1,
    interval_seconds: float = 60.0,
    max_tasks: int = 1,
    execute: bool = False,
    apply_changes: bool = False,
) -> dict[str, Any]:
    return _ok(
        "loop-run",
        lambda: controller.run_loop(
            max_cycles=max_cycles,
            interval_seconds=interval_seconds,
            max_tasks=max_tasks,
            execute=execute,
            apply_changes=apply_changes,
        ),
    )


def urirun_bindings() -> dict[str, Any]:
    return conn.bindings()


def bindings() -> dict[str, Any]:
    return urirun_bindings()


def connector_manifest() -> dict[str, Any]:
    return conn.manifest(urirun.load_manifest(__package__))


def manifest() -> dict[str, Any]:
    return connector_manifest()


def main(argv: list[str] | None = None) -> int:
    return conn.cli(argv, manifest_prose=urirun.load_manifest(__package__))


if __name__ == "__main__":
    raise SystemExit(main())
