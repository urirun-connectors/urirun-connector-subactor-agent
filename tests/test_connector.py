from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from urirun_connector_subactor_agent import controller, core

ROUTES = {
    "subactor-agent://host/cycle/command/run",
    "subactor-agent://host/doctor/query/report",
    "subactor-agent://host/loop/session/run",
    "subactor-agent://host/runs/query/list",
    "subactor-agent://host/state/query/status",
    "subactor-agent://host/tasks/query/discover",
    "subactor-agent://host/trigger/event/emit",
}


def test_package_and_connector_manifest_versions_match() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    manifest = json.loads(
        Path("urirun_connector_subactor_agent/connector.manifest.json").read_text(encoding="utf-8")
    )

    assert project["project"]["version"] == manifest["version"] == "0.3.2"
    assert manifest["install"]["pipSpec"].endswith("@v0.3.2")
    assert "ifuri-todo-agent>=0.15.1" in manifest["requires"]


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "todo-agent"
    (root / "TODO").mkdir(parents=True)
    state = tmp_path / "state"
    monkeypatch.setenv("SUBACTOR_TODO_ROOT", str(root))
    monkeypatch.setenv("SUBACTOR_AGENT_STATE_DIR", str(state))
    monkeypatch.delenv("SUBACTOR_AGENT_ENABLED", raising=False)
    monkeypatch.delenv("SUBACTOR_AGENT_ALLOW_APPLY", raising=False)
    for name in (
        "SUBACTOR_PLANFILE_BACKEND",
        "SUBACTOR_PLANFILE_ONEDEV_URL",
        "SUBACTOR_PLANFILE_ONEDEV_PROJECT",
        "SUBACTOR_PLANFILE_ONEDEV_USER",
        "SUBACTOR_PLANFILE_ONEDEV_PASSWORD_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    return root, state


def _fake_api(tasks: list[dict], *, status: str = "succeeded", calls: list | None = None):
    def discover_tasks(root, *, event, task_id, now_utc):
        selected = [item for item in tasks if not task_id or item["task_id"] == task_id]
        return selected if event in {"manual", "schedule"} else []

    def run_task(root, task_id, *, apply_changes):
        if calls is not None:
            calls.append((root, task_id, apply_changes))
        return {"status": status, "correlation_id": f"todo-agent:{task_id}:run"}, root / ".todo-agent-runs" / task_id / "run"

    return discover_tasks, run_task


def test_bindings_are_complete_and_serializable() -> None:
    document = core.urirun_bindings()
    assert set(document["bindings"]) == ROUTES
    json.dumps(document)
    for uri, binding in document["bindings"].items():
        if "/query/" in uri:
            assert binding["adapter"] == "local-function"
        else:
            assert binding["adapter"] == "local-function-subprocess"


def test_import_is_lazy_and_does_not_import_todo_agent() -> None:
    script = "import sys; import urirun_connector_subactor_agent.core; print(any(k == 'todo_agent' or k.startswith('todo_agent.') for k in sys.modules))"
    completed = subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, text=True)
    assert completed.stdout.strip() == "False"


def test_discovery_maps_external_trigger_to_manual(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    tasks = [{"task_id": "0042", "schedule_slot": "manual"}]
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api(tasks))

    result = core.tasks_discover(event="ticket", task_id="0042")

    assert result["ok"] is True
    assert result["discovery_event"] == "manual"
    assert result["tasks"] == tasks


def test_cycle_is_dry_run_by_default(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    tasks = [{"task_id": "0042", "schedule_slot": "2026-07-21T08:00:00Z"}]
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api(tasks))

    result = core.cycle_run(trigger="schedule", now="2026-07-21T08:00:00Z")

    assert result["ok"] is True
    assert result["execute"] is False
    assert result["results"][0]["status"] == "planned"


def test_execution_requires_operator_gate(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api([]))
    result = core.cycle_run(trigger="manual", execute=True)
    assert result["ok"] is False
    assert "SUBACTOR_AGENT_ENABLED" in str(result["error"])


def test_schedule_slot_is_executed_once(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    root, _ = configured
    calls: list = []
    tasks = [{"task_id": "0042", "schedule_slot": "2026-07-21T08:00:00Z"}]
    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api(tasks, calls=calls))

    first = core.cycle_run(trigger="schedule", execute=True, now="2026-07-21T08:00:00Z")
    second = core.cycle_run(trigger="schedule", execute=True, now="2026-07-21T08:00:30Z")

    assert first["ok"] and first["results"][0]["status"] == "succeeded"
    assert second["ok"] and second["results"][0]["reason"] == "already_succeeded"
    assert calls == [(root, "0042", False)]


def test_trigger_is_deduplicated_and_loop_prioritizes_it(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    tasks = [{"task_id": "0042", "schedule_slot": "manual"}]
    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api(tasks, calls=calls))

    first = core.trigger_emit(kind="ticket", event_id="PLF-42", task_id="0042", context={"ticket_id": "PLF-42"})
    duplicate = core.trigger_emit(kind="ticket", event_id="PLF-42", task_id="0042")
    loop = controller.run_loop(max_cycles=1, interval_seconds=0, execute=True)
    state = controller.status()

    assert first["ok"] and first["deduplicated"] is False
    assert duplicate["deduplicated"] is True
    assert loop["cycles"][0]["trigger"] == "ticket"
    assert calls and state["queue_depth"] == 0


def test_trigger_accepts_json_context_from_generated_cli(configured) -> None:
    result = core.trigger_emit(
        kind="ticket",
        event_id="PLF-43",
        task_id="0042",
        context_json='{"ticket_id":"PLF-43"}',
    )

    assert result["ok"] is True
    assert result["event"]["context"] == {"ticket_id": "PLF-43"}


def test_trigger_rejects_non_object_json_context(configured) -> None:
    result = core.trigger_emit(kind="webhook", event_id="hook-json", context_json='["unsafe"]')

    assert result["ok"] is False
    assert "must encode an object" in result["error"]


def test_planfile_ticket_is_enqueued_executed_and_completed(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    root, _ = configured
    calls: list = []
    transitions: list[tuple[str, str]] = []
    backend = object()
    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setenv("SUBACTOR_PLANFILE_BACKEND", "onedev")
    monkeypatch.setattr(controller, "_planfile_registry", lambda settings: backend)
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api([{"task_id": "0042", "schedule_slot": "manual"}], calls=calls))
    monkeypatch.setattr(
        controller,
        "_todo_planfile_api",
        lambda: (
            lambda repo_root, selected_backend: [{"task_id": "0042"}],
            lambda repo_root, selected_backend, **kwargs: {
                "ticket_id": "91",
                "task_id": "0042",
                "status": "open",
                "updated_at": "2026-07-21T12:00:00Z",
            },
            lambda selected_backend, ticket_id, status: transitions.append((ticket_id, status)),
        ),
    )

    result = controller.run_loop(max_cycles=1, interval_seconds=0, execute=True)

    cycle = result["cycles"][0]
    assert calls == [(root, "0042", False)]
    assert transitions == [("91", "in_progress"), ("91", "done")]
    assert cycle["trigger"] == "ticket"
    assert cycle["planfile_registry"]["action"] == "enqueued"
    assert cycle["planfile_transition"] == {"ticket": "91", "status": "done"}
    assert controller.status()["queue_depth"] == 0


def test_planfile_preview_keeps_ticket_open(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    transitions: list[tuple[str, str]] = []
    backend = object()
    monkeypatch.setenv("SUBACTOR_PLANFILE_BACKEND", "onedev")
    monkeypatch.setattr(controller, "_planfile_registry", lambda settings: backend)
    monkeypatch.setattr(
        controller,
        "_todo_api",
        lambda: _fake_api([{"task_id": "0042", "schedule_slot": "manual"}]),
    )
    monkeypatch.setattr(
        controller,
        "_todo_planfile_api",
        lambda: (
            lambda repo_root, selected_backend: [{"task_id": "0042"}],
            lambda repo_root, selected_backend, **kwargs: {
                "ticket_id": "93",
                "task_id": "0042",
                "status": "open",
                "updated_at": "2026-07-21T12:02:00Z",
            },
            lambda selected_backend, ticket_id, status: transitions.append((ticket_id, status)),
        ),
    )

    result = controller.run_loop(max_cycles=1, interval_seconds=0, execute=False)

    assert result["cycles"][0]["results"][0]["status"] == "planned"
    assert transitions == []
    assert controller.status()["queue_depth"] == 1


def test_failed_planfile_ticket_reopens_and_keeps_retry_event(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    transitions: list[tuple[str, str]] = []
    backend = object()
    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setenv("SUBACTOR_PLANFILE_BACKEND", "onedev")
    monkeypatch.setattr(controller, "_planfile_registry", lambda settings: backend)
    monkeypatch.setattr(
        controller,
        "_todo_api",
        lambda: _fake_api([{"task_id": "0042", "schedule_slot": "manual"}], status="failed"),
    )
    monkeypatch.setattr(
        controller,
        "_todo_planfile_api",
        lambda: (
            lambda repo_root, selected_backend: [],
            lambda repo_root, selected_backend, **kwargs: {
                "ticket_id": "92",
                "task_id": "0042",
                "status": "open",
                "updated_at": "2026-07-21T12:01:00Z",
            },
            lambda selected_backend, ticket_id, status: transitions.append((ticket_id, status)),
        ),
    )

    result = controller.run_loop(max_cycles=1, interval_seconds=0, execute=True)

    assert result["cycles"][0]["results"][0]["status"] == "failed"
    assert transitions == [("92", "in_progress"), ("92", "open")]
    assert controller.status()["queue_depth"] == 1
    resolution = controller.status()["resolutions"]["0042"]
    assert resolution["schema"] == "subactor.resolution.blocker/v1"
    assert resolution["continue_unblocked"] is True
    assert resolution["disposition"] == "retry_after_backoff"


def test_deferred_planfile_retry_does_not_block_next_ticket(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    root, _ = configured
    calls: list = []
    transitions: list[tuple[str, str]] = []
    backend = object()
    tasks = [
        {"task_id": "0042", "schedule_slot": "manual"},
        {"task_id": "0043", "schedule_slot": "manual"},
    ]

    def run_task(selected_root, task_id, *, apply_changes):
        calls.append((selected_root, task_id, apply_changes))
        status = "failed" if task_id == "0042" else "succeeded"
        return {"status": status, "correlation_id": f"todo-agent:{task_id}:run"}, root / "runs" / task_id

    def next_task(repo_root, selected_backend, *, exclude_ticket_ids=None):
        excluded = exclude_ticket_ids or set()
        if "92" not in excluded:
            return {
                "ticket_id": "92",
                "task_id": "0042",
                "status": "open",
                "updated_at": "2026-07-21T12:01:00Z",
            }
        return {
            "ticket_id": "93",
            "task_id": "0043",
            "status": "open",
            "updated_at": "2026-07-21T12:02:00Z",
        }

    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setenv("SUBACTOR_PLANFILE_BACKEND", "onedev")
    monkeypatch.setattr(controller, "_planfile_registry", lambda settings: backend)
    monkeypatch.setattr(
        controller,
        "_todo_api",
        lambda: (
            lambda selected_root, *, event, task_id, now_utc: [
                item for item in tasks if not task_id or item["task_id"] == task_id
            ],
            run_task,
        ),
    )
    monkeypatch.setattr(
        controller,
        "_todo_planfile_api",
        lambda: (
            lambda repo_root, selected_backend: [],
            next_task,
            lambda selected_backend, ticket_id, status: transitions.append((ticket_id, status)),
        ),
    )

    result = controller.run_loop(max_cycles=2, interval_seconds=0, execute=True)

    assert [item[1] for item in calls] == ["0042", "0043"]
    assert transitions == [
        ("92", "in_progress"),
        ("92", "open"),
        ("93", "in_progress"),
        ("93", "done"),
    ]
    assert result["cycles"][1]["planfile_registry"]["excluded_pending"] == ["92"]
    assert controller.status()["queue_depth"] == 1


def test_empty_planfile_backlog_does_not_fall_back_to_local_schedule(
    configured, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list = []
    backend = object()
    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setenv("SUBACTOR_PLANFILE_BACKEND", "onedev")
    monkeypatch.setattr(controller, "_planfile_registry", lambda settings: backend)
    monkeypatch.setattr(
        controller,
        "_todo_api",
        lambda: _fake_api([{"task_id": "0042", "schedule_slot": "due"}], calls=calls),
    )
    monkeypatch.setattr(
        controller,
        "_todo_planfile_api",
        lambda: (
            lambda repo_root, selected_backend: [],
            lambda repo_root, selected_backend, **kwargs: None,
            lambda selected_backend, ticket_id, status: None,
        ),
    )

    cycle = controller.run_loop(max_cycles=1, interval_seconds=0, execute=True)["cycles"][0]

    assert calls == []
    assert cycle["idle_reason"] == "no_open_ticket"
    assert cycle["trigger"] == "ticket"
    assert cycle["generation"] == 0


def test_non_retryable_planfile_policy_failure_moves_to_review(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    transitions: list[tuple[str, str]] = []
    backend = object()

    class TargetScopeError(ValueError):
        pass

    class TaskRunError(RuntimeError):
        pass

    def run_task(root, task_id, *, apply_changes):
        try:
            raise TargetScopeError("target scope is invalid")
        except TargetScopeError as exc:
            raise TaskRunError(str(exc)) from exc

    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setenv("SUBACTOR_PLANFILE_BACKEND", "onedev")
    monkeypatch.setattr(controller, "_planfile_registry", lambda settings: backend)
    monkeypatch.setattr(
        controller,
        "_todo_api",
        lambda: (
            lambda root, *, event, task_id, now_utc: [{"task_id": "0042", "schedule_slot": "manual"}],
            run_task,
        ),
    )
    monkeypatch.setattr(
        controller,
        "_todo_planfile_api",
        lambda: (
            lambda repo_root, selected_backend: [],
            lambda repo_root, selected_backend, **kwargs: {
                "ticket_id": "94",
                "task_id": "0042",
                "status": "open",
                "updated_at": "2026-07-21T12:03:00Z",
            },
            lambda selected_backend, ticket_id, status: transitions.append((ticket_id, status)),
        ),
    )

    result = controller.run_loop(max_cycles=1, interval_seconds=0, execute=True)

    failure = result["cycles"][0]["results"][0]
    assert failure["status"] == "failed"
    assert failure["retryable"] is False
    assert transitions == [("94", "in_progress"), ("94", "review")]
    assert controller.status()["queue_depth"] == 0
    assert controller.status()["resolutions"]["0042"]["disposition"] == "human_review"


def test_loop_progresses_across_more_tasks_than_cycle_limit(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    tasks = [
        {"task_id": "0042", "schedule_slot": "manual"},
        {"task_id": "0043", "schedule_slot": "manual"},
    ]
    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api(tasks, calls=calls))
    controller.emit_trigger(kind="repository", event_id="push-1")

    result = controller.run_loop(max_cycles=2, interval_seconds=0, max_tasks=1, execute=True)

    assert [item[1] for item in calls] == ["0042", "0043"]
    assert result["cycles"][-1]["completed"] is True
    assert controller.status()["queue_depth"] == 0


def test_failed_task_gets_backoff_and_event_stays_pending(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    tasks = [{"task_id": "0042", "schedule_slot": "manual"}]
    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api(tasks, status="failed"))
    controller.emit_trigger(kind="webhook", event_id="hook-1", task_id="0042")

    first = controller.run_loop(max_cycles=1, interval_seconds=0, execute=True)["cycles"][0]
    second = controller.run_loop(max_cycles=1, interval_seconds=0, execute=True)["cycles"][0]

    assert first["results"][0]["status"] == "failed"
    assert first["results"][0]["retry_at"]
    assert second["results"][0]["reason"] == "retry_backoff"
    assert second["completed"] is False
    assert controller.status()["queue_depth"] == 1


def test_successful_retry_clears_stale_task_health_retry_at(configured) -> None:
    _, state_dir = configured
    settings = controller.load_settings()
    store = controller.StateStore(state_dir)

    controller._finish(store, settings, key="retry:0042", task_id="0042", attempts=1, status="failed")
    assert controller.status()["task_health"]["0042"]["retry_at"]

    controller._finish(store, settings, key="retry:0042", task_id="0042", attempts=2, status="succeeded")

    assert "retry_at" not in controller.status()["task_health"]["0042"]


def test_apply_changes_has_a_separate_gate(configured, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUBACTOR_AGENT_ENABLED", "true")
    monkeypatch.setattr(controller, "_todo_api", lambda: _fake_api([]))
    result = core.cycle_run(trigger="manual", execute=True, apply_changes=True)
    assert result["ok"] is False
    assert "SUBACTOR_AGENT_ALLOW_APPLY" in str(result["error"])


def test_payload_cannot_supply_commands_paths_or_environment(configured) -> None:
    assert core.trigger_emit(kind="webhook", event_id="ok", context={"command": "rm -rf /"})["ok"] is False
    with pytest.raises(TypeError):
        core.cycle_run(root="/tmp/other")
    with pytest.raises(TypeError):
        core.cycle_run(command="python arbitrary.py")


def test_read_handlers_return_failure_envelopes_when_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUBACTOR_TODO_ROOT", raising=False)
    assert core.tasks_discover()["ok"] is False
    assert core.state_status()["ok"] is False
    assert core.doctor_report()["ok"] is True
    assert core.doctor_report()["status"] == "not_ready"
