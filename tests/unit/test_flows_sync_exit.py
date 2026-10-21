"""``flows sync`` reaches the scheduler as a failure when the run aborted or every request failed.

2026-07-27..29: 45/50/55 requests, all failed, 0 rows, exit 0 -- Cronicle showed
three successes. A partly failed run stays a partial success (exit 0).
"""

from __future__ import annotations

import ast
import inspect
from types import SimpleNamespace

import pytest

from collector.kr.cli import app


def _exit_code(fn, *args) -> int | str | None:
    try:
        fn(*args)
    except SystemExit as exc:
        return exc.code
    return None


@pytest.mark.parametrize(
    ("attempted", "errors", "expected"),
    [
        (45, {f"foreign:2026-07-{d:02d}:KOSPI": "boom" for d in range(1, 46)}, 1),
        (3, {"a": "x", "b": "y"}, None),
        (0, {}, None),
        (3, {}, None),
        # the abort key is not a request; _exit_if_run_aborted handles it
        (2, {"pipeline": "down", "a": "x"}, None),
    ],
)
def test_exit_if_every_request_failed(attempted: int, errors: dict, expected) -> None:
    assert _exit_code(app._exit_if_every_request_failed, attempted, errors, "X") == expected


def test_flows_sync_handler_routes_both_failure_exits() -> None:
    tree = ast.parse(inspect.getsource(app._handle_flows_sync))
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert {"_exit_if_run_aborted", "_exit_if_every_request_failed"} <= called


# ---------------------------------------------------------------- handler level

_SETTINGS = SimpleNamespace(
    db_dsn="postgresql://unused@localhost/none",
    krx_id="id",
    krx_pw="pw",
    krx_mdc_timeout_seconds=30.0,
    krx_logical_rate_limit_seconds=0.0,
    krx_min_delay_seconds=0.0,
    krx_max_delay_seconds=0.0,
    krx_long_rest_every=0,
    krx_long_rest_min_seconds=0.0,
    krx_long_rest_max_seconds=0.0,
    krx_auth_cooldown_seconds=0.0,
    krx_error_backoff_min_seconds=0.0,
    krx_error_backoff_max_seconds=0.0,
)


class _Provider:
    def __init__(self, **_kwargs) -> None:
        pass

    def source(self):
        return SimpleNamespace(value="krx")


def _run(monkeypatch, capsys, *, attempted: int, errors: dict[str, str], rows: int):
    import collector.kr.adapters.flows_krx.provider as provider_module
    import collector.kr.infra.db_postgres.repositories as repositories
    import collector.kr.service.sync_krx_flows as service

    monkeypatch.setattr(app, "get_settings", lambda: _SETTINGS)
    monkeypatch.setattr(repositories, "PostgresStorage", lambda dsn: object())
    monkeypatch.setattr(provider_module, "KrxDirectFlowProvider", _Provider)
    result = SimpleNamespace(
        errors=dict(errors),
        targets_processed=1,
        requests_attempted=attempted,
        requests_skipped=0,
        rows_upserted=rows,
        no_data_requests=0,
        phase_counts={},
        pending_metrics=[],
    )
    monkeypatch.setattr(service, "sync_krx_security_flows", lambda **_kwargs: result)

    args = app.build_parser().parse_args(
        ["flows", "sync", "--start", "2026-07-27", "--end", "2026-07-27"]
    )
    code = _exit_code(args.handler, args)
    return code, capsys.readouterr()


def test_every_request_failed_exits_1(monkeypatch, capsys) -> None:
    errors = {f"foreign:2026-07-{d:02d}:KOSDAQ": "login failed" for d in range(1, 46)}
    code, out = _run(monkeypatch, capsys, attempted=45, errors=errors, rows=0)
    assert code == 1
    assert "❌ KRX flow sync failed: all 45 attempted requests errored." in out.err
    assert "Requests attempted: 45" in out.out  # the summary still prints first


def test_pipeline_error_exits_1(monkeypatch, capsys) -> None:
    code, out = _run(monkeypatch, capsys, attempted=3, errors={"pipeline": "db down"}, rows=0)
    assert code == 1
    assert "❌ KRX flow sync aborted (pipeline): db down" in out.err


def test_partial_failure_stays_exit_0(monkeypatch, capsys) -> None:
    code, out = _run(monkeypatch, capsys, attempted=10, errors={"a": "x", "b": "y"}, rows=500)
    assert code is None
    assert "⚠ Flow sync completed with 2 errors." in out.err


def test_clean_run_stays_exit_0(monkeypatch, capsys) -> None:
    code, out = _run(monkeypatch, capsys, attempted=32, errors={}, rows=62638)
    assert code is None
    assert "✅ KRX flow sync completed." in out.out
