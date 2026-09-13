"""Every ``args.<name>`` a handler reads must be a dest its parser defines.

This is the cheapest possible check and it was missing, so two commands shipped
in v0.9.4 that died on their first line:

    AttributeError: 'Namespace' object has no attribute 'include_delisted'

Both were stale references left by the T-group rename of ``--include-delisted``
to ``--universe-scope``. Neither is reachable by unit tests that call the
service directly, and both sat inside f-strings, so nothing typed-checked them.
``prices backfill`` is the daily incremental price job and ``prices
market-cap-backfill`` is the N1 backfill — the two highest-volume commands in
the pipeline, broken at the entry point, deployed to prod.

A handler crashing on argument access is not a subtle bug; it is a bug that any
invocation reveals. What was missing is an invocation. Rather than smoke-run
every command (they all need a DB), walk the parser tree for the dests each leaf
command actually defines and AST-scan its handler for the attributes it reads.
"""

from __future__ import annotations

import argparse
import ast
import inspect

import pytest

from collector.kr.cli import app


def _leaf_commands() -> list[tuple[str, str, set[str]]]:
    """(command path, handler name, dests visible to that handler)."""
    found: list[tuple[str, str, set[str]]] = []

    def walk(parser: argparse.ArgumentParser, path: list[str], inherited: set[str]) -> None:
        # Subparsers do not inherit the parent's options, but parse_args builds
        # one flat Namespace, so a handler can legitimately read any dest along
        # its own path.
        dests = set(inherited)
        subparser_actions = []
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                subparser_actions.append(action)
                dests.add(action.dest)
            elif action.dest != argparse.SUPPRESS:
                dests.add(action.dest)
        dests.update(parser._defaults)

        handler = parser.get_default("handler")
        if handler is not None:
            found.append((" ".join(path), handler.__name__, dests))

        for action in subparser_actions:
            for name, subparser in (action.choices or {}).items():
                walk(subparser, [*path, name], dests)

    walk(app.build_parser(), [], set())
    return found


def _args_attributes_read_by_handlers() -> dict[str, set[str]]:
    """Handler name -> attribute names it reads off a parameter called ``args``.

    Only direct ``args.x`` access is collected. ``getattr(args, "x", default)``
    is deliberately excluded: it has a default, so it cannot raise.
    """
    tree = ast.parse(inspect.getsource(app))
    return {
        node.name: {
            child.attr
            for child in ast.walk(node)
            if isinstance(child, ast.Attribute)
            and isinstance(child.value, ast.Name)
            and child.value.id == "args"
        }
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }


def test_every_handler_only_reads_arguments_its_parser_defines() -> None:
    reads = _args_attributes_read_by_handlers()

    offenders: list[str] = []
    for command, handler_name, dests in sorted(_leaf_commands()):
        missing = reads.get(handler_name, set()) - dests
        if missing:
            offenders.append(f"{command} ({handler_name}): {sorted(missing)}")

    assert not offenders, "handlers read arguments their parser never defines:\n  " + "\n  ".join(
        offenders
    )


def test_the_scan_actually_covers_the_cli() -> None:
    # A silent regression here (parser refactor, renamed attribute) would make
    # the check above pass by inspecting nothing.
    commands = _leaf_commands()
    assert len(commands) >= 20

    by_command = {command: dests for command, _, dests in commands}
    assert "prices backfill" in by_command
    assert "prices market-cap-backfill" in by_command
    assert "universe backfill-snapshots" in by_command
    assert "dart sync-corp-profile" in by_command

    # The two dests whose absence broke v0.9.4, from opposite directions:
    # prices backfill has universe_scope and must use that name, market-cap
    # never had it.
    assert "universe_scope" in by_command["prices backfill"]
    assert "universe_scope" not in by_command["prices market-cap-backfill"]

    reads = _args_attributes_read_by_handlers()
    assert "include_delisted" not in reads["_handle_prices_backfill"]


# Handlers that run a collector with a ConsecutiveFailureGuard. Each one can
# abort mid-run, and an aborted run has to reach the scheduler as a non-zero
# exit code.
_ABORTABLE_HANDLERS = (
    "_handle_prices_backfill",
    "_handle_prices_market_cap_backfill",
    "_handle_universe_backfill_snapshots",
)


def test_every_abortable_backfill_turns_an_aborted_run_into_a_failure() -> None:
    # The N3 snapshot backfill stopped at 5 consecutive failures exactly as
    # designed, wrote 23 of 146 snapshots, printed why -- and exited 0, so
    # Cronicle filed it as a success. Two of the three commands had no exit path
    # at all for this.
    tree = ast.parse(inspect.getsource(app))
    bodies = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name in _ABORTABLE_HANDLERS
    }
    assert set(bodies) == set(_ABORTABLE_HANDLERS), "handler renamed; update this list"

    missing = [
        name
        for name, node in bodies.items()
        if not any(
            isinstance(call.func, ast.Name) and call.func.id == "_exit_if_run_aborted"
            for call in ast.walk(node)
            if isinstance(call, ast.Call)
        )
    ]
    assert not missing, f"aborted runs would exit 0 in: {missing}"


def test_exit_if_run_aborted_covers_both_abort_keys_and_passes_partial_runs() -> None:
    for key in app.ABORTED_RUN_ERROR_KEYS:
        with pytest.raises(SystemExit) as excinfo:
            app._exit_if_run_aborted({key: "stopped"}, "Backfill")
        assert excinfo.value.code == 1

    # Per-item failures are `partial` by design and must still exit cleanly,
    # otherwise every bulk backfill with one bad ticker looks like an outage.
    app._exit_if_run_aborted({"005930": "timeout"}, "Backfill")
    app._exit_if_run_aborted({}, "Backfill")
