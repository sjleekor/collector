"""Ports (Protocols) for the profiling subsystem.

These structural interfaces decouple the profiling service from the
PostgreSQL query implementation and the file/notebook renderers.  Concrete
implementations live in ``infra/db_postgres`` and ``adapters/profiling_render``
and are wired in the ``cli/app.py`` composition root.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from collector.kr.domain.profiling import (
    CheckKind,
    CheckResult,
    ColumnInfo,
    ProfileResult,
    RunManifest,
    TablePreflight,
    TableProfileSpec,
)


@runtime_checkable
class ProfileQueryRunner(Protocol):
    """Executes profiling checks against a DB target and returns results.

    Implementations own all SQL generation and execution, including
    identifier whitelisting, sampling, and per-query timeouts.  A failed or
    timed-out check must return a :class:`CheckResult` with ``warning`` set
    rather than raising, so the overall run continues as ``partial``.
    """

    def describe_schema(self, table: str) -> list[ColumnInfo]:
        """Return the columns of ``table`` (empty if the table is absent)."""
        ...

    def preflight(self, spec: TableProfileSpec) -> TablePreflight:
        """Run cheap pre-checks (existence, est. rows, max date, indexes)."""
        ...

    def distinct_values(self, table: str, column: str, limit: int) -> list[str]:
        """Return up to ``limit`` distinct values of ``column`` (for drilldown)."""
        ...

    def run_check(
        self,
        spec: TableProfileSpec,
        kind: CheckKind,
        *,
        drill_value: str | None = None,
    ) -> CheckResult:
        """Execute one standard check, optionally scoped to a drilldown value."""
        ...

    def run_domain_check(
        self,
        spec: TableProfileSpec,
        check_id: str,
    ) -> CheckResult:
        """Execute one domain-specific check by id."""
        ...


@runtime_checkable
class ProfileRenderer(Protocol):
    """Renders a :class:`ProfileResult` into one or more output files."""

    def render(
        self,
        result: ProfileResult,
        *,
        out_dir: Path,
        formats: list[str],
    ) -> list[Path]:
        """Render ``result`` into ``out_dir`` for each requested format."""
        ...


@runtime_checkable
class ProfileIndexRenderer(Protocol):
    """Renders the run-level index/dashboard and manifest."""

    def render_index(
        self,
        manifest: RunManifest,
        results: list[ProfileResult],
        *,
        out_dir: Path,
        formats: list[str],
    ) -> list[Path]:
        """Render the run summary + manifest for the whole catalog run."""
        ...
