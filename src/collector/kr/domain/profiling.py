"""Domain models for the table/feature profiling subsystem.

This module is **pure**: it declares *what* to measure (checks, specs,
results) without any knowledge of *how* to measure it (SQL, DB drivers,
file rendering).  Following the hexagonal dependency rule, nothing here
imports from ``adapters/`` or ``infra/``.

The design mirrors the existing ``metric_catalog`` / ``metric_mapping_rule``
split used elsewhere in the pipeline: a declarative :class:`TableProfileSpec`
describes a table, and the service layer drives the standard checks
(``C1``–``C13`` from ``PLAN.md`` §3) plus domain-specific checks against it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum


class CheckKind(StrEnum):
    """The standard profiling checks, codifying ``PLAN.md`` §3 (C1–C13).

    Each member maps 1:1 to a SQL builder in the query-runner adapter.  A
    check is silently skipped (recorded in the manifest with a reason) when
    the spec lacks the columns it needs — encoding the manual "omit if not
    applicable" rule as code.
    """

    COUNT_KEYS_RANGE = "count_keys_range"  # C1: row/key counts + min/max date
    TIME_DISTRIBUTION = "time_distribution"  # C2: rows/entities per year-month
    CATEGORY_DISTRIBUTION = "category_distribution"  # C3: per-category counts
    NULL_RATIOS = "null_ratios"  # C4: null/empty ratio per column
    DUPLICATE_GROUPS = "duplicate_groups"  # C5: natural-key / PK duplicates
    PER_ENTITY_DISTRIBUTION = "per_entity_distribution"  # C6: rows per entity
    ENTITY_TIME_COVERAGE = "entity_time_coverage"  # C7: entity x time coverage
    NUMERIC_QUANTILES = "numeric_quantiles"  # C8: quantiles, zero/neg ratios
    CATEGORY_TOP_N = "category_top_n"  # C9: Top-N codes/values
    INGEST_TIME_TREND = "ingest_time_trend"  # C10: ingest/freshness trend
    UNIT_SCALE = "unit_scale"  # C11: currency/unit/scale distribution
    FK_INTEGRITY = "fk_integrity"  # C12: orphan / join coverage
    PIT_VALIDITY = "pit_validity"  # C13: point-in-time / look-ahead
    FRESHNESS = "freshness"  # latest collected vs latest data, stale flag


class ProfileWeight(StrEnum):
    """How heavy a table profile is, used to scope ``profile all`` runs."""

    FULL = "full"
    LIGHT = "light"


class ProfileTableRole(StrEnum):
    """Logical table role used to scope profiling runs by data provenance."""

    RAW = "raw"
    DERIVED = "derived"
    REFERENCE = "reference"
    OPERATIONAL = "operational"


class CostClass(StrEnum):
    """Query cost class — drives the sampling/timeout policy."""

    CHEAP = "cheap"
    EXPENSIVE = "expensive"


class SamplePolicy(StrEnum):
    """Per-run sampling intent (mirrors the ``--sample-policy`` CLI flag)."""

    AUTO = "auto"  # sample only expensive checks on large tables
    FULL = "full"  # never sample
    SAMPLE = "sample"  # always sample where supported


@dataclass(frozen=True, slots=True)
class SamplingPolicy:
    """Declarative sampling configuration for a large table.

    Attributes:
        sample_pct: ``TABLESAMPLE SYSTEM (pct)`` percentage for expensive
            checks (quantiles, string Top-N).  ``None`` disables sampling.
        large_row_threshold: Row count above which ``AUTO`` policy starts
            sampling expensive checks.
    """

    sample_pct: float | None = None
    large_row_threshold: int = 5_000_000


@dataclass(frozen=True, slots=True)
class ForeignKeyProfileSpec:
    """A foreign-key relationship to validate for orphan rows / join coverage.

    Attributes:
        ref_table: The referenced (parent) table name.
        columns: Column pairs ``(child_col, parent_col)`` forming the join.
    """

    ref_table: str
    columns: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class TableProfileSpec:
    """Declarative profile configuration for a single table.

    The service layer reads only this spec to decide which standard checks
    apply; missing columns auto-skip their checks.  Adding a new table to
    the profiler is a one-line catalog entry.

    Attributes:
        table: Physical table name (must exist in the schema whitelist).
        weight: ``full`` or ``light`` — scopes ``profile all`` runs.
        role: Logical table role. ``raw`` means externally collected/source
            rows, ``derived`` means processed model-facing facts, ``reference``
            means catalogs/metadata, and ``operational`` means run-control or
            operational-domain tables.
        entity_key: Entity column (e.g. ``ticker``) or ``None``.
        time_col: Primary time axis column or ``None``.
        natural_key: Columns forming the natural key, for duplicate checks.
        numeric_cols: Numeric columns eligible for quantile analysis.
        category_cols: Low-cardinality columns for distribution analysis.
        top_n_cols: High-cardinality code columns for Top-N analysis.
        null_cols: Columns to measure null ratio for (empty = all columns).
        fk_relations: Foreign-key relationships to validate.
        drilldown_dim: Long-format dimension to split per-value (or ``None``).
        drilldown_threshold: Minimum distinct values before drilldown splits.
        ingest_col: Ingestion-timestamp column for freshness/trend checks.
        unit_cols: Currency/unit/scale columns for unit-scale analysis.
        cost_class: Query cost class for the table as a whole.
        sampling: Sampling policy for expensive checks on large tables.
        domain_checks: Domain-specific check ids (see ``service`` registry).
        pit_pairs: ``(available_col, event_col)`` pairs for PIT validation,
            where a violation is ``available_col > event_col``.
    """

    table: str
    weight: ProfileWeight = ProfileWeight.FULL
    role: ProfileTableRole = ProfileTableRole.RAW
    entity_key: str | None = None
    time_col: str | None = None
    natural_key: tuple[str, ...] = ()
    numeric_cols: tuple[str, ...] = ()
    category_cols: tuple[str, ...] = ()
    top_n_cols: tuple[str, ...] = ()
    null_cols: tuple[str, ...] = ()
    fk_relations: tuple[ForeignKeyProfileSpec, ...] = ()
    drilldown_dim: str | None = None
    drilldown_threshold: int = 5
    ingest_col: str | None = None
    unit_cols: tuple[str, ...] = ()
    cost_class: CostClass = CostClass.CHEAP
    sampling: SamplingPolicy = field(default_factory=SamplingPolicy)
    domain_checks: tuple[str, ...] = ()
    pit_pairs: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class ColumnInfo:
    """A single column as reported by ``describe_schema``."""

    name: str
    data_type: str
    is_nullable: bool


@dataclass(frozen=True, slots=True)
class TablePreflight:
    """Cheap pre-checks run before the full profile of a (large) table.

    Attributes:
        table: Table name.
        exists: Whether the table exists in the target DB.
        estimated_rows: ``pg_class.reltuples`` planner estimate.
        actual_rows: Exact ``COUNT(*)`` (may be expensive; populated lazily).
        max_time_value: ISO string of ``MAX(time_col)`` or ``None``.
        has_indexes: Whether the table has any non-PK index.
        columns: The table's columns.
    """

    table: str
    exists: bool
    estimated_rows: int | None = None
    actual_rows: int | None = None
    max_time_value: str | None = None
    has_indexes: bool = False
    columns: tuple[ColumnInfo, ...] = ()


@dataclass
class CheckResult:
    """The result of one check execution.

    Attributes:
        kind: The check that produced this result.
        title: Human-readable section title.
        rows: Row-oriented result records (each a flat ``dict``).
        sampled: Whether the underlying query used ``TABLESAMPLE``/subset.
        sample_pct: Sampling percentage when ``sampled`` is true.
        sql: The exact SQL executed (for reproducibility/notebook display).
        note: Optional explanatory note (e.g. why a check was reduced).
        warning: Set when the check failed/timed out — the run continues
            (mirrors the ``ingestion_runs`` partial-run convention).
    """

    kind: CheckKind
    title: str
    rows: list[dict] = field(default_factory=list)
    sampled: bool = False
    sample_pct: float | None = None
    sql: str | None = None
    note: str | None = None
    warning: str | None = None

    @property
    def ok(self) -> bool:
        """True when the check executed without a recorded warning."""
        return self.warning is None


@dataclass
class ProfileResult:
    """The assembled profile of one table for one run.

    Attributes:
        spec: The spec that drove this profile.
        target: DB target label (``local`` / ``sj2``).
        generated_at: When the profile was assembled (KST).
        preflight: The table preflight result.
        row_count: Authoritative row count (``actual`` if known, else est.).
        checks: Table-level check results.
        drilldown: Per-drilldown-value check results, keyed by value.
        skipped_reason: Set when the whole table was skipped (e.g. empty).
    """

    spec: TableProfileSpec
    target: str
    generated_at: datetime
    preflight: TablePreflight
    row_count: int | None = None
    checks: list[CheckResult] = field(default_factory=list)
    drilldown: dict[str, list[CheckResult]] = field(default_factory=dict)
    skipped_reason: str | None = None

    @property
    def warnings(self) -> list[str]:
        """All warnings raised across table-level and drilldown checks."""
        out = [c.warning for c in self.checks if c.warning]
        for results in self.drilldown.values():
            out.extend(c.warning for c in results if c.warning)
        return out


@dataclass
class RunManifest:
    """Run-level metadata + headline metrics — the diff/regression baseline.

    Attributes:
        run_id: Unique run identifier (``<ts>_<target>``).
        target: DB target label.
        git_sha: Repo commit at run time (best-effort, may be empty).
        run_date: ISO date of the run (KST).
        generated_at: Full KST timestamp of the run.
        tables: Per-table headline metrics keyed by table name.
        sample_policy: The effective sampling policy label.
        lib_versions: Versions of analysis libs used (for reproducibility).
        query_ok: Total checks that executed cleanly.
        query_failed: Total checks that raised a warning.
    """

    run_id: str
    target: str
    run_date: str
    generated_at: datetime
    git_sha: str = ""
    sample_policy: str = SamplePolicy.AUTO.value
    tables: dict[str, dict] = field(default_factory=dict)
    lib_versions: dict[str, str] = field(default_factory=dict)
    query_ok: int = 0
    query_failed: int = 0


@dataclass
class TableDrift:
    """Per-table change between two profiling runs.

    Attributes:
        table: Table name.
        row_delta: ``candidate.row_count - baseline.row_count`` (or ``None``).
        row_pct: Percent change in row count (``None`` if not computable).
        max_time_before: Baseline ``max_time_value``.
        max_time_after: Candidate ``max_time_value``.
        max_time_moved: ``forward`` / ``backward`` / ``same`` / ``unknown``.
        failed_delta: Change in failed-check count.
        skipped_before: Baseline skip reason (or ``None``).
        skipped_after: Candidate skip reason (or ``None``).
        new_warnings: Warnings present in candidate but not baseline.
        status: ``added`` / ``removed`` / ``changed`` / ``unchanged``.
    """

    table: str
    row_delta: int | None = None
    row_pct: float | None = None
    max_time_before: str | None = None
    max_time_after: str | None = None
    max_time_moved: str = "unknown"
    failed_delta: int = 0
    skipped_before: str | None = None
    skipped_after: str | None = None
    new_warnings: list[str] = field(default_factory=list)
    status: str = "unchanged"


@dataclass
class DriftReport:
    """The result of comparing two run manifests.

    Attributes:
        baseline_run_id: The earlier run's id.
        candidate_run_id: The later run's id.
        target: DB target label (must match across both runs).
        generated_at: When the diff was computed (KST).
        tables: Per-table drift, in candidate catalog order.
    """

    baseline_run_id: str
    candidate_run_id: str
    target: str
    generated_at: datetime
    tables: list[TableDrift] = field(default_factory=list)

    @property
    def changed(self) -> list[TableDrift]:
        """Tables whose status is not ``unchanged``."""
        return [t for t in self.tables if t.status != "unchanged"]
