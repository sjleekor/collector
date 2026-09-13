"""Domain models for the KRX data pipeline.

All models are plain dataclasses — no framework dependency — so that the
domain layer remains pure and testable without infrastructure concerns.

Design choices:
    • OHLCV prices use ``int`` (Korean won has no sub-unit for equities).
      If fractional values are ever needed (e.g., index points), switch to
      ``Decimal`` and update the DDL column type accordingly.
    • ``StockUniverseSnapshot.records`` stores the full list so that
      snapshot-items can be persisted for audit/diff purposes.
    • Timestamps (``fetched_at``, ``started_at``, …) are timezone-aware
      ``datetime`` objects in Asia/Seoul.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from collector.kr.domain.enums import (
    ListingStatus,
    Market,
    PeriodicExtraStatement,
    RunStatus,
    RunType,
    SliceStatus,
    Source,
)


@dataclass(frozen=True, slots=True)
class Stock:
    """A single listed stock on KRX.

    Attributes:
        ticker: 6-digit KRX ticker code (e.g. ``"005930"``).
        market: Exchange market segment.
        name: Korean company name.
        status: Current listing status.
        last_seen_date: Date when the stock was last observed in a universe fetch.
        source: Data source that provided this record.
        listing_date: Source-reported listing date (FDR only, best-effort);
            ``None`` when the provider did not supply one.
        first_seen_date: First ``as_of_date`` on which the collector observed
            this ticker as ``ACTIVE``. Storage-managed collector metadata,
            never provider-set; ``None`` until persisted.
    """

    ticker: str
    market: Market
    name: str
    status: ListingStatus
    last_seen_date: date
    source: Source
    listing_date: date | None = None
    first_seen_date: date | None = None


@dataclass(frozen=True, slots=True)
class StockUniverseSnapshot:
    """Point-in-time snapshot of the stock universe.

    Stores metadata about a single universe-fetch run and the full list of
    ``Stock`` records captured.  The ``records`` list is persisted into
    ``stock_master_snapshot_items`` for auditability — enabling diffs between
    snapshots to detect new listings, delistings, or name changes.

    Attributes:
        snapshot_id: Unique identifier (UUID).
        as_of_date: The reference date for the universe.
        source: Data source used.
        fetched_at: KST timestamp when the data was retrieved.
        records: Full list of stocks in this snapshot.
    """

    snapshot_id: str
    as_of_date: date
    source: Source
    fetched_at: datetime
    records: list[Stock]

    @property
    def record_count(self) -> int:
        """Number of stocks in this snapshot."""
        return len(self.records)


@dataclass(frozen=True, slots=True)
class DailyBar:
    """Single daily OHLCV bar for a stock.

    Attributes:
        ticker: 6-digit KRX ticker code.
        market: Exchange market segment.
        trade_date: Trading date.
        open: Opening price (KRW, integer).
        high: High price.
        low: Low price.
        close: Closing price.
        volume: Traded volume.
        source: Data source.
        fetched_at: KST timestamp when the data was retrieved.
    """

    ticker: str
    market: Market
    trade_date: date
    open: int
    high: int
    low: int
    close: int
    volume: int
    source: Source
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class DailyMarketCapRow:
    """Single day of KRX market cap / trading value / listed shares for a stock.

    Same grain as :class:`DailyBar` but a different source, and the difference
    matters.  ``DailyBar.close`` is the naver ADJUSTED close; ``source_close``
    here is the KRX UNADJUSTED session close.  The name says so on purpose.

    Every value field is optional because pykrx casts blanks to ``0`` and the
    adapter maps an unusable response to ``None`` rather than storing a zero
    that cannot be told apart from a real one.

    Attributes:
        ticker: 6-digit KRX ticker code.
        market: Exchange market segment — comes from the CALL ARGUMENT, never
            from the response (which has no market column) and never from a
            ``stock_master`` join (which would leak a stock's present-day
            market into its pre-transfer rows).
        trade_date: Trading date.
        source_close: KRX unadjusted session close (KRW).
        market_cap: Market capitalisation (KRW).
        trading_value: Traded value (KRW).
        listed_shares: Listed share count.
        volume: Traded volume on a KRX basis.
        source_open: KRX unadjusted session open (KRW).  Only the Open API
            carries it; the pykrx response has no open/high/low, so rows from
            that adapter leave these three ``None``.
        source_high: KRX unadjusted session high (KRW).
        source_low: KRX unadjusted session low (KRW).
        source: Data source.
        fetched_at: KST timestamp when the data was retrieved.
    """

    ticker: str
    market: Market
    trade_date: date
    source_close: int | None
    market_cap: int | None
    trading_value: int | None
    listed_shares: int | None
    volume: int | None
    source: Source
    fetched_at: datetime
    # Trailing with defaults so the pykrx adapter's positional construction is
    # unaffected.  These arrive free with the Open API response, and they are
    # the unadjusted series that lets the adjustment factor be computed here
    # instead of inherited from naver's retroactive rewrites (K-7).
    source_open: int | None = None
    source_high: int | None = None
    source_low: int | None = None


@dataclass(frozen=True, slots=True)
class DartCorp:
    """OpenDART corporation-code master row.

    Attributes:
        corp_code: OpenDART corporation code (8 digits).
        corp_name: Legal company name from OpenDART.
        ticker: 6-digit KRX ticker code if the company is listed.
        market: KRX market segment when matched to ``stock_master``.
        stock_name: KRX stock name when available.
        modify_date: Last modified date from the OpenDART master file.
        is_active: Whether the ticker is currently active in ``stock_master``.
        source: Data source.
        fetched_at: KST timestamp when the row was retrieved.
    """

    corp_code: str
    corp_name: str
    ticker: str | None
    market: Market | None
    stock_name: str | None
    modify_date: date | None
    is_active: bool
    source: Source
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class DartFinancialStatementLine:
    """Single raw account line from OpenDART financial statements."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    fs_div: str
    sj_div: str
    sj_nm: str
    account_id: str
    account_nm: str
    account_detail: str
    thstrm_nm: str
    thstrm_amount: Decimal | None
    thstrm_add_amount: Decimal | None
    frmtrm_nm: str
    frmtrm_amount: Decimal | None
    frmtrm_q_nm: str
    frmtrm_q_amount: Decimal | None
    frmtrm_add_amount: Decimal | None
    bfefrmtrm_nm: str
    bfefrmtrm_amount: Decimal | None
    ord: int | None
    currency: str
    rcept_no: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartPeriodicExtraLine:
    """One raw row from a DS002 periodic-report disclosure (N6).

    The response row is kept whole in ``raw_payload`` rather than decomposed
    into columns.  Five endpoints share this shape and their fields have little
    in common, so a decomposed schema would be mostly NULL; and which fields
    matter is still being decided (`07` §6), which is exactly when a raw
    payload is worth more than a guess at the right columns.

    ``rcept_no`` is part of the unique key, so a corrected report adds rows
    instead of overwriting the earlier ones — for audit opinions and changes of
    control, the correction is itself the signal.
    """

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    statement_type: PeriodicExtraStatement
    row_ordinal: int
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartShareCountLine:
    """Single raw row from OpenDART stockTotqySttus."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    corp_cls: str
    se: str
    isu_stock_totqy: int | None
    now_to_isu_stock_totqy: int | None
    now_to_dcrs_stock_totqy: int | None
    redc: str
    profit_incnr: str
    rdmstk_repy: str
    etc: str
    istc_totqy: int | None
    tesstk_co: int | None
    distb_stock_co: int | None
    stlm_dt: date | None
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartShareholderReturnLine:
    """Flattened metric row from dividend / treasury-stock disclosures."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    statement_type: str
    row_name: str
    stock_knd: str
    dim1: str
    dim2: str
    dim3: str
    metric_code: str
    metric_name: str
    value_numeric: Decimal | None
    value_text: str
    unit: str
    rcept_no: str
    stlm_dt: date | None
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartFilingReceiptLine:
    """Single raw disclosure-receipt row from OpenDART list.json (공시검색)."""

    corp_code: str
    ticker: str
    corp_name: str
    stock_code: str
    corp_cls: str
    report_nm: str
    rcept_no: str
    flr_nm: str
    rcept_dt: date | None
    rm: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartCapitalChangeLine:
    """Single raw row from OpenDART irdsSttus (증자(감자)현황)."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    corp_cls: str
    isu_dcrs_de: date | None
    isu_dcrs_stle: str
    isu_dcrs_stock_knd: str
    isu_dcrs_qy: int | None
    isu_dcrs_mstvdv_fval_amount: Decimal | None
    isu_dcrs_mstvdv_fval_amount2: Decimal | None
    stlm_dt: date | None
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class XbrlBackfillTarget:
    """One explicit (corp, filing, receipt) target for a receipt-targeted XBRL refetch."""

    ticker: str
    corp_code: str
    bsns_year: int
    reprt_code: str
    rcept_no: str


@dataclass(frozen=True, slots=True)
class DartXbrlDocument:
    """Metadata for one downloaded OpenDART XBRL ZIP document."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    zip_entry_count: int
    instance_document_name: str
    label_ko_document_name: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class DartXbrlFactLine:
    """Single XBRL fact extracted from an OpenDART XBRL instance document."""

    corp_code: str
    ticker: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    concept_id: str
    concept_name: str
    namespace_uri: str
    context_id: str
    context_type: str
    period_start: date | None
    period_end: date | None
    instant_date: date | None
    dimensions: list[str]
    unit_id: str
    unit_measure: str
    decimals: str
    value_numeric: Decimal | None
    value_text: str
    is_nil: bool
    label_ko: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class SecurityFlowLine:
    """Single daily investor/shorting/ownership raw metric."""

    trade_date: date
    ticker: str
    market: Market
    metric_code: str
    metric_name: str
    value: Decimal | None
    unit: str
    source: Source
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class OperatingSourceDocument:
    """Raw source document used for sector-specific operating metric extraction."""

    document_key: str
    ticker: str
    market: Market
    sector_key: str
    document_type: str
    title: str
    document_date: date | None
    period_end: date | None
    source_system: str
    source_url: str
    language: str
    content_text: str
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class OperatingMetricFact:
    """Extracted sector-specific operating metric fact."""

    ticker: str
    market: Market
    sector_key: str
    metric_code: str
    metric_name: str
    period_end: date | None
    value_numeric: Decimal | None
    value_text: str
    unit: str
    document_key: str
    extractor_code: str
    raw_snippet: str
    fetched_at: datetime
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class MetricCatalogEntry:
    """Canonical metric definition."""

    metric_code: str
    metric_name: str
    category: str
    unit: str
    description: str
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class MetricMappingRule:
    """Rule connecting a raw row shape to a canonical metric."""

    rule_code: str
    metric_code: str
    source_table: str
    value_selector: str
    priority: int
    statement_type: str = ""
    fs_div: str = ""
    sj_div: str = ""
    account_id: str = ""
    account_nm: str = ""
    row_name: str = ""
    stock_knd: str = ""
    dim1: str = ""
    dim2: str = ""
    dim3: str = ""
    metric_code_match: str = ""
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class StockMetricFact:
    """Normalized canonical metric fact for one company and reporting period."""

    ticker: str
    market: Market
    corp_code: str
    metric_code: str
    period_type: str
    period_end: date | None
    bsns_year: int
    reprt_code: str
    fs_div: str
    value_numeric: Decimal | None
    value_text: str
    unit: str
    source_table: str
    source_key: str
    mapping_rule_code: str
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class CommonFeatureSeries:
    """Source series definition for a common market or macro feature."""

    series_id: str
    source: Source
    source_series_key: str
    category: str
    frequency: str
    name_kr: str
    name_en: str = ""
    unit: str = ""
    country: str = ""
    market: str = ""
    endpoint_params: dict[str, object] = field(default_factory=dict)
    availability_policy: str = "release_date"
    manual_lag_days: int = 0
    source_timezone: str = "Asia/Seoul"
    history_start_date: date | None = None
    max_stale_business_days: int = 5
    default_transform: str = ""
    active: bool = True
    notes: str = ""


@dataclass(frozen=True, slots=True)
class CommonFeatureObservation:
    """Single raw observation for a common feature source series."""

    source: Source
    series_id: str
    observation_date: date
    frequency: str
    fetched_at: datetime
    period_end_date: date | None = None
    release_date: date | None = None
    available_from_date: date | None = None
    vintage: str = ""
    value_numeric: Decimal | None = None
    value_text: str = ""
    unit: str = ""
    source_updated_at: datetime | None = None
    raw_payload: dict[str, object] = field(default_factory=dict)
    raw_id: int | None = None


@dataclass(frozen=True, slots=True)
class CommonFeatureCatalogEntry:
    """Model-facing feature definition derived from one or more source series.

    ``input_roles`` runs parallel to ``input_series_ids`` and names each
    input's role in the transform (e.g. ``spread_long``/``spread_short`` or
    ``numerator``/``denominator``). When empty, every input defaults to
    ``primary`` — the single-input case.
    """

    feature_code: str
    feature_name_kr: str
    category: str
    frequency: str = "D"
    unit: str = ""
    transform_code: str = ""
    description: str = ""
    input_series_ids: tuple[str, ...] = ()
    input_roles: tuple[str, ...] = ()
    active: bool = True

    def roles(self) -> tuple[str, ...]:
        """Return one role per input series, defaulting absent roles to primary."""
        if not self.input_roles:
            return tuple("primary" for _ in self.input_series_ids)
        return self.input_roles

    def series_by_role(self) -> dict[str, str]:
        """Map role -> series_id. Assumes each role is used at most once."""
        return dict(zip(self.roles(), self.input_series_ids, strict=False))


@dataclass(frozen=True, slots=True)
class CommonFeatureDailyFact:
    """KRX-date-aligned common feature value safe for point-in-time joins."""

    feature_date: date
    feature_code: str
    asof_available_date: date
    generated_at: datetime
    value_numeric: Decimal | None = None
    value_text: str = ""
    unit: str = ""
    source_series_ids: list[str] = field(default_factory=list)
    source_observation_ids: list[int] = field(default_factory=list)
    selected_vintage: str = ""
    generation_run_id: str | None = None


# ---------------------------------------------------------------------------
# Result / aggregate types
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class UpsertResult:
    """Outcome counters for an upsert operation.

    Attributes:
        inserted: Number of newly inserted rows.
        updated: Number of rows updated (conflict resolved).
        errors: Number of rows that failed.
    """

    inserted: int = 0
    updated: int = 0
    errors: int = 0


@dataclass(slots=True)
class UniverseResult:
    """Result of a universe-fetch operation.

    Attributes:
        snapshot: The captured snapshot (may be ``None`` on failure).
        error: Error message if the fetch failed.
    """

    snapshot: StockUniverseSnapshot | None = None
    error: str | None = None


@dataclass(slots=True)
class DailyPriceResult:
    """Result of a daily-price fetch for one ticker.

    Attributes:
        ticker: Ticker that was fetched.
        bars: List of daily bars retrieved.
        error: Error message if the fetch failed.
    """

    ticker: str = ""
    bars: list[DailyBar] = field(default_factory=list)
    error: str | None = None


@dataclass(slots=True)
class DailyMarketCapResult:
    """Result of a market-cap fetch for one ``(trade_date, market)`` slice.

    Attributes:
        trade_date: Date that was fetched.
        market: Market segment that was fetched.
        rows: Rows retrieved (already missing-value normalised).
        response_rows: Rows the provider saw before dropping unusable ones.
            The service reconciles this against what storage wrote — a slice
            is not complete just because some of its rows landed.
        error: Error message if the fetch failed.
    """

    trade_date: date | None = None
    market: Market | None = None
    rows: list[DailyMarketCapRow] = field(default_factory=list)
    response_rows: int = 0
    error: str | None = None


@dataclass(slots=True)
class MarketCapBackfillResult:
    """Outcome of a market-cap backfill run.

    Attributes:
        slices_attempted: ``(trade_date, market)`` slices requested.
        slices_skipped: Slices already complete and skipped.
        slices_completed: Slices fetched and stored with a matching row count.
        rows_upserted: Total rows written.
        rows_dropped: Response rows dropped as unusable (holiday zero-fill).
        errors: Per-slice error messages, keyed ``"YYYY-MM-DD/MARKET"``.
    """

    slices_attempted: int = 0
    slices_skipped: int = 0
    slices_completed: int = 0
    rows_upserted: int = 0
    rows_dropped: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CompanyProfile:
    """OpenDART company profile (``company.json``, DS001) for one corporation.

    Attributes:
        corp_code: OpenDART corporation code.
        induty_code: KSIC industry code.  **Length varies** — 2, 3, 4 and 5
            digits all occur, so any grouping rule must take a prefix rather
            than assume a fixed width.
        corp_cls: Y (KOSPI) / K (KOSDAQ) / N (KONEX) / E (other).
        est_dt: Incorporation date — the input for firm age.
        acc_mt: Fiscal-year-end month as ``MM``.  Anything other than ``12``
            breaks the mart's hardcoded period-end calendar.
        raw_payload: Full response, kept so unused fields stay recoverable.
        fetched_at: KST timestamp when the profile was retrieved.
        ticker: ``stock_code`` as returned by *this* response (F-1).  Not the
            same thing as ``dart_corp_master.ticker``, which comes from
            ``corpCode.xml``: the two disagreeing is itself a signal, so the
            monthly history keeps the response's own value.
        corp_name: Corporation name from this response.
        stock_name: Listed-issue name from this response.
    """

    corp_code: str
    induty_code: str | None
    corp_cls: str | None
    est_dt: date | None
    acc_mt: str | None
    raw_payload: dict
    fetched_at: datetime
    # Appended with defaults (F-1): every existing caller builds a profile
    # positionally up to fetched_at.
    ticker: str | None = None
    corp_name: str | None = None
    stock_name: str | None = None


@dataclass(slots=True)
class CompanyProfileResult:
    """Result of a single ``company.json`` fetch.

    ``exhaustion_reason`` is the field name every other OpenDART result uses,
    and the one ``apply_call_result_meta`` copies and
    ``is_opendart_daily_limit_exhausted`` reads.  This result used to declare
    ``all_rate_limited`` instead, which no part of the pipeline looks at — so a
    key-exhausted profile run retried three times per corporation and then
    walked its whole target list against an API already refusing it, instead of
    exiting 75 and resuming the next day.  Renamed with F-1, whose monthly
    3,959-call job shares the ``opendart`` lock with the 04:00 chain and is
    therefore the first caller likely to meet a spent quota.
    """

    profile: CompanyProfile | None = None
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    no_data: bool = False
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class CompanyProfileSyncResult:
    """Outcome of the DART company-profile sync.

    Attributes:
        requests_attempted: Corporations fetched.
        requests_skipped: Corporations skipped as already profiled.
        no_data: Corporations OpenDART had no profile for.
        rows_upserted: Profile rows written to ``dart_corp_master``.
        history_rows_appended: Monthly observations added to
            ``dart_corp_profile_history`` (F-1).  0 on a second run in the same
            month, which is the idempotency signal — the master's counter does
            not show it, because the master is an upsert.
        errors: Per-corporation error messages.
        opendart_exhaustion_reason: Set to ``"all_rate_limited"`` when every
            OpenDART key hit its daily limit, so the CLI can exit 75 and the
            scheduler resumes tomorrow.
    """

    requests_attempted: int = 0
    requests_skipped: int = 0
    no_data: int = 0
    rows_upserted: int = 0
    history_rows_appended: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class CompanyProfileHistorySeedResult:
    """Outcome of ``dart seed-corp-profile-history`` (F-1.3).

    Attributes:
        observed_month: Month the seed rows were stamped with.
        rows_inserted: Rows added.  0 means the month was already seeded — the
            command is safe to re-run.
        errors: Pipeline-level error messages.
    """

    observed_month: date | None = None
    rows_inserted: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class UniverseSnapshotBackfillResult:
    """Outcome of a historical universe-snapshot backfill run.

    Attributes:
        snapshots_attempted: Month-end dates considered.
        snapshots_skipped: Dates that already had a backfilled snapshot.
        snapshots_written: Snapshots newly persisted.
        items_written: Snapshot item rows inserted.
        errors: Per-date error messages, keyed ``"YYYY-MM-DD"``.
    """

    snapshots_attempted: int = 0
    snapshots_skipped: int = 0
    snapshots_written: int = 0
    items_written: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class DartCorpCodeResult:
    """Result of fetching the OpenDART corporation-code master."""

    records: list[DartCorp] = field(default_factory=list)
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartFinancialStatementResult:
    """Result of fetching one OpenDART financial statement payload."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    fs_div: str = ""
    records: list[DartFinancialStatementLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartShareCountResult:
    """Result of fetching one OpenDART share-count payload."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    records: list[DartShareCountLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartShareholderReturnResult:
    """Result of fetching one OpenDART dividend or treasury-stock payload."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    statement_type: str = ""
    records: list[DartShareholderReturnLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartFilingReceiptResult:
    """Result of fetching one OpenDART disclosure-receipt window (all pages)."""

    corp_code: str = ""
    ticker: str = ""
    bgn_de: date | None = None
    end_de: date | None = None
    records: list[DartFilingReceiptLine] = field(default_factory=list)
    total_count: int = 0
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartCapitalChangeResult:
    """Result of fetching one OpenDART capital-change (irdsSttus) payload."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    records: list[DartCapitalChangeLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartXbrlResult:
    """Result of fetching and parsing one OpenDART XBRL document."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    rcept_no: str = ""
    document: DartXbrlDocument | None = None
    facts: list[DartXbrlFactLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class SecurityFlowFetchResult:
    """Result of fetching a batch of security-flow raw rows."""

    records: list[SecurityFlowLine] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None


@dataclass(frozen=True, slots=True)
class FlowRequestStats:
    """Transport-level counters a flow provider reports back to its service.

    ``requests_attempted`` in ``ingestion_runs`` has always been a count of
    logical work items, which is why it could neither audit a published quota
    nor account for the traffic that got this host blocked. These are the real
    numbers: calls that left, retries, pages walked.
    """

    http_requests: int = 0
    http_retries: int = 0
    pages_fetched: int = 0
    rate_limited_responses: int = 0
    auth_token_issued: int = 0
    auth_token_cache_hits: int = 0
    throttle_waits: int = 0
    throttle_wait_seconds: float = 0.0
    status_counts: Mapping[str, int] = field(default_factory=dict)

    def as_counts(self) -> dict[str, int]:
        """Flatten into ``ingestion_runs.counts``-shaped integers."""
        counts = {
            "http_requests": self.http_requests,
            "http_retries": self.http_retries,
            "http_pages_fetched": self.pages_fetched,
            "http_rate_limited": self.rate_limited_responses,
            "auth_token_issued": self.auth_token_issued,
            "auth_token_cache_hits": self.auth_token_cache_hits,
            "throttle_waits": self.throttle_waits,
            "throttle_wait_seconds": int(self.throttle_wait_seconds),
        }
        for status, count in sorted(self.status_counts.items()):
            counts[f"http_status_{status}"] = int(count)
        return counts


@dataclass(slots=True)
class CommonFeatureFetchResult:
    """Result of fetching one common feature source series."""

    records: list[CommonFeatureObservation] = field(default_factory=list)
    no_data: bool = False
    error: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None


@dataclass(slots=True)
class OperatingMetricExtractionResult:
    """Result of extracting metrics from one operating source document."""

    document: OperatingSourceDocument | None = None
    facts: list[OperatingMetricFact] = field(default_factory=list)
    error: str | None = None


@dataclass(slots=True)
class SyncResult:
    """Outcome of a universe-sync use-case run.

    Attributes:
        upsert: Aggregated upsert counters.
        new_tickers: Tickers not previously in stock_master.
        delisted_tickers: Tickers no longer in the fetched universe.
        error: Error message if something went wrong.
    """

    upsert: UpsertResult = field(default_factory=UpsertResult)
    new_tickers: list[str] = field(default_factory=list)
    delisted_tickers: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass(slots=True)
class BackfillResult:
    """Outcome of a daily-backfill use-case run.

    Attributes:
        tickers_processed: Number of tickers attempted.
        bars_upserted: Total bars written.
        baseline_clamped_tickers: Incremental baseline-missing tickers whose
            auto-derived start was clamped to the ``max_auto_range_days`` window
            (each needs a separate full-history repair).
        auto_new_ticker_start_tickers: Incremental baseline-missing tickers that
            used an auto-derived ``listing_date`` / ``first_seen_date`` start.
        errors: Per-ticker error messages.
    """

    tickers_processed: int = 0
    bars_upserted: int = 0
    baseline_clamped_tickers: int = 0
    auto_new_ticker_start_tickers: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class DartCorpSyncResult:
    """Outcome of syncing OpenDART corp codes into local storage."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    total_records: int = 0
    matched_active_tickers: int = 0
    unmatched_active_tickers: list[str] = field(default_factory=list)
    unmatched_dart_tickers: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass(slots=True)
class DartFinancialSyncResult:
    """Outcome of syncing OpenDART financial-statement raw rows."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartShareInfoSyncResult:
    """Outcome of syncing share-count, shareholder-return, and capital-change raw rows."""

    share_count_upsert: UpsertResult = field(default_factory=UpsertResult)
    shareholder_return_upsert: UpsertResult = field(default_factory=UpsertResult)
    capital_change_upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    share_count_rows_upserted: int = 0
    shareholder_return_rows_upserted: int = 0
    capital_change_rows_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartPeriodicExtraResult:
    """Result of fetching one DS002 periodic-report disclosure payload."""

    corp_code: str = ""
    ticker: str = ""
    bsns_year: int = 0
    reprt_code: str = ""
    statement_type: PeriodicExtraStatement | None = None
    records: list[DartPeriodicExtraLine] = field(default_factory=list)
    #: Rows the response carried, before any were dropped.  Compared against
    #: what storage wrote so the slice ledger can tell a complete slice from a
    #: half-written one (L-1).
    response_rows: int = 0
    no_data: bool = False
    error: str | None = None
    status_code: str | None = None
    retryable: bool = False
    retry_after_seconds: float | None = None
    exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartPeriodicExtrasSyncResult:
    """Outcome of syncing DS002 periodic-report extras."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    slices_pending: int = 0
    slices_skipped_complete: int = 0
    slices_skipped_no_data: int = 0
    slices_retrying: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartFilingReceiptSyncResult:
    """Outcome of syncing OpenDART disclosure-receipt history raw rows."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class DartXbrlSyncResult:
    """Outcome of syncing parsed OpenDART XBRL documents and facts."""

    document_upsert: UpsertResult = field(default_factory=UpsertResult)
    fact_upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    documents_upserted: int = 0
    facts_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    opendart_exhaustion_reason: str | None = None


@dataclass(slots=True)
class KrxFlowPhaseCounts:
    """Per-phase counters for KRX security-flow sync."""

    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    error_count: int = 0


@dataclass(slots=True)
class KrxFlowSyncResult:
    """Outcome of syncing KRX security-flow raw rows."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    phase_counts: dict[str, KrxFlowPhaseCounts] = field(default_factory=dict)
    pending_metrics: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CommonFeatureSyncResult:
    """Outcome of syncing common feature raw observations."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    series_processed: int = 0
    requests_attempted: int = 0
    requests_skipped: int = 0
    rows_upserted: int = 0
    no_data_requests: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CommonFeatureBuildResult:
    """Outcome of building KRX-date-aligned common feature facts."""

    upsert: UpsertResult = field(default_factory=UpsertResult)
    features_processed: int = 0
    feature_dates_processed: int = 0
    facts_built: int = 0
    null_facts: int = 0
    facts_upserted: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CommonFeatureCatalogSeedResult:
    """Outcome of seeding common feature series and catalog rows."""

    series_upsert: UpsertResult = field(default_factory=UpsertResult)
    catalog_upsert: UpsertResult = field(default_factory=UpsertResult)


@dataclass(slots=True)
class OperatingMetricSyncResult:
    """Outcome of processing operating KPI source documents."""

    document_upsert: UpsertResult = field(default_factory=UpsertResult)
    fact_upsert: UpsertResult = field(default_factory=UpsertResult)
    documents_processed: int = 0
    facts_upserted: int = 0
    extracted_metric_codes: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class MetricNormalizationResult:
    """Outcome of seeding rules and normalizing canonical metrics."""

    catalog_upsert: UpsertResult = field(default_factory=UpsertResult)
    rule_upsert: UpsertResult = field(default_factory=UpsertResult)
    fact_upsert: UpsertResult = field(default_factory=UpsertResult)
    targets_processed: int = 0
    facts_written: int = 0
    stale_facts_deleted: int = 0
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MetricCoverageRow:
    """Coverage summary for one canonical metric."""

    metric_code: str
    metric_name: str
    target_count: int
    covered_count: int
    missing_count: int
    coverage_ratio: Decimal


@dataclass(slots=True)
class MetricCoverageReport:
    """Coverage report over normalized canonical metric facts."""

    target_count: int = 0
    rows: list[MetricCoverageRow] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommonFeatureCoverageRow:
    """Coverage summary for one KRX-date-aligned common feature."""

    feature_code: str
    feature_name_kr: str
    target_count: int
    fact_count: int
    non_null_count: int
    null_count: int
    missing_count: int
    coverage_ratio: Decimal
    pit_violation_count: int


@dataclass(slots=True)
class CommonFeatureCoverageReport:
    """Coverage report over common feature daily facts."""

    target_count: int = 0
    rows: list[CommonFeatureCoverageRow] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommonFeatureReadinessRow:
    """Active-readiness decision for one KRX-date-aligned common feature."""

    feature_code: str
    feature_name_kr: str
    target_count: int
    fact_count: int
    non_null_count: int
    null_count: int
    missing_count: int
    coverage_ratio: Decimal
    pit_violation_count: int
    required_coverage_ratio: Decimal
    ready: bool
    blockers: tuple[str, ...] = ()


@dataclass(slots=True)
class CommonFeatureReadinessReport:
    """Readiness report for common feature active-transition decisions."""

    target_count: int = 0
    rows: list[CommonFeatureReadinessRow] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class IngestionRun:
    """Metadata for a single pipeline execution (persisted to ingestion_runs).

    Attributes:
        run_id: UUID string.
        run_type: Category of the run.
        started_at: KST start time.
        ended_at: KST end time (``None`` while running).
        status: Current run status.
        params: Arbitrary run parameters (JSON-serialisable dict).
        counts: Aggregated counters (JSON-serialisable dict).
        error_summary: Human-readable error summary.
    """

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    run_type: RunType = RunType.UNIVERSE_SYNC
    started_at: datetime | None = None
    ended_at: datetime | None = None
    status: RunStatus = RunStatus.RUNNING
    params: dict[str, object] | None = None
    counts: dict[str, int] | None = None
    error_summary: str | None = None


@dataclass(slots=True)
class CollectionSliceState:
    """Completion state of one collection slice (``collection_slice_state``).

    The unit is whatever one upstream call covers: ``(trade_date, market)`` for
    a market-wide daily endpoint, ``(corp_code, year, report)`` for OpenDART.
    ``slice_key`` is that tuple rendered as a string, because the ledger is
    shared across sources whose slice shapes do not agree.

    Attributes:
        source: Upstream system that owns the slice.
        endpoint: Endpoint name within that source, so two collectors reading
            the same source never collide on a slice key.
        slice_key: Stable identifier for the slice, e.g. ``2024-01-02|KOSPI``.
        status: Completion state.
        expected_rows: Rows the response carried, when the caller knows.
        actual_rows: Rows storage reported writing.  A slice is complete only
            when this agrees with ``expected_rows`` — "it has some rows" is how
            a half-written slice becomes permanent.
        attempt_count: Attempts so far, so a slice that keeps failing is
            visible rather than buried in per-run error lists.
        last_error: Last failure message, truncated by the storage layer.
        updated_at: KST timestamp of the last transition.  The ``no_data`` TTL
            is measured from here.
    """

    source: Source
    endpoint: str
    slice_key: str
    status: SliceStatus
    expected_rows: int | None = None
    actual_rows: int | None = None
    attempt_count: int = 0
    last_error: str | None = None
    updated_at: datetime | None = None

    @property
    def is_complete(self) -> bool:
        """Return ``True`` when this slice needs no further collection.

        ``no_data`` is complete only until its TTL expires, which the caller
        applies — the model cannot know the TTL.
        """
        if self.status is SliceStatus.SUCCESS:
            return self.expected_rows is None or self.expected_rows == self.actual_rows
        return self.status is SliceStatus.NO_DATA
