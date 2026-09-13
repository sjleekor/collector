"""Port: Storage / repository interface.

Defines the contract for persisting domain objects.  The primary
implementation targets PostgreSQL, but the protocol is storage-agnostic
so that a file-based backend (CSV / Parquet) can be swapped in without
touching core logic.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import date
from typing import Protocol, runtime_checkable

from collector.kr.domain.enums import ListingStatus, Market, RunType, Source
from collector.kr.domain.models import (
    CollectionSliceState,
    CommonFeatureCatalogEntry,
    CommonFeatureDailyFact,
    CommonFeatureObservation,
    CommonFeatureSeries,
    CompanyProfile,
    DailyBar,
    DailyMarketCapRow,
    DartCapitalChangeLine,
    DartCorp,
    DartFilingReceiptLine,
    DartFinancialStatementLine,
    DartPeriodicExtraLine,
    DartShareCountLine,
    DartShareholderReturnLine,
    DartXbrlDocument,
    DartXbrlFactLine,
    IngestionRun,
    MetricCatalogEntry,
    MetricMappingRule,
    OperatingMetricFact,
    OperatingSourceDocument,
    SecurityFlowLine,
    Stock,
    StockMetricFact,
    StockUniverseSnapshot,
    UpsertResult,
)


@runtime_checkable
class Storage(Protocol):
    """Repository / unit-of-work style storage port.

    Implementations:
        - ``PostgresStorage`` (infra/db_postgres/repositories.py)
        - Future: ``FileStorage`` (CSV / Parquet writer)
    """

    # -- Schema management ----------------------------------------------------

    def init_schema(self) -> None:
        """Create tables / ensure schema is up to date.

        For PostgreSQL this executes the DDL from ``sql/postgres_ddl.sql``.
        For file-based storage this may create directories.
        """
        ...

    # -- Stock master ---------------------------------------------------------

    def upsert_stock_master(
        self,
        stocks: list[Stock],
        snapshot: StockUniverseSnapshot,
    ) -> UpsertResult:
        """Upsert stock master rows and persist the snapshot for audit.

        Args:
            stocks: Current universe records to upsert.
            snapshot: The snapshot metadata (persisted to
                ``stock_master_snapshot`` + ``stock_master_snapshot_items``).

        Returns:
            Counters of inserted / updated / errored rows.
        """
        ...

    def upsert_dart_corp_master(self, records: list[DartCorp]) -> UpsertResult:
        """Upsert OpenDART corporation-code master rows.

        Args:
            records: OpenDART corp-code records enriched with optional
                KRX market/name linkage.

        Returns:
            Counters of inserted / updated / errored rows.
        """
        ...

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
        include_delisted: bool = False,
    ) -> list[DartCorp]:
        """Return OpenDART corp-code rows mapped to local tickers.

        Args:
            active_only: If ``True``, restrict to rows matched to active
                ``stock_master`` records.
            tickers: Optional ticker allowlist.
            include_delisted: If ``True``, return the HISTORICAL listed set —
                every corp that ever carried a stock_code, listed today or not
                (3,959 vs 2,657).  ``active_only`` is ignored.

                This is not the same as ``active_only=False``, which would also
                return the ~112k corps that never had a ticker.  corpCode.xml
                keeps ``stock_code`` after delisting, so the mapping for the
                1,330 delisted names is already stored — the default filter is
                the only reason every DART raw table covers ~2% of them
                (`poc/survivorship_gap.md`).

        Returns:
            List of OpenDART corp-code rows.
        """
        ...

    def upsert_company_profiles(self, profiles: list[CompanyProfile]) -> UpsertResult:
        """Write ``company.json`` fields onto existing ``dart_corp_master`` rows.

        An UPDATE, not an upsert: a profile for a corp_code that is not already
        in the master is a bug upstream, not a row to create.

        Args:
            profiles: Profiles to persist.

        Returns:
            Counters for the rows updated.
        """
        ...

    def get_profiled_corp_codes(self) -> set[str]:
        """Return corp_codes whose profile has already been fetched.

        The skip-if-present key: ``profile_fetched_at IS NOT NULL``.

        Returns:
            Set of corp_codes to skip.
        """
        ...

    def append_company_profile_history(
        self,
        profiles: list[CompanyProfile],
        observed_month: date,
        run_id: str | None = None,
    ) -> UpsertResult:
        """Append one monthly observation per profile (F-1).

        ``dart_corp_master`` is an upsert and loses the previous industry code,
        so this is the table that makes ``induty_code`` point-in-time going
        forwards.  ``(corp_code, observed_month)`` is the skip-if-present key
        and the insert is ``DO NOTHING``: a second run in the same month adds
        nothing, which is what makes the monthly job safe to retry.

        Args:
            profiles: Profiles just fetched.
            observed_month: First day of the observation month.
            run_id: The ``ingestion_runs`` row this observation came from.

        Returns:
            Counters; ``updated`` counts the rows actually inserted, so a
            re-run in the same month reports 0.
        """
        ...

    def seed_company_profile_history(
        self, observed_month: date, run_id: str | None = None
    ) -> UpsertResult:
        """Copy ``dart_corp_master``'s current profiles in as the first month.

        The history has to start somewhere, and what exists is one current
        value per corporation.  Those rows are marked ``is_seed = true`` and
        carry the master's own ``profile_fetched_at`` as ``observed_at``, so a
        consumer can tell "observed in this month" from "was already true when
        we started looking".  Rows without a profile are skipped — there is
        nothing to record for them.

        Args:
            observed_month: First day of the month to seed as.
            run_id: The ``ingestion_runs`` row this seed came from.

        Returns:
            Counters for the rows inserted.
        """
        ...

    def get_last_successful_run(self, run_type: RunType) -> IngestionRun | None:
        """Return the most recent SUCCESS-status run for the given run_type, or None."""
        ...

    def get_existing_dart_financial_statement_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        fs_divs: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, fs_div) tuples already present in raw."""
        ...

    def get_existing_dart_share_count_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str]]:
        """Return (corp_code, bsns_year, reprt_code) tuples already present in share-count raw."""
        ...

    def get_existing_dart_shareholder_return_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, statement_type) tuples already present."""
        ...

    def get_existing_dart_xbrl_document_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        """Return (corp_code, bsns_year, reprt_code, rcept_no) tuples already parsed."""
        ...

    def upsert_dart_financial_statement_raw(
        self,
        records: list[DartFinancialStatementLine],
    ) -> UpsertResult:
        """Upsert OpenDART financial-statement raw rows."""
        ...

    def upsert_dart_share_count_raw(
        self,
        records: list[DartShareCountLine],
    ) -> UpsertResult:
        """Upsert OpenDART share-count raw rows."""
        ...

    def upsert_dart_shareholder_return_raw(
        self,
        records: list[DartShareholderReturnLine],
    ) -> UpsertResult:
        """Upsert OpenDART dividend / treasury-stock raw rows."""
        ...

    def upsert_dart_periodic_extras_raw(
        self,
        records: list[DartPeriodicExtraLine],
    ) -> UpsertResult:
        """Upsert DS002 periodic-report extras, routing by statement type.

        The two destination tables are an implementation detail of the split
        between people-and-pay and control-and-audit rows, so callers pass one
        list and the repository routes it.
        """
        ...

    def get_existing_dart_capital_change_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str]]:
        """Return (corp_code, bsns_year, reprt_code) tuples already present."""
        ...

    def upsert_dart_capital_change_raw(
        self,
        records: list[DartCapitalChangeLine],
    ) -> UpsertResult:
        """Upsert OpenDART capital-change (irdsSttus) raw rows."""
        ...

    def get_existing_dart_filing_receipt_years(
        self,
        years: list[int],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int]]:
        """Return (corp_code, year) pairs with at least one receipt already stored.

        ``year`` is the calendar year of ``rcept_dt``, used as a coarse
        completion proxy for one fetch window (see ``sync_dart_filings``).
        """
        ...

    def upsert_dart_filing_receipt_raw(
        self,
        records: list[DartFilingReceiptLine],
    ) -> UpsertResult:
        """Upsert OpenDART disclosure-receipt raw rows."""
        ...

    def upsert_dart_xbrl_documents(
        self,
        records: list[DartXbrlDocument],
    ) -> UpsertResult:
        """Upsert OpenDART XBRL document metadata rows."""
        ...

    def upsert_dart_xbrl_fact_raw(
        self,
        records: list[DartXbrlFactLine],
    ) -> UpsertResult:
        """Upsert parsed OpenDART XBRL fact rows."""
        ...

    def upsert_krx_security_flow_raw(
        self,
        records: list[SecurityFlowLine],
    ) -> UpsertResult:
        """Upsert KRX security-flow raw rows."""
        ...

    def count_krx_security_flow_daily_market_tickers(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_code: str,
        source: Source,
    ) -> dict[tuple[date, str], int]:
        """Return existing ticker counts for a daily market-level flow metric."""
        ...

    def count_krx_security_flow_ticker_metric_dates(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_codes: list[str],
        source: Source,
    ) -> dict[str, int]:
        """Return existing distinct (trade_date, metric_code) counts by ticker."""
        ...

    def get_krx_security_flow_metric_max_dates(
        self,
        metric_codes: list[str],
        sources: Sequence[Source],
    ) -> dict[str, date]:
        """Return latest stored trade_date by security-flow metric code.

        ``sources`` is a list rather than one source because the metric is the
        thing being tracked, not its provenance. Scoping the cursor to a single
        source means the day the collector moves from KRX to KIS the cursor
        reads empty and the incremental start point vanishes.
        """
        ...

    def get_krx_security_flow_ticker_metric_coverage(
        self,
        start: date,
        end: date,
        tickers: list[str],
        metric_codes: list[str],
        sources: Sequence[Source],
    ) -> dict[tuple[str, str], tuple[int, date]]:
        """Return ``(session_count, latest_date)`` per ``(ticker, metric_code)``.

        The per-ticker checkpoint for ticker-shaped collectors. A bulk
        collector fails a whole ``(date, market)`` slice, which the aggregate
        counters describe well enough; a per-ticker collector leaves holes in
        individual names, and only this tells you which.
        """
        ...

    def upsert_operating_source_documents(
        self,
        records: list[OperatingSourceDocument],
    ) -> UpsertResult:
        """Upsert operating KPI source documents."""
        ...

    def upsert_operating_metric_facts(
        self,
        records: list[OperatingMetricFact],
    ) -> UpsertResult:
        """Upsert extracted operating KPI facts."""
        ...

    def upsert_metric_catalog(self, records: list[MetricCatalogEntry]) -> UpsertResult:
        """Upsert canonical metric catalog entries."""
        ...

    def replace_metric_mapping_rules(self, records: list[MetricMappingRule]) -> UpsertResult:
        """Replace the active metric mapping rules with the provided set."""
        ...

    def get_metric_mapping_rules(self) -> list[MetricMappingRule]:
        """Return active metric mapping rules ordered by priority."""
        ...

    def get_dart_financial_statement_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartFinancialStatementLine]:
        """Return financial statement raw rows for normalization."""
        ...

    def iter_dart_financial_statement_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_account_ids: list[str] | None = None,
        page_size: int = 5000,
    ) -> Iterator[DartFinancialStatementLine]:
        """Stream skinny financial statement rows for normalization."""
        ...

    def get_dart_share_count_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareCountLine]:
        """Return share-count raw rows for normalization."""
        ...

    def iter_dart_share_count_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_se_values: list[str] | None = None,
        page_size: int = 5000,
    ) -> Iterator[DartShareCountLine]:
        """Stream skinny share-count rows for normalization."""
        ...

    def get_dart_shareholder_return_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartShareholderReturnLine]:
        """Return dividend / treasury-stock raw rows for normalization."""
        ...

    def iter_dart_shareholder_return_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        page_size: int = 5000,
    ) -> Iterator[DartShareholderReturnLine]:
        """Stream skinny shareholder-return rows for normalization."""
        ...

    def get_dart_xbrl_fact_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartXbrlFactLine]:
        """Return parsed OpenDART XBRL fact rows for normalization."""
        ...

    def iter_dart_xbrl_fact_for_normalize(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
        rule_concept_ids: list[str] | None = None,
        page_size: int = 5000,
    ) -> Iterator[DartXbrlFactLine]:
        """Stream skinny XBRL fact rows for normalization."""
        ...

    def upsert_stock_metric_facts(self, records: list[StockMetricFact]) -> UpsertResult:
        """Upsert normalized canonical metric facts."""
        ...

    def delete_stock_metric_facts_for_inactive_rules(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str],
    ) -> int:
        """Delete normalized facts in scope whose mapping rule is no longer active."""
        ...

    def get_metric_catalog_entries(self) -> list[MetricCatalogEntry]:
        """Return active metric catalog entries."""
        ...

    def get_stock_metric_facts(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[StockMetricFact]:
        """Return normalized canonical metric facts for coverage queries."""
        ...

    def get_operating_metric_facts(
        self,
        tickers: list[str] | None = None,
        sector_keys: list[str] | None = None,
    ) -> list[OperatingMetricFact]:
        """Return extracted operating KPI facts."""
        ...

    # -- Common market / macro features --------------------------------------

    def upsert_common_feature_series(
        self,
        records: list[CommonFeatureSeries],
    ) -> UpsertResult:
        """Upsert source-series catalog rows for common features."""
        ...

    def get_common_feature_series(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureSeries]:
        """Return common feature source-series catalog rows."""
        ...

    def upsert_common_feature_observations(
        self,
        records: list[CommonFeatureObservation],
    ) -> UpsertResult:
        """Upsert raw common feature observations."""
        ...

    def count_common_feature_observations(
        self,
        series_ids: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        source: Source | None = None,
    ) -> dict[str, int]:
        """Return raw observation counts grouped by ``series_id``."""
        ...

    def get_common_feature_observations(
        self,
        series_ids: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        source: Source | None = None,
        available_from_end: date | None = None,
    ) -> list[CommonFeatureObservation]:
        """Return raw common feature observations for sync/build services."""
        ...

    def get_common_feature_observation_max_dates(
        self,
        sources: list[Source] | None = None,
        series_ids: list[str] | None = None,
    ) -> dict[str, date]:
        """Return latest raw observation date grouped by ``series_id``."""
        ...

    def get_common_feature_observation_dates(
        self,
        *,
        source: Source,
        series_id: str,
        start: date,
        end: date,
    ) -> set[date]:
        """Return stored raw observation dates for one source series and range."""
        ...

    def upsert_common_feature_catalog(
        self,
        records: list[CommonFeatureCatalogEntry],
    ) -> UpsertResult:
        """Upsert model-facing common feature catalog rows and input links."""
        ...

    def get_common_feature_catalog(
        self,
        feature_codes: list[str] | None = None,
        active_only: bool = True,
    ) -> list[CommonFeatureCatalogEntry]:
        """Return model-facing common feature catalog rows."""
        ...

    def upsert_common_feature_daily_facts(
        self,
        records: list[CommonFeatureDailyFact],
    ) -> UpsertResult:
        """Upsert KRX-date-aligned common feature facts."""
        ...

    def get_common_feature_daily_facts(
        self,
        start: date,
        end: date,
        feature_codes: list[str] | None = None,
    ) -> list[CommonFeatureDailyFact]:
        """Return KRX-date-aligned common feature facts."""
        ...

    def count_common_feature_daily_facts(
        self,
        start: date,
        end: date,
        feature_codes: list[str] | None = None,
    ) -> dict[str, int]:
        """Return daily fact counts grouped by ``feature_code``."""
        ...

    def get_common_feature_daily_fact_max_dates(
        self,
        feature_codes: list[str] | None = None,
    ) -> dict[str, date]:
        """Return latest daily fact date grouped by ``feature_code``."""
        ...

    def get_table_bsns_year_range(self, table_name: str) -> tuple[int, int, int] | None:
        """Return ``(min_year, max_year, rows)`` for a known business-year table."""
        ...

    def get_running_ingestion_runs(self, limit: int = 20) -> list[IngestionRun]:
        """Return currently running ingestion runs ordered by start time."""
        ...

    def get_recent_ingestion_runs(self, run_type: RunType, limit: int = 20) -> list[IngestionRun]:
        """Return recent ingestion runs for one run type, newest first."""
        ...

    def get_collection_slice_states(
        self,
        source: Source,
        endpoint: str,
        slice_keys: list[str] | None = None,
    ) -> dict[str, CollectionSliceState]:
        """Return the per-slice completion ledger for one ``(source, endpoint)``.

        The durable form of what ``ingestion_runs.params`` cannot hold: a run's
        tombstone list is capped and only the newest runs are read back, so a
        backfill spanning days cannot reconstruct its own progress from it.

        Args:
            source: Upstream system.
            endpoint: Endpoint name within that source.
            slice_keys: Optional allowlist.  ``None`` returns every recorded
                slice for the pair.

        Returns:
            ``{slice_key: CollectionSliceState}``.  Slices never attempted are
            simply absent.
        """
        ...

    def upsert_collection_slice_states(self, states: list[CollectionSliceState]) -> UpsertResult:
        """Record slice outcomes, incrementing ``attempt_count`` per write.

        The count is incremented in SQL rather than read-modify-written, so two
        runs working the same endpoint cannot lose each other's attempts.

        Args:
            states: Slice states to record.

        Returns:
            Rows written.
        """
        ...

    def get_active_stocks(self, market: Market | None = None) -> list[Stock]:
        """Return the currently active stocks in the stock master.

        Used to compute diffs (new/delisted) during universe sync.

        Args:
            market: Optional market filter. If None, returns all markets.

        Returns:
            List of active stocks.
        """
        ...

    def get_stocks(
        self,
        market: Market | None = None,
        statuses: list[ListingStatus] | None = None,
        tickers: list[str] | None = None,
    ) -> list[Stock]:
        """Return stock-master rows without assuming they are still listed.

        ``get_active_stocks`` answers "who is listed now", which is the right
        question for universe sync and the wrong one for a historical backfill:
        a delisted ticker can never be targeted through it, so its price and
        filing history stay permanently absent
        (`poc/survivorship_gap.md`: 2.0-2.3% coverage of 1,330 delisted names).

        Args:
            market: Optional market filter.
            statuses: Listing statuses to include.  ``None`` means every status.
            tickers: Optional ticker allowlist, applied in SQL rather than by
                filtering an active-only result.

        Returns:
            Matching stock-master rows.
        """
        ...

    def get_stocks_seen_only_in_snapshots(
        self,
        sources: list[Source] | None = None,
    ) -> list[Stock]:
        """Return securities that appear in a snapshot but not in the stock master.

        ``stock_master`` only knows what the collector has watched since it
        started running, so every security delisted before that is absent —
        1,299 of the 3,959 corps that ever carried a ticker, measured
        2026-08-16. Absent from ``stock_master`` means unreachable: price
        collection resolves its targets from that table, and naming a missing
        ticker with ``--tickers`` returns nothing because the filter runs
        against the same table. ``UniverseScope.HISTORICAL`` cannot reach past
        what the master contains.

        The reconstructed month-end snapshots are the point-in-time record of
        what was listed, so they are what closes the gap.

        Args:
            sources: Snapshot sources to read.  ``None`` reads every source.

        Returns:
            One :class:`Stock` per ``(ticker, market)`` seen in a snapshot and
            missing from the master, carrying the newest snapshot's name and
            the first/last ``as_of_date`` it was observed on.  Status is left
            to the caller.
        """
        ...

    def upsert_stock_master_rows(self, stocks: list[Stock]) -> int:
        """Upsert stock-master rows alone, writing no snapshot.

        Returns:
            Number of rows affected.
        """
        ...

    def insert_stock_master_snapshot_only(
        self,
        snapshot: StockUniverseSnapshot,
    ) -> UpsertResult:
        """Persist a snapshot and its items **without touching stock_master**.

        ``upsert_stock_master`` does three things in one call, and the third is
        an upsert of ``stock_master``.  Calling it with a reconstructed
        historical snapshot would let a 2016 ticker list overwrite the current
        universe — statuses, names and ``last_seen_date`` alike.

        This method exists so a backfill can add rows to the snapshot tables and
        nothing else.  Who is listed *today* stays the job of ``universe sync``;
        deciding who was listed on a past date is the reader's job (the mart).

        Idempotent on ``(as_of_date, source)``: re-running a backfill over a
        date that already has a snapshot from the same source is a no-op.

        Args:
            snapshot: Snapshot to persist.

        Returns:
            Counters for the snapshot items written.
        """
        ...

    def get_universe_as_of(
        self,
        as_of: date,
        market: Market | None = None,
    ) -> tuple[date, set[str]] | None:
        """Return the universe that was listed on or before ``as_of``.

        The point-in-time counterpart to :meth:`get_active_stocks`.  Validating
        a past date against today's active set cannot see a gap, because the
        tickers that are missing are exactly the ones that have since delisted
        and therefore left the active set too — the same blind spot that leaves
        13.9% of the 2016 cross-section uncollected
        (`poc/survivorship_gap.md`).

        Args:
            as_of: Reference date.
            market: Optional market filter.

        Returns:
            ``(snapshot_as_of_date, tickers)`` for the newest snapshot at or
            before ``as_of``, or ``None`` when no snapshot covers that date.
            The snapshot's own date is returned so the caller can report how
            stale the comparison basis is.
        """
        ...

    def get_snapshot_record_counts(
        self,
        limit: int = 24,
    ) -> list[tuple[date, Source, int]]:
        """Return recent snapshot sizes, newest first.

        Feeds the universe-drift check: a sudden change in how many tickers a
        snapshot holds is a collection failure long before it is a market
        event.

        Args:
            limit: Maximum snapshots to return.

        Returns:
            ``(as_of_date, source, record_count)`` newest first.
        """
        ...

    def get_existing_snapshot_dates(self, source: Source) -> set[date]:
        """Return ``as_of_date`` values that already have a snapshot from *source*.

        The skip-if-present key for the universe backfill.  Scoped by source so
        backfilled snapshots and live ones are counted separately.

        Args:
            source: Snapshot source to filter on.

        Returns:
            Set of dates already captured.
        """
        ...

    # -- Daily OHLCV ----------------------------------------------------------

    def upsert_daily_bars(self, bars: list[DailyBar]) -> UpsertResult:
        """Upsert daily OHLCV bars.

        Uses ``INSERT … ON CONFLICT (trade_date, ticker, market) DO UPDATE``
        so that re-fetched data overwrites stale rows (source corrections
        propagate automatically).

        Args:
            bars: Daily bars to persist.

        Returns:
            Counters of inserted / updated / errored rows.
        """
        ...

    # -- Daily market cap -----------------------------------------------------

    def upsert_daily_market_cap(self, rows: list[DailyMarketCapRow]) -> UpsertResult:
        """Upsert one ``(trade_date, market)`` slice of market-cap rows.

        **One call must be one slice.**  The caller relies on this being a
        single transaction: if a slice is split across calls and the process
        dies in between, the slice is left permanently incomplete and any
        "rows exist, therefore done" skip rule will never fill the hole.

        Args:
            rows: All rows of a single slice.

        Returns:
            Counters of inserted / updated / errored rows.
        """
        ...

    def get_market_cap_slice_row_counts(
        self,
        start: date,
        end: date,
        market: Market | None = None,
    ) -> dict[tuple[date, Market], int]:
        """Return stored row counts per ``(trade_date, market)`` slice.

        Row counts, not a boolean — completeness is decided by comparing
        against what the provider returned, never by mere row existence.

        Args:
            start: First trade date (inclusive).
            end: Last trade date (inclusive).
            market: Optional market filter.

        Returns:
            Mapping of slice key to stored row count.  Absent slices are
            simply missing from the mapping.
        """
        ...

    # -- Ingestion runs -------------------------------------------------------

    def record_run(self, run: IngestionRun) -> None:
        """Insert or update an ingestion-run audit record.

        Called at the start (status=running) and end (status=success|failed)
        of each pipeline execution.
        """
        ...

    # -- Query helpers --------------------------------------------------------

    def get_daily_bars(self, target_date: date, market: Market | None = None) -> list[DailyBar]:
        """Return all daily bars for a given date.

        Used for validation.

        Args:
            target_date: Date to query.
            market: Optional market filter.

        Returns:
            List of daily bars.
        """
        ...

    def query_missing_days(
        self,
        ticker: str,
        start: date,
        end: date,
    ) -> list[date]:
        """Return trade dates in [start, end] that have no daily bar stored.

        This is an *optional* optimisation for incremental backfill.  A
        minimal implementation may return all dates in the range.

        Args:
            ticker: 6-digit KRX ticker code.
            start: Range start (inclusive).
            end: Range end (inclusive).

        Returns:
            Sorted list of missing dates.
        """
        ...

    def get_min_trade_date(self, ticker: str) -> date | None:
        """Return the earliest stored ``trade_date`` for *ticker*.

        Used by the backfill service to clamp the effective start date
        so that ranges before the ticker's known data start are not
        re-requested on every run.

        Args:
            ticker: 6-digit KRX ticker code.

        Returns:
            The minimum stored ``trade_date`` for the ticker, or ``None``
            if no rows exist yet.
        """
        ...

    def get_max_trade_date(self, ticker: str) -> date | None:
        """Return the latest stored ``trade_date`` for *ticker*.

        Used by the backfill service in incremental mode to fetch only
        days after the ticker's last known trade date.

        Args:
            ticker: 6-digit KRX ticker code.

        Returns:
            The maximum stored ``trade_date`` for the ticker, or ``None``
            if no rows exist yet.
        """
        ...

    def get_daily_price_date_range(
        self,
        tickers: list[str] | None = None,
    ) -> tuple[date, date] | None:
        """Return the stored daily OHLCV date range for the selected tickers.

        Args:
            tickers: Optional ticker allowlist. If omitted, all stored price
                rows are considered.

        Returns:
            ``(min_trade_date, max_trade_date)`` or ``None`` when no price
            rows exist for the selection.
        """
        ...

    def get_latest_daily_price_date(
        self,
        tickers: list[str] | None = None,
    ) -> date | None:
        """Return the latest stored daily OHLCV trade_date for selected tickers."""
        ...

    def get_latest_market_cap_date(self) -> date | None:
        """Return the latest stored ``daily_market_cap`` trade_date."""
        ...
