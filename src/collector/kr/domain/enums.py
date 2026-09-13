"""Domain enumerations for the KRX data pipeline.

These enums represent core business concepts and are used throughout the
domain models, ports, and adapters.
"""

from enum import StrEnum


class Market(StrEnum):
    """Korean stock exchange market segment."""

    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"


class Source(StrEnum):
    """Data source identifier.

    FDR and PYKRX are implemented for market data fallbacks. OPENDART, KRX,
    KIS and the macro/common-feature sources identify raw upstream systems.
    KIWOOM remains reserved for a future broker-API integration.
    """

    FDR = "FDR"
    PYKRX = "PYKRX"
    # Historical universe snapshots reconstructed after the fact.  Kept
    # distinct from PYKRX so that `sync_universe`'s snapshot diff (which infers
    # delistings from consecutive snapshots) never mixes a backfilled snapshot
    # in with the live series.
    PYKRX_BACKFILL = "PYKRX_BACKFILL"
    OPENDART = "OPENDART"
    KRX = "KRX"
    # 한국투자증권 오픈API.  Writes the same security-flow metric codes as KRX
    # from 2026-08 onward, because KRX restricted this host for a terms-of-
    # service violation.  Provenance stays distinct so the changeover is
    # auditable, which is why every flow cursor reads a *list* of sources.
    KIS = "KIS"
    # KRX Open API (data-dbg.krx.co.kr) — the official replacement for the
    # pykrx/MDC scraping path.  Distinct from KRX because the two carry
    # different price bases and different permission terms, and because a
    # backfill has to be able to tell which rows came from the scraper.
    KRX_OPENAPI = "KRX_OPENAPI"
    # Historical universe snapshots reconstructed from KRX Open API responses.
    # Same reason PYKRX_BACKFILL exists: the live `sync_universe` diff must
    # never read a reconstructed snapshot as a newly observed one.
    KRX_OPENAPI_BACKFILL = "KRX_OPENAPI_BACKFILL"
    # Naver's chart endpoint — where adjusted daily OHLCV has always come from.
    # Rows written before 2026-08 carry ``PYKRX`` instead, but the upstream is
    # the same: pykrx's ``adjusted=True`` path is a thin wrapper over this
    # endpoint.  The label changed when K-5 dropped the wrapper, because
    # importing pykrx logs in to KRX and that login was the last KRX contact in
    # a collector that never needed KRX data.
    NAVER = "NAVER"
    ECOS = "ECOS"
    FRED = "FRED"
    KOSIS = "KOSIS"
    CUSTOMS = "CUSTOMS"
    KITA = "KITA"
    NASDAQ_DATA_LINK = "NASDAQ_DATA_LINK"
    # Future sources (not implemented):
    # KIWOOM = "KIWOOM"


class UniverseScope(StrEnum):
    """Which universe a collection targets.

    Every collector used to resolve its own targets, and every one of them
    reached for the currently-listed set.  That is correct for a daily sync —
    a delisted company files nothing today — and wrong for any backfill that
    feeds a backtest, because the companies that failed are precisely the ones
    that leave the current set.  The result was 2.0-2.3% coverage of 1,330
    delisted names across every raw table, and 13.9% of the 2016 cross-section
    absent (``poc/survivorship_gap.md``).

    Naming the choice makes it visible at the call site.  ``CURRENT`` is a
    decision about time, not a neutral default.
    """

    CURRENT = "current"
    HISTORICAL = "historical"


class ListingStatus(StrEnum):
    """Stock listing status on KRX."""

    ACTIVE = "ACTIVE"
    DELISTED = "DELISTED"
    UNKNOWN = "UNKNOWN"


class RunType(StrEnum):
    """Pipeline run type recorded in ingestion_runs."""

    UNIVERSE_SYNC = "universe_sync"
    UNIVERSE_SNAPSHOT_BACKFILL = "universe_snapshot_backfill"
    DAILY_BACKFILL = "daily_backfill"
    MARKET_CAP_BACKFILL = "market_cap_backfill"
    VALIDATE = "validate"
    REMOTE_DB_SYNC = "remote_db_sync"
    DART_CORP_SYNC = "dart_corp_sync"
    DART_CORP_PROFILE_SYNC = "dart_corp_profile_sync"
    DART_CORP_PROFILE_HISTORY_SEED = "dart_corp_profile_history_seed"
    DART_FINANCIAL_SYNC = "dart_financial_sync"
    DART_SHARE_COUNT_SYNC = "dart_share_count_sync"
    DART_SHAREHOLDER_RETURN_SYNC = "dart_shareholder_return_sync"
    DART_SHARE_INFO_SYNC = "dart_share_info_sync"
    DART_CAPITAL_CHANGE_SYNC = "dart_capital_change_sync"
    DART_FILING_RECEIPT_SYNC = "dart_filing_receipt_sync"
    DART_PERIODIC_EXTRAS_SYNC = "dart_periodic_extras_sync"
    XBRL_RECEIPT_BACKFILL = "xbrl_receipt_backfill"
    METRIC_NORMALIZE = "metric_normalize"
    KRX_FLOW_SYNC = "krx_flow_sync"
    KIS_FLOW_SYNC = "kis_flow_sync"
    XBRL_PARSE = "xbrl_parse"
    OPERATING_METRIC_SYNC = "operating_metric_sync"
    COMMON_FEATURE_SYNC = "common_feature_sync"
    COMMON_FEATURE_BUILD = "common_feature_build"


class RunStatus(StrEnum):
    """Pipeline run execution status."""

    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class SliceStatus(StrEnum):
    """Completion state of one collection slice (``collection_slice_state``).

    Deliberately not :class:`RunStatus`: a run can be ``partial`` because some
    of its slices failed, but a *slice* never is — it either reconciled or it
    did not.  Sharing the enum would let ``partial`` reach a column where it
    has no meaning.

    ``RUNNING`` counts as not done, so a killed process leaves work to retry
    rather than a slice nothing will ever revisit.  ``NO_DATA`` expires on a
    TTL because "upstream has nothing" is a statement about when it was asked;
    ``SUCCESS`` does not.
    """

    RUNNING = "running"
    SUCCESS = "success"
    NO_DATA = "no_data"
    FAILED = "failed"


class PeriodicExtraStatement(StrEnum):
    """Which DS002 periodic-report disclosure a raw row came from (N6).

    Five endpoints, two tables. The split is by what the row describes — people
    and pay versus control and audit — rather than by endpoint, because the
    registration cost of a new raw table is six places (`01` §1) and the query
    patterns follow the split, not the API surface.

    ``EXECUTIVE`` is defined but not collected by default: its only candidate
    feature is a management-turnover rate, the same T3 axis ``MAJOR_CHANGE``
    covers far better, and it would cost 32,400 of the 83,700 calls (PoC §3③).
    """

    #: empSttus — headcount, tenure, per-head pay. → dart_employee_raw
    EMPLOYEE = "employee"
    #: exctvSttus — officers. → dart_employee_raw. Excluded from the backfill.
    EXECUTIVE = "executive"
    #: hyslrSttus — largest shareholder and related parties. → dart_governance_raw
    MAJOR_SHAREHOLDER = "major_shareholder"
    #: hyslrChgSttus — changes of largest shareholder, with ``change_on``.
    MAJOR_CHANGE = "major_change"
    #: accnutAdtorNmNdAdtOpinion — auditor and opinion. → dart_governance_raw
    AUDIT_OPINION = "audit_opinion"
