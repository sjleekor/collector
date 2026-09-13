-- =============================================================================
-- KRX Data Pipeline — PostgreSQL DDL
-- =============================================================================
-- Run this file to initialise the database schema:
--   psql -d krx_data -f sql/postgres_ddl.sql
--
-- Design decisions:
--   • OHLCV values stored as BIGINT (Korean won, no decimals needed).
--   • daily_ohlcv upsert uses ON CONFLICT ... DO UPDATE so re-fetches overwrite
--     stale rows — preferred over DO NOTHING because source corrections should
--     propagate automatically.
--   • stock_master upsert by (ticker, market) keeps the latest state.
--   • All timestamps are TIMESTAMPTZ (UTC-stored, Asia/Seoul in application).
-- =============================================================================

-- 1) stock_master ─ latest state of each listed stock
CREATE TABLE IF NOT EXISTS stock_master (
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,   -- KOSPI | KOSDAQ
    name            TEXT        NOT NULL,
    status          TEXT        NOT NULL,   -- ACTIVE | DELISTED | UNKNOWN
    last_seen_date  DATE        NOT NULL,
    source          TEXT        NOT NULL,   -- FDR | PYKRX
    listing_date    DATE,                   -- source-reported listing date (FDR only)
    first_seen_date DATE,                   -- first as_of_date observed ACTIVE (storage-managed)
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, market)
);

-- 2) stock_master_snapshot ─ point-in-time universe fetch metadata
CREATE TABLE IF NOT EXISTS stock_master_snapshot (
    snapshot_id     UUID        PRIMARY KEY,
    as_of_date      DATE        NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    record_count    INT         NOT NULL
);

-- 3) stock_master_snapshot_items ─ individual stocks captured in a snapshot
--    Recommended for full auditability: you can diff any two snapshots to find
--    new listings, delistings, or name changes.
CREATE TABLE IF NOT EXISTS stock_master_snapshot_items (
    snapshot_id     UUID        NOT NULL REFERENCES stock_master_snapshot(snapshot_id),
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    listing_date    DATE,       -- source-reported listing date (FDR only); NULL for old snapshots
    UNIQUE (snapshot_id, ticker, market)
);

-- New-ticker start-date support (2026-07): listing / first-seen dates.
-- ADD COLUMN IF NOT EXISTS keeps a rerun of this file idempotent for
-- databases created before these columns existed.
ALTER TABLE stock_master
    ADD COLUMN IF NOT EXISTS listing_date    DATE,
    ADD COLUMN IF NOT EXISTS first_seen_date DATE;

ALTER TABLE stock_master_snapshot_items
    ADD COLUMN IF NOT EXISTS listing_date DATE;

-- One-time, idempotent backfill of first_seen_date for pre-existing rows.
-- Prefers the earliest snapshot the ticker appears in; falls back to
-- last_seen_date. Only touches NULLs, so re-running db init is a no-op.
UPDATE stock_master sm
SET first_seen_date = COALESCE(
        (SELECT MIN(s.as_of_date)
           FROM stock_master_snapshot_items i
           JOIN stock_master_snapshot s ON s.snapshot_id = i.snapshot_id
          WHERE i.ticker = sm.ticker
            AND i.market = sm.market),
        sm.last_seen_date)
WHERE sm.first_seen_date IS NULL;

-- 4) daily_ohlcv ─ daily price bars
CREATE TABLE IF NOT EXISTS daily_ohlcv (
    trade_date      DATE        NOT NULL,
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,
    open            BIGINT      NOT NULL,
    high            BIGINT      NOT NULL,
    low             BIGINT      NOT NULL,
    close           BIGINT      NOT NULL,
    volume          BIGINT      NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trade_date, ticker, market)
);

-- Covering index for per-ticker queries ordered by date descending
CREATE INDEX IF NOT EXISTS ix_daily_ohlcv_ticker_date
    ON daily_ohlcv (ticker, market, trade_date DESC);

-- Sync cursor indexes for remote-to-local replication
CREATE INDEX IF NOT EXISTS ix_stock_master_sync_cursor
    ON stock_master (updated_at, ticker, market);

CREATE INDEX IF NOT EXISTS ix_stock_master_snapshot_sync_cursor
    ON stock_master_snapshot (fetched_at, snapshot_id);

CREATE INDEX IF NOT EXISTS ix_daily_ohlcv_sync_cursor
    ON daily_ohlcv (fetched_at, trade_date, ticker, market);

-- 4b) daily_market_cap ─ daily KRX market cap / trading value / listed shares
--
-- Separate from daily_ohlcv on purpose: different pykrx endpoint and a
-- different price basis.  daily_ohlcv comes from the naver ADJUSTED path
-- (pykrx get_market_ohlcv_by_date defaults to adjusted=True, which routes to
-- naver); this table comes from KRX get_market_cap_by_ticker and carries the
-- UNADJUSTED session close.  The column is named source_close, not close, so
-- the difference is visible at the call site.
--
-- market is filled from the CALL ARGUMENT (one request per market), never by
-- joining stock_master — that join would stamp a stock's present-day market
-- onto its pre-transfer rows (look-ahead).
CREATE TABLE IF NOT EXISTS daily_market_cap (
    trade_date      DATE        NOT NULL,
    ticker          TEXT        NOT NULL,
    market          TEXT        NOT NULL,
    source_close    BIGINT,                 -- KRX unadjusted session close
    market_cap      BIGINT,                 -- KRW
    trading_value   BIGINT,                 -- KRW
    listed_shares   BIGINT,
    volume          BIGINT,                 -- KRX basis; cross-check vs daily_ohlcv (naver)
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trade_date, ticker, market)
);

-- 4b-1) unadjusted OHLC, added 2026-08-18 with the KRX Open API adapter (K-4).
--
-- ALTER rather than an edit to the CREATE above, because CREATE TABLE IF NOT
-- EXISTS is a no-op on a database that already has the table — editing it
-- would leave every existing deployment without the columns.
--
-- These arrive in the SAME response as market cap, so the unadjusted OHLC
-- costs no extra request.  That is what makes a point-in-time adjustment
-- factor computable here (K-7) instead of inherited from naver, which
-- rewrites history on every split.  The pykrx path has no open/high/low in
-- its response and leaves all three NULL.
ALTER TABLE daily_market_cap
    ADD COLUMN IF NOT EXISTS source_open BIGINT,
    ADD COLUMN IF NOT EXISTS source_high BIGINT,
    ADD COLUMN IF NOT EXISTS source_low  BIGINT;

CREATE INDEX IF NOT EXISTS ix_daily_market_cap_ticker_date
    ON daily_market_cap (ticker, market, trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_daily_market_cap_sync_cursor
    ON daily_market_cap (fetched_at, trade_date, ticker, market);

-- 5) ingestion_runs ─ audit log for every pipeline execution
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id          UUID        PRIMARY KEY,
    run_type        TEXT        NOT NULL,   -- universe_sync | daily_backfill | validate
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    status          TEXT        NOT NULL,   -- running | success | failed
    params          JSONB,
    counts          JSONB,
    error_summary   TEXT
);

-- 6) sync_checkpoints ─ resume cursors for long-running sync jobs
CREATE TABLE IF NOT EXISTS sync_checkpoints (
    sync_name       TEXT        PRIMARY KEY,
    cursor_payload  JSONB       NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 6b) collection_slice_state ─ per-slice completion ledger (L-1)
--
-- sync_checkpoints is the same idea one row per sync; this is one row per
-- SLICE, for backfills large enough to span days and many runs.
--
-- Why a table and not ingestion_runs.params, where the no-data tombstone
-- currently lives: a run's params list is capped and only the most recent runs
-- are read back, so a backfill spanning several days cannot reconstruct its
-- own completion state from it.  N6 is ~84,000 calls over about three days,
-- which is exactly the case that breaks.
--
-- expected_rows/actual_rows exist because "the slice has at least one row" is
-- not completion.  A response half-written before a crash leaves a slice that
-- every later run skips and nothing ever repairs.  A slice counts as done only
-- when the two agree.
--
-- status:
--   running  ─ claimed by a run that has not reported back.  Treated as NOT
--              done, so a killed process leaves work to retry rather than a
--              permanent block.
--   success  ─ stored and reconciled.  Never expires.
--   no_data  ─ upstream really has nothing here.  DOES expire: a suspended
--              ticker resumes, and a company that filed nothing in 2019 may
--              file a correction later.
--   failed   ─ attempted and errored.  Retried, with attempt_count showing
--              which slices keep failing.
CREATE TABLE IF NOT EXISTS collection_slice_state (
    source        TEXT        NOT NULL,   -- PYKRX | OPENDART | KRX_OPENAPI | ...
    endpoint      TEXT        NOT NULL,   -- market_cap | empSttus | ...
    slice_key     TEXT        NOT NULL,   -- '2024-01-02|KOSPI' | '00126380|2020|11011'
    status        TEXT        NOT NULL,   -- running | success | no_data | failed
    expected_rows INT,
    actual_rows   INT,
    attempt_count INT         NOT NULL DEFAULT 0,
    last_error    TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, endpoint, slice_key)
);

-- Planning reads one (source, endpoint) at a time and filters by status, and
-- the no_data expiry needs updated_at in the same scan.
CREATE INDEX IF NOT EXISTS ix_collection_slice_state_status
    ON collection_slice_state (source, endpoint, status, updated_at);

-- =============================================================================
-- Phase 0 scaffold for account / flow ingestion
-- =============================================================================

-- 7) dart_corp_master ─ OpenDART corp_code to KRX ticker mapping
CREATE TABLE IF NOT EXISTS dart_corp_master (
    corp_code       TEXT        PRIMARY KEY,
    ticker          TEXT,
    corp_name       TEXT        NOT NULL,
    market          TEXT,
    stock_name      TEXT,
    modify_date     DATE,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    source          TEXT        NOT NULL DEFAULT 'OPENDART',
    fetched_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dart_corp_master_ticker
    ON dart_corp_master (ticker);

CREATE INDEX IF NOT EXISTS ix_dart_corp_master_sync_cursor
    ON dart_corp_master (updated_at, corp_code);

-- 7b) dart_corp_master profile columns (N2) — from company.json (DS001).
--
-- corpCode.xml, which fills the columns above, carries only
-- corp_code/corp_name/stock_code/modify_date.  Industry, incorporation date and
-- fiscal-year-end come from a separate per-corp_code endpoint, so they are
-- added here rather than to a new table (same key, same nature) with their own
-- fetch timestamp.
--
-- induty_code is KSIC and its LENGTH VARIES: 2, 3, 4 and 5 digits all occur
-- (measured 3/52/21/74 over a 150-corp sample).  Any grouping rule must take a
-- 2-digit prefix, which yields the KSIC middle category regardless of the
-- source length; a rule that assumes a fixed width is wrong.
--
-- profile_fetched_at is the skip-if-present key: NULL means never fetched.
ALTER TABLE dart_corp_master
    ADD COLUMN IF NOT EXISTS induty_code        TEXT,
    ADD COLUMN IF NOT EXISTS corp_cls           TEXT,
    ADD COLUMN IF NOT EXISTS est_dt             DATE,
    ADD COLUMN IF NOT EXISTS acc_mt             TEXT,
    ADD COLUMN IF NOT EXISTS profile_raw        JSONB,
    ADD COLUMN IF NOT EXISTS profile_fetched_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_dart_corp_master_induty
    ON dart_corp_master (induty_code);

-- 7c) dart_corp_profile_history ─ monthly company.json snapshots (F-1, L1)
--
-- dart_corp_master above is an UPSERT: a re-fetch overwrites induty_code and
-- the previous value is gone.  So the industry code is a *current* value with
-- no history, which is why every industry-neutral variant in the marts is a
-- diagnostic rather than a feature — today's KSIC applied to 2015 is a
-- look-ahead.  KRX index membership (N4) turned out not to be published and the
-- KRX-to-KSIC crosswalk resolved only 36% of tickers 1:1, so versioning this
-- endpoint from now on is the only remaining way to get a point-in-time
-- industry.  See docs/dev/20260907_additional_feature/01_industry_pit.md §2.
--
-- One row per corporation per calendar month, and observed_month is the
-- skip-if-present key: a second run in the same month is a no-op, so the
-- monthly Cronicle event is safe to retry and a manual run costs nothing.
-- The history only ever grows forwards; nothing here reconstructs the past.
--
-- is_seed marks the 2026-09 rows copied from dart_corp_master's current state
-- rather than fetched as a monthly snapshot.  They carry the corp master's own
-- profile_fetched_at as observed_at, which for most corporations is the 2026-08
-- N2 sweep — so a change first *observed* between the seed and the next
-- snapshot may have happened at any point before it.  Consumers that need a
-- real observation window must exclude the seed row.
--
-- run_id is advisory and deliberately NOT a foreign key: ingestion_runs is a
-- local audit table and is not mirrored by db sync-remote, so an FK would make
-- every mirrored row unloadable.
CREATE TABLE IF NOT EXISTS dart_corp_profile_history (
    corp_code       TEXT        NOT NULL,
    observed_month  DATE        NOT NULL,   -- first day of the observation month
    observed_at     TIMESTAMPTZ NOT NULL,   -- when the response was actually read
    ticker          TEXT,
    corp_cls        TEXT,                   -- Y/K/N/E; E is how a delisting shows up
    induty_code     TEXT,                   -- KSIC, length varies (2-5 digits)
    est_dt          DATE,
    acc_mt          TEXT,
    corp_name       TEXT,
    stock_name      TEXT,
    is_seed         BOOLEAN     NOT NULL DEFAULT FALSE,
    profile_raw     JSONB,
    run_id          UUID,
    source          TEXT        NOT NULL DEFAULT 'OPENDART',
    fetched_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (corp_code, observed_month)
);

CREATE INDEX IF NOT EXISTS ix_dart_corp_profile_history_month
    ON dart_corp_profile_history (observed_month, induty_code);

CREATE INDEX IF NOT EXISTS ix_dart_corp_profile_history_ticker
    ON dart_corp_profile_history (ticker, observed_month);

CREATE INDEX IF NOT EXISTS ix_dart_corp_profile_history_sync_cursor
    ON dart_corp_profile_history (fetched_at, corp_code, observed_month);

-- 8) dart_financial_statement_raw ─ raw rows from fnlttSinglAcntAll / XBRL facts
CREATE TABLE IF NOT EXISTS dart_financial_statement_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    corp_code            TEXT        NOT NULL,
    ticker               TEXT,
    bsns_year            INT         NOT NULL,
    reprt_code           TEXT        NOT NULL,
    fs_div               TEXT        NOT NULL,
    sj_div               TEXT        NOT NULL,
    sj_nm                TEXT        NOT NULL DEFAULT '',
    account_id           TEXT        NOT NULL,
    account_nm           TEXT        NOT NULL,
    account_detail       TEXT        NOT NULL DEFAULT '',
    thstrm_nm            TEXT        NOT NULL DEFAULT '',
    thstrm_add_amount    NUMERIC(30, 4),
    frmtrm_nm            TEXT        NOT NULL DEFAULT '',
    frmtrm_q_nm          TEXT        NOT NULL DEFAULT '',
    frmtrm_q_amount      NUMERIC(30, 4),
    frmtrm_add_amount    NUMERIC(30, 4),
    bfefrmtrm_nm         TEXT        NOT NULL DEFAULT '',
    ord                  BIGINT      NOT NULL DEFAULT 0,
    thstrm_amount        NUMERIC(30, 4),
    frmtrm_amount        NUMERIC(30, 4),
    bfefrmtrm_amount     NUMERIC(30, 4),
    currency             TEXT,
    rcept_no             TEXT        NOT NULL DEFAULT '',
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    CONSTRAINT uq_dart_financial_statement_raw
        UNIQUE (corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord, rcept_no)
);

CREATE INDEX IF NOT EXISTS ix_dart_financial_statement_raw_lookup
    ON dart_financial_statement_raw (ticker, bsns_year, reprt_code, fs_div, sj_div);

CREATE INDEX IF NOT EXISTS ix_dart_financial_statement_raw_sync_cursor
    ON dart_financial_statement_raw (fetched_at, raw_id);

ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS sj_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS account_detail TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS thstrm_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS thstrm_add_amount NUMERIC(30, 4);
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS frmtrm_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS frmtrm_q_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS frmtrm_q_amount NUMERIC(30, 4);
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS frmtrm_add_amount NUMERIC(30, 4);
ALTER TABLE dart_financial_statement_raw
    ADD COLUMN IF NOT EXISTS bfefrmtrm_nm TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_financial_statement_raw
    ALTER COLUMN ord SET DEFAULT 0;
UPDATE dart_financial_statement_raw
SET ord = 0
WHERE ord IS NULL;
ALTER TABLE dart_financial_statement_raw
    ALTER COLUMN ord SET NOT NULL;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'dart_financial_statement_raw'
          AND c.contype = 'u'
          AND c.conname <> 'uq_dart_financial_statement_raw'
    LOOP
        EXECUTE format(
            'ALTER TABLE dart_financial_statement_raw DROP CONSTRAINT IF EXISTS %I',
            constraint_name
        );
    END LOOP;
END $$;

ALTER TABLE dart_financial_statement_raw
    DROP CONSTRAINT IF EXISTS uq_dart_financial_statement_raw;
ALTER TABLE dart_financial_statement_raw
    ADD CONSTRAINT uq_dart_financial_statement_raw
    UNIQUE (corp_code, bsns_year, reprt_code, fs_div, sj_div, account_id, ord, rcept_no);

-- 9) dart_share_count_raw ─ stock count disclosures from stockTotqySttus
CREATE TABLE IF NOT EXISTS dart_share_count_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    corp_code            TEXT        NOT NULL,
    ticker               TEXT,
    bsns_year            INT         NOT NULL,
    reprt_code           TEXT        NOT NULL,
    rcept_no             TEXT        NOT NULL DEFAULT '',
    corp_cls             TEXT        NOT NULL DEFAULT '',
    se                   TEXT        NOT NULL DEFAULT '',
    isu_stock_totqy      BIGINT,
    now_to_isu_stock_totqy BIGINT,
    now_to_dcrs_stock_totqy BIGINT,
    redc                 TEXT        NOT NULL DEFAULT '',
    profit_incnr         TEXT        NOT NULL DEFAULT '',
    rdmstk_repy          TEXT        NOT NULL DEFAULT '',
    etc                  TEXT        NOT NULL DEFAULT '',
    istc_totqy           BIGINT,
    tesstk_co            BIGINT,
    distb_stock_co       BIGINT,
    stlm_dt              DATE,
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    CONSTRAINT uq_dart_share_count_raw
        UNIQUE (corp_code, bsns_year, reprt_code, se, rcept_no)
);

CREATE INDEX IF NOT EXISTS ix_dart_share_count_raw_lookup
    ON dart_share_count_raw (ticker, bsns_year, reprt_code);

CREATE INDEX IF NOT EXISTS ix_dart_share_count_raw_sync_cursor
    ON dart_share_count_raw (fetched_at, raw_id);

ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS corp_cls TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS se TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS isu_stock_totqy BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS now_to_isu_stock_totqy BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS now_to_dcrs_stock_totqy BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS redc TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS profit_incnr TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS rdmstk_repy TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS etc TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS istc_totqy BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS tesstk_co BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS distb_stock_co BIGINT;
ALTER TABLE dart_share_count_raw
    ADD COLUMN IF NOT EXISTS stlm_dt DATE;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'dart_share_count_raw'
          AND c.contype = 'u'
          AND c.conname <> 'uq_dart_share_count_raw'
    LOOP
        EXECUTE format(
            'ALTER TABLE dart_share_count_raw DROP CONSTRAINT IF EXISTS %I',
            constraint_name
        );
    END LOOP;
END $$;

ALTER TABLE dart_share_count_raw
    DROP CONSTRAINT IF EXISTS uq_dart_share_count_raw;
ALTER TABLE dart_share_count_raw
    ADD CONSTRAINT uq_dart_share_count_raw
    UNIQUE (corp_code, bsns_year, reprt_code, se, rcept_no);

-- 10) dart_shareholder_return_raw ─ dividend / treasury stock disclosures
CREATE TABLE IF NOT EXISTS dart_shareholder_return_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    corp_code            TEXT        NOT NULL,
    ticker               TEXT,
    bsns_year            INT         NOT NULL,
    reprt_code           TEXT        NOT NULL DEFAULT '',
    statement_type       TEXT        NOT NULL,
    row_name             TEXT        NOT NULL DEFAULT '',
    stock_knd            TEXT        NOT NULL DEFAULT '',
    dim1                 TEXT        NOT NULL DEFAULT '',
    dim2                 TEXT        NOT NULL DEFAULT '',
    dim3                 TEXT        NOT NULL DEFAULT '',
    metric_code          TEXT        NOT NULL,
    metric_name          TEXT        NOT NULL,
    value_numeric        NUMERIC(30, 4),
    value_text           TEXT        NOT NULL DEFAULT '',
    unit                 TEXT,
    rcept_no             TEXT        NOT NULL DEFAULT '',
    stlm_dt              DATE,
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    CONSTRAINT uq_dart_shareholder_return_raw
        UNIQUE (
            corp_code,
            bsns_year,
            reprt_code,
            statement_type,
            row_name,
            stock_knd,
            dim1,
            dim2,
            dim3,
            metric_code,
            rcept_no
        )
);

CREATE INDEX IF NOT EXISTS ix_dart_shareholder_return_raw_lookup
    ON dart_shareholder_return_raw (ticker, bsns_year, reprt_code, statement_type);

CREATE INDEX IF NOT EXISTS ix_dart_shareholder_return_raw_sync_cursor
    ON dart_shareholder_return_raw (fetched_at, raw_id);

ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS row_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS stock_knd TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS dim1 TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS dim2 TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS dim3 TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS value_numeric NUMERIC(30, 4);
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS value_text TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_shareholder_return_raw
    ADD COLUMN IF NOT EXISTS stlm_dt DATE;
ALTER TABLE dart_shareholder_return_raw
    ALTER COLUMN bsns_year SET NOT NULL;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'dart_shareholder_return_raw'
          AND c.contype = 'u'
          AND c.conname <> 'uq_dart_shareholder_return_raw'
    LOOP
        EXECUTE format(
            'ALTER TABLE dart_shareholder_return_raw DROP CONSTRAINT IF EXISTS %I',
            constraint_name
        );
    END LOOP;
END $$;

ALTER TABLE dart_shareholder_return_raw
    DROP CONSTRAINT IF EXISTS uq_dart_shareholder_return_raw;
ALTER TABLE dart_shareholder_return_raw
    ADD CONSTRAINT uq_dart_shareholder_return_raw
    UNIQUE (
        corp_code,
        bsns_year,
        reprt_code,
        statement_type,
        row_name,
        stock_knd,
        dim1,
        dim2,
        dim3,
        metric_code,
        rcept_no
    );

-- 11) dart_xbrl_document ─ parsed XBRL ZIP document metadata
CREATE TABLE IF NOT EXISTS dart_xbrl_document (
    document_id            BIGSERIAL   PRIMARY KEY,
    corp_code              TEXT        NOT NULL,
    ticker                 TEXT,
    bsns_year              INT         NOT NULL,
    reprt_code             TEXT        NOT NULL,
    rcept_no               TEXT        NOT NULL,
    zip_entry_count        INT         NOT NULL DEFAULT 0,
    instance_document_name TEXT        NOT NULL DEFAULT '',
    label_ko_document_name TEXT        NOT NULL DEFAULT '',
    source                 TEXT        NOT NULL,
    fetched_at             TIMESTAMPTZ NOT NULL,
    raw_payload            JSONB       NOT NULL,
    CONSTRAINT uq_dart_xbrl_document
        UNIQUE (corp_code, bsns_year, reprt_code, rcept_no)
);

CREATE INDEX IF NOT EXISTS ix_dart_xbrl_document_lookup
    ON dart_xbrl_document (ticker, bsns_year, reprt_code);

CREATE INDEX IF NOT EXISTS ix_dart_xbrl_document_sync_cursor
    ON dart_xbrl_document (fetched_at, document_id);

ALTER TABLE dart_xbrl_document
    ADD COLUMN IF NOT EXISTS zip_entry_count INT NOT NULL DEFAULT 0;
ALTER TABLE dart_xbrl_document
    ADD COLUMN IF NOT EXISTS instance_document_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_document
    ADD COLUMN IF NOT EXISTS label_ko_document_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_document
    ADD COLUMN IF NOT EXISTS raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

-- 12) dart_xbrl_fact_raw ─ parsed XBRL facts
CREATE TABLE IF NOT EXISTS dart_xbrl_fact_raw (
    raw_id                 BIGSERIAL   PRIMARY KEY,
    corp_code              TEXT        NOT NULL,
    ticker                 TEXT,
    bsns_year              INT         NOT NULL,
    reprt_code             TEXT        NOT NULL,
    rcept_no               TEXT        NOT NULL,
    concept_id             TEXT        NOT NULL,
    concept_name           TEXT        NOT NULL DEFAULT '',
    namespace_uri          TEXT        NOT NULL DEFAULT '',
    context_id             TEXT        NOT NULL DEFAULT '',
    context_type           TEXT        NOT NULL DEFAULT '',
    period_start           DATE,
    period_end             DATE,
    instant_date           DATE,
    dimensions             JSONB       NOT NULL DEFAULT '[]'::jsonb,
    unit_id                TEXT        NOT NULL DEFAULT '',
    unit_measure           TEXT        NOT NULL DEFAULT '',
    decimals               TEXT        NOT NULL DEFAULT '',
    value_numeric          NUMERIC(30, 4),
    value_text             TEXT        NOT NULL DEFAULT '',
    is_nil                 BOOLEAN     NOT NULL DEFAULT FALSE,
    label_ko               TEXT        NOT NULL DEFAULT '',
    source                 TEXT        NOT NULL,
    fetched_at             TIMESTAMPTZ NOT NULL,
    raw_payload            JSONB       NOT NULL,
    CONSTRAINT uq_dart_xbrl_fact_raw
        UNIQUE (corp_code, bsns_year, reprt_code, rcept_no, context_id, concept_id)
);

CREATE INDEX IF NOT EXISTS ix_dart_xbrl_fact_raw_lookup
    ON dart_xbrl_fact_raw (ticker, bsns_year, reprt_code, concept_id);

CREATE INDEX IF NOT EXISTS ix_dart_xbrl_fact_raw_sync_cursor
    ON dart_xbrl_fact_raw (fetched_at, raw_id);

ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS concept_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS namespace_uri TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS context_id TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS context_type TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS period_start DATE;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS period_end DATE;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS instant_date DATE;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS dimensions JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS unit_id TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS unit_measure TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS decimals TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS value_numeric NUMERIC(30, 4);
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS value_text TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS is_nil BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS label_ko TEXT NOT NULL DEFAULT '';
ALTER TABLE dart_xbrl_fact_raw
    ADD COLUMN IF NOT EXISTS raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

-- 13) dart_filing_receipt_raw ─ disclosure receipt history from list.json (공시검색)
CREATE TABLE IF NOT EXISTS dart_filing_receipt_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    corp_code            TEXT        NOT NULL,
    ticker               TEXT,
    corp_name            TEXT        NOT NULL DEFAULT '',
    stock_code           TEXT        NOT NULL DEFAULT '',
    corp_cls             TEXT        NOT NULL DEFAULT '',
    report_nm            TEXT        NOT NULL DEFAULT '',
    rcept_no             TEXT        NOT NULL,
    flr_nm               TEXT        NOT NULL DEFAULT '',
    rcept_dt             DATE,
    rm                   TEXT        NOT NULL DEFAULT '',
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    CONSTRAINT uq_dart_filing_receipt_raw
        UNIQUE (corp_code, rcept_no)
);

CREATE INDEX IF NOT EXISTS ix_dart_filing_receipt_raw_lookup
    ON dart_filing_receipt_raw (ticker, rcept_dt);

CREATE INDEX IF NOT EXISTS ix_dart_filing_receipt_raw_sync_cursor
    ON dart_filing_receipt_raw (fetched_at, raw_id);

-- 14) dart_capital_change_raw ─ issuance/decrease disclosures from irdsSttus (증자(감자)현황)
CREATE TABLE IF NOT EXISTS dart_capital_change_raw (
    raw_id                        BIGSERIAL   PRIMARY KEY,
    corp_code                     TEXT        NOT NULL,
    ticker                        TEXT,
    bsns_year                     INT         NOT NULL,
    reprt_code                    TEXT        NOT NULL,
    rcept_no                      TEXT        NOT NULL DEFAULT '',
    corp_cls                      TEXT        NOT NULL DEFAULT '',
    isu_dcrs_de                   DATE,
    isu_dcrs_stle                 TEXT        NOT NULL DEFAULT '',
    isu_dcrs_stock_knd            TEXT        NOT NULL DEFAULT '',
    isu_dcrs_qy                   BIGINT,
    isu_dcrs_mstvdv_fval_amount   NUMERIC(30, 4),
    isu_dcrs_mstvdv_fval_amount2  NUMERIC(30, 4),
    stlm_dt                       DATE,
    source                        TEXT        NOT NULL,
    fetched_at                    TIMESTAMPTZ NOT NULL,
    raw_payload                   JSONB       NOT NULL,
    CONSTRAINT uq_dart_capital_change_raw
        UNIQUE (
            corp_code, bsns_year, reprt_code, rcept_no,
            isu_dcrs_de, isu_dcrs_stle, isu_dcrs_stock_knd
        )
);

CREATE INDEX IF NOT EXISTS ix_dart_capital_change_raw_lookup
    ON dart_capital_change_raw (ticker, bsns_year, reprt_code);

CREATE INDEX IF NOT EXISTS ix_dart_capital_change_raw_sync_cursor
    ON dart_capital_change_raw (fetched_at, raw_id);

-- 16) krx_security_flow_raw ─ daily investor/short-selling/borrow flow metrics
CREATE TABLE IF NOT EXISTS krx_security_flow_raw (
    raw_id               BIGSERIAL   PRIMARY KEY,
    trade_date           DATE        NOT NULL,
    ticker               TEXT        NOT NULL,
    market               TEXT        NOT NULL,
    metric_code          TEXT        NOT NULL,
    metric_name          TEXT        NOT NULL,
    value                NUMERIC(30, 4),
    unit                 TEXT,
    source               TEXT        NOT NULL,
    fetched_at           TIMESTAMPTZ NOT NULL,
    raw_payload          JSONB       NOT NULL,
    UNIQUE (trade_date, ticker, market, metric_code, source)
);

CREATE INDEX IF NOT EXISTS ix_krx_security_flow_raw_lookup
    ON krx_security_flow_raw (ticker, market, trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_krx_security_flow_raw_sync_cursor
    ON krx_security_flow_raw (fetched_at, raw_id);

-- 19) common_feature_series ─ source series catalog and collection policy
CREATE TABLE IF NOT EXISTS common_feature_series (
    series_id               TEXT        PRIMARY KEY,
    source                  TEXT        NOT NULL,
    source_series_key       TEXT        NOT NULL,
    category                TEXT        NOT NULL,
    frequency               TEXT        NOT NULL,
    name_kr                 TEXT        NOT NULL,
    name_en                 TEXT        NOT NULL DEFAULT '',
    unit                    TEXT        NOT NULL DEFAULT '',
    country                 TEXT        NOT NULL DEFAULT '',
    market                  TEXT        NOT NULL DEFAULT '',
    endpoint_params         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    availability_policy     TEXT        NOT NULL DEFAULT 'release_date',
    manual_lag_days         INT         NOT NULL DEFAULT 0,
    source_timezone         TEXT        NOT NULL DEFAULT 'Asia/Seoul',
    history_start_date      DATE,
    max_stale_business_days INT         NOT NULL DEFAULT 5,
    default_transform       TEXT        NOT NULL DEFAULT '',
    active                  BOOLEAN     NOT NULL DEFAULT TRUE,
    notes                   TEXT        NOT NULL DEFAULT '',
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_common_feature_series_source_active
    ON common_feature_series (source, active, series_id);

CREATE INDEX IF NOT EXISTS ix_common_feature_series_sync_cursor
    ON common_feature_series (updated_at, series_id);

-- 20) common_feature_observation_raw ─ raw source observations with PIT availability
CREATE TABLE IF NOT EXISTS common_feature_observation_raw (
    raw_id                BIGSERIAL   PRIMARY KEY,
    source                TEXT        NOT NULL,
    series_id             TEXT        NOT NULL REFERENCES common_feature_series(series_id),
    observation_date      DATE        NOT NULL,
    period_end_date       DATE,
    release_date          DATE,
    available_from_date   DATE        NOT NULL,
    vintage               TEXT        NOT NULL DEFAULT '',
    value_numeric         NUMERIC(30, 8),
    value_text            TEXT        NOT NULL DEFAULT '',
    unit                  TEXT        NOT NULL DEFAULT '',
    frequency             TEXT        NOT NULL,
    source_updated_at     TIMESTAMPTZ,
    fetched_at            TIMESTAMPTZ NOT NULL,
    raw_payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT uq_common_feature_observation_raw
        UNIQUE NULLS NOT DISTINCT (
            source,
            series_id,
            observation_date,
            period_end_date,
            release_date,
            vintage
        )
);

CREATE INDEX IF NOT EXISTS ix_common_feature_observation_lookup
    ON common_feature_observation_raw (
        series_id,
        available_from_date DESC,
        observation_date DESC
    );

CREATE INDEX IF NOT EXISTS ix_common_feature_observation_date
    ON common_feature_observation_raw (series_id, observation_date DESC);

CREATE INDEX IF NOT EXISTS ix_common_feature_observation_sync_cursor
    ON common_feature_observation_raw (fetched_at, raw_id);

-- 21) dart_employee_raw ─ DS002 empSttus / exctvSttus (직원·임원 현황)
-- 22) dart_governance_raw ─ DS002 hyslrSttus / hyslrChgSttus / 감사의견
--
-- Two tables for five endpoints, split by what a row describes rather than by
-- which endpoint produced it: people and pay on one side, control and audit on
-- the other. `statement_type` + `raw_payload` follows dart_shareholder_return_raw.
--
-- `rcept_no` is NOT NULL *and* part of the UNIQUE key, and that is the point of
-- the design. A corrected periodic report gets a new receipt number, so leaving
-- it out would let the correction overwrite the row and destroy the earlier
-- value. For audit opinions and changes of control the correction IS the signal.
--
-- `row_ordinal` rather than a natural key: one response carries several rows
-- (by division and gender for employees, by related party for shareholders) and
-- those labels are rewritten between years, so a natural-key join silently
-- breaks. Response order was verified stable across repeated requests (N6 PoC).
CREATE TABLE IF NOT EXISTS dart_employee_raw (
    raw_id          BIGSERIAL   PRIMARY KEY,
    corp_code       TEXT        NOT NULL,
    ticker          TEXT,
    bsns_year       INT         NOT NULL,
    reprt_code      TEXT        NOT NULL,
    rcept_no        TEXT        NOT NULL,
    statement_type  TEXT        NOT NULL,
    row_ordinal     INT         NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    raw_payload     JSONB       NOT NULL,
    CONSTRAINT uq_dart_employee_raw
        UNIQUE (corp_code, bsns_year, reprt_code, statement_type, rcept_no, row_ordinal)
);

CREATE INDEX IF NOT EXISTS ix_dart_employee_raw_lookup
    ON dart_employee_raw (ticker, bsns_year, reprt_code, statement_type);

CREATE INDEX IF NOT EXISTS ix_dart_employee_raw_sync_cursor
    ON dart_employee_raw (fetched_at, raw_id);

CREATE TABLE IF NOT EXISTS dart_governance_raw (
    raw_id          BIGSERIAL   PRIMARY KEY,
    corp_code       TEXT        NOT NULL,
    ticker          TEXT,
    bsns_year       INT         NOT NULL,
    reprt_code      TEXT        NOT NULL,
    rcept_no        TEXT        NOT NULL,
    statement_type  TEXT        NOT NULL,
    row_ordinal     INT         NOT NULL,
    source          TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    raw_payload     JSONB       NOT NULL,
    CONSTRAINT uq_dart_governance_raw
        UNIQUE (corp_code, bsns_year, reprt_code, statement_type, rcept_no, row_ordinal)
);

CREATE INDEX IF NOT EXISTS ix_dart_governance_raw_lookup
    ON dart_governance_raw (ticker, bsns_year, reprt_code, statement_type);

CREATE INDEX IF NOT EXISTS ix_dart_governance_raw_sync_cursor
    ON dart_governance_raw (fetched_at, raw_id);

-- =============================================================================
-- Future extension: intraday_ohlcv (OUT OF SCOPE)
-- =============================================================================
-- CREATE TABLE IF NOT EXISTS intraday_ohlcv (
--     trade_ts    TIMESTAMPTZ NOT NULL,
--     ticker      TEXT        NOT NULL,
--     market      TEXT        NOT NULL,
--     interval    TEXT        NOT NULL,   -- 1m | 5m | 1h
--     open        BIGINT      NOT NULL,
--     high        BIGINT      NOT NULL,
--     low         BIGINT      NOT NULL,
--     close       BIGINT      NOT NULL,
--     volume      BIGINT      NOT NULL,
--     source      TEXT        NOT NULL,
--     fetched_at  TIMESTAMPTZ NOT NULL,
--     PRIMARY KEY (trade_ts, ticker, market, interval)
-- );
