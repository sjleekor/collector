-- 2026-10-19: 정규장 보정(`prices fix-regular-session`)의 old -> new 감사 표.
-- sql/postgres_ddl.sql의 4c와 같다. 전부 IF NOT EXISTS라 여러 번 돌려도 안전하다.
-- 새로 만든 서버(db init)는 DDL이 이미 이 테이블을 만들어서 이 파일이 필요 없다.
-- 적용: agents/skills/sdc-db/scripts/dbq.sh sj2 "$(cat sql/migrations/20261019_daily_ohlcv_regular_fix_audit.sql)"
CREATE TABLE IF NOT EXISTS daily_ohlcv_regular_fix_audit (
    audit_id      BIGSERIAL        PRIMARY KEY,
    run_id        UUID             NOT NULL,
    applied_at    TIMESTAMPTZ      NOT NULL,
    trade_date    DATE             NOT NULL,
    ticker        TEXT             NOT NULL,
    market        TEXT             NOT NULL,
    old_high      BIGINT           NOT NULL,
    old_low       BIGINT           NOT NULL,
    old_close     BIGINT           NOT NULL,
    new_high      BIGINT           NOT NULL,
    new_low       BIGINT           NOT NULL,
    new_close     BIGINT           NOT NULL,
    naver_open    BIGINT           NOT NULL,
    krx_open      BIGINT           NOT NULL,
    krx_high      BIGINT           NOT NULL,
    krx_low       BIGINT           NOT NULL,
    krx_close     BIGINT           NOT NULL,
    factor        DOUBLE PRECISION NOT NULL,
    factor_basis  TEXT             NOT NULL,   -- own | prev_day
    krx_source    TEXT             NOT NULL    -- daily_market_cap | ...
);

-- 18:30 naver 재수집이 같은 행을 다시 덮고 20:00 이후 같은 값으로 다시 고치면 같은
-- old -> new가 되풀이된다. 같은 변경은 한 번만 남긴다 (INSERT ... ON CONFLICT DO NOTHING).
CREATE UNIQUE INDEX IF NOT EXISTS ux_daily_ohlcv_regular_fix_audit_change
    ON daily_ohlcv_regular_fix_audit (
        trade_date, ticker, market,
        old_high, old_low, old_close, new_high, new_low, new_close
    );

CREATE INDEX IF NOT EXISTS ix_daily_ohlcv_regular_fix_audit_run
    ON daily_ohlcv_regular_fix_audit (run_id);
