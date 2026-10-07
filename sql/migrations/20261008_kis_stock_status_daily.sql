-- 2026-10-08: KIS 종목 상태 플래그 일간 스냅샷 테이블 (KR 10월 계획 4.2-b).
-- sql/postgres_ddl.sql의 16b와 같다. 전부 IF NOT EXISTS라 여러 번 돌려도 안전하다.
-- 새로 만든 서버(db init)는 DDL이 이미 이 테이블을 만들어서 이 파일이 필요 없다.
-- 적용: agents/skills/sdc-db/scripts/dbq.sh sj2 "$(cat sql/migrations/20261008_kis_stock_status_daily.sql)"
CREATE TABLE IF NOT EXISTS kis_stock_status_daily (
    snapshot_date       DATE        NOT NULL,
    ticker              TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    mang_issu_cls_code  TEXT        NOT NULL,
    temp_stop_yn        TEXT        NOT NULL,
    mrkt_warn_cls_code  TEXT        NOT NULL,
    invt_caful_yn       TEXT,
    short_over_yn       TEXT,
    sltr_yn             TEXT,
    iscd_stat_cls_code  TEXT,
    observed_at         TIMESTAMPTZ NOT NULL,
    source              TEXT        NOT NULL DEFAULT 'KIS',
    run_id              UUID,
    raw_payload         JSONB       NOT NULL,
    PRIMARY KEY (snapshot_date, ticker)
);

CREATE INDEX IF NOT EXISTS ix_kis_stock_status_daily_ticker
    ON kis_stock_status_daily (ticker, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS ix_kis_stock_status_daily_sync_cursor
    ON kis_stock_status_daily (observed_at, ticker);
