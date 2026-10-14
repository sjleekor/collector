-- 2026-10-07: DART 거래소공시(pblntf_ty=I) 시장 전체 수집용 표 둘 (KR 10월 계획 4.2-a1).
-- 새로 만든 서버가 아니라 이미 db init을 한 서버(sj2)에 적용한다. IF NOT EXISTS라 다시 돌려도 된다.
-- 적용: agents/skills/sdc-db/scripts/dbq.sh sj2 "$(cat sql/migrations/20261007_dart_exchange_notice.sql)"
-- 그 뒤 새 이미지로 `collector dart backfill-exchange-notices`를 돌린다.
-- 13b) dart_exchange_notice_raw ─ 거래소공시 (list.json, pblntf_ty=I), 날짜 구간으로 받은 시장 전체
--      관리종목 지정·매매거래정지·상장폐지 등 KRX 시장조치 공시. rcept_dt = 접수일(효력일 아님).
--      dart_filing_receipt_raw(종목별·전 유형)와 달리 상장폐지 종목까지 시장 전체를 담는다.
CREATE TABLE IF NOT EXISTS dart_exchange_notice_raw (
    rcept_no         TEXT        PRIMARY KEY,
    pblntf_ty        TEXT        NOT NULL DEFAULT 'I',
    corp_code        TEXT        NOT NULL,
    corp_name        TEXT        NOT NULL DEFAULT '',
    stock_code       TEXT        NOT NULL DEFAULT '',
    corp_cls         TEXT        NOT NULL DEFAULT '',
    report_nm        TEXT        NOT NULL DEFAULT '',
    flr_nm           TEXT        NOT NULL DEFAULT '',
    rcept_dt         DATE,
    rm               TEXT        NOT NULL DEFAULT '',
    source           TEXT        NOT NULL,
    first_fetched_at TIMESTAMPTZ NOT NULL,
    fetched_at       TIMESTAMPTZ NOT NULL,
    raw_payload      JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_dart_exchange_notice_raw_dt
    ON dart_exchange_notice_raw (rcept_dt);

CREATE INDEX IF NOT EXISTS ix_dart_exchange_notice_raw_stock
    ON dart_exchange_notice_raw (stock_code, rcept_dt);

-- 13c) dart_exchange_notice_window ─ 요청 구간별 진행·완료 장부 (이어 받기·건수 확인용)
--      pages_done 페이지까지 적재했다. status=done 은 total_page까지 받고 저장 건수가
--      total_count에 닿았다는 뜻이다.
CREATE TABLE IF NOT EXISTS dart_exchange_notice_window (
    pblntf_ty     TEXT        NOT NULL DEFAULT 'I',
    bgn_de        DATE        NOT NULL,
    end_de        DATE        NOT NULL,
    status        TEXT        NOT NULL,   -- running | done | failed
    total_count   INT         NOT NULL DEFAULT 0,
    total_page    INT         NOT NULL DEFAULT 0,
    pages_done    INT         NOT NULL DEFAULT 0,
    rows_fetched  INT         NOT NULL DEFAULT 0,
    requests      INT         NOT NULL DEFAULT 0,
    last_error    TEXT,
    started_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ,
    PRIMARY KEY (pblntf_ty, bgn_de, end_de)
);
