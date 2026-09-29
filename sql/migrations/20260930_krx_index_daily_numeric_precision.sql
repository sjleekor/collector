-- 2026-09-30: krx_index_daily의 NUMERIC 열에 정밀도를 준다.
-- raw parquet exporter(tools/raw-parquet-exporter)가 정밀도 없는 NUMERIC을 거부한다
-- ("numeric column close_idx has no precision"). 값 범위: 지수 최대 161,250.90 (소수 2자리),
-- 거래대금 8.9e13, 시가총액 7.7e15 (정수) — NUMERIC(30, 4)에 들어간다. 데이터 손실 없음.
-- v0.15.13 이후 처음 만든 서버(db init)는 DDL이 이미 NUMERIC(30, 4)라 이 파일이 필요 없다.
-- 적용: agents/skills/sdc-db/scripts/dbq.sh sj2 "$(cat sql/migrations/20260930_krx_index_daily_numeric_precision.sql)"
ALTER TABLE krx_index_daily
    ALTER COLUMN close_idx  TYPE NUMERIC(30, 4),
    ALTER COLUMN chg_idx    TYPE NUMERIC(30, 4),
    ALTER COLUMN fluc_rt    TYPE NUMERIC(30, 4),
    ALTER COLUMN open_idx   TYPE NUMERIC(30, 4),
    ALTER COLUMN high_idx   TYPE NUMERIC(30, 4),
    ALTER COLUMN low_idx    TYPE NUMERIC(30, 4),
    ALTER COLUMN acc_trdval TYPE NUMERIC(30, 4),
    ALTER COLUMN mktcap     TYPE NUMERIC(30, 4);
