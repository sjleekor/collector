-- Migration: drop the derived/catalog/operating tables that moved to the DuckDB
-- compute marts (refactor §5). Roll-forward only. Run on BOTH hosts (local mydb
-- and sj2 krx_data) AFTER:
--   1) P2 parity passed (the DuckDB marts reproduce these tables), and
--   2) backups taken (refactor §7.5):
--        - stock_metric_fact / common_feature_daily_fact: a canonical parquet
--          export (or a mart build) is sufficient — they are recomputable.
--        - metric_catalog / metric_mapping_rule / common_feature_catalog /
--          common_feature_catalog_input: values live in code
--          (collector.kr.definitions) — no separate backup needed.
--        - operating_source_document / operating_metric_fact: REQUIRED separate
--          backup (content_text + raw_payload are source documents not in any
--          parquet export). Take pg_dump of these two tables first.
--
-- KEPT (decision 7): common_feature_series — the collector reads it at runtime
-- and the compute mart shares it via the raw lake.
--
-- 8 tables, dropped child-first so FKs resolve without CASCADE surprises.

BEGIN;

-- derived facts (children of catalog/rule) ---------------------------------
DROP TABLE IF EXISTS stock_metric_fact;
DROP TABLE IF EXISTS common_feature_daily_fact;

-- operating pilot (child first) --------------------------------------------
DROP TABLE IF EXISTS operating_metric_fact;
DROP TABLE IF EXISTS operating_source_document;

-- catalog / rule (compute-only; replaced by code definitions) --------------
-- common_feature_catalog_input FK -> common_feature_series resolves by dropping
-- the input table; common_feature_series itself is kept.
DROP TABLE IF EXISTS common_feature_catalog_input;
DROP TABLE IF EXISTS common_feature_catalog;
DROP TABLE IF EXISTS metric_mapping_rule;
DROP TABLE IF EXISTS metric_catalog;

COMMIT;
