"""Pure data definitions shared by the normalization service and DuckDB marts.

These modules hold metric catalog/mapping-rule and common-feature catalog/series
definitions as plain functions with no ``Storage`` or external dependencies, so
both ``service/`` orchestrators (Postgres path) and ``research/etl`` compute
marts (DuckDB path) can import them directly. See
``docs/dev/20260728_refactor_pipeline/00_refactor_plan.md`` §3.0.
"""
