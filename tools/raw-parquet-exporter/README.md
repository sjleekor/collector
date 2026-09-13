# Raw Parquet Exporter

Rust exporter for moving raw PostgreSQL tables into a Parquet lake.

This crate currently implements Phase 1 and a narrow Phase 2 MVP from
`docs/dev/20260619_rust_exporter/raw_parquet_exporter_rust_plan.md`:

- CLI skeleton.
- TOML config loading and validation.
- PostgreSQL connection with read-only session settings.
- `information_schema` table introspection.
- Dry-run job planning for raw-id, monthly-date, full-table, and snapshot-items
  strategies.
- raw-id DART table export to partitioned Parquet files under
  `bsns_year=<YYYY>/reprt_code=<CODE>/`.
- monthly date-range export to `year=<YYYY>/month=<MM>/` partitions.
- unpartitioned full-table export for small dimension tables.
- snapshot item export to `snapshot_date=<YYYY-MM-DD>/` partitions.
- Table manifest generation and Parquet metadata row-count validation.

## Usage

From this directory:

```bash
cargo run -- plan --tables dart_xbrl_fact_raw --runtime config/local.example.toml
```

From the repository root:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  plan \
  --config tools/raw-parquet-exporter/config/export_tables.toml \
  --runtime tools/raw-parquet-exporter/config/local.example.toml \
  --tables dart_xbrl_fact_raw
```

Use `--offline` to validate config and render plan shape without connecting to
PostgreSQL.

```bash
cargo run -- plan --tables dart_xbrl_fact_raw --offline
```

The process loads `.env` from the current working directory when present.
Database secrets should be supplied through `DB_DSN` or the `DB_*` environment
variables, not committed runtime config.

## Supported Tables

| Strategy | Tables | Output layout |
|---|---|---|
| `raw_id_range` | `dart_xbrl_fact_raw`, `dart_financial_statement_raw`, `dart_shareholder_return_raw`, `dart_share_count_raw`, `dart_capital_change_raw` | `bsns_year=<YYYY>/reprt_code=<CODE>/` |
| `date_month` | `krx_security_flow_raw`, `daily_ohlcv` | `year=<YYYY>/month=<MM>/` |
| `full_table` | `dart_filing_receipt_raw`, `dart_xbrl_document`, `common_feature_observation_raw`, `common_feature_series`, `dart_corp_master`, `stock_master`, `stock_master_snapshot` | configured simple column partitions or unpartitioned |
| `snapshot_items` | `stock_master_snapshot_items` | `snapshot_date=<YYYY-MM-DD>/` |

Empty source tables are valid for `date_month`, `full_table`,
and `snapshot_items`: they write a table manifest with
`rows_exported = 0` and no Parquet files. `raw_id_range` tables are expected to
contain source rows before export.

## Phase 2 Raw-ID Export

Export one raw-id chunk:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  export \
  --tables dart_xbrl_fact_raw \
  --start-raw-id 7500001 \
  --chunk-rows 1000000 \
  --batch-rows 65536 \
  --max-rows-per-file 200000 \
  --force
```

The same raw-id export path also supports `dart_financial_statement_raw`,
`dart_shareholder_return_raw`, and other configured `raw_id_range` tables whose
output partitions are `bsns_year/reprt_code`.

`--max-rows-per-file` is optional. When set, each output partition writes
`part-000000.parquet`, `part-000001.parquet`, and so on as the row limit is
reached.

Export all remaining chunks from `--start-raw-id` through the source table's
current max `raw_id`:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  export \
  --tables dart_xbrl_fact_raw \
  --start-raw-id 7500001 \
  --all-chunks \
  --chunk-rows 1000000 \
  --batch-rows 65536 \
  --max-rows-per-file 200000 \
  --force
```

Export one monthly date-range chunk:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  export \
  --tables krx_security_flow_raw \
  --since-date 2007-09 \
  --until-date 2007-09 \
  --batch-rows 65536 \
  --max-rows-per-file 100000 \
  --force
```

Export an unpartitioned full table:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  export \
  --tables dart_corp_master \
  --batch-rows 65536 \
  --max-rows-per-file 200000 \
  --force
```

`full_table` export supports unpartitioned tables and simple source-column
partitions such as `["bsns_year", "reprt_code"]` or `["source"]`. Expression
partitions remain a later implementation step.

Export snapshot item rows partitioned by the parent snapshot date:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  export \
  --tables stock_master_snapshot_items \
  --batch-rows 65536 \
  --max-rows-per-file 200000 \
  --force
```

This strategy joins `stock_master_snapshot_items.snapshot_id` to
`stock_master_snapshot.snapshot_id`, writes only item-table columns to Parquet,
and routes files under `snapshot_date=<YYYY-MM-DD>/`.

Validate the generated manifest:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  validate \
  --manifest data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb/_manifests/table_manifests/dart_xbrl_fact_raw.json
```

Compare exported raw-id samples against PostgreSQL source rows:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  validate-samples \
  --manifest data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb/_manifests/table_manifests/dart_xbrl_fact_raw.json
```

When `--raw-ids` is omitted, the command uses the manifest's min/mid/max
`raw_id` values. You can also choose samples explicitly:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  validate-samples \
  --manifest data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb/_manifests/table_manifests/dart_xbrl_fact_raw.json \
  --raw-ids 1,500000,1000000
```

The sample validator currently supports manifests whose table has a `raw_id`
column. It compares PostgreSQL canonical export values with Parquet values for
every manifest schema column.

Each non-dry-run export writes a checkpoint under
`_manifests/checkpoints/<run_id>.json`. Resume from that checkpoint after an
interrupted run:

```bash
cargo run --manifest-path tools/raw-parquet-exporter/Cargo.toml -- \
  resume \
  --checkpoint data_lake/raw_postgres/snapshot_date=2026-06-19/source=local_mydb/_manifests/checkpoints/<run_id>.json
```

**Resume support is limited to the `raw_id_range` and `date_month`
strategies** (`FullTableExportOptions`/`SnapshotItemsExportOptions` have no
`resume` field at all). For the `full_table`/`snapshot_items` tables — small
dimension tables (~0.2 GB combined) — an interrupted run has no checkpoint to
resume from; re-run the export with `--force` instead.
`bin/raw-parquet-export-all.sh` handles this distinction automatically (see
`bin/README.md`), calling `resume` only for the resumable strategies and
`--force` re-export for the rest.
