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

## sj2 container export (KR 일일 브리핑 입력)

sj2에는 Rust toolchain이 없다. 이미지 빌드(`Dockerfile`의 `raw-parquet-exporter` stage,
`cargo build --locked --release`)가 linux/amd64 바이너리를 만들어
`/usr/local/bin/raw-parquet-exporter`에 넣는다. 래퍼 스크립트와 20개 표 설정은 `/app`에 있다.

```bash
# sj2 (APP_DIR=$HOME/apps/sdc). 이미지 tag는 compose.yaml이 정한다.
cd ~/apps/sdc
bin/kr-raw-parquet-export.sh --snapshot-date 2026-09-30 --dry-run   # 계획만
bin/kr-raw-parquet-export.sh --snapshot-date 2026-09-30             # 전체 (약 35분)
```

- 내부 명령: `bin/raw-parquet-export-all.sh --route remote --direct-db --jobs 1 --no-build`
  (`--entrypoint /app/bin/raw-parquet-export-all.sh`). `--direct-db`는 compose 네트워크의
  `db`에 `DB_HOST`·`DB_PASSWORD` 등으로 붙는다. DSN은 명령줄·로그에 없다.
- 출력: 컨테이너 `/stock_data/kr/raw/raw_postgres/snapshot_date=<D>/source=sj2_remote`,
  호스트 `${STOCK_DATA_HOST_DIR:-/home/whi/data/stock_data}/kr/raw/raw_postgres/...`.
  `route=remote`·`source=sj2_remote`·`_manifests/_SUCCESS.json` 형식은 맥에서 돌릴 때와 같다.
- 고정값: `--jobs 1`, `--no-force`(기존 표는 건너뛰거나 이어받는다), manifest validation 켬.
  같은 snapshot 날짜로 다시 돌리면 끝난 표는 건너뛴다.
- 주의: `_SUCCESS.json`의 `manifest_path`는 컨테이너 경로(`/stock_data/...`)다. 호스트 경로로
  읽는 쪽이 있으면 `SDC_KR_EXPORT_HOST_PATHS=1`을 주면 호스트 디렉터리를 같은 경로로
  마운트하고 그 경로로 기록한다.
- `db_read_connections`·`writer_workers`는 현재 exporter가 plan 출력에만 쓴다. 실제
  연결 수는 `--jobs`(표 하나씩)가 정하고, `--jobs 1`이면 읽기 연결은 1개다.
- Cronicle 제안(등록은 별도): `sdc_kr_raw_export`, Mon-Fri 23:35 KST, `catch_up=0`,
  `max_children=1`, timeout 3시간. 선행 조건은 18:30 KR chain, 20:00\~20:30 수집,
  23:00 `sdc_daily_freshness` 통과다. 23:35 시작이면 23:30 앵커(존재하는 chain 시각)와 안 겹친다. 그 전에 시작하면 marker의
  `collector_overlap`이 참으로 찍힐 수 있다(정보용, modeler는 안 본다). 첫 실행은 `--dry-run`으로 본다.
