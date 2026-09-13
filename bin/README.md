# bin 스크립트 가이드

이 디렉터리의 Parquet/DuckDB 관련 핵심 스크립트는 두 개입니다.

- `parquet-compute-all.sh`: raw 확보(아래 "경로" 참고)부터 raw Parquet export와 DuckDB compute까지 이어서 실행하는 상위 래퍼.
- `raw-parquet-export-all.sh`: PostgreSQL(로컬 `local_mydb` 또는 sj2 직접 `sj2_remote`)의 raw/reference 테이블을 `data_lake/raw_postgres` Parquet lake로 내보내는 export 전용 래퍼.
- `canonical-parquet-export-all.sh` — **폐기 예정.** 읽는 쪽이 없다.
  legacy canonical lake의 폐기 전 백업을 한 번 다시 뜨기 위해서만 남긴다.

리팩터링 이후 sj2-server는 raw 수집 전용입니다. `stock_metric_fact`, `common_feature_daily_fact` 같은 파생 결과는 PostgreSQL에 쓰지 않고, 필요할 때 로컬/compute 노드에서 raw Parquet 위 DuckDB 마트로 재계산합니다.

## 두 경로: `local` (기존) / `remote` (신규)

raw 확보는 두 경로 중 선택할 수 있습니다(설계: `docs/dev/20260730_refactor_dump/00_dual_route_raw_export_plan.md`).

| 경로 | 흐름 | 로컬 SSD | 용도 |
|---|---|---|---|
| **`local`(기본값)** | sj2 → `db sync-remote --full-refresh` → mydb → parquet | ~189 GB 상주 | 반복 재계산, 오프라인 작업, 재현 가능한 재읽기 |
| **`remote`** | sj2 → parquet 직접 캡처 | 0 GB(출력만) | 1회성 최신 캡처, 디스크 절약. 90분짜리 미러 refresh를 생략하므로 전체적으로 더 빠름 |

`--route`를 생략하면 지금까지와 완전히 동일하게 동작합니다(`local_mydb`, 기존 경로/출력 불변).

```text
--route local (기본)                    --route remote
sj2 PostgreSQL                          sj2 PostgreSQL
  -> local mydb mirror                    -> (미러 없음)
  -> raw_postgres/.../source=local_mydb/  -> raw_postgres/.../source=sj2_remote/
  -> derived_mart/... (source=local_mydb)   -> derived_mart/... (source=sj2_remote)
```

두 경로의 산출물은 **같은 `snapshot_date` 아래 `source=` 파티션으로 공존**할 수 있습니다(parity 비교
목적). `remote` 경로는 `db with-remote-dsn`(아래)으로 원격 DSN을 export 하위 프로세스에만 주입하며,
raw-parquet-exporter 바이너리 자체는 두 경로 모두 동일합니다.

**캡처 격리 수준.** 두 경로 모두 export 시점은 여전히 테이블별 read-committed입니다(공유 스냅샷이
아닙니다). raw 테이블이 `ON CONFLICT ... DO UPDATE`로 기존 행도 갱신하므로, export가 Cronicle 수집
창(18:30/20:30/23:30/04:00 KST)과 겹치면 한 테이블 안에서도 서로 다른 시점의 값이 섞일 수 있습니다.
그래서 이 문서와 `_SUCCESS.json`에서는 결과물을 "스냅샷"이 아니라 **"캡처(capture)"**라고 부릅니다.
가능하면 수집 창을 피해 실행하세요.

일반적인 수동 실행은 다음 한 줄입니다.

```bash
bin/parquet-compute-all.sh --snapshot-date 2026-06-29                    # local 경로(기본)
bin/parquet-compute-all.sh --snapshot-date 2026-06-29 --route remote     # sj2 직접 캡처
```

## `parquet-compute-all.sh`

### 역할

`parquet-compute-all.sh`는 "raw 확보부터 derived mart 재계산까지" 한 번에 실행하는 상위 오케스트레이션
스크립트입니다.

`--route local`(기본) 실행 단계:

1. **sj2 -> local mydb full-refresh sync**
   - `uv run collector db sync-remote --full-refresh`를 실행합니다.
   - 관리 대상 raw/reference mirror 테이블을 원격 sj2 DB 기준으로 다시 복사합니다.
   - `--all-tables`는 붙이지 않으므로 schema-reset copy는 아닙니다. 로컬 schema까지 다시 만들 필요가 있으면 `db sync-remote --full-refresh --all-tables`를 별도로 실행해야 합니다.
2. **local mydb -> raw Parquet export**
   - `bin/raw-parquet-export-all.sh --snapshot-date <snapshot-date> --route local`을 실행합니다.
   - 기본 출력 경로는 `data_lake/raw_postgres/snapshot_date=<snapshot-date>/source=local_mydb/`입니다.
3. **DuckDB compute**
   - `research.etl.compute_all --source local_mydb`를 실행합니다.
   - freshness gate -> `stock_metric_fact`/`common_feature_daily_fact` derived mart -> coverage/readiness gate 순서로 진행합니다.
   - `--features`를 주면 feat_*/labels mart까지 추가로 빌드합니다.

`--route remote` 실행 단계 (1번이 생략됩니다):

1. ~~sj2 -> local mydb sync~~ — 건너뜁니다. `--from-step sync`를 명시해도 자동으로 `export`로 승격됩니다.
2. **sj2 직접 -> raw Parquet 캡처**
   - `uv run collector db with-remote-dsn [--ssh-host ...] -- bin/raw-parquet-export-all.sh --route remote --jobs <N>`을 실행합니다.
   - 출력 경로는 `data_lake/raw_postgres/snapshot_date=<snapshot-date>/source=sj2_remote/`입니다.
3. **DuckDB compute**
   - `research.etl.compute_all --source sj2_remote`를 실행합니다.
   - **`--features`는 아직 차단됩니다** — `dataset_dir()`(per-model dataset 경로)가 아직 `source=`로
     분리되지 않아 remote 캡처 기반 feature/label 마트가 local 캡처 결과를 같은 `--snapshot-date`에서
     덮어쓸 수 있기 때문입니다. features가 필요하면 `--route local`로 빌드하세요.

### 옵션

```bash
bin/parquet-compute-all.sh [options]
```

- `--snapshot-date YYYY-MM-DD`: raw lake를 쓰고 compute가 읽을 snapshot 날짜. 기본값은 실행일입니다.
- `--route local|remote`: raw 확보 경로. 기본값은 `local`.
- `--ssh-host HOST`: `--route remote`일 때 SSH 터널 대상(`db with-remote-dsn`으로 전달).
- `--jobs N`: export 병렬도 전달(`raw-parquet-export-all.sh --jobs`와 동일. 기본 local=1/remote=3).
- `--from-step STEP`: 시작 단계. `sync`, `export`, `freshness`, `marts`, `reports`, `features` 중 하나입니다.
- `--skip-sync`: `--from-step export`와 같습니다. 이미 로컬 mirror가 준비된 경우 sync를 건너뜁니다.
- `--features`: freshness/marts/reports 이후 feat_*/labels mart도 빌드합니다(`--route remote`와 병용 불가).
- `--end YYYY-MM-DD`: freshness 기준일. 기본값은 compute 실행 시점의 KST today입니다.
- `--required-coverage-ratio R`: readiness coverage threshold. 기본값은 `1.0`입니다.

### 재실행 패턴

전체를 새로 실행:

```bash
bin/parquet-compute-all.sh --snapshot-date 2026-06-29
bin/parquet-compute-all.sh --snapshot-date 2026-06-29 --route remote --ssh-host sj2-server
```

sj2 sync는 건너뛰고 local mydb export부터 실행:

```bash
bin/parquet-compute-all.sh --skip-sync --snapshot-date 2026-06-29
```

이미 raw Parquet 캡처가 있고 DuckDB compute만 다시 실행:

```bash
bin/parquet-compute-all.sh --from-step freshness --snapshot-date 2026-06-29
bin/parquet-compute-all.sh --from-step freshness --snapshot-date 2026-06-29 --route remote
```

derived mart까지 이미 만들었고 report gate만 다시 확인:

```bash
bin/parquet-compute-all.sh --from-step reports --snapshot-date 2026-06-29
```

같은 snapshot/table export 결과를 덮어써야 하는 경우:

```bash
SDC_RAW_PARQUET_FORCE=1 bin/parquet-compute-all.sh --snapshot-date 2026-06-29
```

또는 export를 수동으로 덮어쓴 뒤 compute만 실행할 수 있습니다.

```bash
bin/raw-parquet-export-all.sh --snapshot-date 2026-06-29 --force
bin/parquet-compute-all.sh --from-step freshness --snapshot-date 2026-06-29
```

### 주의사항

- `--route local`의 기본 sync는 **full-refresh**입니다. 로컬 managed mirror 테이블의 기존 내용은 원격 sj2 기준으로 다시 채워집니다.
- 이 스크립트는 PostgreSQL에 derived fact를 쓰지 않습니다.
- `--from-step export` 또는 `--skip-sync`를 쓰면 sj2 -> local sync가 실행되지 않습니다(`--route remote`는 항상 이렇게 동작합니다).
- `--from-step freshness`, `marts`, `reports`, `features`는 raw Parquet 캡처가 이미 있다고 가정합니다.
- freshness/readiness gate가 실패하면 non-zero로 종료합니다. 실패 요약은 stderr에 출력됩니다.
- `research.etl.compute_all`은 `_SUCCESS.json` 완료 표식이 없으면 거부합니다(아래 "완료 표식과 재실행" 참고).

## `raw-parquet-export-all.sh`

### 역할

`raw-parquet-export-all.sh`는 PostgreSQL의 raw/reference 테이블을 Parquet lake로 export하는 전용
래퍼입니다. 실행 전 **테이블별 상태를 먼저 판정**합니다 — 유효한 완료 manifest가 있으면 skip, 미완료
체크포인트가 정확히 1개면 이어받기(resume 가능한 전략만), 여러 개면 에러를 냅니다. 이 스크립트는 sj2
원격 DB와 직접 동기화하지 않습니다(`--route remote`도 마찬가지로, DSN만 `db with-remote-dsn`을 통해
주입받아 직접 접속할 뿐 로컬 미러를 만들지 않습니다).

runtime TOML(`[source]`/`[output]`)은 **이 스크립트가 `--route`로부터 직접 생성**합니다 — 더 이상
사람이 유지하는 파일과 셸 변수를 따로 맞출 필요가 없습니다(예전에는 `SDC_RAW_PARQUET_SOURCE_NAME` /
`SDC_RAW_PARQUET_OUTPUT_ROOT`가 실제 출력 경로를 바꾸지 않고 이 스크립트의 검증 경로 계산에만
쓰여서, 서로 다른 값을 주면 export는 A에 쓰고 validate는 B를 찾는 "엉뚱한 실패"가 났습니다 — 지금은
구조적으로 불가능합니다).

| route | `source.name` | DSN |
|---|---|---|
| `local`(기본) | `local_mydb` | `.env`의 `DB_DSN` |
| `remote` | `sj2_remote` | `SDC_REMOTE_DSN`(→ `db with-remote-dsn`이 주입) |

`SDC_RAW_PARQUET_RUNTIME`으로 커스텀 runtime TOML을 지정할 수 있지만(escape hatch), 그 파일의
`[source].name`/`[output].root`가 `--route`에서 파생되는 값과 다르면 **바로 실패**합니다(경고 아님) —
기대값/실제값을 모두 stderr에 출력합니다.

실행 시:

1. Rust exporter release binary를 빌드합니다(`--no-build`로 생략 가능).
2. 설정된 13개 테이블에 대해 **테이블별 상태를 판정**하고(skip/export/resume/force/error), `--jobs`
   개까지 동시에 처리합니다.
3. 각 테이블 export/resume 후 manifest 검증을 수행합니다.
4. raw_id 기반 테이블은 `--all-chunks`로 전체 chunk를 export합니다.
5. 전 테이블이 성공하면 `_SUCCESS.json` 완료 표식을 원자적으로 씁니다.

### 테이블별 상태 판정과 재개

| export 전략 | 테이블 | resume 지원 |
|---|---|---|
| `raw_id_range` | `dart_xbrl_fact_raw`, `dart_financial_statement_raw`, `dart_shareholder_return_raw`, `dart_share_count_raw`, `dart_capital_change_raw` | O (체크포인트 단위: chunk) |
| `date_month` | `krx_security_flow_raw`, `daily_ohlcv` | O (체크포인트 단위: 월) |
| `full_table` | `dart_filing_receipt_raw`, `dart_xbrl_document`, `dart_corp_master`, `stock_master`, `stock_master_snapshot`, `common_feature_series`, `common_feature_observation_raw` | X |
| `snapshot_items` | `stock_master_snapshot_items` | X |

판정 순서(테이블마다):

1. 유효한 완료 manifest(`_manifests/table_manifests/<table>.json` + `validate` 통과) → **skip**.
2. 미완료 체크포인트(`.completed == false`)가 정확히 1개:
   - resume 지원 전략 → `resume --checkpoint <파일>`.
   - resume 미지원 전략(위 표의 X) → 저렴하므로 그냥 `--force` 재export.
3. 미완료 체크포인트가 2개 이상(반복 중단이 쌓인 상태) → **에러**, `--force-table <테이블>`을 요구.
4. 아무것도 없음 → 새로 `export`.

한 테이블이 실패해도 나머지 테이블은 계속 진행합니다(`--jobs`가 1보다 크면 특히 중요). 모든 테이블이
끝난 뒤 실패 목록을 요약해 non-zero로 종료합니다. **재실행은 같은 `--snapshot-date`/`--route`로 그냥
다시 실행하면 됩니다** — 완료된 테이블은 skip되고 실패한 테이블만 이어받거나 다시 시도합니다.

### 완료 표식과 재실행

전 테이블이 성공하면 다음 위치에 완료 표식이 원자적으로(temp 파일 → rename) 쓰입니다.

```text
data_lake/raw_postgres/snapshot_date=<D>/source=<S>/_manifests/_SUCCESS.json
```

담긴 내용: `route`, `tables`(테이블별 manifest 경로/행 수/schema hash), `started_at`/`finished_at`(KST),
`jobs`, `collector_overlap`(캡처 창이 Cronicle 수집 창과 겹쳤는지), `snapshot_policy`. `research.etl.compute_all`은
이 표식이 없거나 테이블 목록이 기대치와 다르면 레이크를 거부합니다(`--allow-incomplete-lake`로 우회 가능,
기본 off).

하나라도 실패하면 이 표식은 **쓰이지 않습니다** — 부분 완료 레이크가 완료로 오인되지 않도록 하기
위함입니다.

### 입력과 출력

입력:

- PostgreSQL DSN: `--route local`은 `.env`의 `DB_DSN`, `--route remote`는 `SDC_REMOTE_DSN`(직접 설정하지
  말고 `db with-remote-dsn`을 통해 주입받으세요).
- export table config: `tools/raw-parquet-exporter/config/export_tables.toml`.
- runtime config: 이 스크립트가 `--route`에서 생성(위 참고). `SDC_RAW_PARQUET_RUNTIME`으로 override 가능.

출력:

```text
data_lake/raw_postgres/
  snapshot_date=<YYYY-MM-DD>/
    source=local_mydb/        (또는 source=sj2_remote/)
      <table>/
        schema_version=1/
          ...
      _manifests/
        table_manifests/
          <table>.json
        checkpoints/
          ...
        _SUCCESS.json          (전 테이블 성공 시에만)
```

임시 파일은 기본적으로 `data_lake/_tmp/raw_export/...` 아래에 생성됩니다.

**hive partitioning 함정.** 레이크 경로에 `source=<name>` 세그먼트가 있어 DuckDB `read_parquet`이
hive partitioning을 자동 감지하면 파일 안의 진짜 `source` 컬럼(예: `daily_ohlcv.source = 'PYKRX'`)을
경로값(`local_mydb`/`sj2_remote`)으로 **덮어씁니다**. ad-hoc 쿼리에서 이 레이크를 직접 읽을 때는
`read_parquet(files, hive_partitioning=false)`를 반드시 쓰세요(마트 코드는 이미 그렇게 하고 있습니다).
두 캡처가 공존하게 되면서 이 함정의 파급이 커집니다 — `local_mydb`/`sj2_remote`가 데이터에 섞여
보일 수 있습니다.

### Export 대상

위 "테이블별 상태 판정과 재개" 표를 참고하세요.

### 옵션

```bash
bin/raw-parquet-export-all.sh [options]
```

- `--snapshot-date YYYY-MM-DD`: 출력 snapshot partition. 기본값은 실행일입니다.
- `--route local|remote`: 위 참고. 기본값은 `local`.
- `--jobs N`: 동시 export 프로세스 수. 기본값은 `local`=1, `remote`=3. 4로 클램프됩니다.
- `--force`: 전 테이블의 기존 manifest/checkpoint를 무시하고 강제로 재export합니다.
- `--force-table NAME`: 특정 테이블만 강제 재export합니다(반복 가능). 미완료 체크포인트가 2개 이상
  쌓인 테이블을 해소할 때 필요합니다.
- `--no-build`: Rust release build를 생략합니다. 기존 binary가 있어야 합니다.
- `--no-validate`: export/resume 후 manifest 검증을 생략합니다.
- `--validate-samples`: raw_id 테이블에 대해 PostgreSQL 원본 샘플과 Parquet 값을 비교합니다.
- `--dry-run`: Parquet 파일을 쓰지 않고 export plan만 확인합니다(`_SUCCESS.json`도 쓰지 않습니다).

환경 변수 override:

- `SDC_RAW_PARQUET_SNAPSHOT_DATE`: `--snapshot-date`와 동일.
- `SDC_RAW_PARQUET_ROUTE`: `--route`와 동일.
- `SDC_RAW_PARQUET_JOBS`: `--jobs`와 동일.
- `SDC_RAW_PARQUET_FORCE=1`: `--force`와 동일.
- `SDC_RAW_PARQUET_BUILD_RELEASE=0`: `--no-build`와 동일.
- `SDC_RAW_PARQUET_VALIDATE=0`: `--no-validate`와 동일.
- `SDC_RAW_PARQUET_VALIDATE_SAMPLES=1`: `--validate-samples`와 동일.
- `SDC_RAW_PARQUET_DRY_RUN=1`: `--dry-run`과 동일.
- `SDC_RAW_PARQUET_BATCH_ROWS`: exporter batch rows. 기본값은 `65536`.
- `SDC_RAW_PARQUET_MAX_ROWS_PER_FILE`: 파일당 최대 row 수. 기본값은 `5000000`.
- `SDC_RAW_PARQUET_CONFIG`: export table config 경로.
- `SDC_RAW_PARQUET_RUNTIME`: 커스텀 runtime TOML(escape hatch; `--route`와 불일치하면 fail-fast).
- `SDC_RAW_PARQUET_OUTPUT_ROOT`: 출력 root. 기본값은 `data_lake/raw_postgres`.
- `SDC_RAW_PARQUET_BIN`: exporter 바이너리 경로 override(테스트 주입 지점). 기본값은
  `tools/raw-parquet-exporter/target/release/raw-parquet-exporter`.

`--until-date`/`--since-date`(exporter 자체 옵션, 이 래퍼가 노출하지는 않지만 커스텀 실행 시 참고)는
**월 단위 포함**입니다 — `--until-date 2026-07-01`을 주면 7월 파티션까지 생성됩니다(exclusive 아님).

### 대표 실행

전체 configured table export(local, 기본):

```bash
bin/raw-parquet-export-all.sh --snapshot-date 2026-06-29
```

sj2 직접 캡처(3-way 병렬, SSH 터널 필요 시 `db with-remote-dsn` 경유):

```bash
uv run collector db with-remote-dsn --ssh-host sj2-server -- \
  bin/raw-parquet-export-all.sh --snapshot-date 2026-06-29 --route remote --jobs 3
```

기존 snapshot/table output 덮어쓰기:

```bash
bin/raw-parquet-export-all.sh --snapshot-date 2026-06-29 --force
```

미완료 체크포인트가 쌓인 특정 테이블만 강제 재생성:

```bash
bin/raw-parquet-export-all.sh --snapshot-date 2026-06-29 --force-table stock_master
```

계획만 확인:

```bash
bin/raw-parquet-export-all.sh --snapshot-date 2026-06-29 --dry-run
```

release binary가 이미 있을 때 build 생략:

```bash
bin/raw-parquet-export-all.sh --snapshot-date 2026-06-29 --no-build
```

source-vs-Parquet 샘플 검증까지 수행:

```bash
bin/raw-parquet-export-all.sh --snapshot-date 2026-06-29 --validate-samples
```

### 주의사항

- 이 스크립트는 sj2 -> local sync를 수행하지 않습니다(`--route local`). 최신 raw를 export하려면 먼저
  `db sync-remote`를 실행하거나 `parquet-compute-all.sh`를 사용하세요. `--route remote`는 항상 sj2를
  직접 읽습니다.
- 기존 output이 있는 snapshot에 실패 없이 다시 export하려면 재실행만 하면 됩니다(테이블별 상태 판정이
  자동으로 skip/resume/force를 고릅니다). 전부 강제로 다시 쓰려면 `--force` 또는
  `SDC_RAW_PARQUET_FORCE=1`이 필요합니다.
- `--dry-run`은 파일을 쓰지 않으며 validation과 `_SUCCESS.json` 작성도 비활성화합니다.
- Rust toolchain(`cargo`)과 `python3`(runtime TOML 생성/검증, `_SUCCESS.json` 작성), `jq`(체크포인트
  상태 판정)가 필요합니다.
- `.env`의 `DB_DSN`이 비어 있거나 잘못된 DB를 가리키면 의도하지 않은 DB를 export할 수 있으므로 실행
  전 확인해야 합니다(`--route local`).

## `db with-remote-dsn` (sj2 직접 접속 헬퍼)

`--route remote`는 export 프로세스가 sj2에 직접 접속해야 합니다. `uv run collector db
with-remote-dsn`은 `db sync-remote`가 쓰는 것과 같은 방식으로 원격 DSN을 해석하고(SSH 터널 포함),
`SDC_REMOTE_DSN`을 **자식 프로세스의 환경에만** 주입한 뒤 그 자식을 실행합니다.

```bash
uv run collector db with-remote-dsn [--db-info-path ...] [--remote-host ...] \
  [--ssh-host sj2-server] [--ssh-local-port ...] [--ssh-compression] -- \
  <command...>
```

- 플래그는 `db sync-remote`와 동일합니다(같은 `stock_data_collector_secrets/db_info` 자격증명, 같은
  SSH 터널 옵션).
- `SIGINT`/`SIGTERM`은 자식 프로세스 그룹으로 전달되고, 유예 시간(10초) 안에 종료하지 않으면
  `SIGKILL`합니다. 자식이 정상 종료하면 그 종료코드를 그대로, 신호로 죽었으면 `128+signum`으로
  반환합니다(OpenDART의 exit 75 같은 의미 있는 코드가 뭉개지지 않습니다).
- 이 래퍼 자신은 DSN을 stdout/stderr/로그에 남기지 않습니다. **다만 `--` 뒤의 자식 명령이 그 값을
  스스로 출력하는 것(예: `printenv`)까지는 막지 않습니다** — 신뢰할 수 있는 명령만 `--` 뒤에
  두세요.
- `bin/parquet-compute-all.sh --route remote`가 이 래핑을 내부적으로 수행하므로 보통 직접 호출할
  필요는 없습니다.
