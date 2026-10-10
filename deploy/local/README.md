# deploy/local — 맥에서 하는 일

**수집은 여기서 안 한다. sj2-server가 한다** (미국 계획 D18 — 2026-09-20에 D15를 뒤집었다).
**평일에 이 맥을 수집에 쓸 수 없다**는 것이 이유다.

이 디렉터리에는 **서버에서 당겨 오는 것**만 있다. 한국이 `collector db sync-remote`로
PostgreSQL을 당기는 자리에, 미국은 parquet이라 `rsync`가 온다.

```bash
deploy/local/us-mirror.sh            # 서버 → 맥, derived 만 (기본)
deploy/local/us-mirror.sh --output   # derived + output/scan (서버 쪽 수집 QA)
deploy/local/us-mirror.sh --all      # raw 까지 (dolt clone 14GB 포함)
deploy/local/us-mirror.sh --dry-run
deploy/local/us-mirror.sh --all --progress   # 20GB 받을 때만 진행률을 켠다
```

| | 서버 (`sj2-server`) | 맥 |
|---|---|---|
| 하는 일 | **수집.** Cronicle이 매일 부른다 | **모델링.** `modeler`가 읽는다 |
| 원본 | `/home/whi/data/stock_data/us/` | 미러 |
| 방향 | — | **서버 → 맥 한 방향.** 맥에서 고친 것은 다음 미러에 지워진다 |

**기본이 `derived/`만인 이유.** `modeler`가 읽는 것은 스냅샷이다. `raw/`는 22GB고
그중 14GB가 dolt clone인데 맥에서 그것을 읽을 일이 없다.

**`datasets/`·`output/`은 미러가 건드리지 않는다.** `datasets/`(조립한 패널·라벨)와
`output/`(feature_scan, model_runs)은 `modeler`가 맥에서 만드는 자리다. 서버에는
없거나 다른 것이 들어 있어서, 당겼다가는 `--delete`가 맥에서 만든 걸 지운다.
서버 쪽 수집 QA 산출물은 `output/scan/` 하나뿐이라, `--output`은 **`output/` 전체가
아니라 `output/scan/`만** 당긴다. 그래야 옆의 `feature_scan/`·`model_runs/`가 `--delete`에
안 걸린다.

> **`launchd` 스크립트는 지웠다.** D15(로컬 `launchd`)로 만들었던
> `us-daily.sh`·`install-launchd.sh`가 여기 있었다. **등록한 적은 없다** —
> 결정이 뒤집힌 것이 등록 전이었다.

---

## `kr-baseline-mirror.sh` — R-4 기준선 레이크 (ETF·채권지수·분배금)

```bash
deploy/local/kr-baseline-mirror.sh --dry-run   # 받을 것만 센다
deploy/local/kr-baseline-mirror.sh             # 서버 → 맥
```

| | |
|---|---|
| 어디서 | `whi@sj2-server:/home/whi/data/stock_data/kr/raw/krx_baseline/` |
| 어디로 | `$STOCK_DATA_ROOT/kr/raw/krx_baseline/` (서버와 같은 상대 경로) |
| 언제 | **일요일 KR export 날.** 평일에는 이 맥을 못 쓴다. 스크립트가 막지는 않는다 |
| 환경변수 | `SDC_REMOTE_HOST`, `SDC_KR_BASELINE_REMOTE_DIR`, `STOCK_DATA_ROOT` |

**`--delete`를 쓰지 않는다.** 이 데이터는 추가만 되는 관측이고, 원천에서 다시 못 받는
것(과거 시점의 응답 원문, 최초 관측본)이 섞여 있다. 서버에서 파일이 사라져도 맥 사본은
남아야 한다. 이 점이 `us-mirror.sh`와 다르다.

- **덮어쓰지 않는다 (`--ignore-existing`).** 서버 파일은 한 번 쓰면 안 바뀌는 계약이다.
  같은 경로인데 내용이 다르면 끝에 경고만 찍는다(덮지 않는다).
- **`_manifest/`는 마지막에 받는다.** 읽는 쪽은 완료 manifest가 가리키는 파일만 쓴다.
  수집 중에 미러해도 반쯤 쓴 묶음은 읽히지 않는다. `*.tmp`는 제외한다.
- **끝에 점검한다.** 맥 쪽 `_manifest/*.json`이 가리키는 파일이 모두 있고 크기·sha256이
  맞는지 본다. 빠진 것이 있으면 0이 아닌 코드로 끝난다(`--dry-run`에서는 점검하지 않는다).

**KR raw export와 무관하다 (결정 ⑮).** `raw-parquet-export-all.sh`·`export_tables.toml`
(20개 표)·`verify_raw`·`compute_all`은 그대로다. modeler가 이 레이크를 선택 항목으로
읽는 것은 11월 호환 변경 때 넣는다.
