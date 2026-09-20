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
