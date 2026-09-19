# deploy/local — 맥에서 하는 일

**수집은 여기서 안 한다. sj2-server가 한다** (미국 계획 D18 — 2026-09-20에 D15를 뒤집었다).
**평일에 이 맥을 수집에 쓸 수 없다**는 것이 이유다.

이 디렉터리에는 **서버에서 당겨 오는 것**만 있다. 한국이 `collector db sync-remote`로
PostgreSQL을 당기는 자리에, 미국은 parquet이라 `rsync`가 온다.

```bash
deploy/local/us-mirror.sh            # 서버 → 맥, derived 만 (기본)
deploy/local/us-mirror.sh --all      # raw 까지 (dolt clone 14GB 포함)
deploy/local/us-mirror.sh --dry-run
```

| | 서버 (`sj2-server`) | 맥 |
|---|---|---|
| 하는 일 | **수집.** Cronicle이 매일 부른다 | **모델링.** `modeler`가 읽는다 |
| 원본 | `/home/whi/data/stock_data/us/` | 미러 |
| 방향 | — | **서버 → 맥 한 방향.** 맥에서 고친 것은 다음 미러에 지워진다 |

**기본이 `derived/`만인 이유.** `modeler`가 읽는 것은 스냅샷이다. `raw/`는 22GB고
그중 14GB가 dolt clone인데 맥에서 그것을 읽을 일이 없다.

> **`launchd` 스크립트는 지웠다.** D15(로컬 `launchd`)로 만들었던
> `us-daily.sh`·`install-launchd.sh`가 여기 있었다. **등록한 적은 없다** —
> 결정이 뒤집힌 것이 등록 전이었다.
