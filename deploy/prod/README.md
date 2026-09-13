# SDC production deployment

이 디렉터리는 `sj2-server:/home/whi/apps/sdc`에 배포되는 SDC 운영 파일의 source of truth다.

- 마지막 확인: 2026-08-28 KST
- 확인한 원천: Cronicle API `GET /api/app/get_schedule/v1`, `GET /api/app/get_event/v1`, 원격 파일 `whi@sj2-server:/home/whi/apps/sdc/{compose.yaml,bin/}`
- Cronicle UI: `http://sj2-server:3012/#Schedule`
- 배포 경로: `whi@sj2-server:/home/whi/apps/sdc`
- 현재 compose image: `ghcr.io/sjleekor/sdc:v0.11.4`
- 현재 상시 기동 서비스: `db`만 기동. `collector`는 Cronicle wrapper가 `docker compose run --rm collector ...`로 작업마다 실행한다.

원격 `compose.yaml`과 `bin/*.sh` checksum은 현재 로컬 `deploy/prod`와 일치한다.

> 리팩터 메모(2026-07): sj2는 raw 수집 전용이다. `sdc_daily_common_build`,
> `sdc_daily_common_coverage`, `sdc_daily_common_readiness`, `sdc_daily_metrics_normalize`
> 이벤트는 P4에서 Cronicle에서 제거됐다. 파생 metric/common marts와 readiness는 compute
> 노드에서 `bin/parquet-compute-all.sh`로 실행한다.

## 배포 방법

운영 파일은 서버에서 직접 수정하지 않는다. 이 디렉터리를 수정한 뒤 아래 스크립트로 반영한다.

```bash
./deploy/deploy_to_sj2.sh
```

이 스크립트는 `deploy/prod/compose.yaml`과 `deploy/prod/bin/`을 `sj2-server:/home/whi/apps/sdc`로 `rsync --delete`한다.

> ### ⚠ 배포 전에 이미지 태그를 반드시 맞춘다
>
> **릴리즈 스크립트(`sdc-release`)는 원격 `compose.yaml`만 갱신하고 이 저장소의 파일은
> 건드리지 않았다.** 그래서 릴리즈를 할수록 둘이 벌어지고, **문서대로
> `deploy_to_sj2.sh`를 돌리면 prod가 조용히 롤백된다** — 에러도 안 난다.
>
> 실제로 v0.9.3 릴리즈 시점에 원격은 `v0.9.3`, 저장소는 `v0.8.16`이었다.
> **네 개 릴리즈만큼 뒤처져 있었고 아무것도 그 사실을 보고하지 않았다.**
>
> ```bash
> # 배포 전 확인 — 두 값이 같아야 한다
> grep 'image: ghcr' deploy/prod/compose.yaml
> ssh whi@sj2-server 'grep "image: ghcr" /home/whi/apps/sdc/compose.yaml'
> ```
>
> 로컬 스킬 스크립트에는 저장소 파일도 같이 갱신하도록 고쳐뒀지만, `.agents/`와
> `.claude/`가 `.gitignore`에 있어 **그 수정은 공유되지 않는다.**
> 다른 사람의 환경에서는 여전히 원격만 갱신되므로 위 확인을 거른 채 배포하면 안 된다.

## Cronicle event chain

2026-08-28 Cronicle API 기준 event는 19개다. 정기 event 16개와 수동 one-time event 3개가
있으며 모두 `enabled=1`, `plugin=shellplug`, `category=general`, `target=maingrp`,
`timezone=Asia/Seoul`, `max_children=1`, `multiplex=0`, `catch_up=0`이다. `chain_error`는
모두 비어 있으므로 실패 시 별도 실패 분기로 가지 않고 그 지점에서 chain이 멈춘다.

```mermaid
flowchart TD
  subgraph KRX["KRX daily chain (Mon-Fri 18:30)"]
    U["sdc_daily_fdr_universe<br/>universe-sync.sh<br/>FDR universe"]
    P["sdc_daily_pykrx_prices<br/>prices-backfill-incremental.sh<br/>PYKRX prices"]
    F["sdc_daily_krx_flows<br/>flows-sync.sh<br/>KRX flows"]
    KC["sdc_daily_krx_common<br/>common-sync-krx.sh<br/>KRX common features"]
    U --> P --> F --> KC
  end

  subgraph CommonSync["Common source sync (Mon-Fri 20:30)"]
    CFDR["sdc_daily_fdr_common<br/>common-sync-fdr.sh"]
    CFRED["sdc_daily_fred_common<br/>common-sync-fred.sh"]
    CECOSD["sdc_daily_ecos_common_daily<br/>common-sync-ecos-daily.sh"]
    CECOSM["sdc_daily_ecos_common_macro<br/>common-sync-ecos-macro.sh"]
    CECOSD --> CECOSM
  end

  subgraph OpenDART["OpenDART daily chain (Every day 04:00)"]
    DC["sdc_daily_opendart_corp<br/>dart-sync-corp.sh"]
    DF["sdc_daily_opendart_financials<br/>dart-sync-financials.sh"]
    DS["sdc_daily_opendart_share_info<br/>dart-sync-share-info.sh"]
    DX["sdc_daily_opendart_xbrl<br/>dart-sync-xbrl.sh"]
    DC --> DF --> DS --> DX
  end
```

시간대별로 보면 아래와 같다.

```text
04:00 daily     OpenDART Corp -> Financials -> Share Info -> XBRL
18:30 Mon-Fri  FDR Universe -> PYKRX Prices -> KRX Flows -> KRX Common
19:00 Mon-Fri  KIS Foreign Holding
20:00 Mon-Fri  KRX Open API Market Cap (최근 30일 gap scan, 원천 T+1)
20:30 Mon-Fri  FDR Common, FRED Common, ECOS Daily -> ECOS Macro
20:30 Monday    KIS Investor/Shorting
23:00 daily     Raw Freshness Gate
```

`common_build`/coverage/readiness와 `metrics_normalize`는 더 이상 sj2 compute 책임이 아니다. raw 미러/export 후 compute 노드에서 Parquet/DuckDB 게이트를 실행한다.

## Event 목록

| Event id | Trigger | Chain next | Wrapper | 설명 |
|---|---:|---|---|---|
| `sdc_daily_fdr_universe` | Mon-Fri 18:30 | `sdc_daily_pykrx_prices` | `universe-sync.sh` | FDR 기준 KOSPI/KOSDAQ universe를 동기화한다. |
| `sdc_daily_pykrx_prices` | chain-only | `sdc_daily_krx_flows` | `prices-backfill-incremental.sh` | 전체 시장 일봉 가격을 증분 backfill한다. 기본 lookback은 0일, 자동 range guard는 10일이다. |
| `sdc_daily_krx_flows` | chain-only | `sdc_daily_krx_common` | `flows-sync.sh` | 가격 최신일을 기준으로 KRX 수급 데이터를 증분 동기화한다. 기본 lookback은 14일, 자동 range guard는 30일이다. |
| `sdc_daily_krx_common` | chain-only | 없음 | `common-sync-krx.sh` | KRX 계열 common feature raw series를 증분 동기화한다. 현재는 `krx_flows` 성공 후에만 실행된다. |
| `sdc_kis_flows_trial` | Mon-Fri 19:00 | 없음 | `flows-sync-kis.sh` | KIS `foreign_holding`을 전 종목 동기화한다. |
| `sdc_daily_market_cap` | Mon-Fri 20:00 | 없음 | `prices-market-cap-backfill.sh` | 최근 30일의 `daily_market_cap` gap을 확인하고 T+1 원천의 빠진 세션을 채운다. |
| `sdc_daily_fdr_common` | Mon-Fri 20:30 | 없음 | `common-sync-fdr.sh` | FDR common feature raw series를 증분 동기화한다. |
| `sdc_daily_fred_common` | Mon-Fri 20:30 | 없음 | `common-sync-fred.sh` | FRED common feature raw series를 증분 동기화한다. |
| `sdc_daily_ecos_common_daily` | Mon-Fri 20:30 | `sdc_daily_ecos_common_macro` | `common-sync-ecos-daily.sh` | ECOS 일간 common feature raw series를 증분 동기화한다. |
| `sdc_daily_ecos_common_macro` | chain-only | 없음 | `common-sync-ecos-macro.sh` | ECOS 월간 macro series(`macro_cpi`, `macro_ppi`, `macro_m2`, `macro_consumer_sentiment`)를 긴 lookback으로 동기화한다. |
| `sdc_kis_flows_weekly` | Monday 20:30 | 없음 | `flows-sync-kis.sh` | KIS investor/shorting 지표를 주 1회 동기화한다. |
| `sdc_daily_freshness` | daily 23:00 | 없음 | `ops-freshness-report.sh` | 저녁 수집이 끝난 뒤 raw 최신일을 검사하고 stale이면 실패한다. |
| `sdc_daily_opendart_corp` | daily 04:00 | `sdc_daily_opendart_financials` | `dart-sync-corp.sh` | OpenDART corp master를 동기화한다. |
| `sdc_daily_opendart_financials` | chain-only | `sdc_daily_opendart_share_info` | `dart-sync-financials.sh` | OpenDART 재무제표를 증분 동기화한다. 기본 lookback은 1년, attempt guard는 10,000건이다. |
| `sdc_daily_opendart_share_info` | chain-only | `sdc_daily_opendart_xbrl` | `dart-sync-share-info.sh` | 주식수, 배당, 자기주식 관련 OpenDART 데이터를 증분 동기화한다. Cronicle script에 `DART_SHARE_INFO_MAX_ATTEMPT_TARGETS=35000` override가 있다. |
| `sdc_daily_opendart_xbrl` | chain-only | 없음 | `dart-sync-xbrl.sh` | OpenDART XBRL 데이터를 증분 동기화한다. 기본 attempt guard는 10,000건이다. |

## Wrapper와 lock/throttle

대부분의 daily wrapper는 `bin/lib/sdc-wrapper.sh`를 source하고 `docker compose run --rm collector ...`를 호출한다. wrapper의 주요 실행 함수는 아래와 같다.

- `sdc_run_collector`: source lock 없이 collector command를 실행한다.
- `sdc_run_daily_collector <domain> ...`: host-side source lock을 잡고 실행한다. **기본이 ON**이며 `SDC_DAILY_USE_SOURCE_LOCK=0`으로만 끌 수 있다.
- `sdc_run_collector_with_lock <domain> ...`: 항상 source lock을 잡고 실행한다. manual backfill wrapper에서 주로 쓴다.

> **2026-08-15 변경.** `sdc_run_daily_collector`의 락은 원래 opt-in(`=1`일 때만)이었고,
> **prod은 켠 적이 없었다** — `.env`에도, 호스트 env에도, Cronicle 이벤트 스크립트에도 없었고
> `/tmp/sdc-locks` 자체가 존재하지 않았다. 락을 잡는 것처럼 읽히는 wrapper 7개가 실제로는
> 아무것도 잡고 있지 않았다는 뜻이다. 기본값을 ON으로 뒤집었다. 스케줄 시간대 분리는
> 대체재가 아니다 — 창을 넘기는 run(flows 실측 775s, 백필은 구조적으로 수 시간)이
> 조용히 동시 실행을 되살린다.

source lock은 `/tmp/sdc-locks/<domain>.lock`에 `flock`을 걸고, lock 획득 직후 `/tmp/sdc-throttle/<domain>.last`를 이용해 source별 최소 간격을 둔다. lock conflict 기본 mode는 `fail`이며 conflict 시 exit code 75를 반환한다.

| Domain | 주로 쓰는 wrapper | 기본 throttle |
|---|---|---:|
| `fdr` | `universe-sync.sh`, `common-sync-fdr.sh` | 10s |
| `fred` | `common-sync-fred.sh` | 10s |
| `ecos` | `common-sync-ecos-daily.sh`, `common-sync-ecos-macro.sh` | 10s |
| `krx_marketdata` | `prices-backfill-incremental.sh`, `flows-sync.sh`, `common-sync-krx.sh`, `common-sync-pykrx.sh` | 60s |
| `opendart` | OpenDART sync wrappers, OpenDART backfill | 5s |

### KRX 요청 페이스 (2026-08-16 정정)

**`data.krx.co.kr`을 때리는 모든 수집기는 하나의 정책을 쓴다.** 어떤 라이브러리를
경유하든 상대는 같은 포털이다.

| 설정 | 기본값 | 의미 |
|---|---:|---|
| `KRX_MIN_DELAY_SECONDS` / `KRX_MAX_DELAY_SECONDS` | 1.5 / 4.0 | 요청 간격(랜덤) |
| `KRX_LONG_REST_EVERY` | 15 | 롱 레스트 주기 |
| `KRX_LONG_REST_MIN_SECONDS` / `_MAX_` | 30 / 90 | 롱 레스트 길이 |
| `KRX_ERROR_BACKOFF_MIN_SECONDS` / `_MAX_` | 45 / 180 | **에러 후 백오프** |
| `KRX_AUTH_COOLDOWN_SECONDS` | 10 | 로그인 후 쿨다운 |

이전에는 pykrx 계열(`prices market-cap-backfill`, `universe backfill-snapshots`)만
따로 `--rate-limit-seconds`로 **0.1~0.4초**를 썼고 **에러 백오프가 아예 없었다.**
같은 호스트에 두 개의 페이스가 있었던 셈이고, 2026-08-16에 KRX가
`자동화 수단을 통한 비정상 대량 조회`로 이 호스트의 IP를 제한했다.

두 명령의 per-run override는 `--min-delay-seconds` / `--max-delay-seconds`뿐이다.
나머지는 위 설정을 따른다. 래퍼는 더 이상 페이스를 고정하지 않는다.

> `prices backfill`은 해당 없다. `adjusted=True`라 pykrx가 KRX가 아니라 **naver**로 간다.

#### 그런데 이걸로 풀리지 않았다 (같은 날 확인)

페이스를 통일한 뒤 **0.32 req/s**(직전의 1/3)로 재개했는데 **95요청 만에 다시 차단**됐다.
1차 차단은 9시간 만에 풀렸으나 재차단은 5분 만에 왔다.

**차단 사유가 속도가 아니다.** KRX Data Marketplace 약관 제10조 제2호는
자동화 수단에 의한 수집 자체를 금지한다 — 속도 조건이 붙어 있지 않다.
**위 설정은 남아 있는 KRX 수집기(`flows sync`, `common sync --sources krx`)의
최소 예의일 뿐, 차단에 대한 해결책이 아니다.**

해결은 공식 경로(KRX Open API)로 옮기는 것이다.
`docs/operations.md` "KRX 접근 제한" 절과
`docs/dev/20260731_raw_features/02_data_expansion_plan/poc/krx_open_api.md` 참고.

주의할 점:

- Cronicle event definition 자체에는 `env` 필드가 없다. event script에 직접 들어간 override는 현재 `sdc_daily_opendart_share_info`의 `DART_SHARE_INFO_MAX_ATTEMPT_TARGETS=35000`뿐이다.
- `SDC_DAILY_USE_SOURCE_LOCK`, `SDC_LOCK_WAIT_SECONDS` 같은 host-side 실행 환경값은 Cronicle process 환경까지 함께 확인해야 한다.
- 최근 KRX job log에서는 `krx_marketdata` lock 대기와 exit 75가 관찰됐다. exit 75는 같은 source domain 작업이 이미 lock을 잡고 있어 대기 시간 안에 시작하지 못했다는 뜻이다.

## 배포되어 있지만 Cronicle event가 아닌 wrapper

아래 파일은 `deploy/prod/bin/`에 배포되지만 현재 Cronicle schedule에는 등록되어 있지 않다.

| Wrapper | 용도 |
|---|---|
| `common-features-refresh.sh` | common catalog seed와 source sync만 수행하는 raw refresh wrapper. 파생 mart/readiness는 compute 노드에서 실행한다. |
| `common-seed-catalog.sh` | common feature catalog schema/data 초기화. |
| `common-sync-pykrx.sh` | PYKRX common source 동기화. 현재 운영 필수 source에는 포함되지 않고 Cronicle event도 없다. |
| `dart-backfill-all-years.sh` | OpenDART 연도별 대량 backfill. 기본은 전체 backfill 구간을 `opendart` lock으로 감싼다. |
| `flows-backfill-range.sh` | `FLOW_START`, `FLOW_END`로 지정한 수급 range를 수동 backfill한다. 항상 `krx_marketdata` lock을 사용한다. |
| `db-init.sh` | collector의 `db init` 실행. |
| `pull-image.sh` | collector image pull. |
| `up-db.sh` | DB service 기동. |
| `validate.sh` | `validate --market all` 실행. |

`flows-sync.sh.bak.20260425_2352`는 배포 디렉터리에 남아 있는 backup 파일이며 Cronicle event에서 호출하지 않는다.

## 현재 schedule에서 특히 헷갈리기 쉬운 점

- `sdc_daily_krx_common`은 현재 독립 21:30 schedule이 아니다. 2026-06-16 23:00:17 KST에 `sdc_daily_krx_flows -> sdc_daily_krx_common` chain으로 변경됐다. 그 이전 history에는 21:30 독립 실행 기록이 남아 있을 수 있다.
- `sdc_daily_opendart_share_info`는 2026-06-15 22:48:40 KST에 Cronicle script override로 `DART_SHARE_INFO_MAX_ATTEMPT_TARGETS=35000`이 들어갔다. wrapper 기본값은 10,000이므로 backlog 해소 후 계속 필요한지 재검토해야 한다.
- 모든 event는 `catch_up=0`이다. Cronicle이 꺼져 있거나 schedule 시각을 놓친 경우 자동 catch-up 실행을 기대하면 안 된다.

## 운영 점검 명령

Cronicle API key는 repo 밖의 secret 파일에서 읽고, 값 자체를 출력하거나 문서에 붙이지 않는다.

```bash
APIKEY=$(awk '$1=="APIKEY:" {print $2}' /Users/whishaw/wss_p/stock_data_collector_secrets/cronicle_info)

# 현재 schedule
curl -fsS -H "X-API-Key: $APIKEY" \
  'http://sj2-server:3012/api/app/get_schedule/v1' | jq '.rows[] | {id,title,enabled,timing,chain,modified}'

# 특정 event 최근 history
curl -fsS -H "X-API-Key: $APIKEY" \
  'http://sj2-server:3012/api/app/get_event_history/v1?id=sdc_daily_common_build&limit=5' | jq '.rows[] | {id,time_start,elapsed,code,description}'

# 특정 job log. Cronicle log API는 gzip 본문을 줄 수 있어 --compressed를 붙인다.
curl --compressed -fsS -H "X-API-Key: $APIKEY" \
  'http://sj2-server:3012/api/app/get_job_log/v1?id=<job-id>&format=text'

# 운영 compose 상태
ssh whi@sj2-server 'cd /home/whi/apps/sdc && docker compose ps'

# 운영 wrapper 확인
ssh whi@sj2-server 'cd /home/whi/apps/sdc/bin && find . -type f | sort'
```
