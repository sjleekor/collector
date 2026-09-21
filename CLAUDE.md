# CLAUDE.md — collector

**수집 코드 프로젝트다.** 원천 접근, 적재, 정제, 스키마, 스케줄이 여기 있다.
모델 학습·평가는 `modeler/`, **데이터 파일은 `stock_data/`다.**

배치 원칙은 [`../CLAUDE.md`](../CLAUDE.md)에 있다.

---

## 현재 상태

**한국·미국 수집 코드가 다 들어와 있다.** 버전 `0.15.0`.
prod(sj2-server)가 `ghcr.io/sjleekor/collector:v0.15.0`으로 **두 시장 모두** 정기 수집을 돌린다.

| | 언제 | Cronicle |
|---|---|---|
| 한국 | 2026-09-14 (KR 분리) | 이벤트 18개 · 18:30\~23:30 등 |
| **미국** | **2026-09-20 (C0\~C8)** | **`sdc_daily_us` · 매일 15:00 KST** |

```
src/collector/
├── cli/app.py     진입점 — `uv run collector`. kr 파서에 us-* 를 얹는다
├── lake.py        stock_data/<시장>/ 경로 계약 (kr·us 공용, modeler 도 쓴다)
├── us/            미국 시장 — sources/ ops/ store/ universe/ validate/ cli/
└── kr/            한국 시장 — 옛 stock_data_collector/krx_collector에서 옮겨옴
    ├── adapters/  원천별 어댑터 (KRX·DART·KIS 등)
    ├── domain/    도메인 타입
    ├── service/   유스케이스
    ├── infra/     DB·파일 I/O
    ├── ports/     포트 인터페이스
    ├── analysis/  수집 품질 검사
    ├── cli/       `collector` 서브커맨드
    ├── shared/    양쪽 시장이 쓰는 순수 타입
    └── util/
```

```bash
uv sync --extra dev
uv run pytest
uv run ruff check src/ tests/
uv run black src/ tests/
uv run collector --help
```

설정은 [`../CLAUDE.md`](../CLAUDE.md)의 공통 툴체인을 따른다. **프로젝트마다 다르게 잡지 않는다.**

미국 수집 계획과 실행 결과는
[`../my/milestones/us/plan/20260912_collect/00_candidate_plan/`](../my/milestones/us/plan/20260912_collect/00_candidate_plan/README.md)에 있다.
**표 18장의 계약과 실측은 `03_schema_and_pit.md` §4**, **단계별 결과는 `04_collection_steps.md` §2**다.
조사는 [`../my/milestones/us/research/`](../my/milestones/us/research/README.md)다.

**저장소는 `sjleekor/collector`다.** 한국과 미국 수집 코드가 모두 여기 들어간다.
그래서 이름에 시장이 없다.

한국 시장 코드는 패키지 이름이 `krx_collector`에서 `collector.kr`로 바뀌었다(758곳 치환).
분리 경과는 [`../my/milestones/kr/refactoring/20260912_project_split/02_result.md`](../my/milestones/kr/refactoring/20260912_project_split/02_result.md)에 있다.
`pyproject.toml`은 한국용 의존성을 base로 포함한다(시장별 extra로 안 나눈다). **prod 배포(sj2-server)를 이 저장소가 맡는다.**

---

## 수집 코드가 지켜야 할 것

조사에서 나온 결론이다. **코드로 옮길 때 이것들이 전제다.**

| 원칙 | 왜 |
|---|---|
| **원시값과 이벤트만 저장한다** | 조정된 값을 저장하면 PIT가 깨진다. 조정은 읽을 때 계산한다 |
| **받은 날짜를 같이 남긴다** | 원천이 과거를 고친다. `observed_at` 없이는 되돌릴 수 없다 |
| **HTTP 200을 성공으로 보지 않는다** | 원천마다 성공 판정기가 다르다. 본문을 봐야 한다 |
| **천천히 받는다** | 기본 요청 간격 5초. 날짜당 1요청 경로를 종목당 1요청보다 우선한다. **원천별로 낮춘 값은 코드에 근거와 같이 적는다** (SEC는 5초 그대로, FINRA·Wikipedia·FRED 1초, Nasdaq 1.5초) |
| **인증 값은 환경변수로만 넘긴다** | URL·로그·문서·커밋에 값을 남기지 않는다 (`direnv exec` 사용) |
| **재수집이 안 되는 데이터는 즉시 parquet으로 남긴다** | 원천이 막히면 다시 못 받는다 |

---

## 데이터를 어디에 쓰나

**`stock_data/`다. 이 저장소 안에 데이터를 쓰지 않는다.**

| 항목 | 값 |
|---|---|
| 기본 경로 | `../stock_data/<시장>/` — **최상위가 시장이다** (`kr/`, `us/`) |
| 계층 | `raw/`(재수집으로만 복원) · `derived/` · `datasets/` · `output/` |
| 수집이 쓰는 곳 | **`raw/`다.** 원천에서 받은 것을 그대로 둔다 |
| 바꾸는 법 | 환경변수. 코드에 절대경로를 박지 않는다 |
| git | 산출 데이터는 추적하지 않는다 |
| 예외 | 테스트 fixture처럼 코드와 같이 읽어야 하는 수 KB 파일은 저장소에 둔다 |

---

## 미국 쪽에서 알아 둘 것 (2026-09-20)

| | |
|---|---|
| **명령** | `us-daily run`(raw 하루치) · **`us-derive run`(raw→derived, 주 1회)** · **`us-tickers sync`(과거 티커 맵)** · `us-load <table>` · `us-calendar build` · `us-universe rebuild` · `us-prune` |
| **한 줄로 돈다** | 할 일을 일정이 아니라 **`raw/`에 무엇이 있나**로 만든다. backfill과 상시 운영이 같은 함수를 쓴다 |
| **`us-daily`는 raw만 받는다** | 굳히는 것은 **`us-derive`가 따로 한다.** 2026-09-21까지 그 자리가 없어서 `dolt pull`은 매일 도는데 `prices_daily` 스냅샷이 2026-09-09에 멈춰 있었다 |
| **티커 → CIK 는 PIT 다** | `universe_daily.cik` 이 SEC 의 **오늘자** 맵에서 왔었다 — 상폐·개명 회사가 빠져 `cik` 이 붙었나가 곧 "지금도 살아 있나"였다(남은 종목 98.9% 대 사라진 종목 26.5%). **`us-tickers sync` 로 Wayback 스냅샷 562개를 받아 `ASOF` 로 붙인다** (`build.TICKER_PIT_JOIN`). 합집합으로 합치면 안 된다 — 티커 재사용이 4.34% 다 |
| **날짜를 기본값에 박지 마라** | 세 번 났다. `universe end="2026-09-09"`(유니버스가 이미 안 자라고 있었다) · `calendar end="2026-12-31"`(2027-01-01에 수집이 조용히 멈출 참이었다) · `coverage end`. **끝은 데이터(`prices` 최대일)나 지평(오늘+N년)이 정한다.** `test_us_no_frozen_dates.py` 가 막는다 |
| **무엇을 굳힐지는 입력이 정한다** | dolt는 커밋 해시(`source_rev`), 나머지는 `raw/` mtime을 스냅샷과 비교한다. 굳히는 법과 판단 근거는 전부 `us/ops/derive.py`의 `RECIPES`에 있다 — `us-load` 목록도 거기서 나온다 |
| **`dolt`가 이미지에 있다** | 2.3.5. 미국 가격·IV/HV 원천이다. 없으면 `us-daily`가 첫 원천에서 멈춘다 |
| **레이크 정본은 서버다** | `/home/whi/data/stock_data/us`. 맥은 `deploy/local/us-mirror.sh`로 당겨 읽는다 |
| **태그를 밀면 두 시장이 같이 나간다** | `v0.15.0`부터 미국 코드가 prod 이미지에 들어 있다. 태그 전에 `env -i ... collector --help`를 본다 |

**남은 것은 `modeler/`에 미국이 들어오는 일이다.** 지금 `modeler/`는 한국만 안다.
