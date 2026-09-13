# CLAUDE.md — collector

**수집 코드 프로젝트다.** 원천 접근, 적재, 정제, 스키마, 스케줄이 여기 있다.
모델 학습·평가는 `modeler/`, **데이터 파일은 `stock_data/`다.**

배치 원칙은 [`../CLAUDE.md`](../CLAUDE.md)에 있다.

---

## 현재 상태

**뼈대만 있다 (2026-09-12).** Python 3.12 + `uv` 환경이 서 있고 CLI 진입점과 테스트가 통과한다.
수집 로직은 아직 없다.

```
src/collector/
├── cli/app.py     진입점 — `uv run collector`
├── us/            미국 시장
└── kr/            한국 시장 — stock_data_collector 에서 옮겨올 자리
```

```bash
uv sync --extra dev
uv run pytest
uv run ruff check src/ tests/
uv run black src/ tests/
uv run collector --help
```

설정은 [`../CLAUDE.md`](../CLAUDE.md)의 공통 툴체인을 따른다. **프로젝트마다 다르게 잡지 않는다.**

조사는 끝나 있다. 무엇을 어디서 어떻게 받을지는
[`../my/milestones/us/research/`](../my/milestones/us/research/README.md)에 정리돼 있고,
수집 설계는 [`90_collection_design.md`](../my/milestones/us/research/data/web_scraping/90_collection_design.md)에 있다.

**저장소는 `sjleekor/collector`다** (2026-09-12 확정). 한국과 미국 수집 코드가 모두 여기
들어간다. 그래서 이름에 시장이 없다.

한국 시장 수집 코드는 `stock_data_collector/`에 있고 `src/collector/kr/`로 옮길 예정이다 —
[분리 계획](../my/milestones/kr/refactoring/20260912_project_split/00_candidate_plan/README.md).
옮겨오면서 패키지 이름이 `krx_collector`에서 `collector.kr`로 바뀌고(758곳 치환),
`pyproject.toml`에 한국용 의존성이 합쳐진다. **prod 배포(sj2-server)도 이 저장소가 이어받는다.**

---

## 수집 코드가 지켜야 할 것

조사에서 나온 결론이다. **코드로 옮길 때 이것들이 전제다.**

| 원칙 | 왜 |
|---|---|
| **원시값과 이벤트만 저장한다** | 조정된 값을 저장하면 PIT가 깨진다. 조정은 읽을 때 계산한다 |
| **받은 날짜를 같이 남긴다** | 원천이 과거를 고친다. `observed_at` 없이는 되돌릴 수 없다 |
| **HTTP 200을 성공으로 보지 않는다** | 원천마다 성공 판정기가 다르다. 본문을 봐야 한다 |
| **천천히 받는다** | 기본 요청 간격 5초. 날짜당 1요청 경로를 종목당 1요청보다 우선한다 |
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

## 아직 정해지지 않은 것

- 데이터는 `../stock_data/<시장>/`에 있다 — `us/raw/`, `kr/raw/`. **경로를 코드에 박지 않고 환경변수로 받는다**
- 한국 수집 코드가 `stock_data_collector/`에서 넘어올 때의 통합 방식
