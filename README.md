# collector

시장 데이터 수집 파이프라인. 원천 접근·적재·정제·스키마·스케줄을 담당한다.

역할 경계와 지켜야 할 원칙은 [`CLAUDE.md`](CLAUDE.md)와 [`../CLAUDE.md`](../CLAUDE.md)에 있다.
설계·운영 문서는 [`my/milestones/kr/pipeline/`](../my/milestones/kr/pipeline/)에 있다
(비공개 저장소라 외부에서는 링크가 열리지 않는다).

## 설치

```bash
uv sync --extra dev
cp .env.example .env      # 값은 직접 채운다 — 커밋되는 파일엔 값을 적지 않는다
cp .envrc.example .envrc && direnv allow
```

## 실행

```bash
uv run collector db init
uv run collector universe sync --source krx-openapi --markets kospi,kosdaq
uv run collector prices backfill --market all
uv run collector prices backfill --market all --incremental   # 이후 실행
uv run collector validate --date 2026-01-15 --market all
```

서브커맨드: `db` · `ops` · `dart` · `common` · `flows` · `universe` · `prices` · `profile` · `validate`.
전체 옵션은 `uv run collector <서브커맨드> --help`.

## Docker

```bash
docker build -t collector .
docker run --rm --env-file .env collector db init
```

`docker-compose.yml`로 PostgreSQL과 같이 띄울 수도 있다.

```bash
docker compose up -d
docker compose run --rm collector db init
```

## 개발

```bash
uv run pytest
uv run ruff check src/ tests/
uv run black src/ tests/
```

## 어느 엔드포인트에 어느 키가 필요한가

`.env.example`에 전체 목록이 있다. 주로 쓰는 것만 적는다.

| 명령 | 필요한 키 |
|---|---|
| `dart *` | `OPENDART_API_KEY` |
| `common sync --sources ecos` | `ECOS_API_KEY` |
| `common sync --sources fred` | `FRED_API_KEY` |
| `flows sync-kis` | `KIS_APP_KEY`, `KIS_APP_SECRET` |
| `universe sync --source krx-openapi`, `prices market-cap-backfill` | `AUTH_KEYS` (KRX Open API) |
| `db sync-remote`, `db with-remote-dsn` | `REMOTE_DB_INFO_PATH` (없으면 `--db-info-path`) |

## KIS / KRX Open API 데이터 이용 범위

이 프로젝트가 KIS Developers·KRX Open API로 수집한 데이터는 **사용자가 직접 관리하는
비공개 환경에서 개인 연구·백테스트 용도로만** 사용한다. 공유·재배포·상업적 이용은
계획하지 않는다 — 각 API의 이용약관을 따르기 위한 현재 프로젝트 범위다.
