# collector

시장 데이터 수집 파이프라인. 원천 접근·적재·정제·스키마·스케줄을 담당한다.

역할 경계와 지켜야 할 원칙은 [`CLAUDE.md`](CLAUDE.md)와 [`../CLAUDE.md`](../CLAUDE.md)에 있다.
조사 결과는 [`../my/milestones`](../my/milestones)에 있다.

## 개발

```bash
uv sync --extra dev
uv run pytest
uv run ruff check src/ tests/
uv run black src/ tests/
uv run collector --help
```
