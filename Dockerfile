# dolt 는 미국 가격·IV/HV 원천이다 (DoltHub clone/pull). 파이썬 라이브러리가
# 아니라 바이너리라 이미지에 넣는다 — 호스트에 깔고 bind-mount 하면 이미지에
# 안 적힌 호스트 의존이 생기고, 그 호스트에서만 도는 이미지가 된다.
# 버전을 고정한다. dolt 는 merge 동작이 판마다 달라질 수 있다.
FROM debian:bookworm-slim AS dolt
ARG DOLT_VERSION=2.3.5
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -fsSL "https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-amd64.tar.gz" \
       | tar -xz -C /tmp \
    && install -m 0755 /tmp/dolt-linux-amd64/bin/dolt /usr/local/bin/dolt \
    && /usr/local/bin/dolt version

FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/sjleekor/collector"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    TZ=Asia/Seoul \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/
COPY --from=dolt /usr/local/bin/dolt /usr/local/bin/dolt

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

COPY src ./src
COPY sql ./sql

RUN uv sync --frozen --no-dev

ENTRYPOINT ["collector"]
CMD ["--help"]
