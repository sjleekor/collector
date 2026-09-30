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

# raw-parquet-exporter 는 sj2 DB 의 raw 표를 parquet 로 내보내는 Rust 바이너리다.
# sj2 에는 toolchain 이 없고 셸에서 crate 를 받지도 않는다 — 이미지 빌드(GitHub
# Actions)에서 만들어 이미지에 넣는다. 최종 이미지(python:3.12-slim)는 2026-09-30
# 기준 Debian 13 trixie(glibc 2.41)다. builder 는 더 오래된 bookworm(glibc 2.36)이라
# 여기서 링크한 바이너리가 새 glibc 에서도 돈다(반대 방향은 안 된다). 의존 crate 는 순수
# Rust + zstd-sys(정적 C)라 openssl 같은 공유 라이브러리가 더 필요 없다.
# rust-version 1.83 이상이면 되고 1.90 은 그 위다. 더 고정하려면 tag 뒤에
# @sha256:<digest> 를 붙인다 (digest 는 빌드 가능한 곳에서
# `docker buildx imagetools inspect rust:1.90.0-bookworm` 로 확인해 넣는다).
# --locked 는 Cargo.lock 과 어긋나면 조용히 갱신하지 않고 실패하게 한다.
FROM rust:1.90.0-bookworm AS raw-parquet-exporter
WORKDIR /build
COPY tools/raw-parquet-exporter/Cargo.toml tools/raw-parquet-exporter/Cargo.lock ./
COPY tools/raw-parquet-exporter/src ./src
RUN cargo build --locked --release \
    && install -m 0755 target/release/raw-parquet-exporter /usr/local/bin/raw-parquet-exporter \
    && /usr/local/bin/raw-parquet-exporter --help >/dev/null

FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/sjleekor/collector"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    TZ=Asia/Seoul \
    SDC_RAW_PARQUET_BIN=/usr/local/bin/raw-parquet-exporter \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata jq \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/
COPY --from=dolt /usr/local/bin/dolt /usr/local/bin/dolt
COPY --from=raw-parquet-exporter /usr/local/bin/raw-parquet-exporter /usr/local/bin/raw-parquet-exporter

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

COPY src ./src
COPY sql ./sql
# KR raw export: 래퍼 스크립트와 20개 표 설정만 넣는다 (Rust 소스·target 은 안 넣는다).
# 스크립트는 python3·jq·bash 를 쓴다 (jq 는 위에서 설치).
COPY bin/raw-parquet-export-all.sh ./bin/raw-parquet-export-all.sh
COPY tools/raw-parquet-exporter/config/export_tables.toml ./tools/raw-parquet-exporter/config/export_tables.toml

RUN uv sync --frozen --no-dev

ENTRYPOINT ["collector"]
CMD ["--help"]
