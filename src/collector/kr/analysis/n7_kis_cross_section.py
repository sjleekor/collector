"""Collect one KIS valuation cross-section and compare it with the PIT mart.

N7 was reduced from a 6,000-session KRX history backfill to a one-date mapping
diagnostic.  One ``inquire-price`` call is sent per active KOSPI/KOSDAQ ticker.
The JSONL checkpoint makes the run resume-safe and preserves the public quote
payload without introducing a production table.

Example::

    uv run python -m collector.kr.analysis.n7_kis_cross_section \
        --snapshot-date 2026-08-23 --requests-per-second 5
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from collector.kr.adapters.flows_kis.parsers import (
    INQUIRE_PRICE_PATH,
    INQUIRE_PRICE_TR_ID,
)
from collector.kr.adapters.kis_common.client import KisClient, KisResponseError
from collector.kr.adapters.kis_common.token import KisTokenCache, KisTokenProvider
from collector.kr.infra.config.settings import get_settings
from collector.kr.util.rate_limit import TokenBucket
from collector.kr.util.time import now_kst, today_kst

LOGGER = logging.getLogger(__name__)
REMOTE_SOURCE = "sj2_remote"


@dataclass(frozen=True)
class _DataRoot:
    """stock_data/<market>/ — just the ``raw``/``derived`` branches N7 reads.

    A local, minimal stand-in: this file is the one script that crosses from
    ``collector/`` into the shared lake, so it does not depend on
    ``modeler/``'s ``research.etl.config.DataRoot`` (a separate repo since
    the S4/S5 split).
    """

    base: Path

    @classmethod
    def resolve(cls, market: str = "kr") -> _DataRoot:
        root = os.environ.get("STOCK_DATA_ROOT")
        if not root:
            raise RuntimeError(
                "STOCK_DATA_ROOT가 없습니다. .envrc를 확인하십시오 (direnv allow)."
            )
        return cls(Path(root) / market)

    @property
    def raw(self) -> Path:
        return self.base / "raw"

    @property
    def derived(self) -> Path:
        return self.base / "derived"


def parse_decimal(value: object) -> float | None:
    """Parse one KIS numeric field, keeping zero so missing policy stays explicit."""
    text = str(value or "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def load_targets(*, root: _DataRoot, snapshot_date: str, source: str) -> list[dict[str, str]]:
    path = (
        root.raw
        / "raw_postgres"
        / f"snapshot_date={snapshot_date}"
        / f"source={source}"
        / "stock_master"
        / "schema_version=1"
        / "*.parquet"
    )
    con = duckdb.connect()
    rows = con.execute(
        """
        SELECT ticker, market, name
        FROM read_parquet(?)
        WHERE status = 'ACTIVE' AND market IN ('KOSPI', 'KOSDAQ')
        ORDER BY market, ticker
        """,
        [str(path)],
    ).fetchall()
    con.close()
    return [{"ticker": str(t), "market": str(m), "name": str(n)} for t, m, n in rows]


def load_checkpoint(path: Path) -> dict[str, dict[str, Any]]:
    """Return the newest successful record per ticker from an append-only checkpoint."""
    records: dict[str, dict[str, Any]] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return records
    for line in lines:
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("status") in {"ok", "no_data"}:
            records[str(record["ticker"])] = record
    return records


def append_checkpoint(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def flatten_response(
    *,
    target: dict[str, str],
    output: dict[str, Any],
    fetched_at: str,
    status: str = "ok",
) -> dict[str, Any]:
    """Keep analysis fields typed while retaining the complete quote payload."""
    return {
        **target,
        "status": status,
        "fetched_at": fetched_at,
        "stck_prpr": parse_decimal(output.get("stck_prpr")),
        "per": parse_decimal(output.get("per")),
        "pbr": parse_decimal(output.get("pbr")),
        "eps": parse_decimal(output.get("eps")),
        "bps": parse_decimal(output.get("bps")),
        "response_json": json.dumps(output, ensure_ascii=False, sort_keys=True),
    }


def collect(
    *,
    targets: list[dict[str, str]],
    checkpoint_path: Path,
    requests_per_second: float,
    limit: int | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    settings = get_settings()
    if not settings.kis_app_key or not settings.kis_app_secret:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET are not configured")

    complete = load_checkpoint(checkpoint_path)
    selected = targets if limit is None else targets[:limit]
    pending = [target for target in selected if target["ticker"] not in complete]
    print(
        f"N7 KIS targets={len(selected)} resumed={len(selected) - len(pending)} "
        f"pending={len(pending)} rate={requests_per_second}/s"
    )
    if not pending:
        return [complete[target["ticker"]] for target in selected], {
            "http_requests": 0,
            "http_retries": 0,
            "token_issued": 0,
            "token_cache_hits": 0,
        }

    token_provider = KisTokenProvider(
        app_key=settings.kis_app_key,
        app_secret=settings.kis_app_secret,
        base_url=settings.kis_base_url,
        cache=KisTokenCache(
            settings.kis_token_cache_path,
            refresh_margin_seconds=settings.kis_token_refresh_margin_seconds,
        ),
        timeout_seconds=settings.kis_timeout_seconds,
    )
    client = KisClient(
        token_provider=token_provider,
        app_key=settings.kis_app_key,
        app_secret=settings.kis_app_secret,
        base_url=settings.kis_base_url,
        bucket=TokenBucket(requests_per_second, burst=settings.kis_max_burst_requests),
        timeout_seconds=settings.kis_timeout_seconds,
    )

    failures = 0
    for index, target in enumerate(pending, start=1):
        try:
            response = client.get(
                INQUIRE_PRICE_PATH,
                tr_id=INQUIRE_PRICE_TR_ID,
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": target["ticker"]},
            )
            rows = response.rows("output")
            record = flatten_response(
                target=target,
                output=rows[0] if rows else {},
                fetched_at=now_kst().isoformat(),
                status="ok" if rows else "no_data",
            )
            complete[target["ticker"]] = record
            append_checkpoint(checkpoint_path, record)
            failures = 0
        except KisResponseError as exc:
            failures += 1
            error_record = {
                **target,
                "status": "error",
                "fetched_at": now_kst().isoformat(),
                "error": str(exc),
            }
            append_checkpoint(checkpoint_path, error_record)
            LOGGER.warning("N7 KIS %s failed: %s", target["ticker"], exc)
            if failures >= 10:
                raise RuntimeError("10 consecutive KIS failures; stopping resume-safe run") from exc
        if index % 100 == 0 or index == len(pending):
            print(
                f"N7 KIS progress={index}/{len(pending)} "
                f"http={client.stats.http_requests} retries={client.stats.http_retries}"
            )

    stats = client.stats.as_counts()
    complete_rows = [
        complete[target["ticker"]] for target in selected if target["ticker"] in complete
    ]
    return complete_rows, stats


def _one_row(con: duckdb.DuckDBPyConnection, sql: str) -> dict[str, Any]:
    result = con.execute(sql)
    columns = [item[0] for item in result.description]
    return dict(zip(columns, result.fetchone(), strict=True))


def analyze(
    *,
    rows: list[dict[str, Any]],
    root: _DataRoot,
    snapshot_date: str,
    source: str,
    as_of_date: date,
    output_dir: Path,
    request_stats: dict[str, int],
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    flat_path = output_dir / "n7_kis_cross_section.parquet"
    # from_pylist infers the schema from every row (all values are str or
    # float|None out of flatten_response), matching the previous
    # infer_schema_length=None. zstd keeps the codec this file already uses
    # for its other Parquet output below.
    pq.write_table(pa.Table.from_pylist(rows), flat_path, compression="zstd")

    feature_path = (
        root.derived
        / "feature"
        / f"snapshot_date={snapshot_date}"
        / f"source={source}"
        / "feat_fin_scan_daily"
        / "*.parquet"
    )
    price_path = (
        root.raw
        / "raw_postgres"
        / f"snapshot_date={snapshot_date}"
        / f"source={source}"
        / "daily_ohlcv"
        / "schema_version=1"
        / "**"
        / "*.parquet"
    )
    corp_path = (
        root.raw
        / "raw_postgres"
        / f"snapshot_date={snapshot_date}"
        / f"source={source}"
        / "dart_corp_master"
        / "schema_version=1"
        / "*.parquet"
    )

    con = duckdb.connect()
    con.read_parquet(str(flat_path)).create_view("kis")
    con.read_parquet(str(feature_path)).create_view("fin")
    con.read_parquet(str(price_path), hive_partitioning=True).create_view("prices")
    con.read_parquet(str(corp_path)).create_view("corp")
    con.execute(
        """
        CREATE TEMP VIEW joined AS
        WITH latest AS (SELECT max(trade_date) AS trade_date FROM fin),
        f AS (
            SELECT * FROM fin WHERE trade_date = (SELECT trade_date FROM latest)
        ),
        p AS (
            SELECT * FROM prices WHERE trade_date = (SELECT trade_date FROM latest)
        ),
        c AS (
            SELECT ticker, market, any_value(induty_code) AS induty_code
            FROM corp GROUP BY ticker, market
        )
        SELECT
            k.*,
            f.trade_date AS canonical_date,
            f.fin_book_to_market,
            f.fin_earnings_yield,
            f.fin_log_mcap,
            f.value_component_count,
            f.fin_value_z,
            f.value_fin_age_days,
            p.close AS canonical_close,
            c.induty_code,
            CASE WHEN k.pbr > 0 THEN 1.0 / k.pbr END AS kis_book_to_market,
            CASE WHEN k.per > 0 THEN 1.0 / k.per END AS kis_earnings_yield,
            CASE WHEN f.fin_book_to_market IS NOT NULL AND p.close > 0
                 THEN f.fin_book_to_market * p.close END AS canonical_bps_proxy,
            CASE WHEN f.fin_earnings_yield IS NOT NULL AND p.close > 0
                 THEN f.fin_earnings_yield * p.close END AS canonical_eps_proxy,
            CASE WHEN f.fin_book_to_market IS NOT NULL AND p.close > 0 AND k.stck_prpr > 0
                 THEN f.fin_book_to_market * p.close / k.stck_prpr END
                AS aligned_fin_book_to_market,
            CASE WHEN f.fin_earnings_yield IS NOT NULL AND p.close > 0 AND k.stck_prpr > 0
                 THEN f.fin_earnings_yield * p.close / k.stck_prpr END
                AS aligned_fin_earnings_yield
        FROM kis k
        LEFT JOIN f USING (ticker, market)
        LEFT JOIN p USING (trade_date, ticker, market)
        LEFT JOIN c USING (ticker, market)
        """
    )
    con.execute(
        """
        COPY (SELECT * FROM joined ORDER BY market, ticker)
        TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [str(output_dir / "n7_kis_joined.parquet")],
    )

    coverage = _one_row(
        con,
        """
        SELECT
            count(*) AS targets,
            count(*) FILTER (WHERE status = 'ok') AS response_ok,
            count(*) FILTER (WHERE pbr > 0) AS pbr_valid,
            count(*) FILTER (WHERE per > 0) AS per_valid,
            count(*) FILTER (WHERE bps IS NOT NULL) AS bps_present,
            count(*) FILTER (WHERE eps IS NOT NULL) AS eps_present,
            count(*) FILTER (WHERE fin_book_to_market IS NOT NULL) AS canonical_bm_valid,
            count(*) FILTER (WHERE fin_earnings_yield IS NOT NULL) AS canonical_ep_valid,
            min(canonical_date) AS canonical_date,
            count(*) FILTER (WHERE pbr > 0 AND fin_book_to_market IS NOT NULL) AS bm_pairs,
            count(*) FILTER (WHERE per > 0 AND fin_earnings_yield IS NOT NULL) AS ep_pairs
        FROM joined
        """,
    )
    c1 = _one_row(
        con,
        """
        WITH ranked AS (
            SELECT *,
                rank() OVER (PARTITION BY market ORDER BY kis_book_to_market) AS kis_bm_rank,
                rank() OVER (PARTITION BY market ORDER BY fin_book_to_market) AS raw_bm_rank,
                rank() OVER (PARTITION BY market ORDER BY aligned_fin_book_to_market)
                    AS aligned_bm_rank,
                rank() OVER (PARTITION BY market ORDER BY bps) AS kis_bps_rank,
                rank() OVER (PARTITION BY market ORDER BY canonical_bps_proxy)
                    AS canonical_bps_rank,
                rank() OVER (PARTITION BY market ORDER BY kis_earnings_yield) AS kis_ep_rank,
                rank() OVER (PARTITION BY market ORDER BY fin_earnings_yield) AS raw_ep_rank,
                rank() OVER (PARTITION BY market ORDER BY aligned_fin_earnings_yield)
                    AS aligned_ep_rank,
                rank() OVER (PARTITION BY market ORDER BY eps) AS kis_eps_rank,
                rank() OVER (PARTITION BY market ORDER BY canonical_eps_proxy)
                    AS canonical_eps_rank
            FROM joined
        )
        SELECT
            corr(kis_bm_rank, raw_bm_rank)
                FILTER (WHERE kis_book_to_market IS NOT NULL AND fin_book_to_market IS NOT NULL)
                AS bm_rank_corr_raw,
            corr(kis_bm_rank, aligned_bm_rank)
                FILTER (WHERE kis_book_to_market IS NOT NULL
                        AND aligned_fin_book_to_market IS NOT NULL) AS bm_rank_corr_aligned,
            corr(kis_bps_rank, canonical_bps_rank)
                FILTER (WHERE bps IS NOT NULL AND canonical_bps_proxy IS NOT NULL)
                AS bps_rank_corr,
            corr(kis_ep_rank, raw_ep_rank)
                FILTER (WHERE kis_earnings_yield IS NOT NULL AND fin_earnings_yield IS NOT NULL)
                AS ep_rank_corr_raw,
            corr(kis_ep_rank, aligned_ep_rank)
                FILTER (WHERE kis_earnings_yield IS NOT NULL
                        AND aligned_fin_earnings_yield IS NOT NULL) AS ep_rank_corr_aligned,
            corr(kis_eps_rank, canonical_eps_rank)
                FILTER (WHERE eps IS NOT NULL AND canonical_eps_proxy IS NOT NULL)
                AS eps_rank_corr
        FROM ranked
        """,
    )
    c3 = _one_row(
        con,
        """
        SELECT
            count(*) FILTER (WHERE per IS NULL OR per <= 0) AS kis_per_missing,
            count(*) FILTER (WHERE fin_earnings_yield IS NULL OR fin_earnings_yield <= 0)
                AS canonical_ep_missing_or_loss,
            count(*) FILTER (WHERE value_component_count < 2 OR value_component_count IS NULL)
                AS i1_vulnerable,
            count(*) FILTER (
                WHERE (per IS NULL OR per <= 0)
                  AND (fin_earnings_yield IS NULL OR fin_earnings_yield <= 0)
            ) AS per_vs_ep_overlap,
            count(*) FILTER (
                WHERE (per IS NULL OR per <= 0)
                  AND (value_component_count < 2 OR value_component_count IS NULL)
            ) AS per_vs_i1_overlap
        FROM joined
        """,
    )

    disagreement_sql = """
        WITH ranked AS (
            SELECT *,
                percent_rank() OVER (PARTITION BY market ORDER BY kis_book_to_market)
                    AS kis_rank,
                percent_rank() OVER (PARTITION BY market ORDER BY aligned_fin_book_to_market)
                    AS canonical_rank
            FROM joined
            WHERE kis_book_to_market IS NOT NULL AND aligned_fin_book_to_market IS NOT NULL
        )
        SELECT *, abs(kis_rank - canonical_rank) AS rank_gap
        FROM ranked
        ORDER BY rank_gap DESC, market, ticker
    """
    # A TEMP TABLE, not a view: the window functions below run once and the
    # three queries after the COPY all read the materialized rows. Going
    # through a pandas frame (fetchdf + register) did the same thing with an
    # extra copy out of and back into DuckDB.
    con.execute(f"CREATE TEMP TABLE disagreement_rows AS {disagreement_sql}")
    con.execute(
        "COPY disagreement_rows TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
        [str(output_dir / "n7_bm_rank_disagreement.parquet")],
    )
    c2 = _one_row(
        con,
        """
        SELECT
            median(rank_gap) AS median_rank_gap,
            quantile_cont(rank_gap, 0.9) AS p90_rank_gap,
            avg(value_fin_age_days) FILTER (WHERE rank_gap >= 0.25) AS large_gap_fin_age_mean,
            median(fin_log_mcap) FILTER (WHERE rank_gap >= 0.25) AS large_gap_log_mcap_median,
            count(*) FILTER (WHERE rank_gap >= 0.25) AS large_gap_names
        FROM disagreement_rows
        """,
    )
    by_market_result = con.execute(
        """
        SELECT market, count(*) AS pairs, corr(kis_rank, canonical_rank) AS rank_corr,
               median(rank_gap) AS median_rank_gap, quantile_cont(rank_gap, 0.9) AS p90_rank_gap
        FROM disagreement_rows GROUP BY market ORDER BY market
        """
    )
    by_market = [
        dict(zip([item[0] for item in by_market_result.description], row, strict=True))
        for row in by_market_result.fetchall()
    ]
    top_industry_result = con.execute(
        """
        SELECT substr(induty_code, 1, 2) AS industry2, count(*) AS pairs,
               median(rank_gap) AS median_rank_gap
        FROM disagreement_rows
        WHERE induty_code IS NOT NULL
        GROUP BY 1 HAVING count(*) >= 10
        ORDER BY median_rank_gap DESC, pairs DESC LIMIT 10
        """
    )
    top_industries = [
        dict(zip([item[0] for item in top_industry_result.description], row, strict=True))
        for row in top_industry_result.fetchall()
    ]
    con.close()

    def ratio(num: int | None, den: int | None) -> float | None:
        return None if not den else float(num or 0) / float(den)

    c3["per_vs_ep_recall"] = ratio(c3["per_vs_ep_overlap"], c3["kis_per_missing"])
    c3["per_vs_i1_recall"] = ratio(c3["per_vs_i1_overlap"], c3["kis_per_missing"])
    payload = {
        "run": {
            "as_of_date": as_of_date.isoformat(),
            "snapshot_date": snapshot_date,
            "source": source,
            "canonical_date": str(coverage["canonical_date"]),
            "request_stats": request_stats,
        },
        "coverage": coverage,
        "c1_mapping": c1,
        "c2_disagreement": c2,
        "c2_by_market": by_market,
        "c2_top_industries": top_industries,
        "c3_i1_overlap": c3,
        "c4_disposition": "discarded: KIS inquire_price has no DIV field",
        "c5_disposition": (
            "discarded: one current cross-section cannot produce future-return IC; "
            "do not promote KIS valuation to a feature"
        ),
        "scope_limit": (
            "KIS validates B/M and E/P only; CFO/P, S/P and I7 revenue mapping "
            "remain outside scope"
        ),
    }
    (output_dir / "n7_kis_analysis.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    report = format_report(payload)
    (output_dir / "n7_kis_analysis.md").write_text(report, encoding="utf-8")
    return payload


def _fmt(value: object, digits: int = 4) -> str:
    return "—" if value is None else f"{float(value):.{digits}f}"


def format_report(payload: dict[str, Any]) -> str:
    run = payload["run"]
    cov = payload["coverage"]
    c1 = payload["c1_mapping"]
    c2 = payload["c2_disagreement"]
    c3 = payload["c3_i1_overlap"]
    stats = run["request_stats"]
    lines = [
        "# N7 KIS 횡단면 대조",
        "",
        f"- 실행일: {run['as_of_date']}",
        f"- canonical 입력: snapshot `{run['snapshot_date']}` / source `{run['source']}` / "
        f"최신 거래일 `{run['canonical_date']}`",
        f"- 대상/정상 응답: {cov['targets']:,} / {cov['response_ok']:,}",
        f"- KIS HTTP 요청/재시도/token 발급: {stats.get('http_requests', 0):,} / "
        f"{stats.get('http_retries', 0):,} / {stats.get('token_issued', 0):,}",
        "",
        "## 결론",
        "",
        "KIS 대조 범위는 B/M과 E/P뿐이다. CFO/P·S/P와 I7의 revenue 매핑은 확인하지 못한다.",
        "C4는 `DIV` 필드가 없어 폐기한다. C5는 현재 횡단면으로 미래수익률 IC를 만들 수 없어 "
        "폐기하며 KIS 밸류를 feature로 승격하지 않는다.",
        "",
        "## C1 — 매핑 rank correlation",
        "",
        "| 대조 | 전체 |",
        "|---|---:|",
        f"| 1/PBR ↔ canonical B/M (원시 날짜) | {_fmt(c1['bm_rank_corr_raw'])} |",
        f"| 1/PBR ↔ canonical B/M (가격 정렬) | {_fmt(c1['bm_rank_corr_aligned'])} |",
        f"| BPS ↔ canonical BPS proxy | {_fmt(c1['bps_rank_corr'])} |",
        f"| 1/PER ↔ canonical E/P (원시 날짜) | {_fmt(c1['ep_rank_corr_raw'])} |",
        f"| 1/PER ↔ canonical E/P (가격 정렬) | {_fmt(c1['ep_rank_corr_aligned'])} |",
        f"| EPS ↔ canonical EPS proxy | {_fmt(c1['eps_rank_corr'])} |",
        "",
        f"B/M pair는 {cov['bm_pairs']:,}개, E/P pair는 {cov['ep_pairs']:,}개다.",
        "가격 정렬 값은 canonical 최신 종가 대비 KIS 현재가 비율만 반영한다.",
        "",
        "## C2 — B/M 불일치",
        "",
        f"시장 내 rank gap 중앙값은 {_fmt(c2['median_rank_gap'])}, p90은 "
        f"{_fmt(c2['p90_rank_gap'])}다. 0.25 이상인 종목은 {c2['large_gap_names']:,}개다.",
        "",
        "| 시장 | pair | rank corr | gap 중앙값 | gap p90 |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in payload["c2_by_market"]:
        lines.append(
            f"| {row['market']} | {row['pairs']:,} | {_fmt(row['rank_corr'])} | "
            f"{_fmt(row['median_rank_gap'])} | {_fmt(row['p90_rank_gap'])} |"
        )
    lines.extend(
        [
            "",
            "## C3 — PER 결측과 I1 취약 집합",
            "",
            f"- KIS PER 결측/0: {c3['kis_per_missing']:,}개",
            f"- canonical E/P 결측 또는 손실: {c3['canonical_ep_missing_or_loss']:,}개",
            f"- 겹침: {c3['per_vs_ep_overlap']:,}개 "
            f"({_fmt(c3['per_vs_ep_recall'] * 100, 1)}%)"
            if c3["per_vs_ep_recall"] is not None
            else f"- 겹침: {c3['per_vs_ep_overlap']:,}개 (—)",
            f"- I1 취약 집합(`<2` component): {c3['i1_vulnerable']:,}개",
            f"- KIS PER 결측과 I1 취약 집합 겹침: {c3['per_vs_i1_overlap']:,}개 "
            f"({_fmt(c3['per_vs_i1_recall'] * 100, 1)}%)"
            if c3["per_vs_i1_recall"] is not None
            else f"- KIS PER 결측과 I1 취약 집합 겹침: {c3['per_vs_i1_overlap']:,}개 (—)",
            "",
            "## C4/C5 처분",
            "",
            "- C4: 폐기. KIS `inquire_price`에는 `DIV`가 없다.",
            "- C5: 폐기. 한 시점의 현재값으로 미래수익률 IC를 계산하지 않는다.",
            "- N7 historical 백필과 `daily_market_fundamental` 테이블은 되살리지 않는다.",
            "",
        ]
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--source", default=REMOTE_SOURCE)
    parser.add_argument("--data-lake-root", type=Path, default=None)
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=today_kst())
    parser.add_argument("--requests-per-second", type=float, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    settings = get_settings()
    rate = args.requests_per_second or settings.kis_requests_per_second
    root = _DataRoot(args.data_lake_root) if args.data_lake_root else _DataRoot.resolve(market="kr")
    output_root = args.output_root or (root.output / "scan" / "n7_kis_cross_section")
    output_dir = (
        output_root
        / f"as_of_date={args.as_of_date.isoformat()}"
        / f"snapshot_date={args.snapshot_date}"
        / f"source={args.source}"
    )
    targets = load_targets(
        root=root,
        snapshot_date=args.snapshot_date,
        source=args.source,
    )
    rows, stats = collect(
        targets=targets,
        checkpoint_path=output_dir / "checkpoint.jsonl",
        requests_per_second=rate,
        limit=args.limit,
    )
    if args.limit is not None:
        print("N7 sample collection complete; analysis is skipped when --limit is set")
        return 0
    payload = analyze(
        rows=rows,
        root=root,
        snapshot_date=args.snapshot_date,
        source=args.source,
        as_of_date=args.as_of_date,
        output_dir=output_dir,
        request_stats=stats,
    )
    print(format_report(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
