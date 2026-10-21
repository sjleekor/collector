"""Drift guard for the KR table lists that are maintained by hand in several places.

Compared lists (DDL is the source of truth for what a table is):
  DDL      sql/postgres_ddl.sql                      (CREATE TABLE, comments stripped)
  SYNC     remote_sync.SYNC_TABLE_SPECS              (spec.name)
  RAW_TOML tools/raw-parquet-exporter/config/export_tables.toml
  RAW_SH   bin/raw-parquet-export-all.sh             (three bash arrays)

Separate family, only compared with each other (NOT with the DDL):
  export_canonical_tables.toml == the ``tables=(...)`` array in
  bin/canonical-parquet-export-all.sh.

Tables that legitimately differ are listed with a reason in the exclusion dicts below.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from collector.kr.infra.db_postgres.remote_sync import SYNC_TABLE_SPECS

ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = ROOT / "sql" / "postgres_ddl.sql"
RAW_TOML_PATH = ROOT / "tools" / "raw-parquet-exporter" / "config" / "export_tables.toml"
RAW_SH_PATH = ROOT / "bin" / "raw-parquet-export-all.sh"
CANON_TOML_PATH = (
    ROOT / "tools" / "raw-parquet-exporter" / "config" / "export_canonical_tables.toml"
)
CANON_SH_PATH = ROOT / "bin" / "canonical-parquet-export-all.sh"

RAW_SH_ARRAYS = ("raw_id_tables", "date_month_tables", "non_resumable_tables")

# DDL tables that are intentionally absent from the sync specs and the raw export.
DDL_ONLY_EXCLUSIONS: dict[str, str] = {
    "ingestion_runs": "실행 기록(bookkeeping)이라 미러·export 대상이 아님",
    "sync_checkpoints": "체크포인트(bookkeeping)라 미러·export 대상이 아님",
    "collection_slice_state": "수집 슬라이스 상태(bookkeeping)라 미러·export 대상이 아님",
    # 2026-10-07 추가. 사용자 결정 대기:
    # my/milestones/common/20261020_collector_refactoring/01_plan.md R1
    "dart_exchange_notice_raw": "확인 필요: 사용자 결정 대기(01_plan.md R1). 등록 여부 미정",
    "dart_exchange_notice_window": "확인 필요: 사용자 결정 대기(01_plan.md R1). 등록 여부 미정",
    "kis_stock_status_daily": (
        "확인 필요: 사용자 결정 대기(01_plan.md R1). "
        "export 등록은 브랜치 kr-4-2b-export-registration에 있음"
    ),
}

# Names in the sync specs / raw export that are no longer DDL tables.
DROPPED_TABLE_EXCLUSIONS: dict[str, str] = {
    name: (
        "sql/migrations/20260728_drop_derived_tables.sql 로 DROP됨. "
        "remote_sync에 spec이 남아 있음. 삭제는 결정 대기"
    )
    for name in (
        "metric_catalog",
        "metric_mapping_rule",
        "stock_metric_fact",
        "common_feature_catalog",
        "common_feature_catalog_input",
        "common_feature_daily_fact",
    )
}


# ---------------------------------------------------------------- parsers


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", "", sql)


def parse_ddl_tables(sql: str) -> set[str]:
    pattern = re.compile(
        r"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?\"?(\w+)\"?\s*\(",
        re.IGNORECASE,
    )
    return set(pattern.findall(_strip_sql_comments(sql)))


def parse_toml_tables(text: str) -> list[str]:
    return [t["name"] for t in tomllib.loads(text)["tables"]]


def parse_shell_array(script: str, array_name: str) -> list[str]:
    match = re.search(rf"^{re.escape(array_name)}=\(\s*\n(.*?)^\)", script, re.M | re.S)
    assert match, f"bash array {array_name}=(...) not found"
    items: list[str] = []
    for line in match.group(1).splitlines():
        line = line.split("#", 1)[0].strip()
        items.extend(line.split())
    return items


# ---------------------------------------------------------------- comparison


def find_table_list_problems(
    *,
    ddl: set[str],
    sync: set[str],
    raw_toml: set[str],
    raw_sh: set[str],
    ddl_only: dict[str, str],
    dropped: dict[str, str],
) -> list[str]:
    """Return human-readable problems; empty list means the lists agree."""
    problems: list[str] = []
    exported = sync | raw_toml | raw_sh

    # B. raw toml == raw shell
    for t in sorted(raw_toml - raw_sh):
        problems.append(
            f"{t}: export_tables.toml에는 있고 bin/raw-parquet-export-all.sh 배열"
            f"({'/'.join(RAW_SH_ARRAYS)})에는 없음. 배열에 넣거나 toml에서 빼십시오"
        )
    for t in sorted(raw_sh - raw_toml):
        problems.append(
            f"{t}: bin/raw-parquet-export-all.sh 배열에는 있고 export_tables.toml에는 없음. "
            "toml에 [[tables]]를 넣거나 배열에서 빼십시오"
        )

    # A. every DDL table is mirrored and exported unless excluded
    for t in sorted(ddl - set(ddl_only)):
        if t not in sync:
            problems.append(
                f"{t}: DDL에는 있는데 remote_sync SYNC_TABLE_SPECS에 없음. "
                "spec을 추가하거나 DDL_ONLY_EXCLUSIONS에 이유와 함께 넣으십시오"
            )
        if t not in raw_toml:
            problems.append(
                f"{t}: DDL에는 있는데 export_tables.toml에 없음. "
                "toml(과 raw-parquet-export-all.sh 배열)에 추가하거나 "
                "DDL_ONLY_EXCLUSIONS에 이유와 함께 넣으십시오"
            )

    # C. names outside DDL must be known dropped tables
    for t in sorted(exported - ddl - set(dropped)):
        where = [
            label
            for label, s in (
                ("remote_sync SYNC_TABLE_SPECS", sync),
                ("export_tables.toml", raw_toml),
                ("raw-parquet-export-all.sh 배열", raw_sh),
            )
            if t in s
        ]
        problems.append(
            f"{t}: DDL에 없는 표가 {', '.join(where)}에 있음. "
            "그 목록에서 지우거나 DDL을 고치거나 DROPPED_TABLE_EXCLUSIONS에 넣으십시오"
        )

    # D. stale exclusions
    for t in sorted(ddl_only):
        if t not in ddl:
            problems.append(
                f"{t}: DDL_ONLY_EXCLUSIONS에 있는데 DDL에 표가 없음. "
                "DDL_ONLY_EXCLUSIONS에서 지우십시오"
            )
        if t in exported:
            problems.append(
                f"{t}: DDL_ONLY_EXCLUSIONS에 있는데 이미 sync spec 또는 export에 등록됨. "
                "DDL_ONLY_EXCLUSIONS에서 지우십시오"
            )
    for t in sorted(dropped):
        if t in ddl:
            problems.append(
                f"{t}: DROPPED_TABLE_EXCLUSIONS에 있는데 DDL에 표가 있음. "
                "DROPPED_TABLE_EXCLUSIONS에서 지우십시오"
            )
        if t not in exported:
            problems.append(
                f"{t}: DROPPED_TABLE_EXCLUSIONS에 있는데 어느 목록에도 없음(정리 끝남). "
                "DROPPED_TABLE_EXCLUSIONS에서 지우십시오"
            )
    return problems


# ---------------------------------------------------------------- loaders


def _load_sets() -> dict[str, set[str]]:
    script = RAW_SH_PATH.read_text(encoding="utf-8")
    raw_sh: set[str] = set()
    for name in RAW_SH_ARRAYS:
        items = parse_shell_array(script, name)
        assert len(items) == len(set(items)), f"duplicate in {name}: {items}"
        raw_sh |= set(items)
    return {
        "ddl": parse_ddl_tables(DDL_PATH.read_text(encoding="utf-8")),
        "sync": {spec.name for spec in SYNC_TABLE_SPECS},
        "raw_toml": set(parse_toml_tables(RAW_TOML_PATH.read_text(encoding="utf-8"))),
        "raw_sh": raw_sh,
    }


@pytest.fixture(scope="module")
def sets() -> dict[str, set[str]]:
    return _load_sets()


# ---------------------------------------------------------------- tests


def test_table_lists_agree(sets: dict[str, set[str]]) -> None:
    problems = find_table_list_problems(
        **sets, ddl_only=DDL_ONLY_EXCLUSIONS, dropped=DROPPED_TABLE_EXCLUSIONS
    )
    assert not problems, "KR 표 목록 불일치:\n  " + "\n  ".join(problems)


def test_parsers_see_expected_shapes(sets: dict[str, set[str]]) -> None:
    # Guards the parsers themselves (e.g. commented-out CREATE TABLE must not count).
    assert len(sets["ddl"]) == 26
    assert "intraday_ohlcv" not in sets["ddl"]
    assert len(sets["raw_toml"]) == 20
    assert len(sets["raw_sh"]) == 20


def test_raw_shell_all_tables_uses_the_three_arrays() -> None:
    script = RAW_SH_PATH.read_text(encoding="utf-8")
    refs = {item.strip('"') for item in parse_shell_array(script, "all_tables")}
    assert refs == {f"${{{name}[@]}}" for name in RAW_SH_ARRAYS}


def test_canonical_toml_matches_canonical_shell() -> None:
    toml_names = parse_toml_tables(CANON_TOML_PATH.read_text(encoding="utf-8"))
    sh_names = parse_shell_array(CANON_SH_PATH.read_text(encoding="utf-8"), "tables")
    assert len(toml_names) == len(set(toml_names)) and len(sh_names) == len(set(sh_names))
    assert set(toml_names) == set(sh_names), (
        f"export_canonical_tables.toml에만 있음: {sorted(set(toml_names) - set(sh_names))}, "
        f"bin/canonical-parquet-export-all.sh tables 배열에만 있음: "
        f"{sorted(set(sh_names) - set(toml_names))}"
    )


# ---------------------------------------------------------------- negative tests


def _mutated(sets: dict[str, set[str]], **changes: set[str]) -> dict[str, set[str]]:
    return {**{k: set(v) for k, v in sets.items()}, **changes}


def _problems(s: dict[str, set[str]]) -> list[str]:
    return find_table_list_problems(
        **s, ddl_only=DDL_ONLY_EXCLUSIONS, dropped=DROPPED_TABLE_EXCLUSIONS
    )


def test_detects_ddl_table_missing_from_toml(sets: dict[str, set[str]]) -> None:
    s = _mutated(sets, raw_toml=sets["raw_toml"] - {"daily_ohlcv"})
    text = "\n".join(_problems(s))
    assert "daily_ohlcv" in text and "export_tables.toml" in text


def test_detects_ddl_table_missing_from_sync(sets: dict[str, set[str]]) -> None:
    s = _mutated(sets, sync=sets["sync"] - {"daily_ohlcv"})
    assert any("daily_ohlcv" in p and "SYNC_TABLE_SPECS" in p for p in _problems(s))


@pytest.mark.parametrize("table", sorted(DDL_ONLY_EXCLUSIONS))
def test_detects_stale_ddl_only_exclusion(sets: dict[str, set[str]], table: str) -> None:
    s = _mutated(sets, raw_toml=sets["raw_toml"] | {table}, raw_sh=sets["raw_sh"] | {table})
    text = "\n".join(_problems(s))
    assert table in text and "DDL_ONLY_EXCLUSIONS에서 지우십시오" in text


def test_detects_shell_array_losing_a_table(sets: dict[str, set[str]]) -> None:
    s = _mutated(sets, raw_sh=sets["raw_sh"] - {"stock_master"})
    assert any("stock_master" in p and "raw-parquet-export-all.sh" in p for p in _problems(s))


def test_detects_unknown_non_ddl_table(sets: dict[str, set[str]]) -> None:
    s = _mutated(sets, sync=sets["sync"] | {"ghost_table"})
    assert any("ghost_table" in p and "DROPPED_TABLE_EXCLUSIONS" in p for p in _problems(s))


def test_detects_stale_dropped_exclusion(sets: dict[str, set[str]]) -> None:
    s = _mutated(sets, sync=sets["sync"] - {"metric_catalog"})
    assert any("metric_catalog" in p and "어느 목록에도 없음" in p for p in _problems(s))
    s = _mutated(sets, ddl=sets["ddl"] | {"metric_catalog"})
    assert any("metric_catalog" in p and "DDL에 표가 있음" in p for p in _problems(s))


def test_parse_ddl_ignores_comments() -> None:
    sql = (
        "-- CREATE TABLE IF NOT EXISTS a (\n/* CREATE TABLE b ( */\n"
        "-- note: CREATE TABLE IF NOT EXISTS stays\nCREATE TABLE IF NOT EXISTS c (\n"
        "CREATE TABLE d (\n"
    )
    assert parse_ddl_tables(sql) == {"c", "d"}
