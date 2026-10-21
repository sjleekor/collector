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
    "dart_exchange_notice_window": "거래소공시 수집 진행 장부(bookkeeping)라 미러·export 안 함",
    # export 대기 (결정 ⑮, 2026-10-07): modeler verify_raw·compute_all이 raw 표 집합을
    # 그대로 요구하므로, 11월 modeler 선택 표 변경과 같은 날 export에 넣는다. 그날
    # remote_sync에는 넣지 않고 NOT_SYNCED_EXCLUSIONS로 옮긴다 (2026-10-21 결정,
    # my/milestones/common/20261020_collector_refactoring/README.md §5).
    "dart_exchange_notice_raw": "export 대기(결정 ⑮) — 그날 NOT_SYNCED_EXCLUSIONS로 옮긴다",
    "kis_stock_status_daily": (
        "export 대기(결정 ⑮, 등록 커밋은 kr-4-2b-export-registration) "
        "— 그날 NOT_SYNCED_EXCLUSIONS로 옮긴다"
    ),
}

# DDL tables that are exported to raw parquet but intentionally NOT mirrored by remote_sync.
# remote_sync has been a dev-only path since 2026-09-30, so a new table may reasonably be
# exported without being synced. Such a table must be in export_tables.toml and the shell
# arrays, must not be in SYNC_TABLE_SPECS, and must not also be in DDL_ONLY_EXCLUSIONS.
# Empty until the 결정 ⑮ export day: then kis_stock_status_daily (branch
# kr-4-2b-export-registration) and dart_exchange_notice_raw move here from
# DDL_ONLY_EXCLUSIONS (2026-10-21 decision,
# my/milestones/common/20261020_collector_refactoring/README.md section 5).
NOT_SYNCED_EXCLUSIONS: dict[str, str] = {}

# Names in the sync specs / raw export that are no longer DDL tables.
DROPPED_TABLE_EXCLUSIONS: dict[str, str] = {
    name: (
        "sql/migrations/20260728_drop_derived_tables.sql 로 DROP됨. "
        "remote_sync에 spec이 남아 있음. R4a ⓪에서 spec을 지우고 여기서도 뺀다(2026-10-21 결정)"
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
    not_synced: dict[str, str],
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
        if t not in sync and t not in not_synced:
            problems.append(
                f"{t}: DDL에는 있는데 remote_sync SYNC_TABLE_SPECS에 없음. "
                "spec을 추가하거나 export만 하는 표면 NOT_SYNCED_EXCLUSIONS, "
                "둘 다 안 하는 표면 DDL_ONLY_EXCLUSIONS에 이유와 함께 넣으십시오"
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
    for t in sorted(not_synced):
        if t not in ddl:
            problems.append(
                f"{t}: NOT_SYNCED_EXCLUSIONS에 있는데 DDL에 표가 없음. "
                "NOT_SYNCED_EXCLUSIONS에서 지우십시오"
            )
        if t in sync:
            problems.append(
                f"{t}: NOT_SYNCED_EXCLUSIONS에 있는데 이미 remote_sync SYNC_TABLE_SPECS에 있음. "
                "NOT_SYNCED_EXCLUSIONS에서 지우십시오"
            )
        if t not in raw_toml or t not in raw_sh:
            problems.append(
                f"{t}: NOT_SYNCED_EXCLUSIONS에 있는데 export_tables.toml 또는 "
                "raw-parquet-export-all.sh 배열에 없음(export는 되어야 함). "
                "두 곳에 추가하거나 NOT_SYNCED_EXCLUSIONS에서 지우십시오"
            )
        if t in ddl_only:
            problems.append(
                f"{t}: NOT_SYNCED_EXCLUSIONS와 DDL_ONLY_EXCLUSIONS에 모두 있음. "
                "표는 한 분류에만 둘 수 있으니 둘 중 한 곳에서 지우십시오"
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
        **sets,
        ddl_only=DDL_ONLY_EXCLUSIONS,
        dropped=DROPPED_TABLE_EXCLUSIONS,
        not_synced=NOT_SYNCED_EXCLUSIONS,
    )
    assert not problems, "KR 표 목록 불일치:\n  " + "\n  ".join(problems)


def test_parsers_see_expected_shapes(sets: dict[str, set[str]]) -> None:
    # Only catches parser malfunction; adding a table legitimately must not break this.
    raw_ddl = DDL_PATH.read_text(encoding="utf-8")
    no_block_comments = re.sub(r"/\*.*?\*/", "", raw_ddl, flags=re.DOTALL)
    independent_count = sum(
        1
        for line in no_block_comments.splitlines()
        if re.match(r"^\s*CREATE\s+TABLE", line, re.IGNORECASE)
    )
    assert len(sets["ddl"]) == independent_count
    assert "intraday_ohlcv" not in sets["ddl"]
    assert len(sets["ddl"]) >= 20
    assert len(sets["raw_toml"]) >= 15
    assert len(sets["raw_sh"]) >= 15


def test_raw_shell_all_tables_uses_the_three_arrays() -> None:
    script = RAW_SH_PATH.read_text(encoding="utf-8")
    refs = {item.strip('"') for item in parse_shell_array(script, "all_tables")}
    assert refs == {f"${{{name}[@]}}" for name in RAW_SH_ARRAYS}


def test_canonical_toml_matches_canonical_shell() -> None:
    toml_names = parse_toml_tables(CANON_TOML_PATH.read_text(encoding="utf-8"))
    sh_names = parse_shell_array(CANON_SH_PATH.read_text(encoding="utf-8"), "tables")
    assert toml_names and sh_names, "canonical 표 목록이 비어 있음(파서 오작동 의심)"
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
        **s,
        ddl_only=DDL_ONLY_EXCLUSIONS,
        dropped=DROPPED_TABLE_EXCLUSIONS,
        not_synced=NOT_SYNCED_EXCLUSIONS,
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


# ---------------------------------------------------------------- NOT_SYNCED tests


def _with_not_synced(
    sets: dict[str, set[str]],
    not_synced: dict[str, str],
    ddl_only: dict[str, str] | None = None,
) -> list[str]:
    return find_table_list_problems(
        **sets,
        ddl_only=DDL_ONLY_EXCLUSIONS if ddl_only is None else ddl_only,
        dropped=DROPPED_TABLE_EXCLUSIONS,
        not_synced=not_synced,
    )


def test_not_synced_allows_exported_but_unsynced_table(sets: dict[str, set[str]]) -> None:
    # Simulates branch kr-4-2b-export-registration: export registered, sync not.
    t = "kis_stock_status_daily"
    assert t in sets["ddl"] and t not in sets["sync"]
    s = _mutated(sets, raw_toml=sets["raw_toml"] | {t}, raw_sh=sets["raw_sh"] | {t})
    ddl_only = {k: v for k, v in DDL_ONLY_EXCLUSIONS.items() if k != t}
    assert _with_not_synced(s, {t: "export만 함"}, ddl_only) == []


def test_unsynced_table_without_exclusion_is_still_reported(sets: dict[str, set[str]]) -> None:
    t = "kis_stock_status_daily"
    s = _mutated(sets, raw_toml=sets["raw_toml"] | {t}, raw_sh=sets["raw_sh"] | {t})
    ddl_only = {k: v for k, v in DDL_ONLY_EXCLUSIONS.items() if k != t}
    text = "\n".join(_with_not_synced(s, {}, ddl_only))
    assert t in text and "NOT_SYNCED_EXCLUSIONS" in text


def test_detects_not_synced_table_that_is_in_sync(sets: dict[str, set[str]]) -> None:
    problems = _with_not_synced(sets, {"daily_ohlcv": "x"})
    assert any(
        "daily_ohlcv" in p
        and "SYNC_TABLE_SPECS" in p
        and "NOT_SYNCED_EXCLUSIONS에서 지우십시오" in p
        for p in problems
    )


def test_detects_not_synced_table_not_in_ddl(sets: dict[str, set[str]]) -> None:
    problems = _with_not_synced(sets, {"ghost_table": "x"})
    assert any("ghost_table" in p and "DDL에 표가 없음" in p for p in problems)


def test_detects_not_synced_table_not_exported(sets: dict[str, set[str]]) -> None:
    t = "kis_stock_status_daily"  # in DDL, in neither sync nor export
    problems = _with_not_synced(sets, {t: "x"}, {})
    assert any(t in p and "NOT_SYNCED_EXCLUSIONS" in p and "export" in p for p in problems)


def test_detects_table_in_both_not_synced_and_ddl_only(sets: dict[str, set[str]]) -> None:
    t = "kis_stock_status_daily"
    s = _mutated(sets, raw_toml=sets["raw_toml"] | {t}, raw_sh=sets["raw_sh"] | {t})
    problems = _with_not_synced(s, {t: "x"})
    assert any(t in p and "모두 있음" in p for p in problems)
