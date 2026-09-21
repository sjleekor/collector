"""``us-derive`` — raw 를 derived 로 굳히는 한 번의 실행 (04 C8 · 05 §4.1).

**이 파일이 생긴 이유.** `load_prices_daily`·`load_corp_actions`·
`load_volatility_daily`·`build_listing_snapshots` 넷은 C3 때 만들어졌는데
**부르는 자리가 어디에도 없었다** — CLI 에도, 하루 실행에도, 시험에도.
함수가 멀쩡히 있으니 아무도 몰랐고, `dolt pull` 이 매일 도는 동안
`prices_daily` 스냅샷은 2026-09-09 에서 멈춰 있었다 (2026-09-21 확인).

그래서 "함수가 있나"가 아니라 **"표마다 굳히는 길이 있나"**를 본다.
"""

from __future__ import annotations

import importlib
import os
from datetime import UTC, datetime

import pyarrow as pyar
import pyarrow.parquet as pq
import pytest

from collector.lake import DataRoot
from collector.us.cli.app import _loaders
from collector.us.ops import derive
from collector.us.store.schema import ARROW_SCHEMAS

OBSERVED = datetime(2026, 9, 21, 6, 0, tzinfo=UTC)


# --- 표가 하나도 안 빠졌나 -----------------------------------------------------


@pytest.mark.parametrize("name", sorted(derive.RECIPES))
def test_loader_target_resolves(name: str) -> None:
    """``RECIPES`` 의 문자열이 실제 함수를 가리킨다."""
    module_name, _, func_name = derive.RECIPES[name].loader.partition(":")
    assert callable(getattr(importlib.import_module(module_name), func_name))


def test_every_lake_table_has_a_way_to_be_rebuilt() -> None:
    """표 18장 전부가 ``us-derive`` 나 ``NOT_DERIVED`` 에 적힌 길로 다시 만들어진다."""
    covered = {t for r in derive.RECIPES.values() for t in r.tables} | set(derive.NOT_DERIVED)
    assert not set(ARROW_SCHEMAS) - covered, (
        "굳히는 길이 없는 표가 있다 — RECIPES 에 달거나 NOT_DERIVED 에 이유를 적어라"
    )


def test_no_stale_table_names() -> None:
    """레이크에 없는 표 이름이 남아 있지 않다."""
    named = {t for r in derive.RECIPES.values() for t in r.tables} | set(derive.NOT_DERIVED)
    assert not named - set(ARROW_SCHEMAS)


def test_every_recipe_says_how_it_knows_it_is_stale() -> None:
    """``dolt_repo`` 든 ``raw_inputs`` 든 하나는 있어야 한다."""
    for name, recipe in derive.RECIPES.items():
        assert recipe.dolt_repo or recipe.raw_inputs, f"{name} 은 판단 근거가 없다"


def test_cli_loaders_come_from_recipes() -> None:
    """``us-load`` 목록이 ``RECIPES`` 와 같다 — 둘이 벌어지면 안 된다."""
    assert _loaders() == {n: r.loader for n, r in derive.RECIPES.items()}


# --- 언제 다시 굳히나 ----------------------------------------------------------


def _write_snapshot(root: DataRoot, table: str, day: str, *, source_rev: str | None = None):
    from collector.us.store.writer import snapshot_path

    path = snapshot_path(root, table, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = {"observed_at": pyar.array([OBSERVED], type=pyar.timestamp("us", tz="UTC"))}
    if source_rev is not None:
        cols["source_rev"] = pyar.array([source_rev])
    pq.write_table(pyar.table(cols), path)
    return path


def test_rebuilds_when_no_snapshot_exists(tmp_path) -> None:
    root = DataRoot(tmp_path)
    should, why = derive.needs_rebuild(root, "short-volume", derive.RECIPES["short-volume"])
    assert should and "없다" in why


def test_skips_when_raw_is_older_than_snapshot(tmp_path) -> None:
    root = DataRoot(tmp_path)
    raw = root.raw / "finra" / "regsho"
    raw.mkdir(parents=True)
    (raw / "date=2026-09-18.txt").write_text("x")
    snap = _write_snapshot(root, "short_volume", "2026-09-19")
    # 스냅샷을 raw 보다 뒤로 둔다
    later = raw.stat().st_mtime + 100
    os.utime(snap, (later, later))

    should, why = derive.needs_rebuild(root, "short-volume", derive.RECIPES["short-volume"])
    assert not should and "오래됐다" in why


def test_rebuilds_when_raw_is_newer(tmp_path) -> None:
    root = DataRoot(tmp_path)
    snap = _write_snapshot(root, "short_volume", "2026-09-19")
    raw = root.raw / "finra" / "regsho"
    raw.mkdir(parents=True)
    newer = raw / "date=2026-09-21.txt"
    newer.write_text("x")
    later = snap.stat().st_mtime + 100
    os.utime(newer, (later, later))

    should, why = derive.needs_rebuild(root, "short-volume", derive.RECIPES["short-volume"])
    assert should and "새로 들어왔다" in why


def test_dolt_table_compares_source_rev(tmp_path, monkeypatch) -> None:
    """**커밋 해시가 같으면 안 굳힌다** (05 §5). mtime 은 안 본다."""
    root = DataRoot(tmp_path)
    _write_snapshot(root, "prices_daily", "2026-09-18", source_rev="abcdef0123456789")
    (root.raw / "dolt" / "stocks" / ".dolt").mkdir(parents=True)

    from collector.us.sources import dolt

    monkeypatch.setattr(dolt, "head_commit", lambda _p: "abcdef0123456789")
    should, why = derive.needs_rebuild(root, "prices-daily", derive.RECIPES["prices-daily"])
    assert not should and "그대로" in why

    monkeypatch.setattr(dolt, "head_commit", lambda _p: "9999999999999999")
    should, why = derive.needs_rebuild(root, "prices-daily", derive.RECIPES["prices-daily"])
    assert should and "→ 99999999" in why


def test_dry_run_touches_nothing(tmp_path) -> None:
    root = DataRoot(tmp_path)
    out = derive.run_derive(
        root, snapshot_date="2026-09-21", tables=("short-volume",), dry_run=True
    )
    assert out["ok"] and out["pending"] == 1
    assert not (root.derived / "snapshots" / "short_volume").exists()


def test_one_broken_table_does_not_stop_the_rest(tmp_path, monkeypatch) -> None:
    """표 하나가 죽어도 나머지는 본다. 대신 ``ok`` 가 거짓이 된다."""
    root = DataRoot(tmp_path)

    def _boom(*_a, **_k):
        raise RuntimeError("터졌다")

    from collector.us.sources import finra

    monkeypatch.setattr(finra, "load_short_volume", _boom)
    monkeypatch.setattr(finra, "load_short_interest", lambda *_a, **_k: {"rows": 3})
    out = derive.run_derive(
        root, snapshot_date="2026-09-21", tables=("short-volume", "short-interest")
    )
    assert not out["ok"]
    assert [t["name"] for t in out["tables"]] == ["short-volume", "short-interest"]
    assert out["tables"][0]["error"].startswith("RuntimeError")
    assert out["tables"][1]["rebuilt"] and out["tables"][1]["rows"] == 3


def test_unknown_table_is_refused(tmp_path) -> None:
    with pytest.raises(ValueError, match="모르는 표"):
        derive.run_derive(DataRoot(tmp_path), snapshot_date="2026-09-21", tables=("없는표",))


def test_budget_leaves_the_rest_pending(tmp_path) -> None:
    out = derive.run_derive(
        DataRoot(tmp_path), snapshot_date="2026-09-21", budget_seconds=-1.0, dry_run=True
    )
    assert out["budget_spent"] and out["pending"] == len(derive.RECIPES)
    assert all(t["reason"] == "예산이 다했다 — 다음 실행이 한다" for t in out["tables"])
