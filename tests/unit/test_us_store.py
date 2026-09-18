"""미국 스냅샷 계약 — 미국 계획 03 §3·§4, C2 완료 판정."""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, date, datetime
from decimal import Decimal

import pandas as pd
import pandera.errors
import pytest

from collector.lake import DataRoot
from collector.us.store import (
    MissingProvenanceError,
    UnknownTableError,
    read_snapshot,
    snapshot_path,
    write_snapshot,
    write_snapshot_arrow,
)

OBSERVED = datetime(2026, 9, 18, 6, 0, tzinfo=UTC)


def _prices(**overrides) -> pd.DataFrame:
    base = {
        "date": pd.to_datetime(["2026-09-09", "2026-09-09"]),
        "symbol": ["AAPL", "NVDA"],
        "open": [Decimal("315.4850"), Decimal("170.1000")],
        "high": [Decimal("319.1500"), Decimal("172.0000")],
        "low": [Decimal("309.9000"), Decimal("169.0000")],
        "close": [Decimal("315.3400"), Decimal("171.5000")],
        "volume": [65639962, 120000000],
        "observed_at": pd.to_datetime([OBSERVED, OBSERVED]),
        "source_rev": ["thcpkadaf90t5n202mm3kpvbad7kaoa0"] * 2,
    }
    base.update(overrides)
    return pd.DataFrame(base)


# --- 경로 ------------------------------------------------------------------
# DataRoot 자체 테스트는 test_lake.py에 있다. 여기서는 US store가 그것을
# 어떻게 쓰는지만 본다.


def test_snapshot_path_key_does_not_collide_with_a_column(tmp_path):
    """경로 키가 `date`면 그 값이 같은 이름의 컬럼을 덮어쓴다 (한국 source 사고)."""
    p = snapshot_path(DataRoot(tmp_path), "prices_daily", date(2026, 9, 9))
    assert "snapshot_date=2026-09-09" in p.as_posix()
    assert "/date=" not in p.as_posix()
    assert p.parts[-4:-2] == ("snapshots", "prices_daily")


# --- 왕복 ------------------------------------------------------------------


def test_round_trip(tmp_path):
    p = write_snapshot(
        _prices(), "prices_daily", snapshot_path(DataRoot(tmp_path), "prices_daily", "2026-09-09")
    )
    back = read_snapshot(p)
    assert len(back) == 2
    assert back["volume"].dtype == "int64"
    assert back.loc[back["symbol"] == "AAPL", "close"].iloc[0] == Decimal("315.3400")
    assert back.loc[back["symbol"] == "AAPL", "volume"].iloc[0] == 65639962


def test_empty_snapshot_round_trips(tmp_path):
    """C2 완료 판정 — 빈 스냅샷 하나를 쓰고 읽는 왕복이 된다."""
    empty = _prices().iloc[0:0]
    p = write_snapshot(
        empty, "prices_daily", snapshot_path(DataRoot(tmp_path), "prices_daily", "2026-09-09")
    )
    assert read_snapshot(p).empty


# --- 계약이 실제로 막는가 ---------------------------------------------------


def test_observed_at_is_mandatory(tmp_path):
    frame = _prices().drop(columns=["observed_at"])
    with pytest.raises(MissingProvenanceError, match="observed_at"):
        write_snapshot(frame, "prices_daily", tmp_path / "x.parquet")


def test_observed_at_cannot_be_skipped_by_turning_validation_off(tmp_path):
    frame = _prices().drop(columns=["observed_at"])
    with pytest.raises(MissingProvenanceError):
        write_snapshot(frame, "prices_daily", tmp_path / "x.parquet", validate=False)


def test_float_volume_is_rejected(tmp_path):
    """stockanalysis가 64677043.99999999 를 준다 (03 §4.1)."""
    frame = _prices(volume=[64677043.99999999, 1.0])
    with pytest.raises(pandera.errors.SchemaError):
        write_snapshot(frame, "prices_daily", tmp_path / "x.parquet")


def test_float_price_is_rejected(tmp_path):
    frame = _prices(close=[315.34, 171.5])
    with pytest.raises(pandera.errors.SchemaError):
        write_snapshot(frame, "prices_daily", tmp_path / "x.parquet")


def test_non_positive_close_is_rejected(tmp_path):
    frame = _prices(close=[Decimal("0"), Decimal("171.5")])
    with pytest.raises(pandera.errors.SchemaError):
        write_snapshot(frame, "prices_daily", tmp_path / "x.parquet")


def test_high_below_low_is_rejected(tmp_path):
    frame = _prices(high=[Decimal("1.0"), Decimal("172.0")])
    with pytest.raises((pandera.errors.SchemaErrors, pandera.errors.SchemaError)):
        write_snapshot(frame, "prices_daily", tmp_path / "x.parquet")


def test_duplicate_date_symbol_in_one_file_is_rejected(tmp_path):
    """파일 안에서는 (date, symbol) 유일. 가로질러서는 observed_at이 붙는다 (03 §3.1)."""
    frame = _prices(symbol=["AAPL", "AAPL"])
    with pytest.raises(pandera.errors.SchemaError):
        write_snapshot(frame, "prices_daily", tmp_path / "x.parquet")


def test_unknown_table_is_rejected(tmp_path):
    with pytest.raises(UnknownTableError):
        write_snapshot(_prices(), "no_such_table", tmp_path / "x.parquet")


# --- X16: import만으로 죽지 않는다 ------------------------------------------


def test_importing_us_without_stock_data_root_does_not_die():
    """진입점이 하나라, 여기서 죽으면 한국 prod 컨테이너가 같이 죽는다."""
    code = "import collector.us, collector.us.store; print('ok')"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout


# --- 벌크 경로 (write_snapshot_arrow) ---------------------------------------


def _vol_arrow(rows: int = 2, *, dup: bool = False, provenance: bool = True):
    import pyarrow as pyar

    day = date(2026, 9, 17)
    syms = ["AAPL"] * rows if dup else [f"S{i}" for i in range(rows)]
    cols = {
        "date": pyar.array([day] * rows, type=pyar.date32()),
        "symbol": pyar.array(syms, type=pyar.string()),
    }
    for name in (
        "hv_current",
        "hv_week_ago",
        "hv_month_ago",
        "hv_year_high",
        "hv_year_low",
        "iv_current",
        "iv_week_ago",
        "iv_month_ago",
        "iv_year_high",
        "iv_year_low",
    ):
        cols[name] = pyar.array([Decimal("0.3456")] * rows, type=pyar.decimal128(5, 4))
    for name in (
        "hv_year_high_date",
        "hv_year_low_date",
        "iv_year_high_date",
        "iv_year_low_date",
    ):
        cols[name] = pyar.array([day] * rows, type=pyar.date32())
    if provenance:
        cols["observed_at"] = pyar.array([OBSERVED] * rows, type=pyar.timestamp("us", tz="UTC"))
    cols["source_rev"] = pyar.array(["abc123"] * rows, type=pyar.string())
    return pyar.table(cols)


def test_arrow_round_trip(tmp_path):
    p = write_snapshot_arrow(
        _vol_arrow(3), "volatility_daily", tmp_path / "v.parquet", unique_on=("date", "symbol")
    )
    back = read_snapshot(p)
    assert len(back) == 3
    assert back["iv_current"].iloc[0] == Decimal("0.3456")


def test_arrow_path_also_demands_observed_at(tmp_path):
    with pytest.raises(MissingProvenanceError, match="observed_at"):
        write_snapshot_arrow(
            _vol_arrow(2, provenance=False), "volatility_daily", tmp_path / "v.parquet"
        )


def test_arrow_path_catches_duplicate_keys(tmp_path):
    with pytest.raises(ValueError, match="유일하지 않다"):
        write_snapshot_arrow(
            _vol_arrow(3, dup=True),
            "volatility_daily",
            tmp_path / "v.parquet",
            unique_on=("date", "symbol"),
        )


def test_arrow_path_rejects_unknown_table(tmp_path):
    with pytest.raises(UnknownTableError):
        write_snapshot_arrow(_vol_arrow(1), "no_such_table", tmp_path / "v.parquet")


# --- dolt 원천 --------------------------------------------------------------


def test_dolt_repo_dir_layout(tmp_path):
    from collector.us.sources import dolt

    assert dolt.repo_dir(DataRoot(tmp_path), "options") == tmp_path / "raw" / "dolt" / "options"


def test_dolt_refuses_a_directory_that_is_not_a_repo(tmp_path):
    """중단된 clone은 .dolt 가 있어도 dolt가 in-progress로 본다 — 먼저 걸러낸다."""
    from collector.us.sources import dolt

    with pytest.raises(dolt.DoltError, match="dolt 레포가 아니다"):
        dolt.head_commit(tmp_path)
