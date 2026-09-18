"""미국 스냅샷 계약 — 미국 계획 03 §3·§4, C2 완료 판정."""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, date, datetime
from decimal import Decimal

import pandas as pd
import pandera.errors
import pytest

from collector.us import paths
from collector.us.store import (
    MissingProvenanceError,
    UnknownTableError,
    read_snapshot,
    snapshot_path,
    write_snapshot,
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


def test_lake_root_is_market_under_stock_data_root():
    root = paths.resolve_lake_root(env={"STOCK_DATA_ROOT": "/lake"})
    assert root.as_posix() == "/lake/us"


def test_lake_root_without_env_raises():
    with pytest.raises(RuntimeError, match="STOCK_DATA_ROOT"):
        paths.resolve_lake_root(env={})


def test_lake_root_override_is_taken_as_is(tmp_path):
    """변형 lake 진입로 — 한국이 kr/derived/_e5를 잃었던 자리다 (02 §1.2.1)."""
    assert paths.resolve_lake_root(tmp_path, env={}) == tmp_path


def test_layer_helpers(tmp_path):
    assert paths.raw_dir(tmp_path).name == "raw"
    assert paths.derived_dir(tmp_path).name == "derived"
    assert paths.datasets_dir(tmp_path).name == "datasets"
    assert paths.output_dir(tmp_path).name == "output"
    assert paths.snapshots_dir(tmp_path, "prices_daily").parts[-3:] == (
        "derived",
        "snapshots",
        "prices_daily",
    )


def test_missing_layers_reports_shape(tmp_path):
    assert set(paths.missing_layers(tmp_path)) == set(paths.LAYERS)
    for layer in paths.LAYERS:
        (tmp_path / layer).mkdir()
    assert paths.missing_layers(tmp_path) == ()


def test_snapshot_path_key_does_not_collide_with_a_column(tmp_path):
    """경로 키가 `date`면 그 값이 같은 이름의 컬럼을 덮어쓴다 (한국 source 사고)."""
    p = snapshot_path(tmp_path, "prices_daily", date(2026, 9, 9))
    assert "snapshot_date=2026-09-09" in p.as_posix()
    assert "/date=" not in p.as_posix()


# --- 왕복 ------------------------------------------------------------------


def test_round_trip(tmp_path):
    p = write_snapshot(
        _prices(), "prices_daily", snapshot_path(tmp_path, "prices_daily", "2026-09-09")
    )
    back = read_snapshot(p)
    assert len(back) == 2
    assert back["volume"].dtype == "int64"
    assert back.loc[back["symbol"] == "AAPL", "close"].iloc[0] == Decimal("315.3400")
    assert back.loc[back["symbol"] == "AAPL", "volume"].iloc[0] == 65639962


def test_empty_snapshot_round_trips(tmp_path):
    """C2 완료 판정 — 빈 스냅샷 하나를 쓰고 읽는 왕복이 된다."""
    empty = _prices().iloc[0:0]
    p = write_snapshot(empty, "prices_daily", snapshot_path(tmp_path, "prices_daily", "2026-09-09"))
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
    code = "import collector.us, collector.us.paths, collector.us.store; print('ok')"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout
