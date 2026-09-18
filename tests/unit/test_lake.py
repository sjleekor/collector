"""``collector.lake`` — 두 저장소가 공유하는 경로 계약."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from collector.lake import LAYERS, DataRoot


def test_resolve_joins_market_under_stock_data_root():
    assert DataRoot.resolve(env={"STOCK_DATA_ROOT": "/lake"}).base == Path("/lake/kr")
    assert DataRoot.resolve("us", env={"STOCK_DATA_ROOT": "/lake"}).base == Path("/lake/us")


def test_resolve_without_env_raises():
    with pytest.raises(RuntimeError, match="STOCK_DATA_ROOT"):
        DataRoot.resolve(env={})


def test_layers(tmp_path):
    root = DataRoot(tmp_path)
    assert root.raw.name == "raw"
    assert root.derived.name == "derived"
    assert root.datasets.name == "datasets"
    assert root.output.name == "output"


def test_constructor_is_the_variant_lake_entrance(tmp_path):
    """평소 경로가 아닌 lake를 읽는 진입로. modeler가 kr/derived/_e5 를 이렇게 읽는다."""
    nested = tmp_path / "kr" / "derived" / "_e5"
    nested.mkdir(parents=True)
    assert DataRoot(nested).raw == nested / "raw"


def test_missing_layers_reports_shape(tmp_path):
    root = DataRoot(tmp_path)
    assert set(root.missing_layers()) == set(LAYERS)
    for layer in LAYERS:
        (tmp_path / layer).mkdir()
    assert root.missing_layers() == ()


def test_importing_lake_without_env_does_not_die():
    """모듈 최상위에서 환경변수를 읽으면 import만으로 죽는다 — 진입점이 하나다."""
    proc = subprocess.run(
        [sys.executable, "-c", "import collector.lake; print('ok')"],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout
