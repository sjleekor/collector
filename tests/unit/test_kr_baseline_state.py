"""저장 계층 수정 — 연도·열 단위 상태, 변환 칸 형식, 서비스 이름, CLI 인자."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from collector.kr.baseline import BaselineStore, BaselineWriter, ParsedResponse, TableSpec
from collector.kr.baseline.writer import MAX_LOADED_YEARS

SPEC = TableSpec(
    name="t_daily",
    key_columns=("BAS_DD", "ISU_CD"),
    source_columns=("BAS_DD", "ISU_CD", "PRICE"),
    date_column="BAS_DD",
    parsed_columns={
        "bas_date": "date32",
        "price_num": "int64",
        "ratio_num": "float64",
        "flag": "bool",
    },
)
T0 = datetime(2026, 10, 12, 9, 0, tzinfo=UTC)


def _row(day: str, price: str = "10") -> dict:
    return {
        "BAS_DD": day,
        "ISU_CD": "A",
        "PRICE": price,
        "bas_date": date(int(day[:4]), int(day[4:6]), int(day[6:])),
        "price_num": int(price),
        "ratio_num": 0.5,
        "flag": True,
    }


def _put(writer: BaselineWriter, day: str, price: str = "10", at: datetime = T0) -> str:
    key = f"bas_dd={day}"
    raw = writer.store.write_raw("svc", key, f"{day}{price}{at}".encode(), at)
    parsed = ParsedResponse(SPEC, [_row(day, price)], {"BAS_DD": day})
    return writer.add_response(service="svc", request_key=key, raw=raw, parsed=parsed)


@pytest.fixture
def store(tmp_path: Path) -> BaselineStore:
    return BaselineStore(tmp_path / "krx_baseline")


def _writer(store: BaselineStore, run: str = "r", **kw) -> BaselineWriter:
    return BaselineWriter(store, run, store.next_attempt(run), "daily", **kw)


def test_parsed_columns_keep_their_declared_types(store: BaselineStore) -> None:
    w = _writer(store)
    _put(w, "20260105")
    w.flush()
    path = next((store.base / "t_daily").rglob("*.parquet"))
    schema = pq.read_schema(path)
    assert str(schema.field("bas_date").type) == "date32[day]"
    assert str(schema.field("price_num").type) == "int64"
    assert str(schema.field("ratio_num").type) == "double"
    assert str(schema.field("flag").type) == "bool"
    assert str(schema.field("PRICE").type) == "string"
    with pytest.raises(ValueError, match="모르는 형식"):
        TableSpec("x", ("K",), ("K",), "K", parsed_columns={"a_num": "decimal"})


def test_columns_that_differ_only_by_case_are_refused() -> None:
    with pytest.raises(ValueError, match="대소문자"):
        TableSpec("x", ("BAS_DD",), ("BAS_DD",), "BAS_DD", parsed_columns={"bas_dd": "string"})


def test_service_names_are_lowercase_words_only(store: BaselineStore) -> None:
    for bad in ("a/b", "Etf", "etf-daily", "", ".."):
        with pytest.raises(ValueError, match="서비스 이름"):
            store.write_raw(bad, "bas_dd=20260105", b"{}", T0)
    store.write_raw("etf_bydd_trd", "bas_dd=20260105", b"{}", T0)
    w = _writer(store)
    raw = store.write_raw("svc", "bas_dd=20260105", b"x", T0)
    with pytest.raises(ValueError, match="서비스 이름"):
        w.add_failure(service="Bad/Name", request_key="k", fetched_at=T0, error="x", raw=raw)


def test_state_reads_only_the_needed_year_and_columns(store: BaselineStore) -> None:
    w = _writer(store)
    for year in (2022, 2023, 2024, 2025, 2026):
        _put(w, f"{year}0105")
    w.flush()

    fresh = BaselineStore(store.base)
    w2 = _writer(fresh, "r2")
    assert _put(w2, "20260105", "10") == "same"
    obs_reads = [r for r in fresh.reads if "/etf" not in r[0] and r[0].startswith("t_daily/")]
    assert obs_reads and all("/year=2026/" in path for path, _ in obs_reads)
    wanted = ("BAS_DD", "ISU_CD", "obs_seq", "obs_kind", "row_hash")
    assert all(cols == wanted for _, cols in obs_reads)  # 열을 이 다섯으로만 읽는다


def test_loaded_years_are_capped_and_unflushed_years_are_never_dropped(
    store: BaselineStore,
) -> None:
    w = _writer(store, batch_size=10_000)
    for year in range(2010, 2017):  # 확정 안 된 행이 있는 연도 7개
        _put(w, f"{year}0104")
    assert len(w._state) == 7  # 내리면 디스크에 없는 상태를 잃는다
    w.flush()
    _put(w, "20170104")  # 확정된 연도부터 내려간다
    assert len(w._state) <= MAX_LOADED_YEARS + 1
    w.flush()
    # 오래된 연도를 다시 읽어도 순번이 맞다
    assert _put(w, "20100104", "11", T0 + timedelta(days=1)) == "new_obs"
    w.flush()
    obs = store.read_observations(SPEC, "all", year="2010")
    assert sorted(obs["obs_seq"]) == [1, 2]


def test_an_import_sweep_across_many_years_never_holds_more_than_three_years(
    store: BaselineStore,
) -> None:
    w = _writer(store, batch_size=1)  # 요청마다 확정 = 연도 순서 import와 같은 모양
    peak = 0
    for year in range(2010, 2027):
        _put(w, f"{year}0104")
        peak = max(peak, len(w._state))
    assert peak <= MAX_LOADED_YEARS
    assert len(store.read_observations(SPEC, "all")) == 17


# ------------------------------------------------------------------------ CLI


def test_cli_registers_krx_baseline_and_seibro_commands() -> None:
    from collector.kr.cli import app

    parser = app.build_parser()
    sync = parser.parse_args(["krx-baseline", "sync"])
    assert sync.handler is app._handle_krx_baseline_sync
    assert sync.max_calls == 60 and sync.services == "" and sync.max_consecutive_failures == 5

    back = parser.parse_args(
        [
            "krx-baseline",
            "backfill",
            "--service",
            "bon_dd_trd",
            "--mode",
            "reobserve",
            "--start",
            "2010-01-04",
            "--end",
            "2010-02-01",
            "--run-id",
            "r1",
            "--max-calls",
            "9",
        ]
    )
    assert back.handler is app._handle_krx_baseline_backfill
    assert (back.service, back.mode, back.run_id, back.max_calls) == (
        "bon_dd_trd",
        "reobserve",
        "r1",
        9,
    )
    assert back.start == date(2010, 1, 4)
    assert parser.parse_args(["krx-baseline", "backfill"]).end is None  # 끝은 실행 때 어제

    imp = parser.parse_args(
        ["krx-baseline", "import-research", "--path", "/x", "--expect-manifest-sha256", "ab"]
    )
    assert imp.handler is app._handle_krx_baseline_import_research and not imp.force

    ver = parser.parse_args(["krx-baseline", "verify", "--service", "etf_bydd_trd"])
    assert ver.handler is app._handle_krx_baseline_verify and ver.start is None

    sb = parser.parse_args(["seibro-dist", "sync", "--full"])
    assert sb.handler is app._handle_seibro_dist_sync and sb.full


def test_cli_reobserve_without_run_id_exits_1(capsys: pytest.CaptureFixture[str]) -> None:
    from collector.kr.cli import app

    args = app.build_parser().parse_args(["krx-baseline", "backfill", "--mode", "reobserve"])
    with pytest.raises(SystemExit) as exc:
        app._handle_krx_baseline_backfill(args)
    assert exc.value.code == 1
    assert "run-id" in capsys.readouterr().err


def test_cli_verify_fails_with_exit_1_on_an_empty_lake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from collector.kr.cli import app

    monkeypatch.setenv("STOCK_DATA_ROOT", str(tmp_path))
    args = app.build_parser().parse_args(["krx-baseline", "verify", "--service", "etf_bydd_trd"])
    with pytest.raises(SystemExit) as exc:
        app._handle_krx_baseline_verify(args)
    assert exc.value.code == 1  # 완료 요청 기록이 없는 평일
    assert "결과: 실패" in capsys.readouterr().out


def test_cli_seibro_sync_is_a_no_op_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from collector.kr.cli import app

    monkeypatch.setenv("STOCK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SDC_SEIBRO_ENABLED", "0")
    app._handle_seibro_dist_sync(app.build_parser().parse_args(["seibro-dist", "sync"]))
    assert "꺼져" in capsys.readouterr().out
    assert not (tmp_path / "kr").exists()


def test_cli_missing_stock_data_root_exits_2(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from collector.kr.cli import app

    monkeypatch.delenv("STOCK_DATA_ROOT", raising=False)
    args = app.build_parser().parse_args(["krx-baseline", "verify"])
    with pytest.raises(SystemExit) as exc:
        app._handle_krx_baseline_verify(args)
    assert exc.value.code == 2
