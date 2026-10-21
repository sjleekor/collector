"""R-4 기준선 KRX 파서 시험. fixture는 서버 조사 사본에서 발췌한 실제 응답입니다."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from collector.kr.adapters.krx_baseline_openapi import bond_index, etf_daily
from collector.kr.adapters.krx_baseline_openapi.day_status import DayStatus

FIX = Path(__file__).resolve().parent.parent / "fixtures" / "krx_baseline"


def rows(name: str) -> list[dict]:
    return json.loads((FIX / name).read_text(encoding="utf-8"))["OutBlock_1"]


def test_etf_columns_are_19_and_match_response_order() -> None:
    assert len(etf_daily.SOURCE_COLUMNS) == 19
    for name in ("etf_20100104.json", "etf_20261008.json"):
        for r in rows(name):
            assert tuple(r) == etf_daily.SOURCE_COLUMNS


def test_etf_2010_keeps_raw_strings_and_blank_index() -> None:
    raw = rows("etf_20100104.json")
    parsed = etf_daily.parse_etf_rows(raw)
    p = parsed[0]
    assert p["ISU_CD"] == "108630" and p["bas_date"] == date(2010, 1, 4)
    assert p["TDD_CLSPRC"] == "17200" and p["tdd_clsprc_num"] == 17200
    assert p["NAV"] == "17152.21" and p["nav_num"] == 17152.21
    assert p["OBJ_STKPRC_IDX"] == "" and p["obj_stkprc_idx_num"] is None
    assert p["fluc_rt_idx_num"] is None
    assert p["invstasst_netasst_totamt_num"] == 31217018929
    assert etf_daily.classify_etf_day(raw) == DayStatus("trading", 3, 3, 0, 0)


def test_etf_holiday_is_no_price_without_netasst_zero() -> None:
    raw = rows("etf_20241225.json")
    s = etf_daily.classify_etf_day(raw)
    assert s.day_kind == "no_price"
    assert s.row_count == len(raw) and s.priced_rows == 0
    assert s.netasst_zero_rows == 0 and s.required_missing == 0
    p = etf_daily.parse_etf_rows(raw)[0]
    assert p["INVSTASST_NETASST_TOTAMT"] == "" and p["invstasst_netasst_totamt_num"] is None
    assert p["list_shrs_num"] == 1057000


def test_etf_20261008_has_price_and_netasst_zero() -> None:
    raw = rows("etf_20261008.json")
    s = etf_daily.classify_etf_day(raw)
    assert s.day_kind == "trading"
    assert s.priced_rows == s.row_count == s.netasst_zero_rows == len(raw)
    p = etf_daily.parse_etf_rows(raw)[0]
    assert p["INVSTASST_NETASST_TOTAMT"] == "0" and p["invstasst_netasst_totamt_num"] == 0


def test_etf_alphanumeric_code_stays_string() -> None:
    parsed = etf_daily.parse_etf_rows(rows("etf_20261007.json"))
    codes = [p["ISU_CD"] for p in parsed]
    assert "0184E0" in codes and "0182R0" in codes
    assert all(isinstance(c, str) for c in codes)
    s = etf_daily.classify_etf_day(rows("etf_20261007.json"))
    assert s.day_kind == "trading" and s.netasst_zero_rows == 0


def test_etf_empty_response() -> None:
    assert etf_daily.classify_etf_day([]) == DayStatus("empty", 0, 0, 0, 0)


def test_etf_missing_field_raises() -> None:
    bad = dict(rows("etf_20261007.json")[0])
    del bad["NAV"]
    with pytest.raises(KeyError):
        etf_daily.parse_etf_rows([bad])
    with pytest.raises(KeyError):
        etf_daily.classify_etf_day([{"ISU_CD": "1"}])


def test_etf_comma_and_dash_are_tolerated() -> None:
    r = dict(rows("etf_20261007.json")[0])
    r["ACC_TRDVOL"] = "1,234"
    r["NAV"] = "-"
    p = etf_daily.parse_etf_rows([r])[0]
    assert p["acc_trdvol_num"] == 1234 and p["nav_num"] is None


def test_bond_constants() -> None:
    assert len(bond_index.SOURCE_COLUMNS) == 15  # 조사 문서는 14필드라 했으나 BAS_DD 포함 실제 15개
    assert bond_index.EXPECTED_GROUPS == {"KRX 채권지수", "KTB 지수", "국고채프라임지수"}
    for name in ("idx_bon_dd_trd_20100104.json", "idx_bon_dd_trd_20261008.json"):
        for r in rows(name):
            assert tuple(r) == bond_index.SOURCE_COLUMNS


def test_bond_normal_days_are_trading() -> None:
    for name in ("idx_bon_dd_trd_20100104.json", "idx_bon_dd_trd_20260930.json"):
        raw = rows(name)
        assert bond_index.classify_bond_day(raw) == DayStatus("trading", 3, 3, 0, 0)
    parsed = bond_index.parse_bond_rows(rows("idx_bon_dd_trd_20100104.json"))
    krx, ktb, prime = parsed
    assert krx["TOT_EARNG_IDX"] == "122.81" and krx["tot_earng_idx_num"] == 122.81
    assert ktb["ZERO_REINVST_IDX"] == "" and ktb["zero_reinvst_idx_num"] is None
    assert prime["avg_duration_num"] is None
    assert krx["BND_IDX_GRP_NM"] == "KRX 채권지수"


def test_bond_20261008_is_partial() -> None:
    raw = rows("idx_bon_dd_trd_20261008.json")
    s = bond_index.classify_bond_day(raw)
    assert s == DayStatus("partial", 3, 2, 0, 1)
    assert bond_index.parse_bond_rows(raw)[0]["tot_earng_idx_num"] is None


def test_bond_empty_response() -> None:
    raw = rows("idx_bon_dd_trd_20091230.json")
    assert raw == []
    assert bond_index.classify_bond_day(raw) == DayStatus("empty", 0, 0, 0, 3)


def test_bond_missing_field_raises() -> None:
    bad = dict(rows("idx_bon_dd_trd_20260930.json")[0])
    del bad["BND_IDX_AVG_YD"]
    with pytest.raises(KeyError):
        bond_index.parse_bond_rows([bad])


def test_etf_column_names_do_not_collide_case_insensitively() -> None:
    names = [c.lower() for c in etf_daily.SOURCE_COLUMNS + etf_daily.PARSED_COLUMNS]
    assert len(names) == len(set(names))
    p = etf_daily.parse_etf_rows(rows("etf_20261007.json"))[0]
    assert set(p) == set(etf_daily.SOURCE_COLUMNS) | set(etf_daily.PARSED_COLUMNS)
    assert isinstance(p["nav_num"], float) and isinstance(p["tdd_clsprc_num"], int)


def test_bond_column_names_do_not_collide_case_insensitively() -> None:
    names = [c.lower() for c in bond_index.SOURCE_COLUMNS + bond_index.PARSED_COLUMNS]
    assert len(names) == len(set(names))
    p = bond_index.parse_bond_rows(rows("idx_bon_dd_trd_20260930.json"))[0]
    assert set(p) == set(bond_index.SOURCE_COLUMNS) | set(bond_index.PARSED_COLUMNS)
    assert isinstance(p["tot_earng_idx_num"], float) and p["bas_date"] == date(2026, 9, 30)


def _deriv(name: str) -> list[dict]:
    return rows(f"idx_drvprod_dd_trd_{name}.json")


def test_derivative_columns_and_no_collision() -> None:
    from collector.kr.adapters.krx_baseline_openapi import derivative_index as dv

    assert dv.SOURCE_COLUMNS == (
        "BAS_DD",
        "IDX_CLSS",
        "IDX_NM",
        "CLSPRC_IDX",
        "CMPPREVDD_IDX",
        "FLUC_RT",
        "OPNPRC_IDX",
        "HGPRC_IDX",
        "LWPRC_IDX",
    )
    assert (dv.GROUP, dv.ENDPOINT) == ("idx", "drvprod_dd_trd")
    names = [c.lower() for c in dv.SOURCE_COLUMNS + dv.PARSED_COLUMNS]
    assert len(names) == len(set(names))
    for d in ("20101230", "20110103", "20261007"):
        for r in _deriv(d):
            assert tuple(r) == dv.SOURCE_COLUMNS
    p = dv.parse_derivative_rows(_deriv("20261007"))[0]
    assert set(p) == set(dv.SOURCE_COLUMNS) | set(dv.PARSED_COLUMNS)


def test_derivative_before_first_tr_date_not_missing() -> None:
    from collector.kr.adapters.krx_baseline_openapi import derivative_index as dv

    assert dv.classify_derivative_day(_deriv("20101230")) == DayStatus("trading", 3, 3, 0, 0)


def test_derivative_tr_values() -> None:
    from collector.kr.adapters.krx_baseline_openapi import derivative_index as dv

    for d, close, day in (
        ("20110103", 273.81, date(2011, 1, 3)),
        ("20261007", 1425.15, date(2026, 10, 7)),
    ):
        raw = _deriv(d)
        tr = dv.parse_derivative_rows(raw)[0]
        assert tr["IDX_NM"] == "코스피 200 TR" and tr["bas_date"] == day
        assert tr["CLSPRC_IDX"] == str(close) and tr["clsprc_idx_num"] == close
        assert dv.classify_derivative_day(raw) == DayStatus("trading", 3, 3, 0, 0)
    assert dv.parse_derivative_rows(_deriv("20110103"))[0]["cmpprevdd_idx_num"] is None


def test_derivative_tr_missing_after_first_date() -> None:
    from collector.kr.adapters.krx_baseline_openapi import derivative_index as dv

    raw = _deriv("20261007")[1:]  # TR 행 없음
    assert dv.classify_derivative_day(raw).required_missing == 1
    blank = [dict(r) for r in _deriv("20261007")]
    blank[0]["CLSPRC_IDX"] = ""
    assert dv.classify_derivative_day(blank) == DayStatus("trading", 3, 2, 0, 1)


def test_derivative_no_price_and_empty_and_missing_field() -> None:
    from collector.kr.adapters.krx_baseline_openapi import derivative_index as dv

    blank = [dict(r, CLSPRC_IDX="") for r in _deriv("20261007")]
    assert dv.classify_derivative_day(blank) == DayStatus("no_price", 3, 0, 0, 1)
    assert dv.classify_derivative_day([]) == DayStatus("empty", 0, 0, 0, 0)
    bad = dict(_deriv("20261007")[0])
    del bad["LWPRC_IDX"]
    with pytest.raises(KeyError):
        dv.parse_derivative_rows([bad])
