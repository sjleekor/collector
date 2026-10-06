"""증권 마스터 판정 순서와 규칙 (설계 02 §2.2). 합성 표를 ``classify`` 에 직접 넣는다."""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from collector.us.universe.v2 import master
from collector.us.universe.v2.config import DEFAULT_RULES

BASE = dict(
    symbol="ACME",
    security_name="Acme Inc. - Common Stock",
    l_etf=False,
    l_test=False,
    stale=False,
    cik_e=1.0,
    sic_e="3711",
    s_fund=False,
    s_bdc=False,
    op18=True,
    k6_18=False,
    cname="Acme Inc",
    rel506=False,
    ftd_desc=None,
)


def judge(rules=DEFAULT_RULES, **over) -> pd.Series:
    row = {**BASE, **over}
    return master.classify(pd.DataFrame([row]), rules).iloc[0]


def reason(**over) -> str:
    return judge(**over).reason_code


def test_every_reason_in_the_order_table_is_reachable_and_documented():
    assert master.REASON_ORDER[0] == "fund_sec"
    assert master.REASON_ORDER[-1] == "unknown"
    assert set(master.SOURCE_CODE) == set(master.REASON_ORDER)
    assert master.INCLUDED <= set(master.REASON_ORDER)


@pytest.mark.parametrize(
    "over,expected",
    [
        (dict(s_fund=True, l_etf=True), "fund_sec"),  # 1이 3보다 앞선다
        (dict(s_bdc=True), "bdc_sec"),
        (dict(l_etf=True), "etf_list"),
        (dict(l_test=True), "test_list"),
        (dict(security_name="Acme 6.5% Notes due 2030"), "name_bond"),
        (dict(security_name="Acme Corp - Preferred Shares"), "name_pref"),
        (dict(security_name="Acme Acquisition Corp - Class A Ordinary Shares"), "name_spac"),
        (dict(sic_e="6770"), "spac_sic"),
        (dict(security_name="Acme Corp - Warrant"), "token_other"),
        (dict(sic_e="6221", cname="Acme Gold Trust"), "commodity_trust"),
        (dict(security_name="Acme Income Fund"), "name_fund_strong"),
        (
            dict(security_name="Acme Municipal Portfolio", cname="Acme Municipal Portfolio"),
            "common_weak_issuer",
        ),
        (
            dict(security_name="Acme Municipal Portfolio", cname="Other Co", op18=False),
            "name_fund_weak",
        ),
        (dict(), "common_name"),
        (dict(stale=True), "common_name_stale"),
        (dict(security_name=None, cname="Acme Income Fund"), "issuer_name_noncommon"),
        (dict(security_name=None), "issuer_only"),
        (dict(security_name=None, op18=False, k6_18=True), "f6k_only"),
        (dict(security_name=None, op18=False), "unknown"),
    ],
)
def test_priority_order(over, expected):
    assert reason(**over) == expected


def test_include_set_is_the_policy_d2():
    included = {"common_weak_issuer", "common_name", "common_name_stale", "issuer_only"}
    assert master.INCLUDED == included
    assert judge().include
    assert not judge(l_etf=True).include
    # B2: 6-K 만 있는 갓 상장 외국 회사와 확인 안 됨은 뺀다
    assert not judge(security_name=None, op18=False, k6_18=True).include
    assert not judge(security_name=None, op18=False).include


def test_strong_fund_name_cannot_be_overturned_by_issuer_evidence():
    r = judge(security_name="Acme Income Fund", op18=True, cname="Acme Income Fund")
    assert r.reason_code == "name_fund_strong" and not r.include


def test_unity_united_and_common_units_are_not_token_other():
    for name in (
        "Unity Software Inc. Common Stock",
        "United Rentals, Inc. Common Stock",
        "Barry Energy Partners L.P. - Common Units",
    ):
        r = judge(security_name=name)
        assert r.reason_code == "common_name" and r.include, name


def test_sic_6221_alone_is_not_an_exclusion():
    """SIC 6221 일괄 배제는 쓰지 않는다. 발행사 이름이 trust·fund 꼴일 때만이다."""
    assert judge(sic_e="6221", cname="Seaboard Corp").reason_code == "common_name"
    assert judge(sic_e="6221", cname="Gemini Space Station, Inc.").reason_code == "common_name"
    assert judge(sic_e="6221", cname="Grayscale Bitcoin Trust").reason_code == "commodity_trust"


# --- B3: 8-K item 5.06 이 SPAC 표시를 푼다 -----------------------------------------------


def test_item_506_releases_spac_markers_and_the_rest_of_the_chain_runs():
    spac = dict(security_name="Acme Acquisition Corp - Class A Ordinary Shares")
    assert reason(**spac) == "name_spac"
    released = judge(rel506=True, **spac)
    # 풀린 뒤에는 나머지 규칙을 그대로 탄다 — 이름이 있으니 보통주다
    assert released.reason_code == "common_name" and released.include
    assert released.source_code.endswith("+8k_506")
    # SIC 6770 도 푼다
    assert judge(sic_e="6770", rel506=True).reason_code == "common_name"
    # 워런트·유닛은 풀려도 token_other 로 계속 빠진다
    assert judge(security_name="Acme Acquisition Corp - Warrant", rel506=True).reason_code == (
        "token_other"
    )


# --- 설계 7장 보조 규칙: 설정 플래그, 기본 꺼짐 -------------------------------------------


def test_spac_release_without_506_is_off_by_default():
    assert DEFAULT_RULES.spac_release_name_change is False
    # 이름은 이미 운영회사 꼴인데 SIC 는 아직 6770 인 합병 직후
    stuck = judge(sic_e="6770", name_to_operating=True)
    assert stuck.reason_code == "spac_sic"
    on = dataclasses.replace(DEFAULT_RULES, spac_release_name_change=True)
    assert judge(on, sic_e="6770", name_to_operating=True).reason_code == "common_name"
    # 이름이 안 바뀐 것은 켜도 안 푼다
    assert judge(on, sic_e="6770", name_to_operating=False).reason_code == "spac_sic"


# --- B1: issuer_only 를 FTD 설명과 접미로 거른다 ----------------------------------------------


def test_issuer_only_is_filtered_by_ftd_description_and_symbol_suffix():
    io = dict(security_name=None)
    assert judge(**io).reason_code == "issuer_only"
    for desc in ("AT&T INC 5.350% GLOBAL NTS", "DUKE ENERGY CORP NEW JR SUB DE", "ACME CORP WT"):
        r = judge(ftd_desc=desc, **io)
        assert r.reason_code == "issuer_only_ftd" and not r.include, desc
    # 펀드·ETF 꼴과 보통주 꼴은 넣는다
    assert judge(ftd_desc="ISHARES TRUST", **io).include
    assert judge(ftd_desc="ACME CORP COM", **io).include
    # 설명이 없으면 접미가 남은 근거다
    assert judge(symbol="ABCDU", **io).reason_code == "issuer_only_suffix"
    assert judge(symbol="ABCDA", **io).reason_code == "issuer_only"
    # 이름이 있는 증권에는 FTD 규칙을 안 쓴다
    assert judge(ftd_desc="ACME 5.350% NTS").reason_code == "common_name"


def test_name_invalid_row_with_dollar_symbol_is_preferred():
    assert judge(symbol="ACME$A", security_name=None).reason_code == "name_pref"


def test_security_type_and_issuer_kind_are_separate_columns():
    adr = judge(security_name="Taiwan Semi - American Depositary Shares")
    assert (adr.issuer_kind, adr.security_type, adr.include) == ("operating", "adr", True)
    etf = judge(l_etf=True, op18=False)
    assert (etf.issuer_kind, etf.security_type) == ("unknown", "etf")
    fund = judge(s_fund=True)
    assert fund.issuer_kind == "registered_fund"
    spac = judge(sic_e="6770", op18=False)
    assert spac.issuer_kind == "spac"
    bdc = judge(s_bdc=True)
    assert bdc.issuer_kind == "bdc"
    # 정기보고서는 "발행사가 운영회사"라는 근거일 뿐 증권이 보통주라는 근거가 아니다
    assert judge(security_name="Acme Corp - Warrant").security_type == "warrant"


def test_classify_requires_all_columns():
    with pytest.raises(ValueError, match="필요한 열"):
        master.classify(pd.DataFrame([{"symbol": "X"}]), DEFAULT_RULES)


def test_unknown_breakdown_names_the_cause():
    frame = master.classify(
        pd.DataFrame(
            [
                {**BASE, "symbol": "A", "security_name": None, "op18": False},
                {**BASE, "symbol": "B", "security_name": None, "op18": False, "cik_e": None},
            ]
        ),
        DEFAULT_RULES,
    ).assign(l_asof=[None, pd.Timestamp("2020-01-01")])
    out = master.unknown_breakdown(frame)
    assert out["total"] == 2 and out["no_cik"] == 1 and out["no_listing_row"] == 1
