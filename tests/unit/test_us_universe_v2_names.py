"""유니버스 v2 이름·심볼·FTD 설명 규칙 (설계 02 §2.2). 순수 함수만 부른다."""

from __future__ import annotations

import pytest

from collector.us.universe.v2 import names


@pytest.mark.parametrize(
    "symbol,name,kind",
    [
        ("GOOG", "Alphabet Inc. - Class C Capital Stock", names.KIND_COMMON),
        ("TSM", "Taiwan Semiconductor Manufacturing Company Ltd.", names.KIND_COMMON),
        ("ACME", "Acme Corp 6.5% Notes due 2030", names.KIND_BOND),
        ("ACME", "Acme Corp - 6.5% Preferred Shares", names.KIND_PREFERRED),
        ("ACME$A", "Acme Corp Series A", names.KIND_PREFERRED),
        # BNS 는 이름에 Pfd 가 들어 있지만 보통주다
        ("BNS", "Bank Nova Scotia Halifax Pfd 3 Ordinary Shares", names.KIND_COMMON),
        ("AACI", "Armada Acquisition Corp. III - Class A Ordinary Share", names.KIND_SPAC),
        ("VVR", "Invesco Senior Income Trust Common Shares", names.KIND_FUND),
        ("ACME", None, names.KIND_UNKNOWN),
        ("ACME$B", None, names.KIND_PREFERRED),
        # REIT·은행·로열티 신탁은 Trust 를 써도 펀드가 아니다
        ("REIT", "Foo Realty Trust - Common Shares of Beneficial Interest", names.KIND_COMMON),
    ],
)
def test_classify_name(symbol, name, kind):
    assert names.classify_name(symbol, name) == kind


@pytest.mark.parametrize(
    "name,token",
    [
        ("Acme Corp - Warrant", "warrant"),
        ("Acme Corp - Right", "right"),
        ("Acme Corp - Unit", "unit"),
        ("Acme Corp - Units, each consisting of one share and one warrant", "warrant"),
        # **단어 경계.** 보통주를 오탐하지 않는다 (T3)
        ("Unity Software Inc. Common Stock", None),
        ("United Rentals, Inc. Common Stock", None),
        ("Barry Energy Partners L.P. - Common Units", None),
        ("Foo Holdings Limited Partner Units", None),
        ("Foo Capital LP Units", None),
        ("Foo Corp - Rights Agreement", None),
        ("Warrantee Holdings Common Stock", None),
        (None, None),
    ],
)
def test_token_other_uses_word_boundaries(name, token):
    assert names.token_other(name) == token


def test_norm_drops_security_description_and_stopwords():
    assert names.norm("Pinduoduo Inc. - American Depositary Shares") == ("PINDUODUO",)
    assert names.norm(None) == ()


def test_big_name_change_is_for_different_companies_only():
    old = names.norm_full("Pinduoduo Inc.")
    new = names.norm_full("PDD Holdings Inc.")
    # 이름은 달라도 N 신호다(가격·cik 가 이어지면 분리는 안 한다 — 분리 쪽 시험이 본다)
    assert names.big_name_change(old, new)
    # 발행사 접두만 바뀐 것(Credit Suisse AG - VelocityShares ...)은 아니다
    a = names.norm_full("Credit Suisse AG - VelocityShares 3x Long Silver ETN")
    b = names.norm_full("VelocityShares 3x Long Silver ETN")
    assert not names.big_name_change(a, b)
    # 첫 토큰 접두 일치는 5자 이상일 때만 같은 이름이다: SPAC 과 SPACE 는 다르다
    assert names.big_name_change(names.norm_full("SPAC Alpha"), names.norm_full("SPACE Beta"))
    assert not names.big_name_change(
        names.norm_full("Quantum Computing Inc"), names.norm_full("Quantum Computing Holdings")
    )
    assert not names.big_name_change((), names.norm_full("X Corp"))


def test_jaccard_handles_missing_names():
    assert names.jaccard("Acme Corp", "Acme Corp Inc") == 1.0
    assert names.jaccard(None, "Acme") is None
    assert names.jaccard("Foo Bar", "Baz Qux") == 0.0


@pytest.mark.parametrize(
    "desc,kind",
    [
        ("DUKE ENERGY CORP NEW JR SUB DE", "bond"),
        ("AT&T INC 5.350% GLOBAL NTS", "bond"),
        ("PG&E CORP EQUITY UNIT", "warrant_right_unit"),
        ("SUPER MICRO COMP INC DEP SHS", "preferred"),
        ("ACME ACQUISITION CORP", "spac"),
        ("ACME ACQUISITION CORP WT", "warrant_right_unit"),
        ("ISHARES TRUST", "fund_like"),
        ("ABC CORP COM", "common_candidate"),
        ("BARRY ENERGY PARTNERS LP COM UNIT", "common_candidate"),
        ("BARRY ENERGY PARTNERS UNIT LTD", "common_candidate"),
        (None, None),
        ("  ", None),
    ],
)
def test_ftd_kind(desc, kind):
    assert names.ftd_kind(desc) == kind


def test_only_four_ftd_kinds_exclude():
    """펀드·ETF 꼴은 거르지 않는다 — 63멤버월 중 62개가 정답 보통주였다 (§5.8 1.3)."""
    assert names.FTD_EXCLUDE_KINDS == {"bond", "preferred", "warrant_right_unit", "spac"}


@pytest.mark.parametrize(
    "symbol,flag",
    [
        ("ABCDU", True),  # 유닛
        ("ABCDW", True),  # 워런트
        ("ABCDG", True),
        ("ABCDM", True),
        ("ABCDA", False),  # 클래스 A 보통주(99%)
        ("ABCDB", False),
        ("ABCDK", False),
        ("ABCDL", False),  # 보통주가 섞여 쓰지 않는다
        ("ABCDO", False),
        ("ABCDP", False),
        ("ABCD", False),
        ("ACME$A", True),
        ("ACME.U", True),
        ("ACME.WS", True),
        ("BRK.A", False),
    ],
)
def test_suffix_noncommon(symbol, flag):
    assert names.suffix_noncommon(symbol) is flag


@pytest.mark.parametrize(
    "name,sub",
    [
        ("Taiwan Semi - American Depositary Shares", "adr"),
        ("Foo Realty Trust Common Shares", "reit"),
        ("Barry Energy Partners L.P. - Common Units", "common_equity"),
        ("Acme Inc. Common Stock", "common"),
        (None, "common"),
    ],
)
def test_common_subtype(name, sub):
    assert names.common_subtype(name) == sub
