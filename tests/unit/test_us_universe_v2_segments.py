"""종목 식별 규칙 (설계 02 §2.1). 신호 표를 직접 만들어 ``derive_splits`` 에 넣는다.

심볼·cik·CUSIP 는 전부 지어낸 값이다. 이름은 R3c 의 확인 사례 모양만 흉내 냈다.
"""

from __future__ import annotations

import dataclasses
import datetime as dt

import pandas as pd
import pytest

from collector.us.universe.v2 import segments
from collector.us.universe.v2.cal import Calendar, ftd_publication
from collector.us.universe.v2.config import DEFAULT_RULES

D = dt.date
RULES = DEFAULT_RULES


@pytest.fixture(scope="module")
def cal() -> Calendar:
    return Calendar.xnys(D(2018, 1, 1), D(2023, 12, 31))


def pub(d) -> dt.date:
    return ftd_publication(pd.Timestamp(d).date(), RULES.lag_ftd_days)


def frame(rows, columns):
    return pd.DataFrame(rows, columns=columns).pipe(
        lambda f: f.assign(**{c: pd.to_datetime(f[c]) for c in f.columns if c.endswith("date")})
    )


GAP = ["symbol", "prev_price_date", "resume_date", "gap"]
CIK = ["symbol", "prev_date", "new_date", "prev_cik", "new_cik"]
CUSIP = ["symbol", "c6_before", "c6_after", "old_last", "new_first", "sep_days"]
NAME = ["symbol", "prev_as_of", "new_as_of", "name_before", "name_after"]


def derive(*, gaps=(), ciks=(), cusips=(), names=(), first=None, ranges=(), rules=RULES):
    gaps_df = frame(list(gaps), GAP)
    cik_df = frame(list(ciks), CIK)
    cusip_df = pd.DataFrame(list(cusips), columns=CUSIP)
    for c in ("old_last", "new_first"):
        cusip_df[c] = pd.to_datetime(cusip_df[c])
    name_df = pd.DataFrame(list(names), columns=NAME)
    rng = pd.DataFrame(list(ranges), columns=["symbol", "first_seen", "last_seen"])
    return segments.derive_splits(
        gaps=gaps_df,
        cik_changes=cik_df,
        cusip_events=cusip_df,
        name_events_df=name_df,
        first_price=first or {},
        cusip_ranges=rng,
        rules=rules,
        pub_fn=pub,
    )


# --- 분리하는 사례 -------------------------------------------------------------------------


def test_gap_with_cusip_change_is_g_corr_known_when_the_ftd_is_published():
    """BNY·P 모양: 가격 공백 + CUSIP 앞 6자리 변경. 인지는 FTD 공개 지연 뒤다."""
    sp = derive(
        gaps=[("BNYX", D(2020, 4, 20), D(2020, 7, 1), 50)],
        cusips=[("BNYX", "AAAAAA", "BBBBBB", D(2019, 5, 1), D(2020, 6, 25), 421)],
    )
    assert len(sp) == 1
    r = sp.iloc[0]
    assert r.kind == segments.G_CORR
    assert r.event == pd.Timestamp("2020-07-01")
    assert r.known_pit == pd.Timestamp(pub(D(2020, 6, 25)))  # 6/30 + 20일
    assert r.known_pit == pd.Timestamp("2020-07-20")
    assert r.known_pit > r.event


def test_new_listing_with_a_far_cusip_is_s_dorm():
    """BXDC·HAWK 모양: 옛 가격이 레이크에 없는 새 상장. 경계는 첫 가격일이다."""
    sp = derive(
        cusips=[("HAWKX", "AAAAAA", "BBBBBB", D(2019, 1, 10), D(2020, 6, 10), 517)],
        first={"HAWKX": D(2020, 6, 12)},
    )
    assert list(sp.kind) == [segments.S_DORM]
    r = sp.iloc[0]
    assert r.event == pd.Timestamp("2020-06-12")
    assert r.known_pit == pd.Timestamp(pub(D(2020, 6, 10)))
    # 첫 가격일이 새 first_seen 에서 30일 넘게 떨어졌으면 새 상장이 아니다
    far = derive(
        cusips=[("HAWKX", "AAAAAA", "BBBBBB", D(2019, 1, 10), D(2020, 6, 10), 517)],
        first={"HAWKX": D(2019, 12, 1)},
    )
    assert far.empty


def test_dormant_gap_with_corroboration_is_known_at_resume_by_default():
    gaps = [("REUSE", D(2019, 12, 31), D(2021, 1, 4), 253)]
    ciks = [("REUSE", D(2020, 1, 2), D(2021, 3, 1), 10, 11)]
    on = derive(gaps=gaps, ciks=ciks)
    assert list(on.kind) == [segments.G_DORM]
    assert on.iloc[0].known_pit == pd.Timestamp("2021-01-04")  # 재개일
    assert on.iloc[0].ident_pit == pd.Timestamp("2021-03-01")  # 옛 구간 무효는 보강 뒤
    # 설계 문서 그대로(보강 신호 중 늦은 쪽)로 되돌릴 수 있다
    literal = derive(
        gaps=gaps, ciks=ciks, rules=dataclasses.replace(RULES, dorm_known_at_resume=False)
    )
    assert literal.iloc[0].known_pit == pd.Timestamp("2021-03-01")


def test_dormant_gap_on_a_mass_resume_day_is_known_when_the_corroboration_is(cal):
    """집단 재개일은 신호 없이는 안 끊는다. 보강 신호가 늦게 알려지면 그때 끊는다(T7)."""
    day = D(2021, 1, 4)
    gaps = [(f"M{i:02d}", D(2019, 12, 31), day, 253) for i in range(20)]
    cusips = [("M00", "AAAAAA", "BBBBBB", D(2019, 12, 15), D(2021, 3, 2), 443)]
    sp = derive(gaps=gaps, cusips=cusips)
    assert list(sp.symbol) == ["M00"]
    r = sp.iloc[0]
    assert r.kind == segments.G_DORM and r.event == pd.Timestamp(day)
    assert r.known_pit == pd.Timestamp(pub(D(2021, 3, 2)))  # 재개일이 아니라 보강 공개일


def test_dormant_gap_without_signals_splits_unless_a_mass_resume_day():
    """B4: 252거래일 이상 공백은 신호 없이도 끊는다. 같은 날 20개 이상이 재개하면 결손이다."""
    one = derive(gaps=[("QUIET", D(2019, 12, 31), D(2021, 1, 4), 253)])
    assert list(one.kind) == [segments.G_GAP252]
    r = one.iloc[0]
    assert r.known_pit == r.event and pd.isna(r.ident_pit)

    day = D(2021, 1, 4)
    mass = derive(gaps=[(f"M{i:02d}", D(2019, 12, 31), day, 253) for i in range(20)])
    assert mass.empty
    almost = derive(gaps=[(f"M{i:02d}", D(2019, 12, 31), day, 253) for i in range(19)])
    assert len(almost) == 19


def test_short_gap_without_signals_is_not_split():
    assert derive(gaps=[("QUIET", D(2020, 1, 2), D(2020, 6, 1), 100)]).empty
    # 252 미만은 집단 재개일 여부와 상관없이 안 끊는다
    assert derive(gaps=[("QUIET", D(2020, 1, 2), D(2020, 6, 1), 251)]).empty


def test_gap_and_corroboration_must_be_within_180_days():
    gaps = [("SLOW", D(2020, 1, 2), D(2020, 3, 2), 40)]
    near = derive(
        gaps=gaps, names=[("SLOW", D(2019, 10, 1), D(2019, 10, 2), "Acme Corp", "Other Holdings")]
    )
    assert len(near) == 1  # 재개일 앞 150일
    far = derive(
        gaps=gaps, names=[("SLOW", D(2019, 6, 1), D(2019, 6, 2), "Acme Corp", "Other Holdings")]
    )
    assert far.empty  # 앞 273일


# --- 분리하지 않는 사례 (T2) ----------------------------------------------------------------


def test_rename_with_continuous_prices_is_never_split():
    """PDD·IREN·AHG·AMPX: 이름·cik·CUSIP 가 바뀌어도 가격이 이어지면 같은 증권이다."""
    sp = derive(
        names=[("PDDX", D(2023, 9, 1), D(2023, 9, 28), "Pinduoduo Inc.", "PDD Holdings Ltd")],
        ciks=[("PDDX", D(2023, 9, 1), D(2023, 9, 28), 1, 2)],
        cusips=[("PDDX", "AAAAAA", "BBBBBB", D(2019, 1, 10), D(2023, 9, 28), 1600)],
        first={"PDDX": D(2018, 9, 1)},
    )
    assert sp.empty


def test_mass_cik_update_day_is_not_a_signal():
    """DIS·AA: 같은 날 50건 이상 cik 가 바뀌면 지도 일괄 갱신이다."""
    day = D(2019, 9, 16)
    ciks = [(f"S{i:03d}", D(2019, 9, 13), day, 100 + i, 500 + i) for i in range(50)]
    gaps = [("S000", D(2019, 7, 1), D(2019, 9, 1), 40)]  # 공백 + (일괄 갱신) K 뿐
    assert derive(gaps=gaps, ciks=ciks).empty
    # 49건이면 일괄 갱신이 아니라 실제 전환이다
    ciks49 = ciks[:49]
    assert len(derive(gaps=gaps, ciks=ciks49)) == 1


def test_cik_change_alone_is_not_reuse():
    sp = derive(ciks=[("X", D(2021, 1, 4), D(2021, 1, 5), 1, 2)])
    assert sp.empty


def test_issuer_prefix_rename_is_not_a_name_signal():
    """USLV: ``Credit Suisse AG -`` 접두 변경은 N 신호가 아니다."""
    rows = [
        ("USLV", D(2020, 1, 15), "Credit Suisse AG - VelocityShares 3x Long Silver ETN"),
        ("USLV", D(2020, 2, 15), "VelocityShares 3x Long Silver ETN"),
    ]
    assert segments.name_events(rows).empty


def test_name_events_merge_a_single_flip_back_and_find_a_real_change():
    flip = [
        ("ACME", D(2020, 1, 15), "Acme Corp"),
        ("ACME", D(2020, 2, 15), "Zeta Holdings"),
        ("ACME", D(2020, 3, 15), "Acme Corp"),
        ("ACME", D(2020, 4, 15), "Acme Corp"),
    ]
    assert segments.name_events(flip).empty
    real = [
        ("ACME", D(2020, 1, 15), "Acme Corp"),
        ("ACME", D(2020, 2, 15), "Zeta Holdings"),
        ("ACME", D(2020, 3, 15), "Zeta Holdings"),
    ]
    ev = segments.name_events(real)
    assert list(ev.new_as_of) == [D(2020, 2, 15)]


# --- PIT 와 사후 보기 ----------------------------------------------------------------------


def test_post_view_cancels_same_cusip_and_reversal_only_corroboration(cal):
    gaps = [
        ("SPAN", D(2020, 4, 1), D(2020, 6, 1), 40),
        ("REV", D(2020, 4, 1), D(2020, 6, 1), 40),
    ]
    ciks = [
        ("REV", D(2020, 5, 25), D(2020, 5, 26), 1, 2),
        ("REV", D(2020, 6, 5), D(2020, 6, 8), 2, 1),  # A→B→A 되돌림
    ]
    names = [("SPAN", D(2020, 5, 1), D(2020, 5, 15), "Acme Corp", "Zeta Holdings")]
    ranges = [("SPAN", D(2019, 1, 1), D(2021, 1, 1))]  # 같은 CUSIP 이 경계를 가로지른다
    sp = derive(gaps=gaps, ciks=ciks, names=names, ranges=ranges)
    assert set(sp.symbol) == {"SPAN", "REV"}
    assert bool(sp.set_index("symbol").cusip_span["SPAN"]) is True
    pit = segments.number_segments(sp, cal, view="pit")
    post = segments.number_segments(sp, cal, view="post")
    assert set(pit.symbol) == {"SPAN", "REV"}
    assert post.empty  # 사후 정정이 둘 다 취소한다
    assert (pit.segment_no == 2).all()


def test_pit_uses_the_next_trading_day_and_post_uses_the_event_day(cal):
    sp = derive(
        gaps=[("BNYX", D(2020, 4, 20), D(2020, 7, 1), 50)],
        cusips=[("BNYX", "AAAAAA", "BBBBBB", D(2019, 5, 1), D(2020, 6, 25), 421)],
    )
    pit = segments.number_segments(sp, cal, view="pit").iloc[0]
    post = segments.number_segments(sp, cal, view="post").iloc[0]
    assert pit.known_pit == pd.Timestamp("2020-07-20")  # 월요일
    assert pit.usable_from == pd.Timestamp("2020-07-21")  # 다음 거래일부터
    assert post.usable_from == pd.Timestamp("2020-07-01")  # 사건일부터


def test_nearby_events_of_one_symbol_are_merged(cal):
    sp = derive(
        gaps=[
            ("DUP", D(2020, 4, 1), D(2020, 6, 1), 40),
            ("DUP", D(2020, 6, 5), D(2020, 6, 30), 40),
        ],
        names=[("DUP", D(2020, 5, 1), D(2020, 5, 15), "Acme Corp", "Zeta Holdings")],
    )
    assert len(sp) == 1
    far = derive(
        gaps=[
            ("DUP", D(2020, 4, 1), D(2020, 6, 1), 40),
            ("DUP", D(2020, 8, 5), D(2020, 9, 15), 40),
        ],
        names=[("DUP", D(2020, 5, 1), D(2020, 5, 15), "Acme Corp", "Zeta Holdings")],
    )
    assert len(far) == 2
    numbered = segments.number_segments(far, cal, view="pit")
    assert list(numbered.segment_no) == [2, 3]


def test_label_timeline_never_goes_backwards_when_knowledge_arrives_out_of_order(cal):
    """사건 2 가 사건 3 보다 늦게 알려져도, 사건일이 가장 늦은 사건의 번호가 남는다."""
    pit = pd.DataFrame(
        {
            "symbol": ["X", "X"],
            "event": pd.to_datetime(["2020-03-01", "2020-06-01"]),
            "usable_from": pd.to_datetime(["2020-09-01", "2020-07-01"]),
            "segment_no": [2, 3],
        }
    )
    timeline = segments.label_timeline(pit)
    assert timeline == [("X", D(2020, 7, 1), 3), ("X", D(2020, 9, 1), 3)]


def test_segment_rows_for_both_views(cal):
    sp = derive(
        gaps=[("BNYX", D(2020, 4, 20), D(2020, 7, 1), 50)],
        cusips=[("BNYX", "AAAAAA", "BBBBBB", D(2019, 5, 1), D(2020, 6, 25), 421)],
    )
    pit = segments.number_segments(sp, cal, view="pit")
    rows = segments.segment_rows(
        ["BNYX", "ZZZ"], pit, view="pit", first_dates={"BNYX": D(2019, 1, 2)}, rule_version="v"
    )
    first = rows[(rows.symbol == "BNYX") & (rows.segment_no == 1)].iloc[0]
    second = rows[(rows.symbol == "BNYX") & (rows.segment_no == 2)].iloc[0]
    assert first.security_id == "BNYX#1" and first.seg_end == D(2020, 6, 30)
    assert second.security_id == "BNYX#2" and second.split_reason == "G_corr"
    assert second.usable_from == D(2020, 7, 21)
    zzz = rows[rows.symbol == "ZZZ"]
    assert len(zzz) == 1 and zzz.iloc[0].seg_end is None
