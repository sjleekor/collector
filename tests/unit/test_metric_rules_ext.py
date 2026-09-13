"""F-5.1/F-5.2 — the five metrics added to `metric_rules` (PoC 6-8).

Each assertion here stands for a measurement in
`docs/dev/20260907_additional_feature/poc/metric_rules_ext.md`, so a future edit
that quietly drops one of them fails rather than silently narrowing coverage:

* short-term borrowings needs *three* spellings. With only the two `ifrs` forms
  its 2015 cross-sectional coverage is 0.003; `dart_ShortTermBorrowings` takes
  the same year to 0.823. This is the `ifrs-full_`-only mistake F-5.0 caught,
  and it would be invisible in any test that only checks "a rule exists";
* `borrowings` is deliberately NOT one metric. A mapping rule picks one winner
  per (metric, corp, period, basis), so the sum lives in `feat_fin_risk`;
* `rnd_expense` is deliberately absent — measured, rejected, and the two
  families that needed it cancelled;
* the 29 metrics that existed before are still all there. Their *values* are
  frozen by `golden/stock_metric_fact.json`; what this file adds is that none
  of them was renamed or absorbed.
"""

from __future__ import annotations

from collector.kr.definitions.metric_rules import (
    XBRL_FALLBACK_CFS_PRIORITY,
    XBRL_FALLBACK_OFS_PRIORITY,
    default_metric_catalog,
    default_metric_mapping_rules,
)

#: The catalog as it stood before F-5.1 (29 codes, F-5.0's state).
_METRICS_BEFORE_F51 = frozenset(
    {
        "amortization_intangible_assets",
        "borrowing_proceeds_long_term",
        "borrowing_repayments_long_term",
        "capex_intangible",
        "capex_ppe",
        "cash_and_cash_equivalents",
        "cogs",
        "controlling_net_income",
        "depreciation_expense",
        "diluted_shares",
        "dividends_paid",
        "dps",
        "financing_cash_flow",
        "gross_profit",
        "interest_paid",
        "interest_received",
        "investing_cash_flow",
        "issued_shares",
        "net_income",
        "operating_cash_flow",
        "operating_income",
        "revenue",
        "sga",
        "total_assets",
        "total_equity",
        "total_liabilities",
        "treasury_share_acquisition_amount",
        "treasury_shares",
        "weighted_avg_shares",
    }
)

_ADDED_BY_F51 = frozenset(
    {
        "borrowings_long_term",
        "borrowings_short_term",
        "current_assets",
        "current_liabilities",
        "retained_earnings",
    }
)


def _codes() -> frozenset[str]:
    return frozenset(entry.metric_code for entry in default_metric_catalog())


def _fallback_concepts(metric_code: str) -> set[str]:
    return {
        rule.account_id
        for rule in default_metric_mapping_rules()
        if rule.rule_code.startswith("xbrlfb.") and rule.metric_code == metric_code
    }


def _statement_rules(metric_code: str) -> list:
    return [
        rule
        for rule in default_metric_mapping_rules()
        if rule.rule_code.startswith("fin.") and rule.metric_code == metric_code
    ]


def test_the_catalog_grew_by_exactly_the_five_measured_metrics() -> None:
    assert len(_METRICS_BEFORE_F51) == 29
    assert _codes() == _METRICS_BEFORE_F51 | _ADDED_BY_F51
    assert len(_codes()) == 34


def test_short_term_borrowings_carries_the_third_spelling() -> None:
    # Without dart_ShortTermBorrowings, 2015-2016 coverage is 0.003 / 0.008.
    assert _fallback_concepts("borrowings_short_term") == {
        "ifrs-full_ShorttermBorrowings",
        "ifrs_ShorttermBorrowings",
        "dart_ShortTermBorrowings",
    }


def test_every_new_metric_maps_both_ifrs_spellings() -> None:
    # The F-5.0 lesson: DART switched taxonomy prefix around 2019, so the
    # `ifrs_` form is what covers the early years.
    for metric_code in ("current_assets", "current_liabilities", "retained_earnings"):
        concepts = _fallback_concepts(metric_code)
        assert any(c.startswith("ifrs-full_") for c in concepts), metric_code
        assert any(c.startswith("ifrs_") for c in concepts), metric_code


def test_long_term_borrowings_uses_the_dart_line_as_primary() -> None:
    # ifrs-full_LongtermBorrowings only exists from 2023 (539 corps), so making
    # it the primary would leave 2015-2022 to the fallback path.
    primaries = _statement_rules("borrowings_long_term")

    assert {rule.account_id for rule in primaries} == {"dart_LongTermBorrowingsGross"}
    assert _fallback_concepts("borrowings_long_term") == {
        "dart_LongTermBorrowingsGross",
        "ifrs-full_LongtermBorrowings",
    }


def test_borrowings_is_two_component_metrics_not_one_total() -> None:
    # A rule set cannot express a sum, so a single `borrowings` metric would
    # silently be "whichever component won", not interest-bearing debt.
    assert "borrowings" not in _codes()
    assert {"borrowings_short_term", "borrowings_long_term"} <= _codes()
    # The rejected component (max coverage 0.466, under the 0.5 threshold).
    assert "borrowings_current_portion" not in _codes()


def test_rnd_expense_is_absent_on_purpose() -> None:
    # XBRL has it only from 2023 (447 corps, 0.136 in 2025); the statement side
    # is 203 corps under `-표준계정코드 미사용-`. fin_rnd_to_sales and
    # fin_bm_intangible_adj were cancelled with it (PoC 7 decision 4).
    assert "rnd_expense" not in _codes()


def test_the_new_statement_rules_sit_on_the_balance_sheet() -> None:
    for metric_code in _ADDED_BY_F51:
        rules = _statement_rules(metric_code)
        assert rules, metric_code
        assert {rule.sj_div for rule in rules} == {"BS"}, metric_code
        assert {rule.fs_div for rule in rules} == {"CFS", "OFS"}, metric_code


def test_the_new_fallbacks_never_outrank_a_reported_statement_figure() -> None:
    # The whole point of a fallback priority: it fills a gap, it does not
    # override what fnlttSinglAcntAll actually returned.
    for metric_code in _ADDED_BY_F51:
        statement = max(rule.priority for rule in _statement_rules(metric_code))
        fallbacks = [
            rule
            for rule in default_metric_mapping_rules()
            if rule.rule_code.startswith("xbrlfb.") and rule.metric_code == metric_code
        ]
        assert fallbacks, metric_code
        assert all(rule.priority > statement for rule in fallbacks), metric_code
        assert {rule.fs_div for rule in fallbacks} == {"CFS", "OFS"}, metric_code
        cfs = [r.priority for r in fallbacks if r.fs_div == "CFS"]
        ofs = [r.priority for r in fallbacks if r.fs_div == "OFS"]
        assert min(cfs) == XBRL_FALLBACK_CFS_PRIORITY, metric_code
        assert min(ofs) == XBRL_FALLBACK_OFS_PRIORITY, metric_code


def test_no_new_rule_touches_a_pre_existing_metric() -> None:
    # "기존 29 metric의 규칙은 건드리지 않는다" (03 3.1 step 2), stated as an
    # invariant rather than trusted to review.
    added = {
        rule.rule_code
        for rule in default_metric_mapping_rules()
        if rule.metric_code in _ADDED_BY_F51
    }
    others = {
        rule.rule_code
        for rule in default_metric_mapping_rules()
        if rule.metric_code in _METRICS_BEFORE_F51
    }

    assert not (added & others)
    # Every rule belongs to one of the two sets -- no rule names an unknown metric.
    assert len(added) + len(others) == len(default_metric_mapping_rules())
