"""Metric catalog + mapping-rule definitions (pure data, no Storage).

Moved out of ``service/normalize_metrics.py`` so both the Postgres normalization
orchestrator and the DuckDB ``stock_metric_fact`` mart can import the same rule
set without depending on the service layer. Behavior is unchanged from the
original definitions; only the function names are now public.

See ``docs/dev/20260728_refactor_pipeline/00_refactor_plan.md`` §3.0, §3.1.
"""

from __future__ import annotations

from datetime import date

from collector.kr.domain.models import MetricCatalogEntry, MetricMappingRule


def default_metric_catalog() -> list[MetricCatalogEntry]:
    return [
        MetricCatalogEntry("revenue", "매출액", "financial", "KRW", "손익계산서 매출액"),
        MetricCatalogEntry("cogs", "매출원가", "financial", "KRW", "손익계산서 매출원가"),
        MetricCatalogEntry(
            "gross_profit", "매출총이익", "financial", "KRW", "손익계산서 매출총이익"
        ),
        MetricCatalogEntry("sga", "판매비와관리비", "financial", "KRW", "판매비와관리비"),
        MetricCatalogEntry("operating_income", "영업이익", "financial", "KRW", "영업이익"),
        MetricCatalogEntry("net_income", "당기순이익", "financial", "KRW", "당기순이익"),
        MetricCatalogEntry(
            "controlling_net_income",
            "지배주주순이익",
            "financial",
            "KRW",
            "지배기업 소유주지분 순이익",
        ),
        MetricCatalogEntry("total_assets", "총자산", "financial", "KRW", "자산총계"),
        MetricCatalogEntry("total_liabilities", "총부채", "financial", "KRW", "부채총계"),
        MetricCatalogEntry("total_equity", "총자본", "financial", "KRW", "자본총계"),
        # F-5.1. Five accounts measured on the 2026-09-08 lake before being
        # mapped; coverage tables and the two rejections are in
        # ../modeler/docs/dev/20260907_additional_feature/poc/metric_rules_ext.md 6-8.
        MetricCatalogEntry(
            "current_assets", "유동자산", "financial", "KRW", "재무상태표 유동자산"
        ),
        MetricCatalogEntry(
            "current_liabilities", "유동부채", "financial", "KRW", "재무상태표 유동부채"
        ),
        MetricCatalogEntry(
            "retained_earnings", "이익잉여금", "financial", "KRW", "재무상태표 이익잉여금"
        ),
        # Short and long term are separate metrics, not one `borrowings`: a
        # mapping rule picks ONE winner per (metric, corp, period, basis), so a
        # sum is not expressible here. feat_fin_risk adds them (PoC 7 decision 3).
        MetricCatalogEntry(
            "borrowings_short_term", "단기차입금", "financial", "KRW", "재무상태표 단기차입금"
        ),
        MetricCatalogEntry(
            "borrowings_long_term", "장기차입금", "financial", "KRW", "재무상태표 장기차입금"
        ),
        MetricCatalogEntry(
            "cash_and_cash_equivalents",
            "현금및현금성자산",
            "financial",
            "KRW",
            "재무상태표 현금및현금성자산",
        ),
        MetricCatalogEntry(
            "operating_cash_flow",
            "영업활동현금흐름",
            "financial",
            "KRW",
            "현금흐름표 영업활동현금흐름",
        ),
        MetricCatalogEntry(
            "investing_cash_flow",
            "투자활동현금흐름",
            "financial",
            "KRW",
            "현금흐름표 투자활동현금흐름",
        ),
        MetricCatalogEntry(
            "financing_cash_flow",
            "재무활동현금흐름",
            "financial",
            "KRW",
            "현금흐름표 재무활동현금흐름",
        ),
        MetricCatalogEntry("issued_shares", "발행주식수", "share_count", "shares", "발행주식 총수"),
        MetricCatalogEntry("treasury_shares", "자기주식수", "share_count", "shares", "자기주식 수"),
        MetricCatalogEntry(
            "dps", "주당 현금배당금", "shareholder_return", "KRW", "보통주 기준 DPS"
        ),
        MetricCatalogEntry(
            "interest_received",
            "이자수익",
            "financial",
            "KRW",
            "현금흐름표 이자수취액",
        ),
        MetricCatalogEntry(
            "interest_paid",
            "이자비용",
            "financial",
            "KRW",
            "현금흐름표 이자지급액",
        ),
        MetricCatalogEntry(
            "dividends_paid",
            "배당금 지급액",
            "financial",
            "KRW",
            "현금흐름표 배당금 지급액",
        ),
        MetricCatalogEntry(
            "capex_ppe",
            "유형자산 취득액",
            "financial",
            "KRW",
            "현금흐름표 유형자산 취득액",
        ),
        MetricCatalogEntry(
            "capex_intangible",
            "무형자산 취득액",
            "financial",
            "KRW",
            "현금흐름표 무형자산 취득액",
        ),
        MetricCatalogEntry(
            "borrowing_proceeds_long_term",
            "장기차입금 증가액",
            "financial",
            "KRW",
            "장기차입금 조달 현금유입",
        ),
        MetricCatalogEntry(
            "borrowing_repayments_long_term",
            "장기차입금 상환액",
            "financial",
            "KRW",
            "장기차입금 상환 현금유출",
        ),
        MetricCatalogEntry(
            "treasury_share_acquisition_amount",
            "자사주 매입금액",
            "financial",
            "KRW",
            "자기주식 취득 현금유출",
        ),
        MetricCatalogEntry(
            "weighted_avg_shares",
            "가중평균주식수",
            "xbrl",
            "shares",
            "기본주당이익 계산용 가중평균주식수",
        ),
        MetricCatalogEntry(
            "diluted_shares",
            "희석주식수",
            "xbrl",
            "shares",
            "희석주당이익 계산용 가중평균주식수",
        ),
        MetricCatalogEntry(
            "depreciation_expense",
            "감가상각비",
            "xbrl",
            "KRW",
            "당기 감가상각비",
        ),
        MetricCatalogEntry(
            "amortization_intangible_assets",
            "무형자산상각비",
            "xbrl",
            "KRW",
            "당기 무형자산상각비",
        ),
    ]


#: Priority band for the I7 XBRL fallback rules. Statement rules sit at 10
#: (CFS) and 20 (OFS), and these sit strictly below both so a fallback only
#: ever fills a gap -- it never outranks a figure the financial-statement API
#: actually reported.
XBRL_FALLBACK_CFS_PRIORITY = 100
XBRL_FALLBACK_OFS_PRIORITY = 200


def _financial_rule(
    metric_code: str, account_id: str, sj_div: str, priority: int, fs_div: str
) -> MetricMappingRule:
    return MetricMappingRule(
        rule_code=f"fin.{metric_code}.{fs_div.lower()}.{sj_div.lower()}.{account_id}",
        metric_code=metric_code,
        source_table="dart_financial_statement_raw",
        value_selector="thstrm_amount",
        priority=priority,
        fs_div=fs_div,
        sj_div=sj_div,
        account_id=account_id,
    )


def _xbrl_fallback_rule(
    metric_code: str, concept_id: str, priority: int, fs_div: str
) -> MetricMappingRule:
    """An XBRL rule that fills a financial-statement metric's gaps (I7).

    Different from the ``xbrl.*`` rules above in one way that matters: it names
    a ``fs_div``. The mart reads the basis off the XBRL context's dimensions
    (``ConsolidatedMember`` -> CFS, ``SeparateMember`` -> OFS) for such a rule,
    and only then does the candidate land in the same winner partition as the
    statement rule it is meant to back up. A rule without ``fs_div`` stays at
    ``fs_basis = ''``, which is a different partition, so it cannot fill a gap
    -- it can only add a second row.
    """
    return MetricMappingRule(
        rule_code=f"xbrlfb.{metric_code}.{fs_div.lower()}.{concept_id.lower()}",
        metric_code=metric_code,
        source_table="dart_xbrl_fact_raw",
        value_selector="value_numeric",
        priority=priority,
        fs_div=fs_div,
        account_id=concept_id,
    )


def default_metric_mapping_rules() -> list[MetricMappingRule]:
    rules: list[MetricMappingRule] = []
    financial_specs = [
        ("revenue", "ifrs-full_Revenue", "IS"),
        ("cogs", "ifrs-full_CostOfSales", "IS"),
        ("gross_profit", "ifrs-full_GrossProfit", "IS"),
        ("sga", "dart_TotalSellingGeneralAdministrativeExpenses", "IS"),
        ("operating_income", "dart_OperatingIncomeLoss", "IS"),
        ("total_assets", "ifrs-full_Assets", "BS"),
        ("total_liabilities", "ifrs-full_Liabilities", "BS"),
        ("total_equity", "ifrs-full_Equity", "BS"),
        # F-5.1. Same shape as total_assets/total_liabilities: the statement rule
        # names the `ifrs-full_` spelling and the `ifrs_` one arrives as an XBRL
        # fallback below. `ifrs-full_ShorttermBorrowings` is labelled 차입금 by
        # some filers and 단기차입금 by others -- the concept is the 단기차입금
        # line either way, which is why the label is not what is matched on.
        ("current_assets", "ifrs-full_CurrentAssets", "BS"),
        ("current_liabilities", "ifrs-full_CurrentLiabilities", "BS"),
        ("retained_earnings", "ifrs-full_RetainedEarnings", "BS"),
        ("borrowings_short_term", "ifrs-full_ShorttermBorrowings", "BS"),
        # dart_LongTermBorrowingsGross is the 장기차입금 balance-sheet line.
        # `ifrs-full_LongtermBorrowings` only exists from 2023 (539 corps), so it
        # is the fallback, not the primary. "Gross" is gross of the present-value
        # discount, shown as a separate contra for 148 corps -- a small overstatement,
        # bounded: short + long never exceeds total liabilities in 10,012 corp-years.
        ("borrowings_long_term", "dart_LongTermBorrowingsGross", "BS"),
        ("cash_and_cash_equivalents", "ifrs-full_CashAndCashEquivalents", "BS"),
        ("operating_cash_flow", "ifrs-full_CashFlowsFromUsedInOperatingActivities", "CF"),
        ("investing_cash_flow", "ifrs-full_CashFlowsFromUsedInInvestingActivities", "CF"),
        ("financing_cash_flow", "ifrs-full_CashFlowsFromUsedInFinancingActivities", "CF"),
        (
            "interest_received",
            "ifrs-full_InterestReceivedClassifiedAsOperatingActivities",
            "CF",
        ),
        (
            "interest_paid",
            "ifrs-full_InterestPaidClassifiedAsOperatingActivities",
            "CF",
        ),
        (
            "dividends_paid",
            "ifrs-full_DividendsPaidClassifiedAsFinancingActivities",
            "CF",
        ),
        (
            "capex_ppe",
            "ifrs-full_PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
            "CF",
        ),
        (
            "capex_intangible",
            "ifrs-full_PurchaseOfIntangibleAssetsClassifiedAsInvestingActivities",
            "CF",
        ),
        (
            "borrowing_proceeds_long_term",
            "dart_ProceedsFromLongTermBorrowings",
            "CF",
        ),
        (
            "borrowing_repayments_long_term",
            "ifrs-full_RepaymentsOfNoncurrentBorrowings",
            "CF",
        ),
        (
            "treasury_share_acquisition_amount",
            "dart_AcquisitionOfTreasuryShares",
            "CF",
        ),
    ]
    for metric_code, account_id, sj_div in financial_specs:
        rules.append(_financial_rule(metric_code, account_id, sj_div, 10, "CFS"))
        rules.append(_financial_rule(metric_code, account_id, sj_div, 20, "OFS"))

    income_specs = [
        ("net_income", "ifrs-full_ProfitLoss", "CFS", "CIS", 10),
        ("net_income", "ifrs_ProfitLoss", "CFS", "CIS", 11),
        ("net_income", "ifrs-full_ProfitLoss", "CFS", "IS", 20),
        ("net_income", "ifrs_ProfitLoss", "CFS", "IS", 21),
        ("net_income", "ifrs-full_ProfitLoss", "OFS", "CIS", 30),
        ("net_income", "ifrs_ProfitLoss", "OFS", "CIS", 31),
        ("net_income", "ifrs-full_ProfitLoss", "OFS", "IS", 40),
        ("net_income", "ifrs_ProfitLoss", "OFS", "IS", 41),
        (
            "controlling_net_income",
            "ifrs-full_ProfitLossAttributableToOwnersOfParent",
            "CFS",
            "CIS",
            10,
        ),
        (
            "controlling_net_income",
            "ifrs_ProfitLossAttributableToOwnersOfParent",
            "CFS",
            "CIS",
            11,
        ),
        (
            "controlling_net_income",
            "ifrs-full_ProfitLossAttributableToOwnersOfParent",
            "CFS",
            "IS",
            20,
        ),
        (
            "controlling_net_income",
            "ifrs_ProfitLossAttributableToOwnersOfParent",
            "CFS",
            "IS",
            21,
        ),
    ]
    for metric_code, account_id, fs_div, sj_div, priority in income_specs:
        rules.append(_financial_rule(metric_code, account_id, sj_div, priority, fs_div))

    rules.extend(
        [
            MetricMappingRule(
                rule_code="share.issued_shares.total",
                metric_code="issued_shares",
                source_table="dart_share_count_raw",
                value_selector="istc_totqy",
                priority=10,
                row_name="합계",
            ),
            MetricMappingRule(
                rule_code="share.treasury_shares.total",
                metric_code="treasury_shares",
                source_table="dart_share_count_raw",
                value_selector="tesstk_co",
                priority=10,
                row_name="합계",
            ),
            MetricMappingRule(
                rule_code="return.dps.common",
                metric_code="dps",
                source_table="dart_shareholder_return_raw",
                value_selector="value_numeric",
                priority=10,
                statement_type="dividend",
                row_name="주당 현금배당금(원)",
                stock_knd="보통주",
                metric_code_match="thstrm",
            ),
            MetricMappingRule(
                rule_code="return.dps.default",
                metric_code="dps",
                source_table="dart_shareholder_return_raw",
                value_selector="value_numeric",
                priority=20,
                statement_type="dividend",
                row_name="주당 현금배당금(원)",
                metric_code_match="thstrm",
            ),
        ]
    )

    xbrl_specs = [
        (
            "weighted_avg_shares",
            [
                "ifrs-full_WeightedAverageShares",
                "ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
                "ifrs-full_WeightedAverageNumberOfSharesOutstandingBasic",
            ],
        ),
        (
            "diluted_shares",
            [
                "ifrs-full_AdjustedWeightedAverageShares",
                "ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingDiluted",
                "ifrs-full_WeightedAverageNumberOfSharesOutstandingDiluted",
            ],
        ),
        (
            "depreciation_expense",
            [
                "ifrs-full_DepreciationExpense",
                "ifrs-full_DepreciationAndAmortisationExpense",
                "ifrs-full_DepreciationAmortisationAndImpairmentExpense",
            ],
        ),
        (
            "amortization_intangible_assets",
            [
                "ifrs-full_AmortisationExpense",
                "dart_AmortizationOfIntangibleAssetsExpense",
            ],
        ),
    ]
    for metric_code, concept_ids in xbrl_specs:
        for priority_offset, concept_id in enumerate(concept_ids):
            rules.append(
                MetricMappingRule(
                    rule_code=f"xbrl.{metric_code}.{concept_id.lower()}",
                    metric_code=metric_code,
                    source_table="dart_xbrl_fact_raw",
                    value_selector="value_numeric",
                    priority=10 + priority_offset,
                    account_id=concept_id,
                )
            )

    # I7 — XBRL fallback for the financial-statement metrics.
    #
    # The diagnosis this closes: `fin_value_z`'s sales-to-price component is
    # filled for about 5% of rows, and `fin_gross_profitability` has coverage
    # 0.03, because `revenue` has 8,103 canonical rows against `net_income`'s
    # 141,011. The first reading was that the catalog mapped only the
    # `ifrs-full_` spelling and missed `ifrs_`. Counting the 2026-08-12 lake
    # showed something larger: the financial metrics had no XBRL rule at all.
    # The same facts are in `dart_xbrl_fact_raw` -- `ifrs-full_Revenue` 555,934
    # and `ifrs_Revenue` 184,846 -- and nothing was reading them.
    #
    # Priority sits below every statement rule (10/20 for CFS/OFS) so this only
    # fills gaps and never outranks a figure the financial-statement API
    # reported. Both spellings are mapped, `ifrs-full_` first.
    xbrl_fallback_specs = [
        ("revenue", ["ifrs-full_Revenue", "ifrs_Revenue"]),
        ("cogs", ["ifrs-full_CostOfSales", "ifrs_CostOfSales"]),
        ("gross_profit", ["ifrs-full_GrossProfit", "ifrs_GrossProfit"]),
        (
            "operating_income",
            ["dart_OperatingIncomeLoss", "ifrs-full_ProfitLossFromOperatingActivities"],
        ),
        ("total_assets", ["ifrs-full_Assets", "ifrs_Assets"]),
        ("total_liabilities", ["ifrs-full_Liabilities", "ifrs_Liabilities"]),
        ("total_equity", ["ifrs-full_Equity", "ifrs_Equity"]),
        ("net_income", ["ifrs-full_ProfitLoss", "ifrs_ProfitLoss"]),
        (
            "controlling_net_income",
            [
                "ifrs-full_ProfitLossAttributableToOwnersOfParent",
                "ifrs_ProfitLossAttributableToOwnersOfParent",
            ],
        ),
        (
            "operating_cash_flow",
            [
                "ifrs-full_CashFlowsFromUsedInOperatingActivities",
                "ifrs_CashFlowsFromUsedInOperatingActivities",
            ],
        ),
        # F-5.0. These four had statement rules only, so they existed only
        # where dart_financial_statement_raw reaches -- about 100 rows before
        # 2019 against ~100,000 after, which is what confined five of
        # feat_fin_risk's nine families to 2020+ (F-4.6).
        #
        # The `ifrs_` spelling is the one that matters here, and it is easy to
        # get wrong: DART switched taxonomy prefix around 2019, so measured on
        # the 2026-08-23 lake `ifrs-full_Liabilities` has 364 facts up to 2018
        # against `ifrs_Liabilities`'s 112,827. Mapping only `ifrs-full_` would
        # add nothing to the early years -- exactly the gap being closed. Both
        # spellings are listed for the same reason total_liabilities lists
        # both. See ../modeler/docs/dev/20260907_additional_feature/poc/metric_rules_ext.md.
        #
        # ifrs-full_InterestPaid / ifrs_InterestPaid carry no facts at all, so
        # only the ClassifiedAsOperatingActivities form is mapped.
        (
            "investing_cash_flow",
            [
                "ifrs-full_CashFlowsFromUsedInInvestingActivities",
                "ifrs_CashFlowsFromUsedInInvestingActivities",
            ],
        ),
        (
            "financing_cash_flow",
            [
                "ifrs-full_CashFlowsFromUsedInFinancingActivities",
                "ifrs_CashFlowsFromUsedInFinancingActivities",
            ],
        ),
        (
            "interest_paid",
            [
                "ifrs-full_InterestPaidClassifiedAsOperatingActivities",
                "ifrs_InterestPaidClassifiedAsOperatingActivities",
            ],
        ),
        (
            "cash_and_cash_equivalents",
            [
                "ifrs-full_CashAndCashEquivalents",
                "ifrs_CashAndCashEquivalents",
            ],
        ),
        # F-5.1. The third spelling on short-term borrowings is not decoration:
        # with only the two ifrs forms, 2015 cross-sectional coverage is 0.003,
        # and dart_ShortTermBorrowings (2015-2017, 1,673 corps) takes the same
        # year to 0.823. The `ifrs-full_`-only mistake F-5.0 caught, again.
        ("current_assets", ["ifrs-full_CurrentAssets", "ifrs_CurrentAssets"]),
        (
            "current_liabilities",
            ["ifrs-full_CurrentLiabilities", "ifrs_CurrentLiabilities"],
        ),
        ("retained_earnings", ["ifrs-full_RetainedEarnings", "ifrs_RetainedEarnings"]),
        (
            "borrowings_short_term",
            [
                "ifrs-full_ShorttermBorrowings",
                "ifrs_ShorttermBorrowings",
                "dart_ShortTermBorrowings",
            ],
        ),
        (
            "borrowings_long_term",
            ["dart_LongTermBorrowingsGross", "ifrs-full_LongtermBorrowings"],
        ),
    ]
    for metric_code, concept_ids in xbrl_fallback_specs:
        for priority_offset, concept_id in enumerate(concept_ids):
            for fs_div, base_priority in (
                ("CFS", XBRL_FALLBACK_CFS_PRIORITY),
                ("OFS", XBRL_FALLBACK_OFS_PRIORITY),
            ):
                rules.append(
                    _xbrl_fallback_rule(
                        metric_code, concept_id, base_priority + priority_offset, fs_div
                    )
                )
    return rules


def reprt_code_to_period_type(reprt_code: str) -> str:
    return {
        "11013": "q1",
        "11012": "half",
        "11014": "q3",
        "11011": "annual",
    }.get(reprt_code, "unknown")


def infer_period_end(bsns_year: int, reprt_code: str) -> date | None:
    month_day = {
        "11013": (3, 31),
        "11012": (6, 30),
        "11014": (9, 30),
        "11011": (12, 31),
    }.get(reprt_code)
    if month_day is None:
        return None
    month, day = month_day
    return date(bsns_year, month, day)
