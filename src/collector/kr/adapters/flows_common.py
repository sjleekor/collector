"""Shared definitions for security-flow adapters."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FlowMetricSpec:
    """Canonical security-flow metric metadata."""

    metric_code: str
    metric_name: str
    unit: str


FOREIGN_HOLDING_SHARES = FlowMetricSpec(
    metric_code="foreign_holding_shares",
    metric_name="외국인 보유주식수",
    unit="shares",
)

INSTITUTION_NET_BUY_VOLUME = FlowMetricSpec(
    metric_code="institution_net_buy_volume",
    metric_name="기관 순매수 수량",
    unit="shares",
)
INDIVIDUAL_NET_BUY_VOLUME = FlowMetricSpec(
    metric_code="individual_net_buy_volume",
    metric_name="개인 순매수 수량",
    unit="shares",
)
FOREIGN_NET_BUY_VOLUME = FlowMetricSpec(
    metric_code="foreign_net_buy_volume",
    metric_name="외국인 순매수 수량",
    unit="shares",
)

SHORT_SELLING_VOLUME = FlowMetricSpec(
    metric_code="short_selling_volume",
    metric_name="공매도 거래량",
    unit="shares",
)
SHORT_SELLING_VALUE = FlowMetricSpec(
    metric_code="short_selling_value",
    metric_name="공매도 거래대금",
    unit="KRW",
)
SHORT_SELLING_BALANCE_QUANTITY = FlowMetricSpec(
    metric_code="short_selling_balance_quantity",
    metric_name="공매도 잔고 수량",
    unit="shares",
)

KRX_INVESTOR_COLUMNS = {
    "TRDVAL1": INSTITUTION_NET_BUY_VOLUME,
    "TRDVAL3": INDIVIDUAL_NET_BUY_VOLUME,
    "TRDVAL4": FOREIGN_NET_BUY_VOLUME,
}

UNSUPPORTED_FLOW_METRIC_CODES = ["borrow_balance_quantity"]
