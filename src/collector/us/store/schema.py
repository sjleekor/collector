"""스냅샷 parquet의 계약 — 미국 계획 03 §3·§4.

두 겹으로 잠근다.

* **pyarrow 스키마**가 parquet에 실제로 박히는 타입이다. 가격은
  ``decimal128(14,4)``, 거래량은 ``int64``다 — 부동소수점으로 두면 나중에
  대조가 어긋난다 (stockanalysis가 거래량을 ``64677043.99999999``로 준다).
* **pandera 스키마**가 값을 본다. ``close > 0``, ``high >= low``,
  파일 안에서 ``(date, symbol)`` 유일.

``observed_at``은 예외 없이 모든 테이블에 있다 (03 §3). 원천이 과거를 고치므로
그것 없이는 어느 시점 기준인지 되돌릴 수 없다.
"""

from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pandera.pandas as pa
import pyarrow as pyar

#: 모든 스냅샷이 갖는다. 없으면 쓰기가 거부된다.
PROVENANCE_REQUIRED: tuple[str, ...] = ("observed_at",)

#: 원천이 주면 채우고 없으면 null이다 (03 §3의 표).
PROVENANCE_OPTIONAL: tuple[str, ...] = ("source_asof", "source_rev")

_PRICE = pyar.decimal128(14, 4)


def _is_decimal(series: pd.Series) -> pd.Series:
    """가격은 Decimal로 받는다 — float로 받으면 여기서 걸린다."""
    return series.map(lambda v: v is None or isinstance(v, Decimal))


# --- prices_daily (03 §4.1) -------------------------------------------------

PRICES_DAILY_ARROW = pyar.schema(
    [
        ("date", pyar.date32()),
        ("symbol", pyar.string()),
        ("open", _PRICE),
        ("high", _PRICE),
        ("low", _PRICE),
        ("close", _PRICE),
        ("volume", pyar.int64()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),
    ]
)

PRICES_DAILY = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]"),
        "symbol": pa.Column(str, pa.Check.str_length(min_value=1)),
        "open": pa.Column(object, pa.Check(_is_decimal, element_wise=False)),
        "high": pa.Column(object, pa.Check(_is_decimal, element_wise=False)),
        "low": pa.Column(object, pa.Check(_is_decimal, element_wise=False)),
        "close": pa.Column(
            object,
            [
                pa.Check(_is_decimal, element_wise=False),
                pa.Check(lambda s: s.map(lambda v: v > 0), element_wise=False),
            ],
        ),
        # 조정하지 않은 원시값이다. 수정주가를 저장하지 않는다 (03 §1).
        "volume": pa.Column("int64", pa.Check.ge(0)),
        "observed_at": pa.Column("datetime64[ns, UTC]"),
        "source_rev": pa.Column(str, nullable=True),
    },
    # 스냅샷 파일 하나 안에서만 유일하다. 가로질러서는
    # (date, symbol, observed_at)이다 — 원천이 과거를 고치면 같은 날짜가
    # 새 값으로 다시 온다 (03 §3.1).
    unique=["date", "symbol"],
    checks=pa.Check(
        lambda df: (df["high"] >= df["low"]).all(),
        error="high >= low 가 깨졌다",
    ),
    strict=True,
    coerce=False,
)


# --- 나머지 네 테이블 (03 §4.2~§4.5) ----------------------------------------
# 컬럼 계약만 먼저 고정한다. 값 검사는 그 원천을 실제로 받는 단계에서 채운다.

CORP_ACTIONS_ARROW = pyar.schema(
    [
        ("symbol", pyar.string()),
        ("ex_date", pyar.date32()),
        ("kind", pyar.string()),  # split | dividend
        ("to_factor", pyar.int64()),
        ("for_factor", pyar.int64()),
        ("amount", _PRICE),
        ("declaration_date", pyar.date32()),  # Nasdaq에서만 온다. 없으면 null
        ("record_date", pyar.date32()),
        ("payment_date", pyar.date32()),
        ("source", pyar.string()),
        ("tier", pyar.string()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),
    ]
)

FUNDAMENTALS_ARROW = pyar.schema(
    [
        ("cik", pyar.int64()),
        ("taxonomy", pyar.string()),
        ("tag", pyar.string()),
        ("unit", pyar.string()),
        ("start", pyar.date32()),
        ("end", pyar.date32()),
        ("val", pyar.float64()),
        ("fy", pyar.int32()),
        ("fp", pyar.string()),
        ("form", pyar.string()),
        # filed 가 PIT의 축이다. 같은 end에 값이 여럿이면
        # filed <= 기준일 인 것 중 최신을 쓴다 (03 §4.3).
        ("filed", pyar.date32()),
        ("accn", pyar.string()),
        ("frame", pyar.string()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
    ]
)

SHORT_INTEREST_ARROW = pyar.schema(
    [
        ("settlement_date", pyar.date32()),
        ("symbol", pyar.string()),
        ("current_short_qty", pyar.int64()),
        ("previous_short_qty", pyar.int64()),
        ("avg_daily_volume_qty", pyar.int64()),
        ("days_to_cover", pyar.float64()),
        ("change_percent", pyar.float64()),
        # 원천이 주는 정정 표시. 정정본이 오면 observed_at으로 갈린다 (03 §4.4).
        ("revision_flag", pyar.bool_()),
        ("stock_split_flag", pyar.bool_()),
        ("market_class", pyar.string()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
    ]
)

UNIVERSE_DAILY_ARROW = pyar.schema(
    [
        ("date", pyar.date32()),
        ("symbol", pyar.string()),
        ("cik", pyar.int64()),
        ("in_prices", pyar.bool_()),
        ("in_listing", pyar.bool_()),
        ("listing_source", pyar.string()),
        ("is_etf", pyar.bool_()),
        ("test_issue", pyar.bool_()),
        ("exchange", pyar.string()),
        ("sic", pyar.string()),  # 분기 sub.txt — filing 시점 값이라 PIT다
        ("sic_source", pyar.string()),
        ("mcap_rank", pyar.int32()),  # MIDAS McapRank decile. 2012~
        ("adv_20d", pyar.float64()),
        ("in_universe", pyar.bool_()),
        # 생존편향 하한. 학습 코드가 이 밖을 읽으면 막는다 (07 X3).
        ("usable_from", pyar.date32()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
    ]
)


ARROW_SCHEMAS: dict[str, pyar.Schema] = {
    "prices_daily": PRICES_DAILY_ARROW,
    "corp_actions": CORP_ACTIONS_ARROW,
    "fundamentals": FUNDAMENTALS_ARROW,
    "short_interest": SHORT_INTEREST_ARROW,
    "universe_daily": UNIVERSE_DAILY_ARROW,
}

FRAME_SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "prices_daily": PRICES_DAILY,
}
