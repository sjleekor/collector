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
        # 원천이 decimal(10,5)다. 정수가 아니다 — FBP 2011-01-07 이 1:15 이고
        # 주식배당은 101:100 · 11:10 처럼 온다 (03 §2.1).
        ("to_factor", pyar.decimal128(10, 5)),
        ("for_factor", pyar.decimal128(10, 5)),
        ("amount", pyar.decimal128(10, 5)),
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


# --- volatility_daily (03 §4.6) ---------------------------------------------
# DoltHub options/volatility_history 를 그대로 받는다. 원천이 decimal(5,4)라
# 바꾸지 않는다. 연고점·연저점이 같이 와서 IV rank를 따로 계산할 필요가 없다.
#
# 커버리지가 유니버스의 절반이 안 된다 — 연 1,600여 종목뿐이고 옵션이 활발한
# 큰 종목에 쏠려 있다. 결측이 무작위가 아니므로 피쳐로 쓸 때 isna 플래그를
# 같이 둔다 (03 §4.6).

_VOL = pyar.decimal128(5, 4)

VOLATILITY_DAILY_ARROW = pyar.schema(
    [
        ("date", pyar.date32()),
        ("symbol", pyar.string()),
        ("hv_current", _VOL),
        ("hv_week_ago", _VOL),
        ("hv_month_ago", _VOL),
        ("hv_year_high", _VOL),
        ("hv_year_high_date", pyar.date32()),
        ("hv_year_low", _VOL),
        ("hv_year_low_date", pyar.date32()),
        ("iv_current", _VOL),
        ("iv_week_ago", _VOL),
        ("iv_month_ago", _VOL),
        ("iv_year_high", _VOL),
        ("iv_year_high_date", pyar.date32()),
        ("iv_year_low", _VOL),
        ("iv_year_low_date", pyar.date32()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),  # dolt 커밋 해시
    ]
)


# --- filings_sub (03 §4.7) ---------------------------------------------------
# 분기 재무 데이터셋 sub.txt 의 36컬럼 중 공시 메타만. sic 이 filing 시점
# 값이라 PIT 이고, filed 가 그 축이다.

FILINGS_SUB_ARROW = pyar.schema(
    [
        ("adsh", pyar.string()),  # accession. 행의 키
        ("cik", pyar.int64()),
        ("name", pyar.string()),
        ("sic", pyar.string()),  # 앞자리 0이 있다. 숫자로 만들지 않는다
        ("form", pyar.string()),
        ("period", pyar.date32()),
        ("fy", pyar.int32()),
        ("fp", pyar.string()),
        ("filed", pyar.date32()),  # PIT의 축
        ("fye", pyar.string()),  # MMDD. 역시 앞자리 0이 있다
        ("prevrpt", pyar.bool_()),
        ("countryba", pyar.string()),
        ("former", pyar.string()),
        ("changed", pyar.date32()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),  # 분기 태그 (2018q4)
    ]
)


# --- midas_security_daily (03 §4.8) ------------------------------------------
# 19컬럼 중 순위 넷만. 나머지 호가 미시구조 지표는 raw/ 에 두고 안 뽑는다.
# rank 값 형식이 연도마다 "1" / "1.0" 으로 달라 숫자로 파싱한다.

MIDAS_SECURITY_DAILY_ARROW = pyar.schema(
    [
        ("date", pyar.date32()),
        ("ticker", pyar.string()),
        ("security_type", pyar.string()),  # Stock | ETF
        ("mcap_rank", pyar.int32()),  # 일별 횡단면 decile
        ("turn_rank", pyar.int32()),
        ("volatility_rank", pyar.int32()),
        ("price_rank", pyar.int32()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),
    ]
)


# --- listing_snapshots (03 §4.9) ---------------------------------------------
# Wayback 이 뜬 nasdaqtrader 심볼 디렉터리. is_etf·test_issue 의 PIT 원천이다
# (dolt symbol 은 PK가 act_symbol 하나뿐이라 오늘 값만 있다 — 03 §5.4).
#
# as_of 는 파일이 스스로 밝힌 "File Creation Time" 이다. 아카이브 timestamp 가
# 아니다 (03 §3).

LISTING_SNAPSHOTS_ARROW = pyar.schema(
    [
        ("as_of", pyar.date32()),
        ("as_of_time", pyar.timestamp("us")),  # 거래소 현지 시각. tz 표기가 없다
        ("kind", pyar.string()),  # nasdaqlisted | otherlisted
        ("snapshot", pyar.string()),  # Wayback timestamp
        ("symbol", pyar.string()),
        ("security_name", pyar.string()),
        ("exchange", pyar.string()),
        ("market_category", pyar.string()),
        ("is_etf", pyar.bool_()),  # 옛 파일에 컬럼이 없으면 null
        ("test_issue", pyar.bool_()),
        ("financial_status", pyar.string()),
        ("round_lot_size", pyar.int32()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),  # 원문 파일 이름
    ]
)


# --- insider_trans · insider_owners (03 §4.10) --------------------------------
# 분기 내부자 거래 데이터셋(Form 3·4·5) ZIP의 10개 TSV 중 셋만 쓴다:
# SUBMISSION(공시 메타) · NONDERIV_TRANS/DERIV_TRANS(거래) · REPORTINGOWNER(신고인).
# FOOTNOTES·*_HOLDING·OWNER_SIGNATURE 는 raw/ 에 두고 안 뽑는다.
#
# **PIT의 축은 `filing_date`다.** `trans_date`(거래일)와 며칠에서 몇 달까지
# 벌어진다 — 거래일 기준으로 피쳐를 만들면 공시 전 정보를 쓰는 셈이 된다.
#
# 파생(옵션·RSU)과 비파생을 한 장에 담고 `is_derivative`로 가른다. 파생에만
# 있는 컬럼 셋(행사가·기초주식수·만기)은 비파생 행에서 null이다.

INSIDER_TRANS_ARROW = pyar.schema(
    [
        ("accession", pyar.string()),
        ("is_derivative", pyar.bool_()),
        ("trans_sk", pyar.int64()),  # 원천의 대리키. 위 둘과 합쳐 행을 가른다
        ("issuer_cik", pyar.int64()),
        ("issuer_symbol", pyar.string()),
        ("doc_type", pyar.string()),  # 3 | 4 | 5 | 3/A | 4/A | 5/A
        ("filing_date", pyar.date32()),  # PIT의 축
        ("period_of_report", pyar.date32()),
        ("security_title", pyar.string()),
        ("trans_date", pyar.date32()),
        ("trans_code", pyar.string()),  # P 매수 · S 매도 · A 수여 · F 세금 · M 행사 …
        ("trans_form_type", pyar.string()),
        ("equity_swap_involved", pyar.bool_()),
        ("trans_shares", pyar.float64()),
        ("trans_pricepershare", pyar.float64()),
        ("acquired_disposed", pyar.string()),  # A | D — 방향이다
        ("shares_owned_following", pyar.float64()),
        ("direct_indirect", pyar.string()),  # D | I
        ("conv_exercise_price", pyar.float64()),  # 파생만
        ("underlying_shares", pyar.float64()),  # 파생만
        ("expiration_date", pyar.date32()),  # 파생만
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),  # 분기 태그 (2018q4)
    ]
)

# 신고인은 공시 하나에 여럿이다 (2018q4에 최대 10명). 거래 표에 붙이면 행이
# 곱해지므로 따로 둔다. `relationship`은 원천 문자열 그대로 남긴다 —
# 쉼표로 끊긴 것("Director,Officer")과 붙어 온 것("DirectorOther")이 섞여 있다.

INSIDER_OWNERS_ARROW = pyar.schema(
    [
        ("accession", pyar.string()),
        ("owner_cik", pyar.int64()),
        ("owner_name", pyar.string()),
        ("relationship", pyar.string()),  # 원천 문자열
        ("is_director", pyar.bool_()),
        ("is_officer", pyar.bool_()),
        ("is_ten_percent_owner", pyar.bool_()),
        ("is_other", pyar.bool_()),
        ("officer_title", pyar.string()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),
    ]
)


# --- filings_index · company_meta (03 §4.11·§4.12) ---------------------------
# submissions.zip. `filings_sub`(분기 재무 데이터셋)가 재무제표를 낸 공시만
# 담는 데 비해 이쪽은 **모든 공시**다 — 8-K·13D·S-1까지 날짜가 나온다.
#
# `acceptance_datetime`을 날짜로 깎지 않는다. 장 마감 뒤 접수된 공시는 그날
# 종가에 못 쓴다. 날짜만 남기면 그 판단을 영영 못 한다.

FILINGS_INDEX_ARROW = pyar.schema(
    [
        ("cik", pyar.int64()),
        ("accession", pyar.string()),
        ("form", pyar.string()),
        ("filing_date", pyar.date32()),
        ("report_date", pyar.date32()),
        ("acceptance_datetime", pyar.timestamp("us", tz="UTC")),
        ("act", pyar.string()),
        ("file_number", pyar.string()),
        ("items", pyar.string()),  # 8-K 항목 번호. 쉼표로 여럿
        ("core_type", pyar.string()),
        ("primary_document", pyar.string()),
        ("is_xbrl", pyar.bool_()),
        ("is_inline_xbrl", pyar.bool_()),
        ("size", pyar.int64()),
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),  # <파일>:<바이트> — 크기가 변경 신호다
    ]
)

# 발행사 한 줄. **현재값이다 — PIT가 아니다.** `former_names`만 이력을 갖는다
# (JSON 문자열 그대로 둔다). 업종 PIT는 `filings_sub`의 `sic`을 쓴다.

COMPANY_META_ARROW = pyar.schema(
    [
        ("cik", pyar.int64()),
        ("name", pyar.string()),
        ("entity_type", pyar.string()),
        ("sic", pyar.string()),
        ("sic_description", pyar.string()),
        ("category", pyar.string()),  # Large accelerated filer …
        ("fiscal_year_end", pyar.string()),  # MMDD
        ("state_of_incorporation", pyar.string()),
        ("ein", pyar.string()),
        ("tickers", pyar.string()),  # 쉼표로 여럿. 현재 매핑이다
        ("exchanges", pyar.string()),
        ("former_names", pyar.string()),  # 원천 JSON 그대로
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),
    ]
)


# --- trading_calendar (04 C5) ------------------------------------------------
# exchange_calendars 를 그대로 굳힌다. 라이브러리 판이 바뀌면 과거 휴장일 판정이
# 조용히 달라지므로 `source_rev`에 버전을 박는다.

TRADING_CALENDAR_ARROW = pyar.schema(
    [
        ("date", pyar.date32()),
        ("exchange", pyar.string()),
        ("close_local", pyar.time64("us")),  # 거래소 현지 시각
        ("is_early_close", pyar.bool_()),  # 13:00 마감. 거래량이 절반인 것이 정상이다
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),  # exchange_calendars <버전>
    ]
)


# --- macro_series (04 C5) ----------------------------------------------------
# FRED/ALFRED. **`realtime_start`가 PIT의 축이다** — 그 값이 공개된 날이다.
# 같은 `(series_id, date)`에 값이 여럿 있는 것이 정상이고 개정 이력이 그것이다.
# 기준일 T의 값 = `realtime_start <= T` 중 최신. 재무의 `filed`와 같은 규칙이다.

MACRO_SERIES_ARROW = pyar.schema(
    [
        ("series_id", pyar.string()),
        ("axis", pyar.string()),  # 금리·유가·환율·신용·변동성·물가·고용·생산·통화·지수
        ("date", pyar.date32()),  # 관측 기간
        ("realtime_start", pyar.date32()),  # PIT의 축
        ("value", pyar.float64()),  # 원천이 "."를 주면 null이다 (휴일·미발표)
        ("observed_at", pyar.timestamp("us", tz="UTC")),
        ("source_rev", pyar.string()),
    ]
)


# --- index_constituents (04 C5) ----------------------------------------------
# Wikipedia 리비전. `as_of`가 그 리비전 시각이고 `revid`가 버전 식별자다 (03 §3).
#
# **Wikipedia 반영 지연은 측정하지 않았다** (00 §의 "아직 모르는 것"). 편입·제외
# 실제 발효일과 문서 수정 시각이 다를 수 있으므로 이벤트 스터디에 쓸 때 본다.

INDEX_CONSTITUENTS_ARROW = pyar.schema(
    [
        ("index_id", pyar.string()),  # SP500
        ("as_of", pyar.timestamp("us", tz="UTC")),  # 리비전 시각
        ("revid", pyar.int64()),
        ("symbol", pyar.string()),
        ("security", pyar.string()),
        ("gics_sector", pyar.string()),
        ("gics_sub_industry", pyar.string()),
        ("cik", pyar.string()),  # 앞자리 0이 있다. 문자열로 둔다
        ("observed_at", pyar.timestamp("us", tz="UTC")),
    ]
)


ARROW_SCHEMAS: dict[str, pyar.Schema] = {
    "prices_daily": PRICES_DAILY_ARROW,
    "corp_actions": CORP_ACTIONS_ARROW,
    "fundamentals": FUNDAMENTALS_ARROW,
    "short_interest": SHORT_INTEREST_ARROW,
    "universe_daily": UNIVERSE_DAILY_ARROW,
    "volatility_daily": VOLATILITY_DAILY_ARROW,
    "filings_sub": FILINGS_SUB_ARROW,
    "midas_security_daily": MIDAS_SECURITY_DAILY_ARROW,
    "listing_snapshots": LISTING_SNAPSHOTS_ARROW,
    "insider_trans": INSIDER_TRANS_ARROW,
    "insider_owners": INSIDER_OWNERS_ARROW,
    "filings_index": FILINGS_INDEX_ARROW,
    "company_meta": COMPANY_META_ARROW,
    "trading_calendar": TRADING_CALENDAR_ARROW,
    "macro_series": MACRO_SERIES_ARROW,
    "index_constituents": INDEX_CONSTITUENTS_ARROW,
}

FRAME_SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "prices_daily": PRICES_DAILY,
}
