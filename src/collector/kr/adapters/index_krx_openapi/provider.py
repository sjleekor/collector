"""KRX Open API index daily levels (``idx`` group).

Three endpoints share one response shape (verified 2026-09-29, ``basDd`` =
``YYYYMMDD``, rows under ``OutBlock_1``)::

    BAS_DD, IDX_CLSS, IDX_NM, CLSPRC_IDX, CMPPREVDD_IDX, FLUC_RT,
    OPNPRC_IDX, HGPRC_IDX, LWPRC_IDX, ACC_TRDVOL, ACC_TRDVAL, MKTCAP

* History starts 2010-01-04; earlier dates return 0 rows.
* A non-trading day returns HTTP 200 with 0 rows -- an empty list here, not an
  error.
* Same-day data is not published even late in the evening, so callers must not
  ask for today.

Every value is a string, numbers may carry commas, and ``-`` or an empty string
means "no value".  Those become ``None``, never ``0``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from collector.kr.adapters.market_data_krx_openapi.client import KrxOpenApiClient
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

#: Earliest date the endpoints return rows for.  Used only to clamp ``--start``.
KRX_INDEX_HISTORY_START = date(2010, 1, 4)

#: index_group -> (service group, endpoint)
INDEX_ENDPOINTS: dict[str, tuple[str, str]] = {
    "kospi": ("idx", "kospi_dd_trd"),
    "kosdaq": ("idx", "kosdaq_dd_trd"),
    "krx": ("idx", "krx_dd_trd"),
}

SOURCE_NAME = "krx_openapi"


@dataclass(frozen=True, slots=True)
class KrxIndexDailyRow:
    """One index on one date (table ``krx_index_daily``)."""

    bas_dd: date
    index_group: str
    idx_clss: str | None
    idx_nm: str
    close_idx: Decimal | None
    chg_idx: Decimal | None
    fluc_rt: Decimal | None
    open_idx: Decimal | None
    high_idx: Decimal | None
    low_idx: Decimal | None
    acc_trdvol: int | None
    acc_trdval: Decimal | None
    mktcap: Decimal | None
    fetched_at: datetime
    source: str = SOURCE_NAME


def parse_decimal(value: object) -> Decimal | None:
    """Parse a comma-grouped numeric string; blank, ``-`` or junk -> ``None``."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text == "-":
        return None
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def parse_bigint(value: object) -> int | None:
    """Parse an integer-valued numeric string (volume); fractions are truncated."""
    parsed = parse_decimal(value)
    return None if parsed is None else int(parsed)


def _text(value: object) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def _parse_bas_dd(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def parse_index_rows(
    raw_rows: list[dict[str, Any]],
    *,
    index_group: str,
    fetched_at: datetime,
) -> list[KrxIndexDailyRow]:
    """Convert ``OutBlock_1`` rows; rows without a date or name are dropped."""
    rows: list[KrxIndexDailyRow] = []
    for raw in raw_rows:
        bas_dd = _parse_bas_dd(raw.get("BAS_DD"))
        idx_nm = _text(raw.get("IDX_NM"))
        if bas_dd is None or idx_nm is None:
            logger.warning("Dropping index row without BAS_DD/IDX_NM: %r", raw)
            continue
        rows.append(
            KrxIndexDailyRow(
                bas_dd=bas_dd,
                index_group=index_group,
                idx_clss=_text(raw.get("IDX_CLSS")),
                idx_nm=idx_nm,
                close_idx=parse_decimal(raw.get("CLSPRC_IDX")),
                chg_idx=parse_decimal(raw.get("CMPPREVDD_IDX")),
                fluc_rt=parse_decimal(raw.get("FLUC_RT")),
                open_idx=parse_decimal(raw.get("OPNPRC_IDX")),
                high_idx=parse_decimal(raw.get("HGPRC_IDX")),
                low_idx=parse_decimal(raw.get("LWPRC_IDX")),
                acc_trdvol=parse_bigint(raw.get("ACC_TRDVOL")),
                acc_trdval=parse_decimal(raw.get("ACC_TRDVAL")),
                mktcap=parse_decimal(raw.get("MKTCAP")),
                fetched_at=fetched_at,
            )
        )
    return rows


class KrxOpenApiIndexProvider:
    """Fetch one index group for one date via :class:`KrxOpenApiClient`."""

    def __init__(self, client: KrxOpenApiClient) -> None:
        self._client = client

    def fetch_by_date(self, index_group: str, day: date) -> list[KrxIndexDailyRow]:
        """Return the rows for *day*; an empty list means no session/not published.

        Raises whatever the client raises (quota, auth, not-approved, transport).
        """
        try:
            group, endpoint = INDEX_ENDPOINTS[index_group]
        except KeyError:
            raise ValueError(
                f"Unknown index group {index_group!r}; expected one of {sorted(INDEX_ENDPOINTS)}"
            ) from None
        raw_rows = self._client.fetch_rows(group, endpoint, {"basDd": day.strftime("%Y%m%d")})
        return parse_index_rows(raw_rows, index_group=index_group, fetched_at=now_kst())
