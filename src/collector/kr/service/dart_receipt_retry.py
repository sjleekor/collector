"""Receipt-driven retry of OpenDART "no data" verdicts for periodic reports.

The 04:00 OpenDART chain asks for every (corp, year, report) slot once the
report-type availability date has passed (``dart_target_plan``). A corp that has
not filed yet is answered "no data", and the slice ledger keeps that verdict for
30 days. A report filed the next day is therefore invisible for up to a month.

``dart_filing_receipt_raw`` (collected daily at 23:30) says which corp filed
which periodic report. This module turns those receipts into slots that must be
re-asked even though a no-data verdict is on file, for a bounded window after the
receipt (OpenDART needs a few days to process a filing into its structured
endpoints). After the window the normal TTL applies again.

A receipt also lifts the report-type availability gate for its own slot, so a
non-December fiscal-year corp or an early filer is not held back by the
December-year calendar in ``dart_target_plan``.

Only original filings are mapped. ``[기재정정]``/``[첨부정정]``/``[첨부추가]``
titles and extension notices (``...제출기한연장신고서``) never match, because they
do not start with the bare title and the period in parentheses.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Collection, Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta

from collector.kr.domain.models import DartPeriodicReceipt
from collector.kr.ports.storage import Storage

logger = logging.getLogger(__name__)

#: ``반기보고서 (2026.06)``. Anything before the title (a bracketed prefix) or
#: after the period makes it not an original periodic report.
_TITLE_RE = re.compile(r"^(사업|반기|분기)보고서\s*\((\d{4})\.(\d{1,2})\)\s*$")

#: Default days after a receipt during which a no-data verdict is overridden.
#: Measured 2026-08 half-year: of reports received on the deadline, 55% were
#: readable the next morning, 28% a day later and the rest within a week.
DEFAULT_RETRY_WINDOW_DAYS = 7

#: Default cap on receipt-confirmed slots one run may add.
DEFAULT_RETRY_MAX_SLOTS = 3000

Slot = tuple[str, int, str]  # (corp_code, bsns_year, reprt_code)


@dataclass(frozen=True, slots=True)
class PeriodicTitle:
    """Parsed title of an original periodic report."""

    kind: str  # "사업" | "반기" | "분기"
    year: int
    month: int


def parse_periodic_title(report_nm: str) -> PeriodicTitle | None:
    """Parse ``반기보고서 (2026.06)`` style titles; ``None`` for anything else."""
    match = _TITLE_RE.match(report_nm.strip())
    if match is None:
        return None
    month = int(match.group(3))
    if not 1 <= month <= 12:
        return None
    return PeriodicTitle(kind=match.group(1), year=int(match.group(2)), month=month)


def _fiscal_year_end_month(acc_mt: str) -> int:
    """Return the fiscal-year-end month, defaulting to December when unknown."""
    try:
        value = int(acc_mt)
    except (TypeError, ValueError):
        return 12
    return value if 1 <= value <= 12 else 12


def receipt_to_slots(report_nm: str, acc_mt: str) -> list[tuple[int, str]]:
    """Map a receipt title to the ``(bsns_year, reprt_code)`` slots it can fill.

    ``bsns_year`` is the calendar year of the period end (measured against stored
    raw rows, including non-December fiscal years). The quarter number follows
    from the months between fiscal-year end and period end: 3 -> Q1 (11013),
    6 -> half-year (11012), 9 -> Q3 (11014), 0 -> annual (11011). A 분기보고서
    that fits neither Q1 nor Q3 (stale ``acc_mt``) returns both candidates, so a
    wrong fiscal-year month costs one extra request rather than a missed slot.
    """
    title = parse_periodic_title(report_nm)
    if title is None:
        return []
    if title.kind == "사업":
        return [(title.year, "11011")]
    if title.kind == "반기":
        return [(title.year, "11012")]
    offset = (title.month - _fiscal_year_end_month(acc_mt)) % 12
    if offset == 3:
        return [(title.year, "11013")]
    if offset == 9:
        return [(title.year, "11014")]
    return [(title.year, "11013"), (title.year, "11014")]


def slot_allowed(
    allowed_pairs: Collection[tuple[int, str]] | None,
    retry_slots: Collection[Slot],
    corp_code: str,
    bsns_year: int,
    reprt_code: str,
) -> bool:
    """Whether a slot may be requested: gate open, or confirmed by a receipt."""
    return (
        allowed_pairs is None
        or (bsns_year, reprt_code) in allowed_pairs
        or (corp_code, bsns_year, reprt_code) in retry_slots
    )


@dataclass(frozen=True, slots=True)
class ReceiptRetryPlan:
    """Receipt-confirmed slots to re-ask in this run.

    Attributes:
        slots: ``(corp_code, bsns_year, reprt_code)`` -> receipt date.
        window_days: Days after a receipt a no-data verdict is overridden.
        receipts_considered: Original periodic receipts read in the window.
        slots_capped: Slots dropped (oldest receipts first) by ``max_slots``.
    """

    slots: dict[Slot, date] = field(default_factory=dict)
    window_days: int = 0
    receipts_considered: int = 0
    slots_capped: int = 0

    def __bool__(self) -> bool:
        return bool(self.slots)

    def rank_keys(self, keyed: Iterable[tuple[str, Slot]]) -> list[str]:
        """Return ledger keys whose slot is receipt-confirmed, newest receipt first."""
        ranked = [(self.slots[slot], key) for key, slot in keyed if slot in self.slots]
        ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return list(dict.fromkeys(key for _, key in ranked))

    def audit_params(self) -> dict[str, object]:
        """Fields recorded on ``ingestion_runs.params``."""
        return {
            "receipt_retry_window_days": self.window_days,
            "receipt_retry_receipts": self.receipts_considered,
            "receipt_retry_slots_planned": len(self.slots),
            "receipt_retry_slots_capped": self.slots_capped,
        }


def retry_counts(
    receipt_retry: ReceiptRetryPlan | None, slots_applied: int, requests: int
) -> dict[str, int]:
    """Run counters for ``ingestion_runs.counts``; empty when the feature is not in play."""
    if receipt_retry is None:
        return {}
    return {"receipt_retry_slots": slots_applied, "receipt_retry_requests": requests}


def build_receipt_retry_plan(
    storage: Storage,
    *,
    as_of: date,
    window_days: int,
    max_slots: int = DEFAULT_RETRY_MAX_SLOTS,
    corp_codes: Collection[str] | None = None,
) -> ReceiptRetryPlan:
    """Build the retry plan from receipts filed in the last *window_days*.

    Args:
        storage: Storage holding ``dart_filing_receipt_raw``.
        as_of: Run date. Receipts dated after it are ignored.
        window_days: Window length. ``<= 0`` disables the feature.
        max_slots: Cap on slots. The newest receipts are kept, because they are
            the ones most likely still unprocessed.
        corp_codes: Restrict to these corps (the run's targets).
    """
    if window_days <= 0 or max_slots <= 0:
        return ReceiptRetryPlan(window_days=max(0, window_days))

    receipts: list[DartPeriodicReceipt] = storage.get_dart_periodic_receipts(
        as_of - timedelta(days=window_days), as_of
    )
    allowed_corps = None if corp_codes is None else set(corp_codes)
    slots: dict[Slot, date] = {}
    considered = 0
    for receipt in receipts:
        if allowed_corps is not None and receipt.corp_code not in allowed_corps:
            continue
        mapped = receipt_to_slots(receipt.report_nm, receipt.acc_mt)
        if not mapped:
            continue
        considered += 1
        for bsns_year, reprt_code in mapped:
            slot = (receipt.corp_code, bsns_year, reprt_code)
            if slot not in slots or receipt.rcept_dt > slots[slot]:
                slots[slot] = receipt.rcept_dt

    capped = 0
    if len(slots) > max_slots:
        keep = sorted(slots.items(), key=lambda item: (item[1], item[0]), reverse=True)
        capped = len(slots) - max_slots
        slots = dict(keep[:max_slots])
        logger.warning(
            "receipt retry slots capped: kept %d of %d (newest receipts first)",
            max_slots,
            max_slots + capped,
        )
    return ReceiptRetryPlan(
        slots=slots,
        window_days=window_days,
        receipts_considered=considered,
        slots_capped=capped,
    )
