"""Port for OpenDART disclosure-receipt history."""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from collector.kr.domain.models import DartCorp, DartFilingReceiptResult


@runtime_checkable
class FilingReceiptProvider(Protocol):
    """Fetches OpenDART disclosure-receipt history (공시검색)."""

    def fetch_filing_receipts(
        self,
        corp: DartCorp,
        bgn_de: date,
        end_de: date,
    ) -> DartFilingReceiptResult:
        """Retrieve list.json raw rows for one company within a date window.

        Implementations must paginate internally and return the fully
        aggregated window in a single result — see ``DartFilingReceiptResult``.
        """
        ...
