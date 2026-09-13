"""Port: financial-statement provider interface."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from collector.kr.domain.models import DartCorp, DartFinancialStatementResult


@runtime_checkable
class FinancialStatementProvider(Protocol):
    """Fetches single-company raw financial-statement rows."""

    def fetch_financial_statement(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementResult:
        """Retrieve all raw rows for one corp/year/report/fs_div."""
        ...
