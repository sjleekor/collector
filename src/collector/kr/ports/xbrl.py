"""Port: OpenDART XBRL document provider."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from collector.kr.domain.models import DartCorp, DartXbrlResult


@runtime_checkable
class XbrlProvider(Protocol):
    """Fetch and parse an OpenDART XBRL document for a filing receipt."""

    def fetch_xbrl(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        rcept_no: str,
    ) -> DartXbrlResult:
        """Download and parse one filing's XBRL ZIP."""
        ...
