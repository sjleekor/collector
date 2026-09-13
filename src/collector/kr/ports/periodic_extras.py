"""Port for OpenDART DS002 periodic-report extras (N6)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from collector.kr.domain.enums import PeriodicExtraStatement
from collector.kr.domain.models import DartCorp, DartPeriodicExtraResult


@runtime_checkable
class PeriodicExtrasProvider(Protocol):
    """Fetches employee, governance and audit-opinion disclosures.

    One method rather than one per endpoint, unlike the other DART ports. The
    five DS002 endpoints take identical arguments (``corp_code`` +
    ``bsns_year`` + ``reprt_code``) and return identically-shaped raw rows, so
    five methods would be five copies differing only in a URL — and the service
    would still have to switch on statement type to build its slice keys. The
    enum carries the distinction instead, which keeps it type-checked.
    """

    def fetch_periodic_extra(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        statement_type: PeriodicExtraStatement,
    ) -> DartPeriodicExtraResult:
        """Retrieve one DS002 disclosure for one company, year and report."""
        ...
