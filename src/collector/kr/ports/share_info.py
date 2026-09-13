"""Ports for share-count and shareholder-return disclosures."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from collector.kr.domain.models import (
    DartCapitalChangeResult,
    DartCorp,
    DartShareCountResult,
    DartShareholderReturnResult,
)


@runtime_checkable
class ShareCountProvider(Protocol):
    """Fetches OpenDART stock count disclosures."""

    def fetch_share_count(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareCountResult:
        """Retrieve stockTotqySttus raw rows for one company/report."""
        ...


@runtime_checkable
class ShareholderReturnProvider(Protocol):
    """Fetches dividend and treasury-stock disclosures."""

    def fetch_dividend(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        """Retrieve alotMatter rows for one company/report."""
        ...

    def fetch_treasury_stock(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        """Retrieve tesstkAcqsDspsSttus rows for one company/report."""
        ...


@runtime_checkable
class CapitalChangeProvider(Protocol):
    """Fetches OpenDART issuance/decrease (증자·감자) disclosures."""

    def fetch_capital_change(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartCapitalChangeResult:
        """Retrieve irdsSttus raw rows for one company/report."""
        ...
