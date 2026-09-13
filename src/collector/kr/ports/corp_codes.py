"""Port: OpenDART corporation-code provider interface."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from collector.kr.domain.models import CompanyProfileResult, DartCorp, DartCorpCodeResult


@runtime_checkable
class CorpCodeProvider(Protocol):
    """Fetches the OpenDART corporation-code master file."""

    def fetch_corp_codes(self) -> DartCorpCodeResult:
        """Retrieve all corporation-code rows from OpenDART."""
        ...


@runtime_checkable
class CorpProfileProvider(Protocol):
    """Fetches one corporation's OpenDART profile (``company.json``, DS001).

    Separate from :class:`CorpCodeProvider` because the endpoint is different:
    the corp-code master is one bulk zip, while a profile is one call per
    corporation.
    """

    def fetch_company_profile(self, corp: DartCorp) -> CompanyProfileResult:
        """Retrieve industry code, incorporation date and fiscal-year-end.

        Implementations never raise for an upstream failure — the error goes in
        ``CompanyProfileResult.error``.
        """
        ...
