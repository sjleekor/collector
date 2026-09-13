"""Target planning helpers for OpenDART incremental runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from collector.kr.domain.enums import RunType
from collector.kr.ports.storage import Storage
from collector.kr.util.pipeline import recent_no_data_request_keys
from collector.kr.util.time import today_kst

DEFAULT_REPRT_CODES = ("11011", "11012", "11013", "11014")


@dataclass(frozen=True, slots=True)
class DartTargetPlan:
    bsns_years: list[int]
    reprt_codes: list[str]
    allowed_year_report_pairs: set[tuple[int, str]]
    skipped_year_report_pairs: dict[str, str] = field(default_factory=dict)
    negative_cache_request_keys: set[str] = field(default_factory=set)
    estimated_request_count: int = 0

    def audit_params(self) -> dict[str, object]:
        return {
            "incremental": True,
            "bsns_years": self.bsns_years,
            "reprt_codes": self.reprt_codes,
            "allowed_year_report_pairs": [
                f"{year}:{reprt_code}"
                for year, reprt_code in sorted(self.allowed_year_report_pairs)
            ],
            "skipped_year_report_pairs": self.skipped_year_report_pairs,
            "negative_cache_request_key_count": len(self.negative_cache_request_keys),
            "estimated_request_count": self.estimated_request_count,
        }


def build_dart_target_plan(
    storage: Storage,
    *,
    run_type: RunType,
    active_corp_count: int,
    requests_per_corp_target: int,
    lookback_years: int = 1,
    reprt_codes: list[str] | None = None,
    as_of: date | None = None,
    negative_cache_ttl_days: int = 3,
) -> DartTargetPlan:
    """Resolve available OpenDART year/report targets for an incremental run."""
    if lookback_years < 0:
        raise ValueError("lookback_years must be >= 0")
    if requests_per_corp_target <= 0:
        raise ValueError("requests_per_corp_target must be positive")

    effective_as_of = as_of or today_kst()
    years = [effective_as_of.year - offset for offset in range(lookback_years + 1)]
    candidate_codes = reprt_codes or list(DEFAULT_REPRT_CODES)
    allowed_pairs: set[tuple[int, str]] = set()
    skipped_pairs: dict[str, str] = {}
    for year in years:
        for reprt_code in candidate_codes:
            if _is_report_available(year, reprt_code, effective_as_of):
                allowed_pairs.add((year, reprt_code))
            else:
                skipped_pairs[f"{year}:{reprt_code}"] = "not_yet_available"

    negative_keys = _recent_no_data_request_keys(
        storage=storage,
        run_type=run_type,
        as_of=effective_as_of,
        ttl_days=negative_cache_ttl_days,
    )
    return DartTargetPlan(
        bsns_years=years,
        reprt_codes=candidate_codes,
        allowed_year_report_pairs=allowed_pairs,
        skipped_year_report_pairs=skipped_pairs,
        negative_cache_request_keys=negative_keys,
        estimated_request_count=len(allowed_pairs) * active_corp_count * requests_per_corp_target,
    )


def _is_report_available(bsns_year: int, reprt_code: str, as_of: date) -> bool:
    available_dates = {
        "11013": date(bsns_year, 5, 15),
        "11012": date(bsns_year, 8, 15),
        "11014": date(bsns_year, 11, 15),
        "11011": date(bsns_year + 1, 3, 31),
    }
    available_from = available_dates.get(reprt_code)
    return available_from is not None and as_of >= available_from


def _recent_no_data_request_keys(
    *,
    storage: Storage,
    run_type: RunType,
    as_of: date,
    ttl_days: int,
) -> set[str]:
    return recent_no_data_request_keys(
        storage,
        run_type=run_type,
        as_of=as_of,
        ttl_days=ttl_days,
    )
