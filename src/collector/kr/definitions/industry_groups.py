"""Industry grouping rules for KSIC ``induty_code``.

Pure data + pure functions.  No ``Storage`` dependency — the DuckDB marts import
this the same way they import ``metric_rules`` and ``common_features``
(refactor 3.0).

Why this file exists
--------------------
Every cross-sectional z-score in ``research/etl/features/fin_scan.py``
partitions on ``(trade_date, market)``, so banks, biotech, shipbuilders and game
studios normalise against one KOSPI pool.  Barra keeps industry as a first-class
block alongside country and style; Gu, Kelly & Xiu (2020) attach 74 industry
dummies to 94 characteristics.  This module is the missing block.

The grouping decision (fixed 2026-08-15, before results)
--------------------------------------------------------
Measured over a 150-corporation sample of live ``company.json`` responses,
``induty_code`` length is **2, 3, 4 or 5 digits** (3/52/21/74).  So a rule of
the form "take N digits" is only well-defined as a PREFIX:

    ``code[:2]`` yields the KSIC middle category for every observed length.
    264 -> 26, 5821 -> 58, 21100 -> 21, 65121 -> 65.  A 2-digit code already
    IS the middle category.

Groups smaller than ``MIN_GROUP_SIZE`` fold up into the KSIC top-level section
(the letter A..U), because a group of five names makes a cross-sectional
z-score meaningless.  The sample had 15 single-member groups out of 36, so this
is not hypothetical.

Amended 2026-08-15, after the full collection (N2-7b, 3,959 corps).  One fold-up
level does not reach the criterion: 11 of 43 active groups were still under 20
after folding into sections, because the sections themselves are thinly
populated in a listed universe.  The fold-up now repeats into ``OTHER_GROUP``,
so "every group has at least ``MIN_GROUP_SIZE`` members" holds by construction.
The threshold itself is unchanged — what changed is the mechanism that was
failing to deliver it.

What was rejected, and why
--------------------------
The plan recommended "2-digit prefix + a financial/holding override".  The
override is not implementable from this field: **LG (a non-financial holding
company) and KB Financial Group (a bank holding company) both return 64992**,
identical at full length, and ``corp_cls`` is ``Y`` for both.  Splitting
financials properly needs the accounting structure (financial firms have no
comparable revenue / COGS), which lives in ``metric_rules``, not here.

So financials are kept as their own 2-digit groups (64 banking, 65 insurance,
66 financial support) with holding companies mixed in, and that limitation is
stated rather than papered over.

Look-ahead
----------
``company.json`` returns only the CURRENT industry, with no history.  Using it
puts a future classification into a past z-score no matter what the value is
used for downstream, so ``03_w1_company_profile.md`` 6 restricts anything built
on this module to diagnostics.  A scored variant waits for N4's index-membership
path, which is point-in-time.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

# Fold a group into its section once it holds fewer than this many names on a
# given date.  Fixed before looking at any result (02_feature_candidate.md 6.2).
MIN_GROUP_SIZE = 20

UNKNOWN_GROUP = "XX"
UNKNOWN_SECTION = "Z"

# Terminal bucket for sections that are themselves below the minimum.
#
# One fold-up level does not reach the stated criterion. Measured on the full
# N2 collection (2026-08-15, 3,959 corps / 2,629 active): folding 2-digit groups
# into KSIC sections still left 11 of 43 active groups under 20, down to n=2.
# The small sections are small because the listed universe barely populates them
# -- section A holds 4 names in total -- so there is nothing left to fold into
# at that level.
#
# The reason this matters is not tidiness. These groups exist to z-score within
# an industry, and a z-score over two members is ±0.707 whatever the inputs are:
# not a weak signal, a fabricated one. Leaving such a group in place and hoping
# a downstream caller notices is the failure mode this repo keeps finding, so the
# fold-up repeats until no group is under the minimum and the invariant holds by
# construction.
OTHER_GROUP = "OTHER"

# KSIC top-level sections, as inclusive 2-digit ranges.  Used only as the
# fold-up target for small groups.
_SECTION_RANGES: tuple[tuple[str, int, int], ...] = (
    ("A", 1, 3),  # 농업, 임업 및 어업
    ("B", 5, 8),  # 광업
    ("C", 10, 34),  # 제조업
    ("D", 35, 35),  # 전기, 가스, 증기 및 공기조절 공급업
    ("E", 36, 39),  # 수도, 하수 및 폐기물 처리, 원료 재생업
    ("F", 41, 42),  # 건설업
    ("G", 45, 47),  # 도매 및 소매업
    ("H", 49, 52),  # 운수 및 창고업
    ("I", 55, 56),  # 숙박 및 음식점업
    ("J", 58, 63),  # 정보통신업
    ("K", 64, 66),  # 금융 및 보험업
    ("L", 68, 68),  # 부동산업
    ("M", 70, 73),  # 전문, 과학 및 기술 서비스업
    ("N", 74, 76),  # 사업시설 관리, 사업 지원 및 임대 서비스업
    ("O", 84, 84),  # 공공 행정, 국방 및 사회보장 행정
    ("P", 85, 85),  # 교육 서비스업
    ("Q", 86, 87),  # 보건업 및 사회복지 서비스업
    ("R", 90, 91),  # 예술, 스포츠 및 여가관련 서비스업
    ("S", 94, 96),  # 협회 및 단체, 수리 및 기타 개인 서비스업
    ("T", 97, 98),  # 가구 내 고용활동 등
    ("U", 99, 99),  # 국제 및 외국기관
)

# 2-digit groups where the code cannot separate financial firms from holding
# companies (LG and KB Financial Group are both 64992).  Callers that need that
# distinction must get it from the accounting structure instead.
FINANCIAL_GROUPS: frozenset[str] = frozenset({"64", "65", "66"})


def industry_group(induty_code: str | None) -> str:
    """Return the 2-digit KSIC group for ``induty_code``.

    Works for every observed code length because it takes a prefix rather than
    assuming a width.

    Args:
        induty_code: Raw ``induty_code`` from ``company.json``.

    Returns:
        Two-digit group, or ``UNKNOWN_GROUP`` when the code is missing or not
        numeric.
    """
    text = (induty_code or "").strip()
    if len(text) < 2 or not text[:2].isdigit():
        return UNKNOWN_GROUP
    return text[:2]


def industry_section(group: str) -> str:
    """Return the KSIC section letter for a 2-digit ``group``."""
    if not group.isdigit():
        return UNKNOWN_SECTION
    value = int(group)
    for letter, low, high in _SECTION_RANGES:
        if low <= value <= high:
            return letter
    return UNKNOWN_SECTION


def is_financial(group: str) -> bool:
    """Return ``True`` for groups where financials and holdcos are conflated."""
    return group in FINANCIAL_GROUPS


def resolve_groups(
    codes_by_key: Mapping[str, str | None],
    min_group_size: int = MIN_GROUP_SIZE,
) -> dict[str, str]:
    """Assign a normalisation group to each key, folding up small groups.

    Call this per date with that date's universe: membership counts move as
    listings change, so the fold-up decision is date-dependent.

    Two fold-up levels, applied until the minimum holds: 2-digit group → KSIC
    section → :data:`OTHER_GROUP`. One level is not enough on the real universe
    (see :data:`OTHER_GROUP`), and a group below the minimum is worse than a
    coarse one, so the loop is what makes the criterion an invariant instead of
    an aspiration.

    Args:
        codes_by_key: ``{ticker or corp_code: induty_code}`` for one date.
        min_group_size: Smallest group allowed to stand on its own.

    Returns:
        ``{key: group_label}`` where a label is a 2-digit group, a section
        letter, :data:`OTHER_GROUP`, or :data:`UNKNOWN_GROUP`.

        Every returned group holds at least ``min_group_size`` keys, except
        ``UNKNOWN_GROUP`` and ``OTHER_GROUP`` — those two are terminal, so when
        the input is small enough they can come back under it. A caller that
        needs a hard floor checks those two by name.
    """

    def _counts(assignment: Mapping[str, str]) -> dict[str, int]:
        sizes: dict[str, int] = {}
        for group in assignment.values():
            sizes[group] = sizes.get(group, 0) + 1
        return sizes

    resolved = {key: industry_group(code) for key, code in codes_by_key.items()}

    # Unknowns are held out of every fold-up: a missing code is not evidence of
    # any industry, so folding it into a real section would invent one.
    unknown_keys = {key for key, group in resolved.items() if group == UNKNOWN_GROUP}

    counts = _counts(resolved)
    resolved = {
        key: (
            group
            if key in unknown_keys or counts[group] >= min_group_size
            else industry_section(group)
        )
        for key, group in resolved.items()
    }

    counts = _counts(resolved)
    resolved = {
        key: (group if key in unknown_keys or counts[group] >= min_group_size else OTHER_GROUP)
        for key, group in resolved.items()
    }

    return resolved


def group_sizes(groups: Iterable[str]) -> dict[str, int]:
    """Return ``{group: count}``, for coverage diagnostics."""
    sizes: dict[str, int] = {}
    for group in groups:
        sizes[group] = sizes.get(group, 0) + 1
    return sizes
