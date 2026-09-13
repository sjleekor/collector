"""Unit tests for KSIC industry grouping (N2).

The codes here are real ``induty_code`` values observed on 2026-08-15. They are
the reason the grouping rule takes a PREFIX rather than a fixed width: lengths
of 2, 3, 4 and 5 all occur in the same response set.
"""

from __future__ import annotations

from collector.kr.definitions.industry_groups import (
    MIN_GROUP_SIZE,
    OTHER_GROUP,
    UNKNOWN_GROUP,
    UNKNOWN_SECTION,
    group_sizes,
    industry_group,
    industry_section,
    is_financial,
    resolve_groups,
)


def test_prefix_rule_handles_every_observed_code_length() -> None:
    # 3, 4 and 5 digits, all from the same live sample.
    assert industry_group("264") == "26"  # 삼성전자
    assert industry_group("5821") == "58"  # 크래프톤
    assert industry_group("21100") == "21"  # 셀트리온
    assert industry_group("65121") == "65"  # 삼성화재
    assert industry_group("50112") == "50"  # HMM
    # A 2-digit code already IS the middle category.
    assert industry_group("26") == "26"


def test_missing_or_junk_code_becomes_unknown() -> None:
    assert industry_group(None) == UNKNOWN_GROUP
    assert industry_group("") == UNKNOWN_GROUP
    assert industry_group("  ") == UNKNOWN_GROUP
    assert industry_group("2") == UNKNOWN_GROUP
    assert industry_group("A1") == UNKNOWN_GROUP


def test_code_is_stripped_before_slicing() -> None:
    assert industry_group(" 264 ") == "26"


def test_section_lookup_covers_the_ksic_ranges() -> None:
    assert industry_section("26") == "C"  # 제조업
    assert industry_section("58") == "J"  # 정보통신업
    assert industry_section("64") == "K"  # 금융 및 보험업
    assert industry_section("50") == "H"  # 운수 및 창고업
    assert industry_section("68") == "L"  # 부동산업
    # 43 falls between the F (41-42) and G (45-47) ranges.
    assert industry_section("43") == UNKNOWN_SECTION
    assert industry_section(UNKNOWN_GROUP) == UNKNOWN_SECTION


def test_financial_groups_are_flagged() -> None:
    # LG (holding) and KB Financial Group (bank holding) both return 64992, so
    # this flag marks "cannot be separated here", not "is a bank".
    assert is_financial(industry_group("64992")) is True
    assert is_financial("65") is True
    assert is_financial("66") is True
    assert is_financial("26") is False


def test_large_groups_are_kept_and_small_groups_fold_into_their_section() -> None:
    codes = {f"big{i}": "26100" for i in range(MIN_GROUP_SIZE)}
    # Section J needs to reach the minimum on its own, otherwise it folds again.
    codes.update({f"j{i}": "5821" for i in range(MIN_GROUP_SIZE - 2)})
    codes["small1"] = "5921"  # also section J, too small alone
    codes["small2"] = "5922"

    resolved = resolve_groups(codes)

    assert resolved["big0"] == "26"
    assert resolved["small1"] == "J"
    assert resolved["small2"] == "J"


def test_a_section_that_is_itself_too_small_folds_into_other() -> None:
    # One fold-up level does not reach the criterion on the real universe:
    # section A holds 4 listed names in total, so folding 01/03 into it leaves a
    # group of 4. A z-score over 4 names -- over 2, in the worst observed case --
    # is a number the data cannot support, so the fold-up repeats.
    codes = {f"big{i}": "26100" for i in range(MIN_GROUP_SIZE)}
    codes["farm1"] = "01110"  # section A
    codes["farm2"] = "03120"

    resolved = resolve_groups(codes)

    assert resolved["big0"] == "26"
    assert resolved["farm1"] == OTHER_GROUP
    assert resolved["farm2"] == OTHER_GROUP


def test_no_real_group_is_left_below_the_minimum() -> None:
    # The invariant the whole fold-up exists to produce. UNKNOWN and OTHER are
    # terminal by design and are the only labels allowed to come back short.
    codes: dict[str, str | None] = {f"big{i}": "26100" for i in range(MIN_GROUP_SIZE)}
    codes.update({f"mid{i}": "58210" for i in range(MIN_GROUP_SIZE)})
    codes.update(
        {
            "a": "01110",  # section A
            "b": "05100",  # section B
            "c": "35110",  # section D
            "d": "68100",  # section L
            "e": None,  # unknown
        }
    )

    sizes = group_sizes(resolve_groups(codes).values())

    short = {
        group: size
        for group, size in sizes.items()
        if size < MIN_GROUP_SIZE and group not in {UNKNOWN_GROUP, OTHER_GROUP}
    }
    assert short == {}
    assert sizes[OTHER_GROUP] == 4


def test_unknown_codes_stay_their_own_bucket() -> None:
    # A missing code is not evidence of any industry, so it must not be folded
    # into a real section.
    codes = {f"big{i}": "26100" for i in range(MIN_GROUP_SIZE)}
    codes["missing"] = None

    resolved = resolve_groups(codes)

    assert resolved["missing"] == UNKNOWN_GROUP


def test_fold_up_threshold_is_configurable() -> None:
    # 26 has 3 members; 58 and 59 have 1 each and share section J.
    codes = {"a": "26100", "b": "26200", "c": "26300", "d": "5821", "e": "5921"}

    lenient = resolve_groups(codes, min_group_size=2)
    strict = resolve_groups(codes, min_group_size=3)

    assert lenient["a"] == "26"
    assert lenient["d"] == "J"  # 1 alone, but section J has 2 and clears 2
    assert strict["a"] == "26"
    assert strict["d"] == OTHER_GROUP  # section J has 2 < 3, so it folds again


def test_group_sizes_counts_members() -> None:
    assert group_sizes(["26", "26", "58"]) == {"26": 2, "58": 1}
