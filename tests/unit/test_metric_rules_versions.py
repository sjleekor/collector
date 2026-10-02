"""Versioned metric rule sets: the frozen training-time set and the current one."""

from __future__ import annotations

import pytest

from collector.kr.shared import (
    CURRENT_METRIC_RULES_VERSION,
    METRIC_RULES_MRV1,
    METRIC_RULES_MRV2,
    default_metric_catalog,
    default_metric_mapping_rules,
    metric_rules_content_hash,
    resolve_metric_rules_version,
)

# Pinned. A change here means a frozen model would read different inputs: add a new
# version instead of editing mrv1.
MRV1_HASH = "0052288c06d78fb915018998b9d83ae93ac9d34baf0b5ff5749b3c2f4df32b0e"
MRV2_HASH = "284369d4b8f436abdfb8d60b8fa6e7fa251226d8efd3c98903957d71a0f011b1"


def test_version_ids_and_default() -> None:
    assert METRIC_RULES_MRV1 == "mrv1_20260818"
    assert METRIC_RULES_MRV2 == "mrv2_20260909" == CURRENT_METRIC_RULES_VERSION
    assert resolve_metric_rules_version(None) == METRIC_RULES_MRV2
    with pytest.raises(ValueError):
        resolve_metric_rules_version("mrv0")
    with pytest.raises(ValueError):
        default_metric_mapping_rules("mrv0")


def test_default_is_current() -> None:
    assert default_metric_mapping_rules() == default_metric_mapping_rules(METRIC_RULES_MRV2)
    assert default_metric_catalog() == default_metric_catalog(METRIC_RULES_MRV2)
    assert len(default_metric_mapping_rules()) == 155
    assert len(default_metric_catalog()) == 34


def test_mrv1_counts() -> None:
    assert len(default_metric_mapping_rules(METRIC_RULES_MRV1)) == 107
    assert len(default_metric_catalog(METRIC_RULES_MRV1)) == 29


def test_content_hashes_are_pinned() -> None:
    assert metric_rules_content_hash(METRIC_RULES_MRV1) == MRV1_HASH
    assert metric_rules_content_hash(METRIC_RULES_MRV2) == MRV2_HASH
    assert metric_rules_content_hash() == MRV2_HASH


def test_mrv1_is_strict_subset_of_mrv2() -> None:
    old = default_metric_mapping_rules(METRIC_RULES_MRV1)
    new = default_metric_mapping_rules(METRIC_RULES_MRV2)
    assert len({r.rule_code for r in old}) == len(old)
    # Whole-rule equality: no rule was changed, only added.
    assert set(old) < set(new)
    old_catalog = default_metric_catalog(METRIC_RULES_MRV1)
    new_catalog = default_metric_catalog(METRIC_RULES_MRV2)
    assert set(old_catalog) < set(new_catalog)
