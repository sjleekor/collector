import pytest

from collector.kr.cli import app


def test_sync_corp_defaults_are_none() -> None:
    args = app.build_parser().parse_args(["dart", "sync-corp"])

    assert args.max_age_days is None
    assert args.stale_fail_days is None


def test_sync_corp_accepts_zero_max_age() -> None:
    args = app.build_parser().parse_args(["dart", "sync-corp", "--max-age-days", "0"])

    assert args.max_age_days == 0
    assert app._resolve_corp_refresh_days(args) == (0, 14)


def test_sync_corp_uses_module_defaults() -> None:
    args = app.build_parser().parse_args(["dart", "sync-corp"])

    assert app._resolve_corp_refresh_days(args) == (7, 14)


def test_sync_corp_rejects_max_age_not_below_stale_fail() -> None:
    args = app.build_parser().parse_args(
        ["dart", "sync-corp", "--max-age-days", "14", "--stale-fail-days", "14"]
    )

    with pytest.raises(SystemExit) as excinfo:
        app._resolve_corp_refresh_days(args)

    assert excinfo.value.code == 2


def test_sync_corp_rejects_negative_values() -> None:
    with pytest.raises(SystemExit):
        app.build_parser().parse_args(["dart", "sync-corp", "--max-age-days", "-1"])
