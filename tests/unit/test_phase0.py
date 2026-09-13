import os

import pytest
from pydantic import ValidationError

from collector.kr.domain.enums import RunType, Source
from collector.kr.infra.config.settings import Settings


def test_settings_compute_dsn_and_opendart_key() -> None:
    settings = Settings(
        db_dsn="",
        db_host="db-host",
        db_port=15432,
        db_name="krx_db",
        db_user="collector",
        db_password="secret",
        opendart_api_key="test-opendart-key",
    )

    assert settings.db_dsn == "postgresql://collector:secret@db-host:15432/krx_db"
    assert settings.opendart_api_key == "test-opendart-key"
    assert settings.opendart_api_keys == ("test-opendart-key",)


def test_settings_normalize_multiple_opendart_keys() -> None:
    settings = Settings(
        opendart_api_key=" key_b ",
        opendart_api_keys_raw=" key_a , key_b ,, key_c , ",
    )

    assert settings.opendart_api_key == "key_b"
    assert settings.opendart_api_keys == ("key_a", "key_b", "key_c")


def test_settings_accept_comma_separated_opendart_keys_from_env(monkeypatch) -> None:
    monkeypatch.setenv("OPENDART_API_KEYS", " key_a , key_b ,, key_c , ")
    monkeypatch.setenv("OPENDART_API_KEY", " key_b ")

    settings = Settings(_env_file=None)

    assert settings.opendart_api_key == "key_b"
    assert settings.opendart_api_keys == ("key_a", "key_b", "key_c")


def test_settings_accept_ecos_api_key_from_env(monkeypatch) -> None:
    monkeypatch.setenv("ECOS_API_KEY", "ecos-key")

    settings = Settings(_env_file=None)

    assert settings.ecos_api_key == "ecos-key"
    assert settings.ecos_timeout_seconds == 20.0


def test_settings_accept_fred_api_key_from_env(monkeypatch) -> None:
    monkeypatch.setenv("FRED_API_KEY", "fred-key")

    settings = Settings(_env_file=None)

    assert settings.fred_api_key == "fred-key"
    assert settings.fred_timeout_seconds == 20.0


def test_settings_accept_comma_separated_krx_openapi_keys_from_env(monkeypatch) -> None:
    monkeypatch.setenv("AUTH_KEYS", " key_a , key_b ,, key_a , ")

    settings = Settings(_env_file=None)

    assert settings.krx_openapi_auth_keys == ("key_a", "key_b")


def test_settings_accept_datago_key_from_env(monkeypatch) -> None:
    # The Encoding form carries percent escapes; they must survive verbatim.
    monkeypatch.setenv("DATAGO_KEY", "sE%2BE3t%3D%3D")

    settings = Settings(_env_file=None)

    assert settings.datago_api_key == "sE%2BE3t%3D%3D"


def test_settings_reject_unknown_env_names() -> None:
    # ``extra`` is forbidden, so a credential added to .env without a field
    # here does not fail at its first use -- it fails every command that
    # builds Settings.  AUTH_KEYS and DATAGO_KEY did exactly that.
    with pytest.raises(ValidationError):
        Settings(_env_file=None, krx_openapi_auth_key="typo")


def test_settings_accept_krx_mdc_timeout_with_seconds_suffix(monkeypatch) -> None:
    monkeypatch.setenv("KRX_MDC_TIMEOUT_SECONDS", "150s")

    settings = Settings(_env_file=None)

    assert settings.krx_mdc_timeout_seconds == 150.0


def test_settings_expose_conservative_krx_throttle_defaults() -> None:
    settings = Settings(_env_file=None)

    assert settings.krx_logical_rate_limit_seconds == 8.0
    assert settings.krx_min_delay_seconds == 1.5
    assert settings.krx_max_delay_seconds == 4.0
    assert settings.krx_long_rest_every == 15
    assert settings.krx_long_rest_min_seconds == 30.0
    assert settings.krx_long_rest_max_seconds == 90.0
    assert settings.krx_auth_cooldown_seconds == 10.0
    assert settings.krx_error_backoff_min_seconds == 45.0
    assert settings.krx_error_backoff_max_seconds == 180.0


def test_settings_allow_empty_multiple_opendart_keys() -> None:
    settings = Settings(
        opendart_api_key="",
        opendart_api_keys_raw=" , , ",
    )

    assert settings.opendart_api_keys == ()


def test_settings_exports_krx_credentials_to_environment(monkeypatch) -> None:
    monkeypatch.delenv("KRX_ID", raising=False)
    monkeypatch.delenv("KRX_PW", raising=False)
    settings = Settings(_env_file=None, krx_id="krx-user", krx_pw="krx-pass")

    settings.export_krx_credentials_to_environment()

    assert os.environ["KRX_ID"] == "krx-user"
    assert os.environ["KRX_PW"] == "krx-pass"


def test_future_sources_and_run_types_are_declared() -> None:
    assert Source.OPENDART.value == "OPENDART"
    assert Source.KRX.value == "KRX"
    assert Source.ECOS.value == "ECOS"
    assert Source.FRED.value == "FRED"
    assert Source.KOSIS.value == "KOSIS"
    assert Source.CUSTOMS.value == "CUSTOMS"
    assert Source.KITA.value == "KITA"
    assert Source.NASDAQ_DATA_LINK.value == "NASDAQ_DATA_LINK"
    assert RunType.DART_CORP_SYNC.value == "dart_corp_sync"
    assert RunType.DART_FINANCIAL_SYNC.value == "dart_financial_sync"
    assert RunType.KRX_FLOW_SYNC.value == "krx_flow_sync"
    assert RunType.COMMON_FEATURE_SYNC.value == "common_feature_sync"
    assert RunType.COMMON_FEATURE_BUILD.value == "common_feature_build"
