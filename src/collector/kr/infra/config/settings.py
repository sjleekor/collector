"""Application settings loaded from environment / ``.env`` file.

Uses ``pydantic-settings`` for typed, validated configuration.  The
``Settings`` class reads from environment variables (and ``.env`` in dev)
and exposes a computed ``db_dsn`` that falls back to individual DB_*
components when ``DB_DSN`` is not provided.

Usage::

    from collector.kr.infra.config.settings import get_settings

    settings = get_settings()
    print(settings.db_dsn)
"""

from __future__ import annotations

import os
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, PrivateAttr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_KRX_MDC_TIMEOUT_SECONDS = 20.0
DEFAULT_KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"

# KIS documents 20 requests/second per account for a live account. This one
# does not deliver that: measured on 2026-08-16, 20 sequential quotation calls
# came back clean at 1.0/s, while 1.2/s already drew 3 rejections and 1.5/s
# drew 6. Effective throughput topped out near 1.1/s at *every* rate tried, so
# asking for more only converts successes into retries.
DEFAULT_KIS_REQUESTS_PER_SECOND = 1.0

DEFAULT_KRX_OPENAPI_BASE_URL = "https://data-dbg.krx.co.kr/svc/apis"

# Ten consecutive unpaced calls on 2026-08-18 drew no rejection, and each one
# takes 1.3-1.7s anyway, so a request-per-second cap is a safety rail rather
# than a real constraint.  Two is loose enough not to slow the backfill and
# tight enough that a bug cannot spend the daily quota in a minute.
DEFAULT_KRX_OPENAPI_REQUESTS_PER_SECOND = 2.0


def _split_key_list(raw: str) -> list[str]:
    """Split a comma-separated API key string, trimming blanks and duplicates."""
    ordered: list[str] = []
    for key in raw.split(","):
        normalized = key.strip()
        if normalized and normalized not in ordered:
            ordered.append(normalized)
    return ordered


class RunMode(StrEnum):
    """Application run mode."""

    DEV = "dev"
    PROD = "prod"


class LogFormat(StrEnum):
    """Log output format."""

    PLAIN = "plain"
    JSON = "json"


class UniverseSourceDefault(StrEnum):
    """Default universe data source.

    ``KRX_OPENAPI`` is the official endpoint and the only one of the three that
    does not scrape (K-5). ``FDR`` and ``PYKRX`` remain selectable so a host
    without ``AUTH_KEYS`` is not stranded, but both reach KRX outside the
    permitted path and are on their way out.
    """

    KRX_OPENAPI = "krx-openapi"
    FDR = "fdr"
    PYKRX = "pykrx"


class Settings(BaseSettings):
    """Central application configuration.

    Reads from environment variables (prefix-free) and an optional ``.env``
    file located in the project root.

    Attributes:
        db_dsn: Full PostgreSQL DSN.  If empty, computed from DB_HOST etc.
        db_host: Database host (used if ``db_dsn`` is empty).
        db_port: Database port.
        db_name: Database name.
        db_user: Database user.
        db_password: Database password.
        log_level: Python logging level name.
        log_format: Output format (plain text or JSON).
        log_dir: Optional directory for rotating file logs.
        run_mode: dev or prod.
        universe_source_default: Default source for universe sync.
        opendart_api_key: Optional OpenDART API key for future DART-based
            ingestion features.
        rate_limit_seconds: Delay between API calls (seconds).
        long_rest_interval: Number of API requests between long rests
            (0 disables long rests).
        long_rest_seconds: Duration of each long rest, in seconds.
        remote_db_info_path: Path to the remote DB metadata file. No default —
            set ``REMOTE_DB_INFO_PATH`` in ``.env`` (host-specific secret path).
        remote_db_batch_size: Batch size for remote-to-local DB sync.
        remote_db_host_override: Optional hostname override for the remote DB.
        remote_db_ssh_host: Optional SSH host for local port forwarding.
        remote_db_ssh_local_port: Optional fixed local port for SSH tunnel.
        remote_db_ssh_compression: Enable SSH compression for DB tunnels.
        allow_krx_scraping: Opt in to the pykrx login path, off by default (K-5).
        krx_mdc_timeout_seconds: HTTP timeout for KRX MDC requests.
        krx_logical_rate_limit_seconds: Delay between higher-level KRX flow
            requests.
        krx_min_delay_seconds: Minimum delay between actual KRX HTTP calls.
        krx_max_delay_seconds: Maximum delay between actual KRX HTTP calls.
        krx_long_rest_every: Number of KRX HTTP calls between long rests.
        krx_long_rest_min_seconds: Minimum duration of a long KRX rest.
        krx_long_rest_max_seconds: Maximum duration of a long KRX rest.
        krx_auth_cooldown_seconds: Delay after a successful KRX login.
        krx_error_backoff_min_seconds: Minimum delay after a KRX error.
        krx_error_backoff_max_seconds: Maximum delay after a KRX error.
        kis_app_key: 한국투자증권 오픈API app key (quotation endpoints need no
            account number).
        kis_app_secret: 한국투자증권 오픈API app secret.
        kis_base_url: KIS REST base URL (real vs paper trading domain).
        kis_timeout_seconds: HTTP timeout for KIS requests.
        kis_token_cache_path: Where the OAuth access token is cached.  This
            **must** point at a host volume: tokens last a day, and every
            issuance sends the account holder a KakaoTalk notification, so a
            cache inside an ephemeral container reissues on every run.
        kis_token_refresh_margin_seconds: Reissue this long before expiry.
        kis_requests_per_second: Token-bucket rate for KIS calls.  The default
            is measured, not documented — see
            :data:`DEFAULT_KIS_REQUESTS_PER_SECOND`.
        kis_max_burst_requests: Token-bucket burst size.  One, because a burst
            is what draws the throttle rejection.
        krx_openapi_auth_keys_raw: Comma-separated KRX Open API keys, read from
            ``AUTH_KEYS`` — the header the API itself expects is ``AUTH_KEY``.
            Use :attr:`krx_openapi_auth_keys` rather than this raw string.
        krx_openapi_base_url: KRX Open API service base URL.
        krx_openapi_timeout_seconds: HTTP timeout for KRX Open API requests.
        krx_openapi_requests_per_second: Token-bucket rate — a safety rail, not
            an observed limit; see
            :data:`DEFAULT_KRX_OPENAPI_REQUESTS_PER_SECOND`.
        krx_openapi_max_burst_requests: Token-bucket burst size.
        datago_api_key: 공공데이터포털 (data.go.kr) service key.  Store the
            *Encoding* form and put it in the URL verbatim: passing it through
            a query-parameter encoder turns ``%2B`` into ``%252B`` and the
            server then receives a different key.
        ecos_api_key: Optional Bank of Korea ECOS API key.
        ecos_timeout_seconds: HTTP timeout for ECOS requests.
        fred_api_key: Optional FRED API key.
        fred_timeout_seconds: HTTP timeout for FRED requests.
        sec_user_agent: Contact-bearing User-Agent for SEC (US). Declared here
            because ``BaseSettings`` defaults ``extra`` to ``forbid`` and the
            dotenv source passes every key in ``.env`` through — an undeclared
            key there makes ``get_settings()`` raise for the whole CLI.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        populate_by_name=True,
    )

    # Database
    db_dsn: str = ""
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "krx_data"
    db_user: str = "krx_user"
    db_password: str = ""

    # Logging
    log_level: str = "INFO"
    log_format: LogFormat = LogFormat.PLAIN
    log_dir: Path | None = None

    # Runtime
    run_mode: RunMode = RunMode.DEV

    # Universe
    universe_source_default: UniverseSourceDefault = UniverseSourceDefault.FDR

    # KRX / pykrx authentication
    krx_id: str = ""
    krx_pw: str = ""
    # Off by default (K-5). pykrx logs in to KRX at import, and that login is
    # the collection path KRX restricted this host for; the Open API and Naver
    # cover the same data without it. Kept as a flag rather than deleted so a
    # deliberate comparison run is still possible.
    allow_krx_scraping: bool = Field(default=False, validation_alias="ALLOW_KRX_SCRAPING")
    krx_mdc_timeout_seconds: float = DEFAULT_KRX_MDC_TIMEOUT_SECONDS
    krx_logical_rate_limit_seconds: float = 8.0
    krx_min_delay_seconds: float = 1.5
    krx_max_delay_seconds: float = 4.0
    krx_long_rest_every: int = 15
    krx_long_rest_min_seconds: float = 30.0
    krx_long_rest_max_seconds: float = 90.0
    krx_auth_cooldown_seconds: float = 10.0
    krx_error_backoff_min_seconds: float = 45.0
    krx_error_backoff_max_seconds: float = 180.0

    # OpenDART
    opendart_api_key: str = ""
    opendart_api_keys_raw: str = Field(default="", validation_alias="OPENDART_API_KEYS")
    _opendart_api_keys: tuple[str, ...] = PrivateAttr(default=())

    # KIS (한국투자증권 오픈API)
    kis_app_key: str = ""
    kis_app_secret: str = ""
    kis_base_url: str = DEFAULT_KIS_BASE_URL
    kis_timeout_seconds: float = 20.0
    kis_token_cache_path: Path = Path("state/kis_token.json")
    kis_token_refresh_margin_seconds: float = 3600.0
    kis_requests_per_second: float = DEFAULT_KIS_REQUESTS_PER_SECOND
    kis_max_burst_requests: int = 1

    # KRX Open API (data-dbg.krx.co.kr).  Multi-key like OpenDART; KRX
    # publishes 10,000 requests/day but does not say whether that budget is per
    # key or per account, so nothing here assumes it.
    krx_openapi_auth_keys_raw: str = Field(default="", validation_alias="AUTH_KEYS")
    _krx_openapi_auth_keys: tuple[str, ...] = PrivateAttr(default=())
    krx_openapi_base_url: str = DEFAULT_KRX_OPENAPI_BASE_URL
    krx_openapi_timeout_seconds: float = 20.0
    krx_openapi_requests_per_second: float = DEFAULT_KRX_OPENAPI_REQUESTS_PER_SECOND
    krx_openapi_max_burst_requests: int = 1

    # 공공데이터포털 (data.go.kr)
    datago_api_key: str = Field(default="", validation_alias="DATAGO_KEY")

    # ECOS
    ecos_api_key: str = ""
    ecos_timeout_seconds: float = 20.0

    # FRED
    fred_api_key: str = ""
    fred_timeout_seconds: float = 20.0

    # 미국 — www.sec.gov는 연락처 없는 UA에 403을 준다. 값은 .env에만 둔다
    # (이 저장소는 public이다). 형식: "<name>/<ver> (<contact email>)".
    sec_user_agent: str = ""

    # Rate limiting
    rate_limit_seconds: float = 0.2
    long_rest_interval: int = 100
    long_rest_seconds: float = 10.0

    # Remote DB sync
    remote_db_info_path: Path | None = None
    remote_db_batch_size: int = 50000
    remote_db_host_override: str | None = None
    remote_db_ssh_host: str | None = None
    remote_db_ssh_local_port: int | None = None
    remote_db_ssh_compression: bool = False

    @property
    def opendart_api_keys(self) -> tuple[str, ...]:
        """Normalized OpenDART key list from OPENDART_API_KEYS and OPENDART_API_KEY."""
        return self._opendart_api_keys

    @property
    def krx_openapi_auth_keys(self) -> tuple[str, ...]:
        """Normalized KRX Open API key list from ``AUTH_KEYS``."""
        return self._krx_openapi_auth_keys

    @field_validator("krx_mdc_timeout_seconds", mode="before")
    @classmethod
    def _parse_krx_mdc_timeout_seconds(cls, value: Any) -> float:
        """Accept timeout values as seconds, with an optional ``s`` suffix."""
        if value is None or value == "":
            return DEFAULT_KRX_MDC_TIMEOUT_SECONDS
        if isinstance(value, str):
            normalized = value.strip().lower()
            for suffix in ("seconds", "second", "secs", "sec", "s"):
                if normalized.endswith(suffix):
                    normalized = normalized[: -len(suffix)].strip()
                    break
            value = normalized
        try:
            seconds = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "KRX_MDC_TIMEOUT_SECONDS must be a positive number of seconds"
            ) from exc
        if seconds <= 0:
            raise ValueError("KRX_MDC_TIMEOUT_SECONDS must be greater than zero")
        return seconds

    @model_validator(mode="after")
    def _compute_dsn(self) -> Settings:
        """Build ``db_dsn`` from individual components if not set directly."""
        if not self.db_dsn:
            self.db_dsn = (
                f"postgresql://{self.db_user}:{self.db_password}"
                f"@{self.db_host}:{self.db_port}/{self.db_name}"
            )

        ordered_keys = _split_key_list(self.opendart_api_keys_raw)

        legacy_key = self.opendart_api_key.strip()
        if legacy_key and legacy_key not in ordered_keys:
            ordered_keys.append(legacy_key)

        self.opendart_api_key = legacy_key
        self._opendart_api_keys = tuple(ordered_keys)
        self._krx_openapi_auth_keys = tuple(_split_key_list(self.krx_openapi_auth_keys_raw))
        return self

    def export_krx_credentials_to_environment(self) -> None:
        """Expose .env-loaded KRX credentials for pykrx's import-time auth hook."""
        if self.krx_id and not os.environ.get("KRX_ID"):
            os.environ["KRX_ID"] = self.krx_id
        if self.krx_pw and not os.environ.get("KRX_PW"):
            os.environ["KRX_PW"] = self.krx_pw


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton ``Settings`` instance (cached)."""
    return Settings()


def configure_krx_credentials_from_settings() -> None:
    """Load KRX credentials from settings before importing pykrx modules."""
    get_settings().export_krx_credentials_to_environment()
