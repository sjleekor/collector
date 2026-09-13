"""KIS OAuth access-token cache and issuer.

Every token issuance sends the account holder a KakaoTalk notification, and
collectors run as ephemeral ``docker compose run --rm`` containers.  Without a
cache on a host volume, a daily schedule means a daily notification per job —
so the cache is not an optimisation here, it is the reason the adapter is
usable at all.

KIS returns the *same* token for a reissue within six hours, so a warm cache
also costs nothing upstream.  The rule the code enforces is simply: never ask
for a token while a usable one is on disk.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import requests

from collector.kr.util.pipeline import SourceAuthError
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)

KIS_TOKEN_PATH = "/oauth2/tokenP"
DEFAULT_TOKEN_TTL_SECONDS = 86400.0


@dataclass(frozen=True, slots=True)
class KisAccessToken:
    """One issued access token and the moment it stops being usable."""

    access_token: str
    expires_at: datetime

    def seconds_remaining(self, *, as_of: datetime) -> float:
        return (self.expires_at - as_of).total_seconds()


class KisTokenCache:
    """Read/write one access token as JSON at ``path``.

    Writes are atomic (temp file + ``os.replace``) so a crashed run never
    leaves a half-written cache that forces a reissue.
    """

    def __init__(
        self,
        path: Path,
        *,
        refresh_margin_seconds: float = 3600.0,
        now_fn: Callable[[], datetime] = now_kst,
    ) -> None:
        self._path = Path(path)
        self._refresh_margin_seconds = max(0.0, refresh_margin_seconds)
        self._now_fn = now_fn

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> KisAccessToken | None:
        """Return the cached token, or ``None`` if it is missing or too old.

        A corrupt cache is treated as absent rather than fatal: the cost of
        being wrong is one token issuance, and refusing to run would be worse.
        """
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            logger.warning("KIS token cache at %s is unreadable (%s); ignoring", self._path, exc)
            return None

        token = str(payload.get("access_token") or "")
        expires_at_raw = payload.get("expires_at")
        if not token or not expires_at_raw:
            logger.warning("KIS token cache at %s is incomplete; ignoring", self._path)
            return None

        try:
            expires_at = _parse_expires_at(expires_at_raw)
        except (TypeError, ValueError) as exc:
            logger.warning("KIS token cache at %s has a bad expiry (%s); ignoring", self._path, exc)
            return None

        remaining = (expires_at - self._now_fn()).total_seconds()
        if remaining <= self._refresh_margin_seconds:
            logger.info(
                "KIS token cache expires in %.0fs (margin %.0fs); a new token is needed",
                remaining,
                self._refresh_margin_seconds,
            )
            return None
        return KisAccessToken(access_token=token, expires_at=expires_at)

    def store(self, token: KisAccessToken) -> None:
        """Write ``token`` atomically with owner-only permissions."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "access_token": token.access_token,
                "expires_at": token.expires_at.isoformat(),
            }
        )
        handle, temp_path = tempfile.mkstemp(dir=self._path.parent, prefix=".kis_token.")
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                stream.write(payload)
            os.chmod(temp_path, 0o600)
            os.replace(temp_path, self._path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(temp_path)
            raise


def _parse_expires_at(value: object) -> datetime:
    """Parse an expiry written by us (ISO) or by KIS (``%Y-%m-%d %H:%M:%S``)."""
    if isinstance(value, int | float):
        return datetime.fromtimestamp(float(value), tz=now_kst().tzinfo)
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=now_kst().tzinfo)
    return parsed


class KisTokenProvider:
    """Hand out an access token, issuing one only when the cache cannot."""

    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        base_url: str,
        cache: KisTokenCache,
        session: requests.Session | None = None,
        timeout_seconds: float = 20.0,
        now_fn: Callable[[], datetime] = now_kst,
    ) -> None:
        self._app_key = app_key.strip()
        self._app_secret = app_secret.strip()
        self._base_url = base_url.rstrip("/")
        self._cache = cache
        self._session = session or requests.Session()
        self._timeout_seconds = timeout_seconds
        self._now_fn = now_fn
        self._token: KisAccessToken | None = None
        self.issued_count = 0
        self.cache_hit_count = 0

    def token(self, *, force_refresh: bool = False) -> str:
        """Return a usable access token.

        ``force_refresh`` is for the one case that justifies a notification:
        the server rejected the token we just used.
        """
        if not force_refresh:
            if self._token is not None:
                return self._token.access_token
            cached = self._cache.load()
            if cached is not None:
                self._token = cached
                self.cache_hit_count += 1
                logger.info(
                    "KIS token loaded from cache (%.0fs remaining); no issuance",
                    cached.seconds_remaining(as_of=self._now_fn()),
                )
                return cached.access_token

        issued = self._issue()
        self._token = issued
        self._cache.store(issued)
        return issued.access_token

    def _issue(self) -> KisAccessToken:
        if not self._app_key or not self._app_secret:
            raise SourceAuthError(
                "KIS_APP_KEY / KIS_APP_SECRET are not configured; cannot issue an access token."
            )

        logger.warning(
            "Issuing a new KIS access token — this sends a KakaoTalk notification "
            "to the account holder (cache: %s)",
            self._cache.path,
        )
        try:
            response = self._session.post(
                f"{self._base_url}{KIS_TOKEN_PATH}",
                json={
                    "grant_type": "client_credentials",
                    "appkey": self._app_key,
                    "appsecret": self._app_secret,
                },
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as exc:
            raise SourceAuthError(f"KIS token request failed: {exc}") from exc

        if response.status_code != 200:
            raise SourceAuthError(
                f"KIS token request returned HTTP {response.status_code}: {response.text[:200]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise SourceAuthError("KIS token response was not JSON") from exc

        access_token = str(payload.get("access_token") or "")
        if not access_token:
            raise SourceAuthError(
                f"KIS token response carried no access_token: "
                f"{payload.get('error_description') or payload}"
            )

        expires_at = self._resolve_expiry(payload)
        self.issued_count += 1
        logger.warning("KIS access token issued; valid until %s", expires_at.isoformat())
        return KisAccessToken(access_token=access_token, expires_at=expires_at)

    def _resolve_expiry(self, payload: dict[str, object]) -> datetime:
        expires_in = payload.get("expires_in")
        if isinstance(expires_in, int | float) and expires_in > 0:
            return self._now_fn() + timedelta(seconds=float(expires_in))
        expired_at = payload.get("access_token_token_expired")
        if expired_at:
            try:
                return _parse_expires_at(expired_at)
            except (TypeError, ValueError):
                logger.warning("KIS token expiry %r was unparseable; assuming 1 day", expired_at)
        return self._now_fn() + timedelta(seconds=DEFAULT_TOKEN_TTL_SECONDS)
