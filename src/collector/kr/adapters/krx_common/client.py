"""Shared KRX MDC HTTP client used by direct KRX adapters."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import requests

from collector.kr.util.pipeline import HumanThrottle

logger = logging.getLogger(__name__)

KRX_MDC_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
KRX_LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
KRX_LOGIN_JSP = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
KRX_LOGIN_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
KRX_REFERER = "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

CHUNKED_BLDS = {
    "dbms/MDC/STAT/standard/MDCSTAT00301",
    "dbms/MDC/STAT/standard/MDCSTAT02302",
    "dbms/MDC/STAT/srt/MDCSTAT30001",
    "dbms/MDC/STAT/srt/MDCSTAT30502",
}

AUTH_ERROR_CODES: frozenset[str] = frozenset()

AUTH_ERROR_MESSAGE_PATTERNS: tuple[str, ...] = (
    "로그인",
    "login required",
    "session expired",
    "session timeout",
    "권한이 없",
)


class KrxMdcError(RuntimeError):
    """Base error raised by the KRX MDC client."""


class KrxMdcAuthenticationError(KrxMdcError):
    """Raised when KRX returns an authentication/session response."""


class KrxMdcResponseError(KrxMdcError):
    """Raised when KRX returns an invalid HTTP or JSON payload."""


@dataclass(frozen=True, slots=True)
class KrxMdcRow:
    """One KRX output row with the exact request params that produced it."""

    row: dict[str, Any]
    request: dict[str, Any]


class KrxMdcClient:
    """Small POST client for KRX MDC JSON endpoints."""

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = 20.0,
        login_id: str = "",
        login_pw: str = "",
        auto_login: bool = True,
        warmup: bool = True,
        human_throttle: HumanThrottle | None = None,
    ) -> None:
        self._session = session or requests.Session()
        self._timeout_seconds = timeout_seconds
        self._login_id = login_id
        self._login_pw = login_pw
        self._auto_login = auto_login
        self._human_throttle = human_throttle
        self._warmed_up = False
        if warmup:
            self.warmup()

    @property
    def headers(self) -> dict[str, str]:
        return {
            "User-Agent": USER_AGENT,
            "Referer": KRX_REFERER,
            "X-Requested-With": "XMLHttpRequest",
        }

    def warmup(self) -> None:
        """Prime KRX cookies without requiring credentials."""
        if self._warmed_up:
            return
        try:
            self._before_http("warmup_login_page")
            self._session.get(
                KRX_LOGIN_PAGE,
                headers={"User-Agent": USER_AGENT},
                timeout=self._timeout_seconds,
            )
            self._after_http()
            self._before_http("warmup_login_jsp")
            self._session.get(
                KRX_LOGIN_JSP,
                headers={"User-Agent": USER_AGENT, "Referer": KRX_LOGIN_PAGE},
                timeout=self._timeout_seconds,
            )
            self._after_http()
            self._warmed_up = True
        except requests.RequestException as exc:
            logger.debug("KRX warmup failed: %s", exc)

    def post_json(
        self,
        bld: str,
        params: dict[str, Any],
        *,
        output_key: str | None = None,
    ) -> dict[str, Any]:
        """POST to KRX and return decoded JSON, optionally validating an output key."""
        try:
            payload = self._post_json_once(bld, params)
        except KrxMdcAuthenticationError:
            if not self._auto_login or not (self._login_id and self._login_pw):
                raise
            self.login()
            payload = self._post_json_once(bld, params)

        if output_key is not None and output_key not in payload:
            keys = ", ".join(sorted(str(key) for key in payload.keys()))
            raise KrxMdcResponseError(
                f"KRX response for bld={bld} does not contain output key "
                f"{output_key!r}; keys=[{keys}]"
            )
        return payload

    def post_rows(
        self,
        bld: str,
        params: dict[str, Any],
        *,
        output_key: str,
    ) -> list[KrxMdcRow]:
        """POST to KRX and return rows from one output key, chunking long date ranges."""
        records: list[KrxMdcRow] = []
        for request_params in self._iter_chunked_params(bld, params):
            payload = self.post_json(bld, request_params, output_key=output_key)
            rows = payload[output_key]
            if rows is None:
                continue
            if not isinstance(rows, list):
                raise KrxMdcResponseError(
                    f"KRX output key {output_key!r} for bld={bld} is not a list"
                )
            for row in rows:
                if not isinstance(row, dict):
                    raise KrxMdcResponseError(
                        f"KRX row for bld={bld}, output_key={output_key} is not an object"
                    )
                records.append(KrxMdcRow(row=dict(row), request=dict(request_params)))
        return records

    def login(self) -> None:
        """Authenticate against KRX using configured credentials."""
        if not (self._login_id and self._login_pw):
            raise KrxMdcAuthenticationError("KRX credentials are not configured.")

        self.warmup()
        payload = {
            "mbrNm": "",
            "telNo": "",
            "di": "",
            "certType": "",
            "mbrId": self._login_id,
            "pw": self._login_pw,
        }
        data = self._post_login(payload)
        error_code = str(data.get("_error_code", ""))

        if error_code == "CD011":
            payload["skipDup"] = "Y"
            data = self._post_login(payload)
            error_code = str(data.get("_error_code", ""))

        if error_code != "CD001":
            message = data.get("_error_message") or data.get("error") or "unknown login error"
            raise KrxMdcAuthenticationError(
                f"KRX login failed with code={error_code or 'missing'}: {message}"
            )
        self._cooldown_after_auth("login")

    def _post_login(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._before_http("login_post")
        try:
            response = self._session.post(
                KRX_LOGIN_URL,
                headers={"User-Agent": USER_AGENT, "Referer": KRX_LOGIN_PAGE},
                data=payload,
                timeout=self._timeout_seconds,
            )
        finally:
            self._after_http()
        try:
            return self._decode_json_response(response, bld="login", request=payload)
        except KrxMdcError:
            self._backoff_after_error("login_post")
            raise

    def _post_json_once(self, bld: str, params: dict[str, Any]) -> dict[str, Any]:
        request_payload = dict(params)
        request_payload["bld"] = bld
        self._before_http(bld)
        try:
            response = self._session.post(
                KRX_MDC_URL,
                headers=self.headers,
                data=request_payload,
                timeout=self._timeout_seconds,
            )
        finally:
            self._after_http()
        try:
            return self._decode_json_response(response, bld=bld, request=request_payload)
        except KrxMdcError:
            self._backoff_after_error(bld)
            raise

    def _decode_json_response(
        self,
        response: requests.Response,
        *,
        bld: str,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        status_code = getattr(response, "status_code", 0)
        text = getattr(response, "text", "") or ""
        normalized_text = text.strip()
        if normalized_text.upper() == "LOGOUT":
            raise KrxMdcAuthenticationError(
                f"KRX returned LOGOUT for bld={bld}; login credentials are required."
            )
        if _looks_like_login_html(normalized_text):
            raise KrxMdcAuthenticationError(f"KRX returned login HTML for bld={bld}.")

        if status_code < 200 or status_code >= 300:
            raise KrxMdcResponseError(f"KRX HTTP {status_code} for bld={bld}: {text[:300].strip()}")

        try:
            data = response.json()
        except ValueError as exc:
            raise KrxMdcResponseError(
                f"KRX returned non-JSON for bld={bld}: {normalized_text[:300]}"
            ) from exc

        if not isinstance(data, dict):
            raise KrxMdcResponseError(f"KRX JSON for bld={bld} is not an object.")

        error_code = data.get("_error_code")
        if bld != "login" and error_code and error_code not in {"CD001"}:
            message = data.get("_error_message") or data.get("error") or ""
            if str(error_code) in AUTH_ERROR_CODES or _looks_like_auth_message(message):
                raise KrxMdcAuthenticationError(
                    f"KRX auth error for bld={bld}, code={error_code}: {message}"
                )
            raise KrxMdcResponseError(f"KRX error for bld={bld}, code={error_code}: {message}")

        return data

    def _before_http(self, label: str) -> None:
        if self._human_throttle is not None:
            self._human_throttle.before_request(label)

    def _after_http(self) -> None:
        if self._human_throttle is not None:
            self._human_throttle.after_request()

    def _cooldown_after_auth(self, label: str) -> None:
        if self._human_throttle is not None:
            self._human_throttle.cooldown_after_auth(label)

    def _backoff_after_error(self, label: str) -> None:
        if self._human_throttle is not None:
            self._human_throttle.backoff_after_error(label)

    def _iter_chunked_params(
        self,
        bld: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if bld not in CHUNKED_BLDS or "strtDd" not in params or "endDd" not in params:
            return [dict(params)]

        start = _parse_yyyymmdd(str(params["strtDd"]))
        end = _parse_yyyymmdd(str(params["endDd"]))
        if start > end:
            raise KrxMdcResponseError(
                f"Invalid KRX date range for bld={bld}: {start.isoformat()} > {end.isoformat()}"
            )

        chunks: list[dict[str, Any]] = []
        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + timedelta(days=730), end)
            chunk_params = dict(params)
            chunk_params["strtDd"] = cursor.strftime("%Y%m%d")
            chunk_params["endDd"] = chunk_end.strftime("%Y%m%d")
            chunks.append(chunk_params)
            cursor = chunk_end + timedelta(days=1)
        return chunks


def _parse_yyyymmdd(value: str) -> date:
    compact = value.replace("-", "").replace("/", "")
    if len(compact) != 8 or not compact.isdigit():
        raise KrxMdcResponseError(f"Invalid KRX yyyymmdd date: {value!r}")
    return date(int(compact[:4]), int(compact[4:6]), int(compact[6:8]))


def _looks_like_login_html(text: str) -> bool:
    lowered = text.lower()
    return "<html" in lowered and ("login" in lowered or "로그인" in text)


def _looks_like_auth_message(message: object) -> bool:
    text = str(message or "")
    if not text:
        return False
    lowered = text.lower()
    return any(pattern.lower() in lowered for pattern in AUTH_ERROR_MESSAGE_PATTERNS)
