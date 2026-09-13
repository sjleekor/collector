"""Backward-compatible imports for the shared KRX MDC client."""

from collector.kr.adapters.krx_common.client import (
    KRX_LOGIN_URL,
    KRX_MDC_URL,
    KrxMdcAuthenticationError,
    KrxMdcClient,
    KrxMdcError,
    KrxMdcResponseError,
    KrxMdcRow,
)

__all__ = [
    "KRX_LOGIN_URL",
    "KRX_MDC_URL",
    "KrxMdcAuthenticationError",
    "KrxMdcClient",
    "KrxMdcError",
    "KrxMdcResponseError",
    "KrxMdcRow",
]
