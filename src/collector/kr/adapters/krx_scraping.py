"""The opt-in gate on KRX scraping paths (K-5).

KRX restricted this host on 2026-08-16 under 이용약관 제10조 제2호 — automated
collection — so the fix was never pacing, it was leaving the scraping path. The
replacements exist and are verified: the Open API for market data and universe,
Naver for adjusted OHLCV, KIS for security flows.

What remains is that the old paths still *work*, and a path that works is a path
somebody runs. ``universe sync --source pykrx``, ``common-sync-pykrx.sh`` and the
opt-in live tests are all one command away, and the next person to reach for one
will not have read this file. So the login is off unless a host says otherwise.

This gate covers the **pykrx** door only. There are three (K-1b):

* **pykrx** — logs in with ``KRX_ID``/``KRX_PW`` at import. Gated here.
* **MDC direct** — ``flows_krx`` and ``common_features_krx``. Not gated: KIS
  cannot fill ``short_selling_balance_quantity`` and prod has no KIS credentials
  yet, so gating these would stop collection that currently has no replacement.
* **FDR anonymous** — ``fdr.StockListing`` reads its rows from a GitHub CSV
  cache, but first calls ``data.krx.co.kr`` twice per invocation just to read
  ``max_work_dt``. Not gated: prod's ``universe-sync.sh`` still passes
  ``--source fdr`` because prod has no ``AUTH_KEYS`` yet.

Both exceptions close when prod gets its credentials; until then, gating them
would break daily collection rather than move it.
"""

from __future__ import annotations

from collector.kr.infra.config.settings import get_settings

#: Env var that re-enables the gated paths.
ALLOW_KRX_SCRAPING_ENV = "ALLOW_KRX_SCRAPING"


class KrxScrapingDisabledError(RuntimeError):
    """A KRX scraping path was reached on a host that has not opted in."""


def ensure_krx_scraping_allowed(path: str, replacement: str) -> None:
    """Raise unless this host has opted in to the KRX scraping paths.

    Args:
        path: What was about to run, for the message.
        replacement: What to use instead. Naming it is the point — an error
            that only says "no" gets worked around, usually by setting the flag.

    Raises:
        KrxScrapingDisabledError: When scraping is not enabled.
    """
    if get_settings().allow_krx_scraping:
        return
    raise KrxScrapingDisabledError(
        f"{path} logs in to KRX, which is the collection path KRX restricted this "
        f"host for on 2026-08-16 (이용약관 제10조 제2호). Use {replacement} instead. "
        f"If you have decided this run is warranted anyway, set "
        f"{ALLOW_KRX_SCRAPING_ENV}=1 — and note that the restriction is about "
        f"automated collection, not about request rate, so going slower does not "
        f"make it permitted."
    )
