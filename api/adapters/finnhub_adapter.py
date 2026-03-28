"""
Adapter that wraps the existing FinnhubService to produce NormalizedQuote.

Does NOT rewrite FinnhubService — calls through to its fetch_quote() method
and maps the raw Finnhub response dict to NormalizedQuote format.
"""

import logging
from datetime import datetime, timezone

from data_normalizer import NormalizedQuote

logger = logging.getLogger(__name__)


class FinnhubAdapter:
    """Wraps FinnhubService, mapping its output to NormalizedQuote."""

    def __init__(self, service):
        self._service = service

    def fetch_quote(self, symbol: str, instrument_type: str = "equity") -> NormalizedQuote | None:
        raw = self._service.fetch_quote(symbol)
        if raw is None:
            return None

        price = raw.get("c")
        if price is None or price == 0:
            return None

        ts = raw.get("t")
        timestamp = (
            datetime.fromtimestamp(ts, tz=timezone.utc)
            if ts
            else datetime.now(tz=timezone.utc)
        )

        # Finnhub free tier: realtime for crypto/forex, 15-min delayed for US equities
        if ":" in symbol or instrument_type == "crypto":
            delay = "realtime"
        elif instrument_type == "forex":
            delay = "realtime"
        else:
            delay = "delayed_15m"

        return NormalizedQuote(
            symbol=symbol,
            instrument_type=instrument_type,
            price=float(price),
            open=float(raw["o"]) if raw.get("o") is not None else None,
            high=float(raw["h"]) if raw.get("h") is not None else None,
            low=float(raw["l"]) if raw.get("l") is not None else None,
            close=float(raw["pc"]) if raw.get("pc") is not None else None,
            volume=None,  # Finnhub quote endpoint doesn't return volume
            timestamp=timestamp,
            source="finnhub",
            delay=delay,
        )
