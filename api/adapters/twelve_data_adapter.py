"""
Twelve Data REST API adapter.

Free tier: 8 calls/min, 800/day.
Uses the same sliding-window deque rate limiter pattern as FinnhubService.
"""

import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from data_normalizer import NormalizedQuote

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


class TwelveDataAdapter:
    """Fetch quotes from Twelve Data REST API, returning NormalizedQuote."""

    _BASE_URL = "https://api.twelvedata.com/quote"

    def __init__(self, api_key: str):
        self._api_key = api_key.strip()
        # Rate limiter: 8 calls per 60-second window
        self._call_times: deque = deque(maxlen=8)
        self._rate_lock = threading.Lock()

    def is_enabled(self) -> bool:
        return bool(self._api_key)

    def _wait_for_rate_limit(self) -> bool:
        """Returns True if request can proceed, False if rate-limited."""
        with self._rate_lock:
            now = time.time()
            while self._call_times and (now - self._call_times[0]) >= 60:
                self._call_times.popleft()

            if len(self._call_times) < 8:
                self._call_times.append(time.time())
                return True

            sleep_for = 60.0 - (now - self._call_times[0])
            if sleep_for > 2:
                logger.debug("TwelveData rate-limited, skipping")
                return False

        # Sleep OUTSIDE the lock so other threads aren't blocked
        if sleep_for > 0:
            time.sleep(sleep_for)

        with self._rate_lock:
            now = time.time()
            while self._call_times and (now - self._call_times[0]) >= 60:
                self._call_times.popleft()
            if len(self._call_times) >= 8:
                return False  # Another thread filled the window while we slept
            self._call_times.append(time.time())
            return True

    def fetch_quote(self, symbol: str, instrument_type: str = "equity") -> NormalizedQuote | None:
        if not self.is_enabled():
            return None

        if not self._wait_for_rate_limit():
            return None

        try:
            resp = requests.get(
                self._BASE_URL,
                params={"symbol": symbol, "apikey": self._api_key},
                timeout=10,
            )
            if resp.status_code != 200:
                logger.warning("Twelve Data HTTP %d for %s", resp.status_code, symbol)
                return None

            data = resp.json()
            if "code" in data:
                # Twelve Data returns {"code": 400, "message": "..."} on error
                logger.warning("Twelve Data error for %s: %s", symbol, data.get("message"))
                return None

            price = data.get("close")
            if price is None:
                return None

            ts_str = data.get("datetime", "")
            try:
                naive_ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                # Twelve Data returns exchange-local time for equities, UTC for crypto/forex
                if instrument_type == "equity":
                    timestamp = naive_ts.replace(tzinfo=_ET).astimezone(timezone.utc)
                else:
                    timestamp = naive_ts.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                timestamp = datetime.now(tz=timezone.utc)

            return NormalizedQuote(
                symbol=symbol,
                instrument_type=instrument_type,
                price=float(price),
                open=float(data["open"]) if data.get("open") is not None else None,
                high=float(data["high"]) if data.get("high") is not None else None,
                low=float(data["low"]) if data.get("low") is not None else None,
                close=float(data["close"]) if data.get("close") is not None else None,
                volume=float(data["volume"]) if data.get("volume") is not None else None,
                timestamp=timestamp,
                source="twelve_data",
                delay="delayed_15m",
            )
        except Exception as e:
            # Log type only — str(e) can contain URL with API key from requests lib
            logger.warning("Twelve Data fetch failed for %s: %s", symbol, type(e).__name__)
            return None
