"""
Finnhub REST API client with in-memory caching and rate limiting.

This module is a pure Python peer to api/main.py — no FastAPI imports.
Used by api/main.py for background price refresh of TRACKED_SECURITIES.

Free tier constraints:
  - 60 API calls per minute maximum
  - Quote endpoint: GET https://finnhub.io/api/v1/quote?symbol={symbol}&token={key}
"""

import logging
import threading
import time
from collections import deque

import requests

logger = logging.getLogger(__name__)


class FinnhubService:
    """Fetch stock quotes from Finnhub with caching and rate limiting."""

    _BASE_URL = "https://finnhub.io/api/v1/quote"

    def __init__(self, api_key: str, cache_ttl: int = 60):
        """
        Args:
            api_key: Finnhub API key. Empty string = service disabled.
            cache_ttl: Seconds before a cached quote is considered stale.
        """
        self._api_key = api_key.strip()
        self._cache_ttl = cache_ttl
        # Cache: symbol → (fetched_at_timestamp, quote_dict)
        self._cache: dict[str, tuple[float, dict]] = {}
        # Rate limiter: stores timestamps of the last ≤60 calls
        self._call_times: deque = deque(maxlen=60)
        self._rate_lock = threading.Lock()

    def is_enabled(self) -> bool:
        """Returns True only if an API key is configured."""
        return bool(self._api_key)

    def _wait_for_rate_limit(self) -> None:
        """
        Block until we can make a call without exceeding 60 calls/minute.
        Thread-safe: acquires lock, purges old timestamps, sleeps outside
        the lock if the window is full, then re-acquires to record the call.
        """
        with self._rate_lock:
            now = time.time()
            while self._call_times and (now - self._call_times[0]) >= 60:
                self._call_times.popleft()

            if len(self._call_times) < 60:
                self._call_times.append(time.time())
                return

            sleep_for = 60.0 - (now - self._call_times[0])

        # Sleep OUTSIDE the lock so other threads aren't blocked
        if sleep_for > 0:
            time.sleep(sleep_for)

        with self._rate_lock:
            now = time.time()
            while self._call_times and (now - self._call_times[0]) >= 60:
                self._call_times.popleft()
            if len(self._call_times) >= 60:
                # Another thread filled the window while we slept — don't exceed
                return
            self._call_times.append(time.time())

    def _is_cached(self, symbol: str) -> bool:
        """Returns True if symbol has a valid non-expired cache entry."""
        if symbol not in self._cache:
            return False
        fetched_at, _ = self._cache[symbol]
        return (time.time() - fetched_at) < self._cache_ttl

    def _get_cached(self, symbol: str) -> dict | None:
        """Returns cached quote dict or None if not cached/expired."""
        if symbol in self._cache:
            _, quote = self._cache[symbol]
            return quote
        return None

    def fetch_quote(self, symbol: str) -> dict | None:
        """
        Fetch a single symbol's quote from Finnhub.

        Returns:
            The raw Finnhub response dict on success, None otherwise.

        Behavior:
          - Returns None immediately if api_key is empty.
          - Returns cached data if within TTL (no HTTP call).
          - On HTTP 429: back off 1s and retry once; returns None if still 429.
          - On network failure: returns cached data (even if expired) if available, else None.
        """
        if not self.is_enabled():
            return None

        # Serve from cache if still valid
        if self._is_cached(symbol):
            return self._get_cached(symbol)

        self._wait_for_rate_limit()

        def _do_request() -> requests.Response | None:
            try:
                resp = requests.get(
                    self._BASE_URL,
                    params={"symbol": symbol, "token": self._api_key},
                    timeout=10,
                )
                return resp
            except requests.RequestException:
                return None

        resp = _do_request()

        if resp is None:
            logger.warning("Finnhub network failure for %s, serving stale cache", symbol)
            return self._get_cached(symbol)

        if resp.status_code == 429:
            logger.warning("Finnhub 429 rate-limited for %s, backing off 1s", symbol)
            time.sleep(1)
            self._wait_for_rate_limit()
            resp = _do_request()
            if resp is None or resp.status_code == 429:
                return self._get_cached(symbol)

        if resp.status_code != 200:
            logger.warning("Finnhub HTTP %d for %s", resp.status_code, symbol)
            return self._get_cached(symbol)

        try:
            data = resp.json()
        except Exception:
            logger.warning("Finnhub JSON parse error for %s", symbol)
            return self._get_cached(symbol)

        # Cache the result
        self._cache[symbol] = (time.time(), data)
        return data

    def fetch_quotes_batch(self, symbols: list[str]) -> dict[str, dict | None]:
        """
        Fetch quotes for multiple symbols sequentially.

        Returns:
            Dict mapping symbol → quote dict (or None if unavailable).
        """
        results: dict[str, dict | None] = {}
        for symbol in symbols:
            results[symbol] = self.fetch_quote(symbol)
        return results

    def get_current_price(self, symbol: str) -> float | None:
        """Returns the current price (Finnhub field 'c') or None."""
        quote = self.fetch_quote(symbol)
        if quote is None:
            return None
        val = quote.get("c")
        if val is None or val == 0:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def get_price_change_24h(self, symbol: str) -> float | None:
        """Returns the change vs previous close (Finnhub field 'd') or None."""
        quote = self.fetch_quote(symbol)
        if quote is None:
            return None
        val = quote.get("d")
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
