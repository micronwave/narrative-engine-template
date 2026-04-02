"""
StockTwits free API adapter for social sentiment.

Endpoint: GET https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json
No API key required for basic access. Rate limit: 200 requests/hour.

Each message may carry pre-tagged sentiment: "Bullish" or "Bearish" (or null).
"""

import datetime
import json
import logging
import urllib.request
from collections import deque

logger = logging.getLogger(__name__)


class StockTwitsAdapter:
    """Fetch and cache social sentiment from StockTwits free stream."""

    _BASE_URL = "https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    _RATE_LIMIT = 200  # requests per hour
    _WINDOW_SECONDS = 3600

    def __init__(self):
        self._rate_limiter: deque = deque()
        self._cache: dict = {}           # symbol -> (data, unix_ts)
        self._cache_ttl = 300            # 5 minutes

    def _under_rate_limit(self) -> bool:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        cutoff = now - self._WINDOW_SECONDS
        while self._rate_limiter and self._rate_limiter[0] < cutoff:
            self._rate_limiter.popleft()
        return len(self._rate_limiter) < self._RATE_LIMIT

    def get_sentiment(self, symbol: str) -> dict | None:
        """
        Fetch recent StockTwits messages for a symbol.

        Returns:
            {
                "symbol": str,
                "total_messages": int,
                "bullish_count": int,
                "bearish_count": int,
                "neutral_count": int,
                "sentiment_score": float,   # -1 to 1 (bearish to bullish)
                "volume_24h": int,
                "trending": bool,
                "fetched_at": str,
            }
        or None on failure / rate-limit exceeded.
        """
        now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()

        # Cache hit
        cached = self._cache.get(symbol)
        if cached is not None:
            data, ts = cached
            if now_ts - ts < self._cache_ttl:
                return data

        # Rate limit guard
        if not self._under_rate_limit():
            logger.debug("StockTwits rate limit reached — skipping %s", symbol)
            return None

        try:
            url = self._BASE_URL.format(symbol=symbol)
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "NarrativeEngine/1.0"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                raw = json.loads(resp.read().decode())

            self._rate_limiter.append(now_ts)

            messages = raw.get("messages", [])
            bullish = sum(
                1 for m in messages
                if (m.get("entities") or {}).get("sentiment", {}) and
                   m["entities"]["sentiment"].get("basic") == "Bullish"
            )
            bearish = sum(
                1 for m in messages
                if (m.get("entities") or {}).get("sentiment", {}) and
                   m["entities"]["sentiment"].get("basic") == "Bearish"
            )
            neutral = len(messages) - bullish - bearish
            total = len(messages) or 1

            sentiment_score = round((bullish - bearish) / total, 4)

            # Trending signal: use watchlist_count from symbol metadata (primary),
            # supplemented by message volume (full API page = high current activity).
            symbol_meta = raw.get("symbol") or {}
            watchlist_count = symbol_meta.get("watchlist_count", 0)
            trending = watchlist_count > 10_000 or len(messages) >= 30

            result = {
                "symbol": symbol,
                "total_messages": len(messages),
                "bullish_count": bullish,
                "bearish_count": bearish,
                "neutral_count": neutral,
                "sentiment_score": sentiment_score,
                "volume_24h": len(messages),
                "trending": trending,
                "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }

            self._cache[symbol] = (result, now_ts)
            return result

        except Exception as exc:
            logger.debug("StockTwits fetch failed for %s: %s", symbol, exc)
            return None
