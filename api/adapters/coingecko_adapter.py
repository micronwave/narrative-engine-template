"""
CoinGecko Demo API adapter for crypto prices.

Free Demo plan: ~30 calls/min, requires API key via x-cg-demo-api-key header.
Uses the same sliding-window deque rate limiter pattern as FinnhubService.
"""

import logging
import re
import threading
import time
from collections import deque
from datetime import datetime, timezone

import requests

from data_normalizer import NormalizedQuote

logger = logging.getLogger(__name__)

# Top 50 crypto symbols → CoinGecko IDs
_SYMBOL_TO_COINGECKO_ID = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "DOT": "polkadot",
    "AVAX": "avalanche-2",
    "POL": "polygon-ecosystem-token",
    "MATIC": "polygon-ecosystem-token",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "ATOM": "cosmos",
    "LTC": "litecoin",
    "FIL": "filecoin",
    "NEAR": "near",
    "APT": "aptos",
    "ARB": "arbitrum",
    "OP": "optimism",
    "SUI": "sui",
    "TON": "the-open-network",
    "TRX": "tron",
    "SHIB": "shiba-inu",
    "BCH": "bitcoin-cash",
    "PEPE": "pepe",
    "ICP": "internet-computer",
    "HBAR": "hedera-hashgraph",
    "RENDER": "render-token",
    "IMX": "immutable-x",
    "INJ": "injective-protocol",
    "FET": "fetch-ai",
    "STX": "blockstack",
    "MKR": "maker",
    "AAVE": "aave",
    "GRT": "the-graph",
    "ALGO": "algorand",
    "FTM": "fantom",
    "THETA": "theta-token",
    "XLM": "stellar",
    "VET": "vechain",
    "MANA": "decentraland",
    "SAND": "the-sandbox",
    "AXS": "axie-infinity",
    "EOS": "eos",
    "FLOW": "flow",
    "XTZ": "tezos",
    "CRO": "crypto-com-chain",
    "EGLD": "elrond-erd-2",
    "XMR": "monero",
}

# Regex to strip common suffixes from crypto symbols
_SUFFIX_RE = re.compile(r"[-/](USD[T]?|EUR|GBP|BTC)$", re.IGNORECASE)
_EXCHANGE_PREFIX_RE = re.compile(r"^[A-Z]+:", re.IGNORECASE)


class CoinGeckoAdapter:
    """Fetch crypto prices from CoinGecko Demo API, returning NormalizedQuote."""

    _BASE_URL = "https://api.coingecko.com/api/v3/simple/price"

    def __init__(self, api_key: str):
        self._api_key = api_key.strip()
        # Rate limiter: 30 calls per 60-second window
        self._call_times: deque = deque(maxlen=30)
        self._rate_lock = threading.Lock()

    def is_enabled(self) -> bool:
        return bool(self._api_key)

    def _wait_for_rate_limit(self) -> bool:
        """Returns True if request can proceed, False if rate-limited."""
        with self._rate_lock:
            now = time.time()
            while self._call_times and (now - self._call_times[0]) >= 60:
                self._call_times.popleft()

            if len(self._call_times) < 30:
                self._call_times.append(time.time())
                return True

            sleep_for = 60.0 - (now - self._call_times[0])
            if sleep_for > 2:
                logger.debug("CoinGecko rate-limited, skipping")
                return False

        # Sleep OUTSIDE the lock so other threads aren't blocked
        if sleep_for > 0:
            time.sleep(sleep_for)

        with self._rate_lock:
            now = time.time()
            while self._call_times and (now - self._call_times[0]) >= 60:
                self._call_times.popleft()
            if len(self._call_times) >= 30:
                return False  # Another thread filled the window while we slept
            self._call_times.append(time.time())
            return True

    def _resolve_coingecko_id(self, symbol: str) -> str | None:
        """Resolve various crypto symbol formats to a CoinGecko coin ID."""
        s = symbol.upper().strip()
        # Strip exchange prefix (e.g., "BINANCE:BTCUSDT" → "BTCUSDT")
        s = _EXCHANGE_PREFIX_RE.sub("", s)
        # Strip currency suffixes (e.g., "BTC-USD", "BTC/USD", "BTCUSD")
        s = _SUFFIX_RE.sub("", s)
        # Handle "BTCUSD" or "BTCUSDT" without separator
        if s.endswith("USDT"):
            s = s[:-4]
        elif s.endswith("USD"):
            s = s[:-3]
        return _SYMBOL_TO_COINGECKO_ID.get(s)

    def fetch_quote(self, symbol: str, instrument_type: str = "crypto") -> NormalizedQuote | None:
        if not self.is_enabled():
            return None

        coin_id = self._resolve_coingecko_id(symbol)
        if coin_id is None:
            return None

        if not self._wait_for_rate_limit():
            return None

        try:
            resp = requests.get(
                self._BASE_URL,
                params={
                    "ids": coin_id,
                    "vs_currencies": "usd",
                    "include_24hr_vol": "true",
                    "include_24hr_change": "true",
                },
                headers={"x-cg-demo-api-key": self._api_key},
                timeout=10,
            )
            if resp.status_code != 200:
                logger.warning("CoinGecko HTTP %d for %s", resp.status_code, symbol)
                return None

            data = resp.json()
            coin_data = data.get(coin_id)
            if coin_data is None:
                return None

            price = coin_data.get("usd")
            if price is None:
                return None

            return NormalizedQuote(
                symbol=symbol,
                instrument_type="crypto",
                price=float(price),
                open=None,
                high=None,
                low=None,
                close=None,
                volume=float(coin_data["usd_24h_vol"]) if coin_data.get("usd_24h_vol") is not None else None,
                timestamp=datetime.now(tz=timezone.utc),
                source="coingecko",
                delay="realtime",
            )
        except Exception as e:
            logger.warning("CoinGecko fetch failed for %s: %s", symbol, e)
            return None
