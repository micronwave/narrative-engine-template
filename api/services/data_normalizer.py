"""
Unified data normalization layer.

Transforms every provider's response into a standard NormalizedQuote format.
Downstream consumers (charting, alerts, portfolio, correlation) only see this format.
When you add or swap a data provider, you only touch the adapter, never the consumers.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

# Shared executor — prevents per-call thread pool creation (C3 hardening)
_BATCH_EXECUTOR = ThreadPoolExecutor(max_workers=5)
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class NormalizedQuote(BaseModel):
    """Standard quote format consumed by all downstream systems."""

    symbol: str
    instrument_type: str  # equity, crypto, forex, etf, option
    price: float
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    timestamp: datetime
    source: str  # finnhub, twelve_data, coingecko, yfinance
    delay: str  # realtime, delayed_15m, eod


# Adapter ordering by instrument type
_ADAPTER_ORDER = {
    "equity": ["finnhub", "twelve_data"],
    "crypto": ["coingecko", "finnhub", "twelve_data"],
    "forex": ["finnhub", "twelve_data"],
}


class DataNormalizer:
    """
    Manages the adapter chain for price data.

    Tries adapters in order (based on instrument_type); returns the first
    non-None NormalizedQuote.
    Adapter protocol: any object with fetch_quote(symbol, instrument_type) -> NormalizedQuote | None.
    """

    # Map class names to the keys used in _ADAPTER_ORDER
    _CLASS_NAME_TO_KEY = {
        "FinnhubAdapter": "finnhub",
        "TwelveDataAdapter": "twelve_data",
        "CoinGeckoAdapter": "coingecko",
    }

    def __init__(self, adapters: list, repository=None):
        from circuit_breaker import CircuitBreaker

        self._adapters = adapters
        self._adapters_by_name: dict = {}
        self._breakers: dict = {}
        self._repository = repository
        for adapter in adapters:
            cls_name = type(adapter).__name__
            name = self._CLASS_NAME_TO_KEY.get(
                cls_name, cls_name.lower().replace("adapter", "")
            )
            self._adapters_by_name[name] = adapter
            self._breakers[id(adapter)] = CircuitBreaker(cls_name)

    def _ordered_adapters(self, instrument_type: str) -> list:
        """Return adapters in the preferred order for the given instrument type."""
        order = _ADAPTER_ORDER.get(instrument_type, _ADAPTER_ORDER["equity"])
        ordered = []
        for name in order:
            adapter = self._adapters_by_name.get(name)
            if adapter:
                ordered.append(adapter)
        # Append any adapters not in the order list
        for adapter in self._adapters:
            if adapter not in ordered:
                ordered.append(adapter)
        return ordered

    def get_quote(
        self, symbol: str, instrument_type: str = "equity", source: str = "unknown"
    ) -> Optional[NormalizedQuote]:
        for adapter in self._ordered_adapters(instrument_type):
            breaker = self._breakers.get(id(adapter))
            if breaker and breaker.is_open:
                continue
            try:
                quote = adapter.fetch_quote(symbol, instrument_type=instrument_type)
                if quote is not None:
                    if breaker:
                        breaker.record_success()
                    if self._repository is not None:
                        try:
                            from datetime import date as _date
                            adapter_name = self._CLASS_NAME_TO_KEY.get(
                                type(adapter).__name__,
                                type(adapter).__name__.lower().replace("adapter", ""),
                            )
                            self._repository.increment_api_usage(
                                adapter_name, _date.today().isoformat(), 0
                            )
                        except Exception:
                            pass
                    return quote
                # None means "not found" — adapter is working, just doesn't cover
                # this symbol. Do NOT touch the breaker.
            except Exception as e:
                if breaker:
                    breaker.record_failure(source=source)
                logger.warning(
                    "Adapter %s failed for %s: %s",
                    type(adapter).__name__,
                    symbol,
                    type(e).__name__,
                )
        return None

    def get_quotes_batch(
        self, symbols: list[str], instrument_type: str = "equity"
    ) -> dict[str, Optional[NormalizedQuote]]:
        results: dict[str, Optional[NormalizedQuote]] = {}

        def _fetch_one(symbol: str) -> tuple[str, Optional[NormalizedQuote]]:
            return symbol, self.get_quote(symbol, instrument_type)

        futures = {_BATCH_EXECUTOR.submit(_fetch_one, sym): sym for sym in symbols}
        for future in futures:
            sym = futures[future]
            try:
                _, quote = future.result(timeout=15)
                results[sym] = quote
            except (FuturesTimeout, Exception) as e:
                logger.warning("Batch fetch timeout/error for %s: %s", sym, e)
                results[sym] = None

        return results
