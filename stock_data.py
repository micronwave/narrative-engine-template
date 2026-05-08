"""
Stock Data Provider — yfinance-backed quotes, sparklines, and price history.
Cache TTLs: 15 min for quotes, 24 h for company info/sparklines.
"""

import json
import logging
import time
from datetime import datetime, timezone

import yfinance as yf

from repository import SqliteRepository

logger = logging.getLogger(__name__)

CACHE_TTL_QUOTES = 900   # 15 minutes
CACHE_TTL_INFO = 86400   # 24 hours

_price_history_cache: dict[str, tuple[float, list]] = {}
_PRICE_HISTORY_TTL = 3600  # 1 hour
_MAX_CACHE_SIZE = 500


def get_price_history(symbol: str, days: int = 30, interval: str = "1d") -> list:
    """Returns OHLCV price history from yfinance, cached for 1 hour."""
    _VALID_INTERVALS = {"1d", "5d", "1wk", "1mo"}
    yf_interval = interval if interval in _VALID_INTERVALS else "1d"
    cache_key = f"{symbol}:{days}:{yf_interval}"
    now = time.time()
    if cache_key in _price_history_cache:
        ts, data = _price_history_cache[cache_key]
        if now - ts < _PRICE_HISTORY_TTL:
            return data

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{days}d", interval=yf_interval)
        result = []
        prev_close: float | None = None
        for date, row in hist.iterrows():
            close = float(row["Close"])
            open_price = float(row.get("Open", close))
            high = float(row.get("High", close))
            low = float(row.get("Low", close))
            volume = int(row.get("Volume", 0))
            change_pct = ((close - prev_close) / prev_close * 100) if prev_close else 0.0
            result.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": round(open_price, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "close": round(close, 2),
                "volume": volume,
                "change_pct": round(change_pct, 2),
            })
            prev_close = close
        _price_history_cache[cache_key] = (now, result)
        if len(_price_history_cache) > _MAX_CACHE_SIZE:
            oldest_key = min(_price_history_cache, key=lambda k: _price_history_cache[k][0])
            del _price_history_cache[oldest_key]
        return result
    except Exception as exc:
        logger.debug("get_price_history failed for %s: %s", symbol, exc)
        return []


class StockDataProvider:
    def __init__(self, repository: SqliteRepository):
        self.repository = repository

    def get_quote(self, ticker: str, force_refresh: bool = False) -> dict | None:
        """Gets single stock quote. Uses cache if fresh (< 15 min old)."""
        ticker = ticker.upper()
        cached = self.repository.get_stock_cache(ticker)

        if cached and not force_refresh:
            try:
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                    cached["updated_at"].replace("Z", "+00:00")
                )).total_seconds()
                if age < CACHE_TTL_QUOTES:
                    # Deserialise sparklines stored as JSON strings
                    return self._deserialise_sparklines(cached)
            except Exception as exc:
                logger.debug("Cache timestamp parse failed for %s: %s", ticker, exc)

        try:
            stock = yf.Ticker(ticker)
            info = stock.info or {}

            # yfinance field names vary across versions; try both
            price = (
                info.get("currentPrice")
                or info.get("regularMarketPrice")
                or info.get("previousClose")
            )
            if price is None:
                logger.warning("StockDataProvider: no price data for %s", ticker)
                return self._deserialise_sparklines(cached) if cached else None

            _pct = info.get("regularMarketChangePercent")
            if _pct is None:
                _change = info.get("regularMarketChange")
                _pct = (_change / price * 100) if (_change is not None and price) else 0.0

            data = {
                "ticker": ticker,
                "name": info.get("shortName") or info.get("longName") or ticker,
                "price": float(price),
                "change_pct": float(_pct),
                "volume": int(info.get("regularMarketVolume") or info.get("volume") or 0),
                "market_cap": int(info.get("marketCap") or 0),
                "sector": info.get("sector") or "Unknown",
                "industry": info.get("industry") or "Unknown",
                "sparkline_7d": self._get_sparkline(ticker, 7, ticker_obj=stock),
                "sparkline_30d": self._get_sparkline(ticker, 30, ticker_obj=stock),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            self.repository.save_stock_cache(data)
            return data

        except Exception as exc:
            logger.warning("StockDataProvider: fetch failed for %s — %s", ticker, exc)
            return self._deserialise_sparklines(cached) if cached else None

    def get_quotes_batch(self, tickers: list[str], force_refresh: bool = False) -> dict[str, dict]:
        """Gets multiple quotes. Returns {ticker: data}. Skips failed tickers silently."""
        results = {}
        for ticker in tickers:
            ticker = ticker.upper()
            if not force_refresh:
                cached = self.repository.get_stock_cache(ticker)
                if cached:
                    try:
                        age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                            cached["updated_at"].replace("Z", "+00:00")
                        )).total_seconds()
                        if age < CACHE_TTL_QUOTES:
                            results[ticker] = self._deserialise_sparklines(cached)
                            continue
                    except Exception as exc:
                        logger.debug("Batch cache parse failed for %s: %s", ticker, exc)
            quote = self.get_quote(ticker, force_refresh=True)
            if quote:
                results[ticker] = quote
        return results

    def _get_sparkline(self, ticker: str, days: int, ticker_obj=None) -> list[float]:
        """Gets closing prices for last N trading days."""
        try:
            obj = ticker_obj or yf.Ticker(ticker)
            hist = obj.history(period=f"{days}d")
            if hist.empty:
                return []
            return [round(float(p), 2) for p in hist["Close"].tolist()]
        except Exception as exc:
            logger.debug("Sparkline fetch failed for %s (%dd): %s", ticker, days, exc)
            return []

    def get_price_history(self, ticker: str, days: int = 30) -> list[dict]:
        """Gets OHLCV data for last N days."""
        try:
            hist = yf.Ticker(ticker.upper()).history(period=f"{days}d")
            return [
                {
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": round(float(row["Open"]), 2),
                    "high": round(float(row["High"]), 2),
                    "low": round(float(row["Low"]), 2),
                    "close": round(float(row["Close"]), 2),
                    "volume": int(row["Volume"]),
                }
                for idx, row in hist.iterrows()
            ]
        except Exception as exc:
            logger.debug("Price history fetch failed for %s: %s", ticker, exc)
            return []

    @staticmethod
    def _deserialise_sparklines(data: dict) -> dict | None:
        """Returns copy of cached row with sparklines parsed from JSON strings."""
        if data is None:
            return None
        out = dict(data)
        for key in ("sparkline_7d", "sparkline_30d"):
            val = out.get(key)
            if isinstance(val, str):
                try:
                    out[key] = json.loads(val)
                except Exception:
                    out[key] = []
        return out
