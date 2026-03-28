"""
V3 Phase 3.3 — Earnings Calendar Service

Uses yfinance (already installed) for upcoming earnings dates.
Falls back gracefully when data is unavailable.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Simple in-memory cache {ticker: {"data": {...}, "fetched_at": float}}
_cache: dict[str, dict] = {}
_CACHE_TTL = 86400  # 24 hours


def get_upcoming_earnings(tickers: list[str]) -> list[dict]:
    """Returns upcoming earnings dates for given tickers using yfinance."""
    import time
    results = []

    for ticker in tickers:
        # Check cache
        cached = _cache.get(ticker)
        if cached and (time.time() - cached["fetched_at"]) < _CACHE_TTL:
            if cached["data"]:
                results.append(cached["data"])
            continue

        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            cal = t.calendar
            if cal is not None and not (hasattr(cal, 'empty') and cal.empty):
                # yfinance returns a DataFrame or dict depending on version
                if hasattr(cal, 'to_dict'):
                    cal_dict = cal.to_dict()
                    # Typically has 'Earnings Date' column
                    earnings_dates = cal_dict.get("Earnings Date", {})
                    if earnings_dates:
                        first_date = list(earnings_dates.values())[0] if isinstance(earnings_dates, dict) else None
                        if first_date:
                            if hasattr(first_date, 'isoformat'):
                                date_str = first_date.isoformat()[:10]
                            else:
                                date_str = str(first_date)[:10]
                            now = datetime.now(timezone.utc).date()
                            try:
                                ed = datetime.fromisoformat(date_str).date()
                                days_until = (ed - now).days
                            except Exception:
                                days_until = None

                            entry = {
                                "ticker": ticker,
                                "earnings_date": date_str,
                                "days_until": days_until,
                            }
                            results.append(entry)
                            _cache[ticker] = {"data": entry, "fetched_at": time.time()}
                            continue

                elif isinstance(cal, dict):
                    ed = cal.get("Earnings Date")
                    if ed:
                        date_str = str(ed[0])[:10] if isinstance(ed, list) else str(ed)[:10]
                        entry = {
                            "ticker": ticker,
                            "earnings_date": date_str,
                            "days_until": None,
                        }
                        results.append(entry)
                        _cache[ticker] = {"data": entry, "fetched_at": time.time()}
                        continue

            # No data found
            _cache[ticker] = {"data": None, "fetched_at": time.time()}

        except Exception as e:
            logger.debug("earnings_service: %s error: %s", ticker, e)
            _cache[ticker] = {"data": None, "fetched_at": time.time()}

    return results
