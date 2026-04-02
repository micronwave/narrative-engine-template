"""
Phase 4: Catalyst Anchoring — Event calendar + macro data service.

Anchors narrative signals to known upcoming events (earnings, FOMC) and
macro conditions (FRED data) so the system knows when a narrative is
approaching a catalyst that could trigger price action.
"""

import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ── FOMC meeting dates (8 per year) ─────────────────────────────────────
# Source: Federal Reserve Board schedule. Update annually.

FOMC_DATES_2026 = [
    "2026-01-28",
    "2026-03-18",
    "2026-05-06",
    "2026-06-17",
    "2026-07-29",
    "2026-09-16",
    "2026-11-04",
    "2026-12-16",
]

FOMC_DATES_2027 = [
    "2027-01-27",
    "2027-03-17",
    "2027-05-05",
    "2027-06-16",
    "2027-07-28",
    "2027-09-22",
    "2027-11-03",
    "2027-12-15",
]


def get_fomc_dates() -> list[str]:
    """Return all known FOMC meeting dates as ISO date strings."""
    return FOMC_DATES_2026 + FOMC_DATES_2027


# ── FRED data ───────────────────────────────────────────────────────────

# In-memory cache: {series_id: {"data": [...], "fetched_at": float}}
_fred_cache: dict[str, dict] = {}


def get_fred_series(series_id: str, lookback_days: int = 90) -> list[dict]:
    """
    Fetch from FRED API (https://api.stlouisfed.org/fred/series/observations).

    Uses FRED_API_KEY if set, otherwise tries without (limited keyless access).
    Results cached in memory for FRED_CACHE_TTL_HOURS.
    Returns [{"date": str, "value": float}] ordered by date.
    On failure, returns empty list.
    """
    from settings import get_settings
    settings = get_settings()

    cache_ttl = settings.FRED_CACHE_TTL_HOURS * 3600
    now = time.time()

    cached = _fred_cache.get(series_id)
    if cached and (now - cached["fetched_at"]) < cache_ttl:
        return cached["data"]

    try:
        import urllib.request
        import json as _json
        from datetime import timedelta

        start_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        params = f"series_id={series_id}&observation_start={start_date}&file_type=json"
        if settings.FRED_API_KEY:
            params += f"&api_key={settings.FRED_API_KEY}"

        url = f"https://api.stlouisfed.org/fred/series/observations?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "narrative-engine/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = _json.loads(resp.read().decode())

        observations = body.get("observations", [])
        result = []
        for obs in observations:
            val_str = obs.get("value", ".")
            if val_str == ".":
                continue
            try:
                result.append({"date": obs["date"], "value": float(val_str)})
            except (ValueError, KeyError):
                continue

        _fred_cache[series_id] = {"data": result, "fetched_at": now}
        return result

    except Exception as exc:
        logger.debug("get_fred_series(%s) failed: %s", series_id, exc)
        return []


# ── Macro alignment ─────────────────────────────────────────────────────

def compute_macro_alignment(narrative_direction: str, fred_data: dict) -> float:
    """
    Rules-based heuristic alignment score in [-1.0, 1.0].

    fred_data keys: "vix", "yield_curve", "unemployment" — each a list of
    {"date", "value"} from FRED. Empty/missing = 0.0.

    Bearish narrative + rising VIX + inverting yield curve = positive alignment.
    Bullish narrative + falling unemployment + stable yield curve = positive alignment.
    Conflicting signals = near zero.
    """
    if not fred_data or narrative_direction not in ("bullish", "bearish"):
        return 0.0

    signals = []

    # VIX trend (VIXCLS)
    vix = fred_data.get("vix", [])
    if len(vix) >= 2:
        recent = vix[-1]["value"]
        older = vix[-min(len(vix), 10)]["value"]
        if older > 0:
            vix_change = (recent - older) / older
            if narrative_direction == "bearish":
                signals.append(min(max(vix_change * 2, -1.0), 1.0))
            else:
                signals.append(min(max(-vix_change * 2, -1.0), 1.0))

    # Yield curve (T10Y2Y) — negative = inverted
    yc = fred_data.get("yield_curve", [])
    if len(yc) >= 1:
        spread = yc[-1]["value"]
        if narrative_direction == "bearish":
            # Inverted (negative spread) supports bearish
            signals.append(min(max(-spread * 0.5, -1.0), 1.0))
        else:
            # Positive spread supports bullish
            signals.append(min(max(spread * 0.5, -1.0), 1.0))

    # Unemployment (UNRATE)
    unemp = fred_data.get("unemployment", [])
    if len(unemp) >= 2:
        recent = unemp[-1]["value"]
        older = unemp[-min(len(unemp), 5)]["value"]
        if older > 0:
            unemp_change = (recent - older) / older
            if narrative_direction == "bullish":
                # Falling unemployment supports bullish
                signals.append(min(max(-unemp_change * 3, -1.0), 1.0))
            else:
                signals.append(min(max(unemp_change * 3, -1.0), 1.0))

    if not signals:
        return 0.0

    return min(max(sum(signals) / len(signals), -1.0), 1.0)


# ── Catalyst proximity ──────────────────────────────────────────────────

def _fetch_macro_data() -> dict:
    """Fetch key FRED series for macro alignment. Returns dict of lists."""
    return {
        "vix": get_fred_series("VIXCLS", lookback_days=30),
        "yield_curve": get_fred_series("T10Y2Y", lookback_days=30),
        "unemployment": get_fred_series("UNRATE", lookback_days=90),
    }


def compute_catalyst_proximity(
    ticker: str,
    narrative_direction: str,
    narrative_sectors: list[str],
) -> dict:
    """
    Check earnings calendar + FOMC proximity + macro alignment.

    Returns dict with days_to_earnings, days_to_fomc, macro_alignment,
    has_near_catalyst, catalyst_type, proximity_score.
    """
    from settings import get_settings
    settings = get_settings()
    lookforward = settings.CATALYST_LOOKFORWARD_DAYS

    today = datetime.now(timezone.utc).date()

    # Earnings proximity
    days_to_earnings = None
    try:
        from api.earnings_service import get_upcoming_earnings
        earnings = get_upcoming_earnings([ticker])
        if earnings:
            for e in earnings:
                d = e.get("days_until")
                if d is not None and d >= 0:
                    days_to_earnings = d
                    break
    except Exception as exc:
        logger.debug("Earnings lookup failed for %s: %s", ticker, exc)

    # FOMC proximity
    days_to_fomc = None
    for date_str in get_fomc_dates():
        try:
            fomc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            delta = (fomc_date - today).days
            if delta >= 0:
                if days_to_fomc is None or delta < days_to_fomc:
                    days_to_fomc = delta
                break  # dates are sorted, first future date is the nearest
        except ValueError:
            continue

    # Macro alignment
    try:
        fred_data = _fetch_macro_data()
        macro_align = compute_macro_alignment(narrative_direction, fred_data)
    except Exception as exc:
        logger.debug("Macro alignment failed: %s", exc)
        macro_align = 0.0

    # Nearest catalyst
    candidates = []
    if days_to_earnings is not None and days_to_earnings <= lookforward:
        candidates.append(("earnings", days_to_earnings))
    if days_to_fomc is not None and days_to_fomc <= lookforward:
        candidates.append(("fomc", days_to_fomc))

    if candidates:
        candidates.sort(key=lambda x: x[1])
        catalyst_type = candidates[0][0]
        days_to_nearest = candidates[0][1]
        has_near_catalyst = days_to_nearest <= 7
    else:
        catalyst_type = "none"
        days_to_nearest = None
        has_near_catalyst = False

    # Proximity score: decays linearly over lookforward days, boosted by macro alignment
    if days_to_nearest is not None and days_to_nearest <= lookforward:
        proximity_score = max(0.0, 1.0 - days_to_nearest / lookforward) * (0.5 + 0.5 * abs(macro_align))
    else:
        proximity_score = 0.0

    return {
        "days_to_earnings": days_to_earnings,
        "days_to_fomc": days_to_fomc,
        "macro_alignment": round(macro_align, 4),
        "has_near_catalyst": has_near_catalyst,
        "catalyst_type": catalyst_type,
        "proximity_score": round(proximity_score, 4),
    }
