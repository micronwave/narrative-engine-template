"""
Sentiment API test suite — Part C (Social Sentiment System).

Tests:
 1. StockTwitsAdapter.get_sentiment() returns None on network failure (no exception)
 2. StockTwitsAdapter respects rate limit (200/hour)
 3. SentimentAggregator.compute_ticker_sentiment() returns valid composite score in [-1, 1]
 4. SentimentAggregator.compute_market_sentiment() returns market_score in [-1, 1]
 5. Spike detection: score > 2 std dev from mean returns spike_detected=True
 6. sentiment_timeseries table exists after migrate()
 7. social_mentions table exists after migrate()
 8. GET /api/sentiment/market returns 200 with valid structure
 9. GET /api/social/trending returns list sorted by volume (or empty list)
10. GET /api/signals/leaderboard returns enriched narratives sorted by signal_strength

Run with:
    python -X utf8 tests/test_sentiment_api.py
"""

import logging
import sqlite3
import sys
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s", stream=sys.stderr)

# ---------------------------------------------------------------------------
# Custom test runner
# ---------------------------------------------------------------------------

_results: list[dict] = []
_current_section: str = "Unset"
_pass = 0
_fail = 0


def S(section_name: str) -> None:
    global _current_section
    _current_section = section_name


def T(name: str, condition: bool, details: str = "") -> None:
    global _pass, _fail
    _results.append({"section": _current_section, "name": name, "passed": bool(condition), "details": details})
    if condition:
        _pass += 1
    else:
        _fail += 1
        print(f"  FAIL [{_current_section}] {name}" + (f" — {details}" if details else ""), file=sys.stderr)


def _print_summary() -> None:
    sections: dict[str, dict] = {}
    for r in _results:
        sec = r["section"]
        if sec not in sections:
            sections[sec] = {"pass": 0, "fail": 0}
        if r["passed"]:
            sections[sec]["pass"] += 1
        else:
            sections[sec]["fail"] += 1

    print("\n" + "=" * 60)
    print(f"{'Section':<35} {'Pass':>5} {'Fail':>5}")
    print("-" * 60)
    for sec, counts in sections.items():
        marker = "" if counts["fail"] == 0 else " <--"
        print(f"  {sec:<33} {counts['pass']:>5} {counts['fail']:>5}{marker}")
    print("=" * 60)
    print(f"  TOTAL: {_pass} passed, {_fail} failed out of {_pass + _fail} tests")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "api" / "adapters"))
sys.path.insert(0, str(Path(__file__).parent.parent / "api" / "services"))

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app, STUB_AUTH_TOKEN  # noqa: E402

client = TestClient(app)
AUTH_HEADER = {"x-auth-token": STUB_AUTH_TOKEN}

# ===========================================================================
# Section 1: StockTwitsAdapter — returns None on failure
# ===========================================================================
S("1: StockTwitsAdapter.get_sentiment() returns None on network failure")

from stocktwits_adapter import StockTwitsAdapter  # noqa: E402

adapter = StockTwitsAdapter()

with patch("urllib.request.urlopen", side_effect=OSError("network error")):
    result = adapter.get_sentiment("AAPL")
    T("returns None (not exception)", result is None, f"got {result!r}")

# ===========================================================================
# Section 2: StockTwitsAdapter rate limit
# ===========================================================================
S("2: StockTwitsAdapter respects rate limit (200/hour)")

adapter2 = StockTwitsAdapter()
# Fill rate limiter window to the limit
import time as _time
now_ts = _time.time()
for _ in range(200):
    adapter2._rate_limiter.append(now_ts)

# Next call should hit the rate limit guard
with patch("urllib.request.urlopen") as mock_urlopen:
    result2 = adapter2.get_sentiment("TSLA")
    T("returns None when rate limit reached", result2 is None, f"got {result2!r}")
    T("urlopen not called when rate-limited", not mock_urlopen.called)

# ===========================================================================
# Section 3: SentimentAggregator.compute_ticker_sentiment() valid score range
# ===========================================================================
S("3: SentimentAggregator.compute_ticker_sentiment() in [-1, 1]")

from sentiment_aggregator import SentimentAggregator  # noqa: E402

# Create a minimal mock repo
mock_repo = MagicMock()
mock_repo.get_all_narrative_signals.return_value = []
mock_repo.get_all_active_narratives.return_value = []
mock_repo.get_sentiment_timeseries.return_value = []

mock_adapter = MagicMock()
mock_adapter.get_sentiment.return_value = {
    "symbol": "AAPL",
    "total_messages": 100,
    "bullish_count": 60,
    "bearish_count": 25,
    "neutral_count": 15,
    "sentiment_score": 0.35,
    "volume_24h": 100,
    "trending": False,
    "fetched_at": "2026-04-01T00:00:00+00:00",
}

agg = SentimentAggregator(mock_repo, mock_adapter)
sentiment = agg.compute_ticker_sentiment("AAPL")

T("returns dict", isinstance(sentiment, dict), str(type(sentiment)))
T("has ticker field", sentiment.get("ticker") == "AAPL")
T("composite_score is float", isinstance(sentiment.get("composite_score"), float))
T("composite_score in [-1, 1]",
  -1.0 <= sentiment.get("composite_score", 999) <= 1.0,
  f"got {sentiment.get('composite_score')}")
T("has spike_detected", "spike_detected" in sentiment)
T("has computed_at", "computed_at" in sentiment)

# ===========================================================================
# Section 4: SentimentAggregator.compute_market_sentiment() valid market_score
# ===========================================================================
S("4: SentimentAggregator.compute_market_sentiment() in [-1, 1]")

market = agg.compute_market_sentiment(["AAPL", "NVDA", "TSLA"])

T("returns dict", isinstance(market, dict))
T("has market_score", "market_score" in market)
T("market_score in [-1, 1]",
  -1.0 <= market.get("market_score", 999) <= 1.0,
  f"got {market.get('market_score')}")
T("has bullish_pct", "bullish_pct" in market)
T("has bearish_pct", "bearish_pct" in market)
T("has top_bullish list", isinstance(market.get("top_bullish"), list))
T("has top_bearish list", isinstance(market.get("top_bearish"), list))

# ===========================================================================
# Section 5: Spike detection
# ===========================================================================
S("5: Spike detection — score > 2 std dev returns spike_detected=True")

import statistics

# Use near-zero history so any large composite triggers spike
past_scores = [0.0, 0.01, -0.01, 0.005, -0.005, 0.002, 0.003]

mock_repo_spike = MagicMock()
mock_repo_spike.get_all_narrative_signals.return_value = []
mock_repo_spike.get_all_active_narratives.return_value = []
mock_repo_spike.get_sentiment_timeseries.return_value = [
    {"composite_score": s} for s in past_scores
]

mock_adapter_spike = MagicMock()
mock_adapter_spike.get_sentiment.return_value = None

agg_spike = SentimentAggregator(mock_repo_spike, mock_adapter_spike)

mean_val = statistics.mean(past_scores)
stdev_val = statistics.stdev(past_scores)

# Patch both news and momentum to 1.0 -> composite = 0.4 + 0.3 = 0.7 >> 2*stdev
with patch.object(agg_spike, "_news_sentiment", return_value=1.0), \
     patch.object(agg_spike, "_narrative_momentum", return_value=1.0):
    r = agg_spike.compute_ticker_sentiment("AAPL")

T("spike_detected is True when > 2 std dev",
  r.get("spike_detected") is True,
  f"score={r.get('composite_score')} mean={mean_val:.4f} stdev={stdev_val:.4f} spike={r.get('spike_detected')}")

# ===========================================================================
# Section 6: sentiment_timeseries table exists after migrate()
# ===========================================================================
S("6: sentiment_timeseries table exists after migrate()")

from repository import SqliteRepository  # noqa: E402

with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
    tmp_db = tf.name

try:
    repo = SqliteRepository(tmp_db)
    repo.migrate()
    conn = sqlite3.connect(tmp_db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    T("sentiment_timeseries table created", "sentiment_timeseries" in tables, str(tables))
finally:
    try:
        os.unlink(tmp_db)
    except Exception:
        pass

# ===========================================================================
# Section 7: social_mentions table exists after migrate()
# ===========================================================================
S("7: social_mentions table exists after migrate()")

with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
    tmp_db2 = tf.name

try:
    repo2 = SqliteRepository(tmp_db2)
    repo2.migrate()
    conn2 = sqlite3.connect(tmp_db2)
    tables2 = {r[0] for r in conn2.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn2.close()
    T("social_mentions table created", "social_mentions" in tables2, str(tables2))
finally:
    try:
        os.unlink(tmp_db2)
    except Exception:
        pass

# ===========================================================================
# Section 8: GET /api/sentiment/market returns 200 with valid structure
# ===========================================================================
S("8: GET /api/sentiment/market returns 200")

resp = client.get("/api/sentiment/market", headers=AUTH_HEADER)
T("status 200", resp.status_code == 200, f"got {resp.status_code}")
body = resp.json()
T("has market_score", "market_score" in body, str(list(body.keys())[:5]))
T("has bullish_pct", "bullish_pct" in body)
T("has bearish_pct", "bearish_pct" in body)
T("has spikes list", isinstance(body.get("spikes"), list))

# ===========================================================================
# Section 9: GET /api/social/trending returns list sorted by volume (or empty)
# ===========================================================================
S("9: GET /api/social/trending returns sorted list")

resp = client.get("/api/social/trending", headers=AUTH_HEADER)
T("status 200", resp.status_code == 200, f"got {resp.status_code}")
body = resp.json()
T("has hours field", "hours" in body)
T("has tickers field", "tickers" in body)
tickers_list = body.get("tickers", [])
T("tickers is list", isinstance(tickers_list, list))
if len(tickers_list) >= 2:
    sorted_by_vol = sorted(tickers_list, key=lambda x: x.get("total_mentions", 0), reverse=True)
    T("sorted by mention volume DESC",
      [t["ticker"] for t in tickers_list] == [t["ticker"] for t in sorted_by_vol],
      "not sorted")
else:
    T("sorted by mention volume (skip — empty list)", True, "skipped")

# ===========================================================================
# Section 10: GET /api/signals/leaderboard returns enriched narratives
# ===========================================================================
S("10: GET /api/signals/leaderboard returns sorted entries")

resp = client.get("/api/signals/leaderboard", headers=AUTH_HEADER)
T("status 200", resp.status_code == 200, f"got {resp.status_code}")
body = resp.json()
T("returns list", isinstance(body, list))
if body:
    entry = body[0]
    T("entry has narrative_id", "narrative_id" in entry)
    T("entry has direction", "direction" in entry)
    T("entry has confidence", "confidence" in entry)
    T("entry has signal_strength", "signal_strength" in entry)
    strengths = [e.get("signal_strength", 0) for e in body]
    T("sorted by signal_strength DESC",
      all(strengths[i] >= strengths[i + 1] for i in range(len(strengths) - 1)),
      str(strengths[:5]))
else:
    T("leaderboard entry fields (skip — empty DB)", True, "skipped")
    T("direction field present (skip)", True, "skipped")
    T("confidence field present (skip)", True, "skipped")
    T("signal_strength field present (skip)", True, "skipped")
    T("sorted by signal_strength (skip)", True, "skipped")

# ===========================================================================
# Summary
# ===========================================================================
_print_summary()

sys.exit(1 if _fail > 0 else 0)
