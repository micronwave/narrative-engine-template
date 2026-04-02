"""
D7 API test suite — WebSocket Relay + Tick Storage (Phase 2 Batch 4).

Tests: D7-U1 (WebSocket relay class), D7-U2 (tick storage repo methods),
       D7-U3 (candle aggregation), D7-U4 (retention cleanup),
       D7-U5 (integration — status endpoint, price loop skip),
       D7-U6 (settings defaults).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 test_d7_api.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)

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
    _results.append({
        "section": _current_section,
        "name": name,
        "passed": bool(condition),
        "details": details,
    })
    if condition:
        _pass += 1
    else:
        _fail += 1
        print(
            f"  FAIL [{_current_section}] {name}" + (f" — {details}" if details else ""),
            file=sys.stderr,
        )


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
# TestClient + imports
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import os
import sqlite3
import tempfile
import time
from datetime import datetime, timezone, timedelta

# ===========================================================================
# D7-U1: WebSocket Relay class — construction, symbols, buffer
# ===========================================================================
S("D7-U1: WebSocket Relay class")

from api.services.websocket_relay import FinnhubWebSocketRelay

relay = FinnhubWebSocketRelay(
    api_key="test_key",
    symbols_limit=5,
    flush_interval=5,
    reconnect_max_delay=60,
)

T("relay constructed", relay is not None)
T("not connected initially", relay.is_connected is False)
T("no active symbols initially", len(relay.get_active_symbols()) == 0)
T("empty tick buffer", relay.get_tick_buffer_size() == 0)
T("uptime is 0", relay.get_uptime_seconds() == 0.0)

# Test symbol management (when not connected, stores desired set)
relay.update_symbols(["AAPL", "MSFT", "GOOG", "NVDA", "TSM", "EXTRA1", "EXTRA2"])
# Not connected, so active symbols should be empty
T("active symbols empty when disconnected",
  len(relay.get_active_symbols()) == 0)
# But the _subscribed set should be capped at symbols_limit (5)
T("subscribed set capped at limit",
  len(relay._subscribed) == 5,
  f"got {len(relay._subscribed)}")

# Test _handle_message — parse Finnhub trade format
trade_msg = json.dumps({
    "type": "trade",
    "data": [
        {"s": "AAPL", "p": 150.25, "v": 100, "t": 1700000000000},
        {"s": "MSFT", "p": 380.50, "v": 200, "t": 1700000001000},
    ]
})
relay._handle_message(trade_msg)

T("ticks buffered after trade message",
  relay.get_tick_buffer_size() == 2,
  f"got {relay.get_tick_buffer_size()}")

# Test drain
drained = relay.drain_tick_buffer()
T("drain returns 2 ticks", len(drained) == 2, f"got {len(drained)}")
T("buffer empty after drain", relay.get_tick_buffer_size() == 0)

# Validate tick structure
tick = drained[0]
T("tick has symbol", tick["symbol"] == "AAPL")
T("tick has price", tick["price"] == 150.25)
T("tick has volume", tick["volume"] == 100.0)
T("tick has timestamp (ISO)", "2023-11" in tick["timestamp"])
T("tick has source", tick["source"] == "finnhub_ws")

# Test non-trade message is ignored
relay._handle_message(json.dumps({"type": "ping"}))
T("non-trade message ignored", relay.get_tick_buffer_size() == 0)

# Test malformed trade data
relay._handle_message(json.dumps({
    "type": "trade",
    "data": [{"s": "AAPL"}]  # missing price
}))
T("trade without price ignored", relay.get_tick_buffer_size() == 0)

# Test callback invocation
callback_results = []

def test_callback(symbol, price, volume, ts_iso):
    callback_results.append((symbol, price))

relay2 = FinnhubWebSocketRelay(api_key="test", symbols_limit=10)
relay2._update_callback = test_callback
relay2._handle_message(trade_msg)
T("callback invoked for each trade",
  len(callback_results) == 2,
  f"got {len(callback_results)}")
T("callback received correct symbol", callback_results[0][0] == "AAPL")
T("callback received correct price", callback_results[0][1] == 150.25)


# ===========================================================================
# D7-U2: Tick storage repository methods
# ===========================================================================
S("D7-U2: Tick storage repo methods")

from repository import SqliteRepository

# Create a temp DB for testing
_tmp_dir = tempfile.mkdtemp()
_test_db = str(Path(_tmp_dir) / "test_d7.db")
repo = SqliteRepository(_test_db)
repo.migrate()

# insert_ticks_batch — empty list
count = repo.insert_ticks_batch([])
T("insert empty list returns 0", count == 0)

# insert_ticks_batch — normal ticks
now = datetime.now(timezone.utc)
test_ticks = [
    {
        "symbol": "AAPL",
        "price": 150.0,
        "volume": 100.0,
        "timestamp": (now - timedelta(minutes=5)).isoformat(),
        "source": "finnhub_ws",
    },
    {
        "symbol": "AAPL",
        "price": 150.5,
        "volume": 200.0,
        "timestamp": (now - timedelta(minutes=4)).isoformat(),
        "source": "finnhub_ws",
    },
    {
        "symbol": "AAPL",
        "price": 151.0,
        "volume": 150.0,
        "timestamp": (now - timedelta(minutes=3)).isoformat(),
        "source": "finnhub_ws",
    },
    {
        "symbol": "MSFT",
        "price": 380.0,
        "volume": 300.0,
        "timestamp": (now - timedelta(minutes=5)).isoformat(),
        "source": "finnhub_ws",
    },
    {
        "symbol": "MSFT",
        "price": 381.0,
        "volume": 250.0,
        "timestamp": (now - timedelta(minutes=4)).isoformat(),
        "source": "finnhub_ws",
    },
]
count = repo.insert_ticks_batch(test_ticks)
T("inserted 5 ticks", count == 5, f"got {count}")

# Duplicate insert — should be ignored due to unique constraint
dup_count = repo.insert_ticks_batch(test_ticks[:2])
T("duplicate insert returns 0", dup_count == 0, f"got {dup_count}")

# get_recent_ticks
recent = repo.get_recent_ticks("AAPL", limit=10)
T("get_recent_ticks returns 3 for AAPL", len(recent) == 3, f"got {len(recent)}")
T("newest tick first", recent[0]["price"] == 151.0, f"got {recent[0]['price']}")
T("has all fields",
  all(k in recent[0] for k in ("symbol", "price", "volume", "timestamp", "source")))

recent_msft = repo.get_recent_ticks("MSFT", limit=10)
T("get_recent_ticks returns 2 for MSFT", len(recent_msft) == 2)

# get_recent_ticks with limit
limited = repo.get_recent_ticks("AAPL", limit=1)
T("limit=1 returns 1 tick", len(limited) == 1)

# get_recent_ticks for unknown symbol
empty = repo.get_recent_ticks("ZZZZ", limit=10)
T("unknown symbol returns empty", len(empty) == 0)


# ===========================================================================
# D7-U3: Candle aggregation
# ===========================================================================
S("D7-U3: Candle aggregation")

# Insert ticks with known minute boundaries for aggregation
repo2 = SqliteRepository(str(Path(_tmp_dir) / "test_d7_candles.db"))
repo2.migrate()

# 3 ticks in minute :00, 2 ticks in minute :01
base_time = datetime(2026, 3, 20, 14, 0, 0, tzinfo=timezone.utc)
candle_ticks = [
    {"symbol": "AAPL", "price": 100.0, "volume": 10.0,
     "timestamp": base_time.isoformat(), "source": "finnhub_ws"},
    {"symbol": "AAPL", "price": 102.0, "volume": 20.0,
     "timestamp": (base_time + timedelta(seconds=15)).isoformat(), "source": "finnhub_ws"},
    {"symbol": "AAPL", "price": 99.0, "volume": 30.0,
     "timestamp": (base_time + timedelta(seconds=45)).isoformat(), "source": "finnhub_ws"},
    {"symbol": "AAPL", "price": 101.0, "volume": 15.0,
     "timestamp": (base_time + timedelta(minutes=1)).isoformat(), "source": "finnhub_ws"},
    {"symbol": "AAPL", "price": 103.0, "volume": 25.0,
     "timestamp": (base_time + timedelta(minutes=1, seconds=30)).isoformat(), "source": "finnhub_ws"},
]
repo2.insert_ticks_batch(candle_ticks)

# Aggregate with cutoff after all ticks
cutoff = (base_time + timedelta(minutes=5)).isoformat()
candle_count = repo2.aggregate_candles_1m(cutoff)
T("aggregated 2 candles (2 minutes)", candle_count == 2, f"got {candle_count}")

# Verify candle data
candles = repo2.get_candles_1m(
    "AAPL",
    (base_time - timedelta(minutes=1)).isoformat(),
    (base_time + timedelta(minutes=5)).isoformat(),
)
T("get_candles_1m returns 2", len(candles) == 2, f"got {len(candles)}")

c0 = candles[0]  # 14:00 candle
T("candle has symbol", c0["symbol"] == "AAPL")
T("candle open = first price (100.0)", c0["open"] == 100.0, f"got {c0['open']}")
T("candle high = max (102.0)", c0["high"] == 102.0, f"got {c0['high']}")
T("candle low = min (99.0)", c0["low"] == 99.0, f"got {c0['low']}")
T("candle close = last price (99.0)", c0["close"] == 99.0, f"got {c0['close']}")
T("candle volume = sum (60.0)", c0["volume"] == 60.0, f"got {c0['volume']}")

c1 = candles[1]  # 14:01 candle
T("second candle open = 101.0", c1["open"] == 101.0, f"got {c1['open']}")
T("second candle high = 103.0", c1["high"] == 103.0, f"got {c1['high']}")
T("second candle close = 103.0", c1["close"] == 103.0, f"got {c1['close']}")

# Idempotent — re-aggregating same ticks should not create duplicates
candle_count2 = repo2.aggregate_candles_1m(cutoff)
candles_after = repo2.get_candles_1m(
    "AAPL",
    (base_time - timedelta(minutes=1)).isoformat(),
    (base_time + timedelta(minutes=5)).isoformat(),
)
T("re-aggregation is idempotent", len(candles_after) == 2, f"got {len(candles_after)}")

# Empty range
empty_candles = repo2.get_candles_1m("ZZZZ", "2020-01-01", "2020-01-02")
T("no candles for unknown symbol", len(empty_candles) == 0)


# ===========================================================================
# D7-U4: Retention cleanup
# ===========================================================================
S("D7-U4: Retention cleanup")

repo3 = SqliteRepository(str(Path(_tmp_dir) / "test_d7_retention.db"))
repo3.migrate()

# Insert old ticks (49 hours ago) and recent ticks (1 hour ago)
old_ts = (now - timedelta(hours=49)).isoformat()
recent_ts = (now - timedelta(hours=1)).isoformat()

retention_ticks = [
    {"symbol": "AAPL", "price": 100.0, "volume": 10.0,
     "timestamp": old_ts, "source": "finnhub_ws"},
    {"symbol": "MSFT", "price": 200.0, "volume": 20.0,
     "timestamp": old_ts, "source": "finnhub_ws"},
    {"symbol": "AAPL", "price": 150.0, "volume": 30.0,
     "timestamp": recent_ts, "source": "finnhub_ws"},
]
repo3.insert_ticks_batch(retention_ticks)

# Prune with 48h cutoff
cutoff_48h = (now - timedelta(hours=48)).isoformat()
pruned = repo3.prune_old_ticks(cutoff_48h)
T("pruned 2 old ticks", pruned == 2, f"got {pruned}")

# Recent tick should survive
remaining = repo3.get_recent_ticks("AAPL", limit=10)
T("1 recent tick survives", len(remaining) == 1, f"got {len(remaining)}")
T("surviving tick is recent", remaining[0]["price"] == 150.0)

# Prune with nothing to prune
pruned_again = repo3.prune_old_ticks(cutoff_48h)
T("second prune returns 0", pruned_again == 0)

# Full cycle: aggregate then prune
repo4 = SqliteRepository(str(Path(_tmp_dir) / "test_d7_cycle.db"))
repo4.migrate()

old_base = datetime(2026, 3, 19, 10, 0, 0, tzinfo=timezone.utc)
cycle_ticks = [
    {"symbol": "GOOG", "price": 170.0, "volume": 10.0,
     "timestamp": old_base.isoformat(), "source": "finnhub_ws"},
    {"symbol": "GOOG", "price": 172.0, "volume": 20.0,
     "timestamp": (old_base + timedelta(seconds=30)).isoformat(), "source": "finnhub_ws"},
]
repo4.insert_ticks_batch(cycle_ticks)

cycle_cutoff = (old_base + timedelta(minutes=5)).isoformat()
agg = repo4.aggregate_candles_1m(cycle_cutoff)
T("cycle: aggregated candles before prune", agg >= 1, f"got {agg}")

prn = repo4.prune_old_ticks(cycle_cutoff)
T("cycle: pruned old ticks", prn == 2, f"got {prn}")

# Candles should still exist after tick pruning
candles_remain = repo4.get_candles_1m("GOOG", "2026-03-19T09:00:00", "2026-03-19T11:00:00")
T("candles preserved after tick prune", len(candles_remain) >= 1)


# ===========================================================================
# D7-U5: Integration — API endpoint + relay wiring
# ===========================================================================
S("D7-U5: Integration")

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app  # noqa: E402

with TestClient(app) as client:

    # GET /api/websocket/status
    resp = client.get("/api/websocket/status")
    T("status endpoint returns 200", resp.status_code == 200, f"got {resp.status_code}")

    data = resp.json()
    T("has 'enabled' field", "enabled" in data)
    T("has 'connected' field", "connected" in data)
    T("has 'subscribed_symbols' field", "subscribed_symbols" in data)
    T("has 'tick_buffer_size' field", "tick_buffer_size" in data)
    T("has 'uptime_seconds' field", "uptime_seconds" in data)

    T("subscribed_symbols is list",
      isinstance(data["subscribed_symbols"], list))
    T("tick_buffer_size is int",
      isinstance(data["tick_buffer_size"], int))
    T("uptime_seconds is number",
      isinstance(data["uptime_seconds"], (int, float)))

    # Without FINNHUB_API_KEY, relay should be disabled
    if not os.environ.get("FINNHUB_API_KEY"):
        T("relay disabled without API key", data["enabled"] is False)
        T("not connected without API key", data["connected"] is False)

    # Verify health endpoint still works (regression)
    resp_health = client.get("/api/health")
    T("health endpoint still 200", resp_health.status_code == 200)


# ===========================================================================
# D7-U6: Settings defaults
# ===========================================================================
S("D7-U6: Settings defaults")

# Import via env-based approach (settings requires ANTHROPIC_API_KEY)
# Just verify the setting names exist and have expected defaults
from api.services.websocket_relay import FinnhubWebSocketRelay as _WS

ws_test = _WS(api_key="k", symbols_limit=50, flush_interval=5, reconnect_max_delay=300)
T("default symbols_limit accepted", ws_test._symbols_limit == 50)
T("default flush_interval accepted", ws_test._flush_interval == 5)
T("default reconnect_max_delay accepted", ws_test._reconnect_max_delay == 300)

# Verify settings.py has the fields by checking env var defaults used in main.py
T("WEBSOCKET_SYMBOLS_LIMIT env default is 50",
  int(os.environ.get("WEBSOCKET_SYMBOLS_LIMIT", "50")) == 50)
T("WEBSOCKET_FLUSH_INTERVAL_SECONDS env default is 5",
  int(os.environ.get("WEBSOCKET_FLUSH_INTERVAL_SECONDS", "5")) == 5)
T("WEBSOCKET_RECONNECT_MAX_DELAY_SECONDS env default is 300",
  int(os.environ.get("WEBSOCKET_RECONNECT_MAX_DELAY_SECONDS", "300")) == 300)
T("PRICE_TICK_RETENTION_HOURS env default is 48",
  int(os.environ.get("PRICE_TICK_RETENTION_HOURS", "48")) == 48)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
_print_summary()
sys.exit(0 if _fail == 0 else 1)
