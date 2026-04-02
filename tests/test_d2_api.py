"""
D2 — Finnhub Integration Tests

Unit:
  D2-U1: FinnhubService.is_enabled() returns False when api_key is ""
  D2-U2: FinnhubService.is_enabled() returns True when api_key is non-empty
  D2-U3: fetch_quote() returns mocked quote dict when api_key is set
  D2-U4: fetch_quote() returns None when api_key is "" (no HTTP call made)
  D2-U5: fetch_quote() returns cached result within TTL without a new HTTP call
  D2-U6: fetch_quote() makes a new HTTP call after TTL expires
  D2-U7: fetch_quotes_batch() returns a dict with one entry per symbol
  D2-U8: GET /api/securities/{symbol}/quote returns {"available": false} when api_key is ""
  D2-U9: GET /api/securities returns securities list; current_price field exists

Integration:
  D2-I1: Rapid calls to fetch_quote() engage the rate limiter (mock: 65 calls, ≥1 rate-limited/delayed)
"""

import sys
import time
import unittest.mock as mock
from pathlib import Path

# Add project root + api/ directory to sys.path
_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
_API_DIR = str(Path(__file__).parent.parent / "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

from finnhub_service import FinnhubService  # noqa: E402

# TestClient import — must come AFTER sys.path manipulation
from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402

# ---------------------------------------------------------------------------
# Test runner helpers (same as C/D-phase pattern)
# ---------------------------------------------------------------------------
_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    status = "PASS" if condition else "FAIL"
    _results.append((name, condition))
    marker = "✓" if condition else "✗"
    msg = f"  [{marker}] {name}"
    if details and not condition:
        msg += f"\n      details: {details}"
    elif details and condition:
        msg += f"  ({details})"
    print(msg)


# ---------------------------------------------------------------------------
# Mock quote data
# ---------------------------------------------------------------------------
MOCK_QUOTE = {
    "c": 142.35,
    "d": 1.23,
    "dp": 0.87,
    "h": 143.00,
    "l": 140.50,
    "o": 141.00,
    "pc": 141.12,
    "t": 1710000000,
}


def _make_mock_response(status_code: int = 200, data: dict = None):
    m = mock.MagicMock()
    m.status_code = status_code
    m.json.return_value = data or MOCK_QUOTE
    return m


# ===========================================================================
# D2-U1: is_enabled() returns False when api_key is ""
# ===========================================================================
S("D2-U1: is_enabled() — empty key")
svc_disabled = FinnhubService(api_key="", cache_ttl=60)
T("is_enabled() returns False for empty string", not svc_disabled.is_enabled())
T("is_enabled() returns False for whitespace string", not FinnhubService(api_key="   ").is_enabled())

# ===========================================================================
# D2-U2: is_enabled() returns True when api_key is non-empty
# ===========================================================================
S("D2-U2: is_enabled() — non-empty key")
svc_enabled = FinnhubService(api_key="test-key-123", cache_ttl=60)
T("is_enabled() returns True for non-empty key", svc_enabled.is_enabled())

# ===========================================================================
# D2-U3: fetch_quote() returns mocked quote dict when api_key is set
# ===========================================================================
S("D2-U3: fetch_quote() — mocked HTTP success")
svc = FinnhubService(api_key="test-key", cache_ttl=60)
with mock.patch("requests.get", return_value=_make_mock_response(200, MOCK_QUOTE)) as mock_get:
    result = svc.fetch_quote("TSM")
    T("fetch_quote() returns a dict", isinstance(result, dict))
    T("result has 'c' (current price)", "c" in (result or {}))
    T("result has 'd' (change)", "d" in (result or {}))
    T("current price matches mock", (result or {}).get("c") == 142.35)
    T("HTTP GET was called once", mock_get.call_count == 1,
      f"call_count={mock_get.call_count}")

# ===========================================================================
# D2-U4: fetch_quote() returns None when api_key is ""
# ===========================================================================
S("D2-U4: fetch_quote() — disabled service")
with mock.patch("requests.get") as mock_get:
    result = svc_disabled.fetch_quote("TSM")
    T("fetch_quote() returns None when disabled", result is None)
    T("no HTTP call made when disabled", mock_get.call_count == 0,
      f"call_count={mock_get.call_count}")

# ===========================================================================
# D2-U5: fetch_quote() returns cached result within TTL — no new HTTP call
# ===========================================================================
S("D2-U5: fetch_quote() — cache hit within TTL")
svc_cache = FinnhubService(api_key="test-key", cache_ttl=60)
with mock.patch("requests.get", return_value=_make_mock_response(200, MOCK_QUOTE)):
    # First call — populates cache
    first = svc_cache.fetch_quote("NVDA")

with mock.patch("requests.get") as mock_get2:
    # Second call within TTL — should NOT hit network
    second = svc_cache.fetch_quote("NVDA")
    T("cached result returned", second == first,
      f"first={first}, second={second}")
    T("no HTTP call on cache hit", mock_get2.call_count == 0,
      f"call_count={mock_get2.call_count}")

# ===========================================================================
# D2-U6: fetch_quote() makes a new HTTP call after TTL expires
# ===========================================================================
S("D2-U6: fetch_quote() — cache expiry")
svc_ttl = FinnhubService(api_key="test-key", cache_ttl=1)
with mock.patch("requests.get", return_value=_make_mock_response(200, MOCK_QUOTE)):
    svc_ttl.fetch_quote("GLD")

# Expire the cache
time.sleep(1.1)

with mock.patch("requests.get", return_value=_make_mock_response(200, MOCK_QUOTE)) as mock_get3:
    svc_ttl.fetch_quote("GLD")
    T("new HTTP call made after TTL", mock_get3.call_count == 1,
      f"call_count={mock_get3.call_count}")

# ===========================================================================
# D2-U7: fetch_quotes_batch() returns dict with one entry per symbol
# ===========================================================================
S("D2-U7: fetch_quotes_batch()")
symbols = ["TSM", "NVDA", "INTC"]
svc_batch = FinnhubService(api_key="test-key", cache_ttl=60)
with mock.patch("requests.get", return_value=_make_mock_response(200, MOCK_QUOTE)):
    batch = svc_batch.fetch_quotes_batch(symbols)
    T("batch returns a dict", isinstance(batch, dict))
    T("dict has one entry per symbol", set(batch.keys()) == set(symbols),
      f"keys={set(batch.keys())}")
    T("each value is a dict or None", all(v is None or isinstance(v, dict) for v in batch.values()))
    T("NVDA entry has current price", (batch.get("NVDA") or {}).get("c") == 142.35)

# ===========================================================================
# D2-U8: GET /api/securities/{symbol}/quote — returns available:false when no key
# ===========================================================================
S("D2-U8: GET /api/securities/{symbol}/quote — no key")
with TestClient(app) as client:
    # Patch the module-level finnhub instance's is_enabled to return False
    with mock.patch.object(app.state if hasattr(app, "state") else object(), "__init__", create=True):
        pass
    # Directly test with a fresh service instance that has no key
    import api.main as main_module
    original_finnhub = main_module.finnhub
    main_module.finnhub = FinnhubService(api_key="", cache_ttl=60)
    try:
        resp = client.get("/api/securities/TSM/quote")
        T("status 200", resp.status_code == 200, f"status={resp.status_code}")
        data = resp.json()
        T("available is False", data.get("available") is False, f"data={data}")
        T("symbol field present", data.get("symbol") == "TSM", f"data={data}")
    finally:
        main_module.finnhub = original_finnhub

# ===========================================================================
# D2-U9: GET /api/securities returns list with current_price field
# ===========================================================================
S("D2-U9: GET /api/securities — current_price field exists")
with TestClient(app) as client:
    resp = client.get("/api/securities")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("returns a list", isinstance(data, list))
    T("list is non-empty", len(data) > 0)
    first = data[0] if data else {}
    T("each item has current_price field", "current_price" in first,
      f"fields={list(first.keys())}")
    T("each item has price_change_24h field", "price_change_24h" in first,
      f"fields={list(first.keys())}")
    # Without Finnhub key active, these will be null
    T("current_price is null or a number", first.get("current_price") is None or isinstance(first.get("current_price"), (int, float)))

# ===========================================================================
# D2-I1: Rapid calls engage rate limiter (65 calls → ≥1 rate-limited/delayed)
# ===========================================================================
S("D2-I1: Rate limiter engages on 65 rapid calls")

call_count = 0
sleep_count = 0
original_sleep = time.sleep

def counting_sleep(seconds):
    global sleep_count
    sleep_count += 1
    # Don't actually sleep in tests — just count
    pass

svc_rl = FinnhubService(api_key="test-key", cache_ttl=0)  # TTL=0 forces fresh calls

def fresh_response(*args, **kwargs):
    global call_count
    call_count += 1
    return _make_mock_response(200, MOCK_QUOTE)

with mock.patch("requests.get", side_effect=fresh_response):
    with mock.patch("time.sleep", side_effect=counting_sleep):
        # Make 65 calls — the rate limiter should kick in after 60
        # Use different symbols to avoid cache hits
        symbols_rl = [f"SYM{i}" for i in range(65)]
        for sym in symbols_rl:
            svc_rl.fetch_quote(sym)

T(
    "≥1 sleep() call from rate limiter on 65 rapid requests",
    sleep_count >= 1,
    f"sleep_count={sleep_count}, http_calls={call_count}",
)
T(
    "all 65 quotes were attempted",
    call_count == 65,
    f"http_calls={call_count}",
)

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"D2 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All D2 tests passed.")
