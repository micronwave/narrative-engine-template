"""
Security Audit S1-A test suite — Rate Limiting (C2) + Singleton ThreadPoolExecutor (C3).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 tests/test_sec_s1a.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import logging
import os
import re
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
# TestClient
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from concurrent.futures import ThreadPoolExecutor  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from api.main import (  # noqa: E402
    app,
    limiter,
    _BG_EXECUTOR,
    _REQUEST_EXECUTOR,
)

client = TestClient(app)

# ===========================================================================
# C2: Rate Limiting — Structural Checks
# ===========================================================================
S("C2: Rate limiter configuration")

T("slowapi importable", True, "import succeeded at module level")

T(
    "app.state.limiter exists",
    hasattr(app.state, "limiter"),
    f"app.state keys: {list(vars(app.state).keys()) if hasattr(app, 'state') else 'no state'}",
)

T(
    "limiter is a Limiter instance",
    type(app.state.limiter).__name__ == "Limiter",
    f"type: {type(app.state.limiter).__name__}",
)

# Check that rate-limited endpoints have the limiter decorators
# We verify by reading the source and checking for @limiter.limit patterns
main_py = Path(__file__).parent.parent / "api" / "main.py"
source = main_py.read_text(encoding="utf-8")

limit_count = len(re.findall(r"@limiter\.limit\(", source))
T(
    "rate limit decorators applied (>=26)",
    limit_count >= 26,
    f"found {limit_count} @limiter.limit decorators",
)

exempt_count = len(re.findall(r"@limiter\.exempt", source))
T(
    "health endpoint exempt",
    exempt_count >= 1,
    f"found {exempt_count} @limiter.exempt decorators",
)

# ===========================================================================
# C2: Rate Limiting — Behavioral Check (429 enforcement)
# ===========================================================================
S("C2: Rate limit enforcement")

# Enable rate limiting for this test
limiter.enabled = True

# Test that rate limiting returns 429 after exceeding limit.
# Use /api/auth/login (5/minute limit) — it's fast and doesn't hang.
# We send 7 rapid POST requests with a dummy body.
statuses = []
for i in range(7):
    r = client.post("/api/auth/login", json={"email": "test@x.com", "password": "bad"})
    statuses.append(r.status_code)

# First 5 should be 401 (bad creds) or 404 (stub mode), 6th+ should be 429
got_429 = 429 in statuses
T(
    "429 returned after exceeding auth rate limit (5/min)",
    got_429,
    f"statuses: {statuses}",
)

if got_429:
    # Find first 429 response
    r429 = client.post("/api/auth/login", json={"email": "test@x.com", "password": "bad"})
    T(
        "Retry-After header present in 429",
        "Retry-After" in r429.headers or "retry-after" in r429.headers,
        f"headers: {dict(r429.headers)}",
    )
    retry_val = r429.headers.get("Retry-After") or r429.headers.get("retry-after")
    T(
        "Retry-After value is numeric and reasonable",
        retry_val is not None and retry_val.isdigit() and 1 <= int(retry_val) <= 3600,
        f"got Retry-After: {retry_val}",
    )
else:
    T("Retry-After header present in 429", False, "no 429 was returned to check headers")
    T("Retry-After value is numeric and reasonable", False, "no 429 was returned")

# Reset limiter state for remaining tests
try:
    limiter.reset()
except AttributeError:
    # slowapi versions may not have reset(); clear internal storage instead
    if hasattr(limiter, "_limiter") and hasattr(limiter._limiter, "reset"):
        limiter._limiter.reset()

# Health endpoint should be exempt — never 429
health_statuses = []
for i in range(5):
    r = client.get("/api/health")
    health_statuses.append(r.status_code)

T(
    "health endpoint exempt from rate limiting",
    all(s == 200 for s in health_statuses),
    f"statuses: {health_statuses}",
)

# Reset limiter state
try:
    limiter.reset()
except AttributeError:
    pass

# Restore original enabled state
limiter.enabled = os.environ.get("RATE_LIMIT_ENABLED", "1") != "0"

# ===========================================================================
# C3: Singleton ThreadPoolExecutor — Structural Checks
# ===========================================================================
S("C3: Singleton executors")

T(
    "_BG_EXECUTOR is ThreadPoolExecutor",
    isinstance(_BG_EXECUTOR, ThreadPoolExecutor),
    f"type: {type(_BG_EXECUTOR).__name__}",
)

T(
    "_REQUEST_EXECUTOR is ThreadPoolExecutor",
    isinstance(_REQUEST_EXECUTOR, ThreadPoolExecutor),
    f"type: {type(_REQUEST_EXECUTOR).__name__}",
)

T(
    "_BG_EXECUTOR max_workers == 5",
    _BG_EXECUTOR._max_workers == 5,
    f"got {_BG_EXECUTOR._max_workers}",
)

T(
    "_REQUEST_EXECUTOR max_workers == 10",
    _REQUEST_EXECUTOR._max_workers == 10,
    f"got {_REQUEST_EXECUTOR._max_workers}",
)

# Verify no per-request ThreadPoolExecutor instantiation remains
with_tpe_count = len(re.findall(r"with\s+ThreadPoolExecutor\(", source))
T(
    "no per-request 'with ThreadPoolExecutor' blocks remain",
    with_tpe_count == 0,
    f"found {with_tpe_count} occurrences of 'with ThreadPoolExecutor('",
)

# Only the import + 2 shared declarations should reference ThreadPoolExecutor
tpe_refs = re.findall(r"ThreadPoolExecutor", source)
T(
    "ThreadPoolExecutor referenced only 3 times (import + 2 singletons)",
    len(tpe_refs) == 3,
    f"found {len(tpe_refs)} references",
)

# ===========================================================================
# C3: Shutdown hook
# ===========================================================================
S("C3: Shutdown hook")

shutdown_handlers = [h for h in app.router.on_shutdown]
T(
    "shutdown handler registered",
    len(shutdown_handlers) >= 1,
    f"found {len(shutdown_handlers)} shutdown handlers",
)

# Check the source has _shutdown_executors
has_shutdown_fn = "_shutdown_executors" in source
T(
    "_shutdown_executors function defined",
    has_shutdown_fn,
    "function not found in source",
)

# ===========================================================================
# C2: Data-tier rate limit enforcement (30/min)
# ===========================================================================
S("C2: Data-tier rate limit (30/min)")

limiter.enabled = True

# Reset limiter state
try:
    limiter.reset()
except AttributeError:
    if hasattr(limiter, "_limiter") and hasattr(limiter._limiter, "reset"):
        limiter._limiter.reset()

# Clear internal storage directly (most reliable for slowapi)
try:
    if hasattr(limiter, "_limiter") and hasattr(limiter._limiter, "_storage"):
        limiter._limiter._storage.storage.clear()
except Exception:
    pass

data_statuses = []
for i in range(35):
    r = client.get("/api/narratives")
    data_statuses.append(r.status_code)

count_429 = data_statuses.count(429)
count_ok = len([s for s in data_statuses if s in (200, 503)])  # 503 if no DB
T(
    "first 30 requests succeed (200 or 503)",
    count_ok >= 30,
    f"got {count_ok} non-429 responses before limit hit",
)
T(
    "requests 31+ return 429",
    count_429 >= 4,
    f"got {count_429} 429s out of 35 total; last 10 statuses: {data_statuses[-10:]}",
)

# Reset for next section
try:
    limiter.reset()
except AttributeError:
    pass
try:
    if hasattr(limiter, "_limiter") and hasattr(limiter._limiter, "_storage"):
        limiter._limiter._storage.storage.clear()
except Exception:
    pass

# ===========================================================================
# C2: Multi-IP rate tracking (structural verification)
# ===========================================================================
S("C2: Multi-IP rate tracking")

# TestClient uses a fixed client address ("testclient"), so we cannot
# behaviorally simulate different IPs. Instead, verify the structural
# guarantee: every route limit uses get_remote_address as its key_func,
# which extracts the client IP from the ASGI scope. In production behind
# a reverse proxy, this provides per-IP tracking.

from slowapi.util import get_remote_address as _gra  # noqa: E402

T(
    "limiter default key_func is get_remote_address",
    limiter._key_func is _gra,
    f"key_func: {limiter._key_func}",
)

# Verify all route limits use get_remote_address (not a custom/broken key_func)
all_use_gra = True
bad_routes = []
for route_name, limits in limiter._route_limits.items():
    for lim in limits:
        if lim.key_func is not _gra:
            all_use_gra = False
            bad_routes.append(route_name)

T(
    "all route limits use get_remote_address key_func",
    all_use_gra,
    f"non-conforming routes: {bad_routes}" if bad_routes else "all OK",
)

# Verify get_remote_address reads from ASGI scope client field
import inspect  # noqa: E402
gra_source = inspect.getsource(_gra)
T(
    "get_remote_address reads client IP from request",
    "client" in gra_source or "remote" in gra_source.lower(),
    f"function source checks client/remote address",
)

limiter.enabled = os.environ.get("RATE_LIMIT_ENABLED", "1") != "0"

# ===========================================================================
# C3: Concurrent request load test
# ===========================================================================
S("C3: Concurrent load test")

import concurrent.futures  # noqa: E402
import threading  # noqa: E402

thread_count_before = threading.active_count()

with concurrent.futures.ThreadPoolExecutor(max_workers=20) as test_executor:
    futures = [
        test_executor.submit(lambda: client.get("/api/correlations/top"))
        for _ in range(20)
    ]
    results = [f.result(timeout=60) for f in futures]

thread_count_after = threading.active_count()

T(
    "all 20 concurrent requests completed",
    len(results) == 20,
    f"got {len(results)} results",
)

T(
    "thread count stayed reasonable (<30 above baseline)",
    thread_count_after < thread_count_before + 30,
    f"before={thread_count_before}, after={thread_count_after}",
)

# ===========================================================================
# C3: Request timeout structure
# ===========================================================================
S("C3: Request timeout structure")

timeout_30_count = len(re.findall(r"\.result\(timeout=30\)", source))
T(
    "request endpoints use 30s timeout (>=3 occurrences)",
    timeout_30_count >= 3,
    f"found {timeout_30_count} occurrences of .result(timeout=30)",
)

# Verify the FuturesTimeout import exists (needed to catch timeout errors)
has_futures_timeout = "FuturesTimeout" in source or "TimeoutError as FuturesTimeout" in source
T(
    "FuturesTimeout imported for timeout handling",
    has_futures_timeout,
    "FuturesTimeout not found in source",
)

# ===========================================================================
# C3-audit: data_normalizer.py executor fix
# ===========================================================================
S("C3-audit: data_normalizer singleton executor")

dn_path = Path(__file__).parent.parent / "api" / "services" / "data_normalizer.py"
dn_source = dn_path.read_text(encoding="utf-8")

dn_with_tpe = len(re.findall(r"with\s+ThreadPoolExecutor\(", dn_source))
T(
    "no per-request 'with ThreadPoolExecutor' in data_normalizer.py",
    dn_with_tpe == 0,
    f"found {dn_with_tpe} occurrences",
)

has_batch_executor = "_BATCH_EXECUTOR" in dn_source
T(
    "_BATCH_EXECUTOR module-level singleton defined",
    has_batch_executor,
    "not found in data_normalizer.py source",
)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
_print_summary()
sys.exit(0 if _fail == 0 else 1)
