"""
Security Audit S2-A test suite — Auth on All Data Endpoints (C5).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 tests/test_sec_s2a.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import inspect
import logging
import os
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
    print(f"{'Section':<40} {'Pass':>5} {'Fail':>5}")
    print("-" * 60)
    for sec, counts in sections.items():
        marker = "" if counts["fail"] == 0 else " <--"
        print(f"  {sec:<38} {counts['pass']:>5} {counts['fail']:>5}{marker}")
    print("=" * 60)
    print(f"  TOTAL: {_pass} passed, {_fail} failed out of {_pass + _fail} tests")
    print("=" * 60)


# ---------------------------------------------------------------------------
# TestClient
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient  # noqa: E402
from api.main import (  # noqa: E402
    app,
    get_optional_user,
    get_current_user,
    STUB_AUTH_TOKEN,
    _AUTH_MODE,
)

client = TestClient(app)

STUB_HEADER = {"x-auth-token": STUB_AUTH_TOKEN}

# ---------------------------------------------------------------------------
# All 38 endpoints that should now require get_optional_user
# ---------------------------------------------------------------------------

# (method, path, needs_db) — needs_db=True means the endpoint calls get_repo()
# and will 503 if no DB; needs_db=False means it returns in-memory data.
PROTECTED_ENDPOINTS = [
    ("GET", "/api/narratives", True),
    ("GET", "/api/ticker", True),
    ("GET", "/api/narratives/test-id-000", True),  # will 404/503 not 403
    ("GET", "/api/constellation", True),
    ("GET", "/api/asset-classes", False),
    ("GET", "/api/securities", False),
    ("GET", "/api/narratives/test-id-000/assets", True),
    ("GET", "/api/securities/AAPL/quote", False),
    ("GET", "/api/signals", True),
    ("GET", "/api/stocks", False),
    ("GET", "/api/stocks/AAPL", False),
    ("GET", "/api/alerts/types", False),
    ("GET", "/api/manipulation", True),
    ("GET", "/api/narratives/test-id-000/manipulation", True),
    ("GET", "/api/brief/AAPL", False),
    ("GET", "/api/narratives/test-id-000/history", True),
    ("GET", "/api/ticker/AAPL/price-history", True),
    ("GET", "/api/correlations/test-id-000/AAPL", True),
    ("GET", "/api/narratives/test-id-000/coordination", True),
    ("GET", "/api/coordination/summary", True),
    ("GET", "/api/correlations/top", True),
    ("GET", "/api/narratives/test-id-000/correlations", True),
    ("GET", "/api/analytics/signal-ranking", True),
    ("GET", "/api/narratives/test-id-000/sources", True),
    ("GET", "/api/pipeline/buffer", True),
    ("GET", "/api/narratives/test-id-000/documents", True),
    ("GET", "/api/narratives/test-id-000/timeline", True),
    ("GET", "/api/narratives/test-id-000/changelog", True),
    ("GET", "/api/narratives/test-id-000/compare?date1=2026-01-01&date2=2026-01-02", True),
    ("GET", "/api/earnings/upcoming", True),
    ("GET", "/api/analytics/narrative-histories", True),
    ("GET", "/api/analytics/momentum-leaderboard", True),
    ("GET", "/api/analytics/narrative-overlap", True),
    ("GET", "/api/analytics/sector-convergence", True),
    ("GET", "/api/analytics/lifecycle-funnel", True),
    ("GET", "/api/analytics/lead-time-distribution", True),
    ("GET", "/api/analytics/contrarian-signals", True),
    ("POST", "/api/narratives/test-id-000/analyze", True),
]

PUBLIC_ENDPOINTS = [
    ("GET", "/api/health"),
    ("GET", "/api/websocket/status"),
]

# ===========================================================================
# Section 1: Stub mode — all 38 endpoints accept requests (no 403)
# ===========================================================================
S("C5: Stub mode — protected endpoints accessible")

for method, path, _needs_db in PROTECTED_ENDPOINTS:
    if method == "GET":
        r = client.get(path, headers=STUB_HEADER)
    else:
        r = client.post(path, headers=STUB_HEADER)

    short_path = path.split("?")[0]
    # In stub mode: should NOT get 403 (auth failure).
    # 503 (no DB) or 404 (no such narrative) or 200 are all acceptable.
    T(
        f"stub+token {method} {short_path} != 403",
        r.status_code != 403,
        f"got {r.status_code}",
    )

# Also verify stub mode works WITHOUT token (get_optional_user returns local user)
for method, path, _needs_db in PROTECTED_ENDPOINTS:
    if method == "GET":
        r = client.get(path)
    else:
        r = client.post(path)

    short_path = path.split("?")[0]
    T(
        f"stub no-token {method} {short_path} != 403",
        r.status_code != 403,
        f"got {r.status_code}",
    )

# ===========================================================================
# Section 2: Public endpoints remain public
# ===========================================================================
S("C5: Public endpoints stay public")

for method, path in PUBLIC_ENDPOINTS:
    r = client.get(path)
    T(
        f"public {path} returns 200",
        r.status_code == 200,
        f"got {r.status_code}",
    )

# ===========================================================================
# Section 3: Phantom x_auth_token removed from /api/narratives
# ===========================================================================
S("C5: Phantom x_auth_token cleanup")

# Find the get_narratives endpoint function
from api.main import get_narratives  # noqa: E402

sig = inspect.signature(get_narratives)
param_names = list(sig.parameters.keys())

T(
    "get_narratives has no x_auth_token param",
    "x_auth_token" not in param_names,
    f"params: {param_names}",
)

T(
    "get_narratives has user param",
    "user" in param_names,
    f"params: {param_names}",
)

# Verify the docstring no longer mentions "Monetization gating"
docstring = get_narratives.__doc__ or ""
T(
    "get_narratives docstring no 'Monetization gating' line",
    "Monetization gating" not in docstring,
    f"docstring: {docstring[:100]}...",
)

# ===========================================================================
# Section 4: All 38 endpoints have get_optional_user dependency
# ===========================================================================
S("C5: Endpoint signatures include user param")

# Build a mapping of route path -> endpoint function
_route_funcs = {}
for route in app.routes:
    if hasattr(route, "endpoint"):
        # route.path gives us the FastAPI path pattern
        _route_funcs[route.path] = route.endpoint

# Check each protected endpoint has 'user' in its signature
_ROUTE_PATTERNS = [
    "/api/narratives",
    "/api/ticker",
    "/api/narratives/{narrative_id}",
    "/api/constellation",
    "/api/asset-classes",
    "/api/securities",
    "/api/narratives/{narrative_id}/assets",
    "/api/securities/{symbol}/quote",
    "/api/signals",
    "/api/stocks",
    "/api/stocks/{symbol}",
    "/api/alerts/types",
    "/api/manipulation",
    "/api/narratives/{narrative_id}/manipulation",
    "/api/brief/{ticker}",
    "/api/narratives/{narrative_id}/history",
    "/api/ticker/{symbol}/price-history",
    "/api/correlations/{narrative_id}/{ticker}",
    "/api/narratives/{narrative_id}/coordination",
    "/api/coordination/summary",
    "/api/correlations/top",
    "/api/narratives/{narrative_id}/correlations",
    "/api/analytics/signal-ranking",
    "/api/narratives/{narrative_id}/sources",
    "/api/pipeline/buffer",
    "/api/narratives/{narrative_id}/documents",
    "/api/narratives/{narrative_id}/timeline",
    "/api/narratives/{narrative_id}/changelog",
    "/api/narratives/{narrative_id}/compare",
    "/api/earnings/upcoming",
    "/api/analytics/narrative-histories",
    "/api/analytics/momentum-leaderboard",
    "/api/analytics/narrative-overlap",
    "/api/analytics/sector-convergence",
    "/api/analytics/lifecycle-funnel",
    "/api/analytics/lead-time-distribution",
    "/api/analytics/contrarian-signals",
    "/api/narratives/{narrative_id}/analyze",
]

for route_path in _ROUTE_PATTERNS:
    func = _route_funcs.get(route_path)
    if func is None:
        T(f"route {route_path} found", False, "route not found in app.routes")
        continue
    sig = inspect.signature(func)
    has_user = "user" in sig.parameters
    T(
        f"route {route_path} has user param",
        has_user,
        f"params: {list(sig.parameters.keys())}",
    )

# ===========================================================================
# Section 5: JWT mode — unauthenticated requests get 403
# ===========================================================================
S("C5: JWT mode rejects unauthenticated")

# Temporarily switch to JWT mode
import api.main as _main_mod  # noqa: E402

_saved_auth_mode = _main_mod._AUTH_MODE
_main_mod._AUTH_MODE = "jwt"

try:
    for method, path, _needs_db in PROTECTED_ENDPOINTS:
        if method == "GET":
            r = client.get(path)
        else:
            r = client.post(path)

        short_path = path.split("?")[0]
        T(
            f"jwt no-token {method} {short_path} == 403",
            r.status_code == 403,
            f"got {r.status_code}",
        )
finally:
    # Restore stub mode
    _main_mod._AUTH_MODE = _saved_auth_mode

# ===========================================================================
# Section 6: Pre-existing auth endpoints still work
# ===========================================================================
S("C5: Pre-existing auth endpoints unchanged")

# These endpoints already had get_optional_user or get_current_user
_PREEXISTING_AUTH = [
    ("GET", "/api/activity"),
    ("GET", "/api/watchlist"),
    ("GET", "/api/alerts/rules"),
    ("GET", "/api/alerts"),
    ("GET", "/api/alerts/count"),
    ("GET", "/api/portfolio"),
    ("GET", "/api/portfolio/exposure"),
]

for method, path in _PREEXISTING_AUTH:
    r = client.get(path, headers=STUB_HEADER)
    T(
        f"pre-existing auth {path} != 403",
        r.status_code != 403,
        f"got {r.status_code}",
    )


# ===========================================================================
# Print summary
# ===========================================================================
_print_summary()
sys.exit(0 if _fail == 0 else 1)
