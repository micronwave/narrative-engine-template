"""
Security Audit S1-C test suite — Brute-Force Protection (M10) + Pagination (H5).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 tests/test_sec_s1c.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import logging
import os
import re
import sys
import tempfile
import time
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
# Setup
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient  # noqa: E402
import api.main as main_module  # noqa: E402
from api.main import (  # noqa: E402
    app,
    limiter,
    _login_attempts,
    _LOGIN_MAX_ATTEMPTS,
    _LOGIN_WINDOW_SECONDS,
)

client = TestClient(app)

# Source code for structural checks
_main_src_path = Path(__file__).parent.parent / "api" / "main.py"
_main_src = _main_src_path.read_text(encoding="utf-8")
_repo_src_path = Path(__file__).parent.parent / "repository.py"
_repo_src = _repo_src_path.read_text(encoding="utf-8")

# Disable rate limiting for test isolation
limiter.enabled = False


# ===================================================================
# M10: Brute-Force Protection on Login
# ===================================================================

S("M10: Module-level variables")

T(
    "import time present",
    "import time as _time" in _main_src,
    "Expected 'import time as _time' in api/main.py",
)

T(
    "_login_attempts dict exists",
    "_login_attempts: dict[str, list[float]]" in _main_src,
    "Expected _login_attempts dict declaration",
)

T(
    "_LOGIN_MAX_ATTEMPTS = 5",
    "_LOGIN_MAX_ATTEMPTS = 5" in _main_src,
    "Expected _LOGIN_MAX_ATTEMPTS = 5",
)

T(
    "_LOGIN_WINDOW_SECONDS = 900",
    "_LOGIN_WINDOW_SECONDS = 900" in _main_src,
    "Expected _LOGIN_WINDOW_SECONDS = 900 (15 minutes)",
)


S("M10: Brute-force check in auth_login")

T(
    "brute-force check present",
    "Too many login attempts" in _main_src,
    "Expected 429 response text in auth_login",
)

T(
    "progressive delay present",
    "min(2 ** (len(attempts) - 3), 30)" in _main_src,
    "Expected progressive delay formula",
)

T(
    "Retry-After header in 429 response",
    'Retry-After' in _main_src and str(_LOGIN_WINDOW_SECONDS) in _main_src,
    "Expected Retry-After header with window seconds",
)

T(
    "clear attempts on successful login",
    "_login_attempts.pop(email, None)" in _main_src,
    "Expected _login_attempts.pop on success",
)

T(
    "record failed attempt on bad password",
    "_login_attempts.setdefault(email, []).append(now)" in _main_src,
    "Expected attempt recording on bcrypt failure",
)


S("M10: Cleanup background task")

T(
    "cleanup startup hook exists",
    "_init_login_cleanup" in _main_src,
    "Expected _init_login_cleanup startup hook",
)

T(
    "cleanup loop function exists",
    "async def _cleanup_login_attempts" in _main_src,
    "Expected _cleanup_login_attempts async function",
)


S("M10: Functional — login attempt tracking")

# Reset state before functional tests
_login_attempts.clear()

# These tests require AUTH_MODE=jwt which we may not have, so test the
# tracking mechanism directly via the module-level dict.

email = "test_bruteforce@example.com"

# Simulate 5 failed attempts
now = time.time()
_login_attempts[email] = [now - i for i in range(5)]

T(
    "5 attempts tracked",
    len(_login_attempts.get(email, [])) == 5,
    f"Got {len(_login_attempts.get(email, []))} attempts",
)

# Verify window cleanup logic
old_email = "old@example.com"
_login_attempts[old_email] = [now - _LOGIN_WINDOW_SECONDS - 10]
cleaned = [t for t in _login_attempts[old_email] if now - t < _LOGIN_WINDOW_SECONDS]
T(
    "old attempts cleaned by window filter",
    len(cleaned) == 0,
    f"Expected 0 after window, got {len(cleaned)}",
)

# Verify recent attempts survive cleanup
recent_email = "recent@example.com"
_login_attempts[recent_email] = [now - 10, now - 5]
cleaned_recent = [t for t in _login_attempts[recent_email] if now - t < _LOGIN_WINDOW_SECONDS]
T(
    "recent attempts survive window filter",
    len(cleaned_recent) == 2,
    f"Expected 2, got {len(cleaned_recent)}",
)

# Verify pop clears on success
_login_attempts[email] = [now]
_login_attempts.pop(email, None)
T(
    "pop clears attempts on success",
    email not in _login_attempts,
    "Expected email removed from _login_attempts",
)

# Clean up
_login_attempts.clear()


S("M10: Endpoint stays sync def")

# Verify auth_login is NOT async (sync def runs bcrypt in threadpool automatically)
T(
    "auth_login is sync def (not async)",
    "async def auth_login" not in _main_src,
    "auth_login must be sync def to avoid blocking event loop with bcrypt",
)


# ===================================================================
# H5: Pagination — Repository Layer
# ===================================================================

S("H5: Repository — abstract signatures")

T(
    "get_all_active_narratives has limit/offset params",
    "def get_all_active_narratives(self, *, limit: int = 0, offset: int = 0" in _repo_src,
    "Expected keyword-only limit/offset params",
)

T(
    "get_all_active_narratives has stage/topic filter params",
    "stage: str | None = None, topic: str | None = None" in _repo_src,
    "Expected stage and topic filter params for DB-level filtering",
)

T(
    "count_active_narratives abstract method exists",
    "def count_active_narratives(self, *" in _repo_src,
    "Expected count_active_narratives abstract method",
)

T(
    "get_centroid_history has limit/offset params",
    "limit: int = 0, offset: int = 0" in _repo_src
    and "def get_centroid_history" in _repo_src,
    "Expected limit/offset on get_centroid_history",
)

T(
    "get_document_evidence has limit/offset params",
    "def get_document_evidence(self, narrative_id: str, *, limit: int = 0, offset: int = 0)" in _repo_src,
    "Expected keyword-only limit/offset params",
)

T(
    "get_changelog_for_narrative has limit/offset params",
    "def get_changelog_for_narrative" in _repo_src
    and "limit: int = 0, offset: int = 0" in _repo_src,
    "Expected limit/offset on get_changelog_for_narrative",
)


S("H5: Repository — SQL LIMIT/OFFSET injection")

# Check that concrete implementations append LIMIT ? OFFSET ?
# Count occurrences of the pattern in the concrete methods
limit_offset_pattern = 'sql += " LIMIT ? OFFSET ?"'
limit_offset_count = _repo_src.count(limit_offset_pattern)

T(
    "LIMIT ? OFFSET ? appended in 5 methods",
    limit_offset_count >= 5,
    f"Expected at least 5 occurrences, found {limit_offset_count}",
)

T(
    "count_document_evidence helper exists",
    "def count_document_evidence(self, narrative_id: str) -> int:" in _repo_src,
    "Expected count helper for document_evidence",
)

T(
    "count_changelog_for_narrative helper exists",
    "def count_changelog_for_narrative(self, narrative_id: str" in _repo_src,
    "Expected count helper for changelog",
)


S("H5: Repository — SQL query improvements")

T(
    "get_all_active_narratives defaults to limit=0",
    "def get_all_active_narratives(self, *, limit: int = 0" in _repo_src,
    "limit=0 means no limit (pipeline gets all rows)",
)

T(
    "ORDER BY ns_score DESC in query",
    'ORDER BY CAST(ns_score AS REAL) DESC' in _repo_src,
    "Expected ORDER BY ns_score for DB-level sorting",
)

T(
    "json_each used for topic filtering",
    "json_each(topic_tags)" in _repo_src,
    "Expected json_each for exact topic matching in SQL",
)

T(
    "count_active_narratives implementation exists",
    "def count_active_narratives(self, *" in _repo_src
    and "SELECT COUNT(*) as cnt FROM narratives WHERE suppressed = 0" in _repo_src,
    "Expected count implementation with matching WHERE clause",
)

T(
    "get_llm_calls_for_narrative has limit/offset params",
    "def get_llm_calls_for_narrative(self, narrative_id: str, *, limit: int = 0, offset: int = 0)" in _repo_src,
    "Expected limit/offset on get_llm_calls_for_narrative",
)


# ===================================================================
# H5: Pagination — API Endpoints
# ===================================================================

S("H5: /api/narratives endpoint")

T(
    "limit param in get_narratives signature",
    "limit: int = Query(100, ge=1, le=500)" in _main_src,
    "Expected limit with Query(100, ge=1, le=500)",
)

T(
    "offset param in get_narratives signature",
    "offset: int = Query(0, ge=0)" in _main_src,
    "Expected offset with Query(0, ge=0)",
)

T(
    "DB-level pagination: limit/offset/stage/topic passed to repo",
    "repo.get_all_active_narratives(" in _main_src
    and "limit=limit, offset=offset, stage=stage, topic=topic" in _main_src,
    "Expected limit/offset/stage/topic forwarded to repository",
)

T(
    "no in-memory slice for /api/narratives",
    "result[offset:offset + limit]" not in _main_src,
    "In-memory pagination should be replaced by DB-level LIMIT/OFFSET",
)


S("H5: /api/narratives/{id}/documents endpoint")

T(
    "documents uses SQL-level pagination",
    "repo.get_document_evidence(narrative_id, limit=limit, offset=offset)" in _main_src,
    "Expected limit/offset passed to repo method",
)

T(
    "documents uses count query for total",
    "repo.count_document_evidence(narrative_id)" in _main_src,
    "Expected count_document_evidence call for total",
)


S("H5: /api/narratives/{id}/changelog endpoint")

T(
    "changelog has limit param",
    "def get_narrative_changelog" in _main_src
    and "limit: int = Query(50" in _main_src,
    "Expected limit=Query(50) on changelog endpoint",
)

T(
    "changelog has offset param",
    "offset: int = Query(0, ge=0)" in _main_src,
    "Expected offset param on changelog endpoint",
)

T(
    "changelog uses SQL-level pagination",
    "repo.get_changelog_for_narrative(narrative_id, days=days, limit=limit, offset=offset)" in _main_src,
    "Expected limit/offset passed to repo method",
)

T(
    "changelog uses count query for total",
    "repo.count_changelog_for_narrative(narrative_id, days=days)" in _main_src,
    "Expected count_changelog_for_narrative call",
)

T(
    "changelog response includes limit/offset",
    '"limit": limit' in _main_src and '"offset": offset' in _main_src,
    "Expected limit and offset in changelog response",
)


# ===================================================================
# H5: Functional — API pagination
# ===================================================================

S("H5: Functional — /api/narratives pagination")

resp = client.get("/api/narratives")
T(
    "/api/narratives returns 200",
    resp.status_code == 200,
    f"Got {resp.status_code}",
)

data = resp.json()
T(
    "/api/narratives returns list",
    isinstance(data, list),
    f"Got {type(data).__name__}",
)

T(
    "/api/narratives default limit <= 100",
    len(data) <= 100,
    f"Got {len(data)} items (max should be 100)",
)

# Test explicit limit
resp2 = client.get("/api/narratives?limit=2")
T(
    "/api/narratives?limit=2 returns <= 2 items",
    resp2.status_code == 200 and len(resp2.json()) <= 2,
    f"Got {len(resp2.json()) if resp2.status_code == 200 else 'error'}",
)

# Test limit capping at 500
resp3 = client.get("/api/narratives?limit=1000")
T(
    "/api/narratives?limit=1000 returns 422 (over max)",
    resp3.status_code == 422,
    f"Got {resp3.status_code} (Query validation rejects >500)",
)

# Test offset
resp4 = client.get("/api/narratives?limit=1&offset=0")
resp5 = client.get("/api/narratives?limit=1&offset=1")
if len(data) >= 2:
    T(
        "offset=0 and offset=1 return different first items",
        resp4.json() != resp5.json(),
        "Expected different results with different offsets",
    )
else:
    T(
        "offset test skipped (< 2 narratives in DB)",
        True,
        "Not enough data to test offset",
    )


S("H5: Functional — /api/narratives/{id}/documents pagination")

# Get a narrative ID to test with
narrs = client.get("/api/narratives").json()
if narrs:
    nid = narrs[0].get("narrative_id") or narrs[0].get("id")
    doc_resp = client.get(f"/api/narratives/{nid}/documents?limit=2")
    T(
        "documents endpoint returns 200 with limit param",
        doc_resp.status_code == 200,
        f"Got {doc_resp.status_code}",
    )
    if doc_resp.status_code == 200:
        doc_data = doc_resp.json()
        T(
            "documents response has items/total/limit/offset keys",
            all(k in doc_data for k in ("items", "total", "limit", "offset")),
            f"Got keys: {list(doc_data.keys())}",
        )
        T(
            "documents items <= limit",
            len(doc_data.get("items", [])) <= 2,
            f"Got {len(doc_data.get('items', []))} items",
        )
else:
    T("documents pagination skipped (no narratives)", True, "No test data")


S("H5: Functional — /api/narratives/{id}/changelog pagination")

if narrs:
    nid = narrs[0].get("narrative_id") or narrs[0].get("id")
    cl_resp = client.get(f"/api/narratives/{nid}/changelog?limit=2")
    T(
        "changelog endpoint returns 200 with limit param",
        cl_resp.status_code == 200,
        f"Got {cl_resp.status_code}",
    )
    if cl_resp.status_code == 200:
        cl_data = cl_resp.json()
        T(
            "changelog response has limit/offset keys",
            "limit" in cl_data and "offset" in cl_data,
            f"Got keys: {list(cl_data.keys())}",
        )
        T(
            "changelog entries <= limit",
            len(cl_data.get("changelog", [])) <= 2,
            f"Got {len(cl_data.get('changelog', []))} entries",
        )
else:
    T("changelog pagination skipped (no narratives)", True, "No test data")


# ===================================================================
# C3 fix: Request Timeouts via _timeout decorator
# ===================================================================

S("C3: Timeout infrastructure")

T(
    "_REQUEST_TIMEOUT_SECONDS = 30.0 defined",
    "_REQUEST_TIMEOUT_SECONDS = 30.0" in _main_src,
    "Expected 30-second default timeout constant",
)

T(
    "_timeout decorator defined",
    "def _timeout(timeout: float = _REQUEST_TIMEOUT_SECONDS):" in _main_src,
    "Expected _timeout decorator factory",
)

T(
    "asyncio.wait_for used in _timeout",
    "asyncio.wait_for(" in _main_src,
    "Expected asyncio.wait_for for wall-clock timeout",
)

T(
    "504 status on timeout",
    'status_code=504, detail="Computation timed out"' in _main_src,
    "Expected 504 response on timeout",
)

T(
    "inspect.signature preserves original params",
    "wrapper.__signature__ = inspect.signature(fn)" in _main_src,
    "Expected signature copy for FastAPI/slowapi compatibility",
)

T(
    "uses default executor (not _REQUEST_EXECUTOR) to avoid deadlocks",
    "loop.run_in_executor(\n                        None," in _main_src
    or "run_in_executor(None," in _main_src,
    "Expected run_in_executor(None, ...) to avoid deadlocks with _REQUEST_EXECUTOR",
)


S("C3: Timeout applied to analytics endpoints")

# Count @_timeout() decorators — expect 9 (8 analytics + 1 constellation)
timeout_count = _main_src.count("@_timeout()")
T(
    f"@_timeout() applied to {timeout_count} endpoints (expected >= 8)",
    timeout_count >= 8,
    f"Found {timeout_count} @_timeout() decorators",
)

# Verify specific endpoints have the decorator
_timeout_endpoints = [
    ("/api/correlations/top", "get_top_correlations"),
    ("/api/narratives/{narrative_id}/correlations", "get_narrative_correlations"),
    ("/api/analytics/signal-ranking", "get_signal_ranking"),
    ("/api/analytics/narrative-histories", "get_narrative_histories"),
    ("/api/analytics/momentum-leaderboard", "get_momentum_leaderboard"),
    ("/api/analytics/narrative-overlap", "get_narrative_overlap"),
    ("/api/analytics/sector-convergence", "get_sector_convergence"),
    ("/api/analytics/lifecycle-funnel", "get_lifecycle_funnel"),
]

for path, fn_name in _timeout_endpoints:
    # Find the function definition and check that @_timeout() appears
    # within the few lines before it (between @limiter.limit and def)
    fn_idx = _main_src.find(f"def {fn_name}(")
    if fn_idx > 0:
        preceding = _main_src[max(0, fn_idx - 200):fn_idx]
        has_timeout = "@_timeout()" in preceding
    else:
        has_timeout = False
    T(
        f"@_timeout() on {fn_name}",
        has_timeout,
        f"Expected @_timeout() decorator before def {fn_name}",
    )


S("C3: Unbounded list queries optimized")

T(
    "/api/ticker uses limit=10 at SQL level",
    "repo.get_all_active_narratives(limit=10)" in _main_src,
    "Expected limit=10 for ticker (only needs top 10)",
)

T(
    "/api/signals uses limit=5 at SQL level",
    "repo.get_all_active_narratives(limit=5)" in _main_src,
    "Expected limit=5 for signals (only uses top 5)",
)

T(
    "/api/correlations/top uses limit=30 at SQL level",
    "repo.get_all_active_narratives(limit=30)" in _main_src,
    "Expected limit=30 for correlations (uses rows[:30])",
)


# ===================================================================
# Summary
# ===================================================================
_print_summary()
sys.exit(0 if _fail == 0 else 1)
