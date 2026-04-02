"""
Security Audit S3 test suite — Transport & Headers (M1 HSTS + HTTPS redirect).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 tests/test_sec_s3.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import logging
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

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
        print(f"  FAIL: [{_current_section}] {name}", file=sys.stderr)
        if details:
            print(f"        {details}", file=sys.stderr)


def _report():
    from collections import Counter
    sections = []
    seen = set()
    for r in _results:
        if r["section"] not in seen:
            seen.add(r["section"])
            sections.append(r["section"])
    print("=" * 60)
    print(f"{'Section':<46} {'Pass':>4}  {'Fail':>4}")
    print("-" * 60)
    for sec in sections:
        items = [r for r in _results if r["section"] == sec]
        p = sum(1 for r in items if r["passed"])
        f = sum(1 for r in items if not r["passed"])
        print(f"  {sec:<44} {p:>4} {f:>5}")
    print("=" * 60)
    print(f"  TOTAL: {_pass} passed, {_fail} failed out of {_pass + _fail} tests")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Test-isolated DB setup
# ---------------------------------------------------------------------------

_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp_db.close()
os.environ.setdefault("DB_PATH", _tmp_db.name)
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-real")

_project_root = str(Path(__file__).parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _get_client():
    """Import and return a fresh TestClient."""
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


# --- M1: ENVIRONMENT setting in settings.py ---
S("M1: settings.py — ENVIRONMENT field")

from settings import Settings

_fields = {name: field for name, field in Settings.model_fields.items()}
T("ENVIRONMENT field exists in Settings",
  "ENVIRONMENT" in _fields,
  f"fields: {list(_fields.keys())[:10]}...")

T("ENVIRONMENT defaults to 'development'",
  _fields.get("ENVIRONMENT") and _fields["ENVIRONMENT"].default == "development",
  f"default: {_fields.get('ENVIRONMENT', {})}")


# --- M1: Module-level _ENVIRONMENT variable ---
S("M1: api/main.py — _ENVIRONMENT variable")

import api.main as main_mod

T("_ENVIRONMENT variable exists",
  hasattr(main_mod, "_ENVIRONMENT"),
  "Expected module-level _ENVIRONMENT")

T("_ENVIRONMENT defaults to 'development' (test env)",
  getattr(main_mod, "_ENVIRONMENT", None) == "development",
  f"got: {getattr(main_mod, '_ENVIRONMENT', 'MISSING')}")


# --- M1: RedirectResponse import ---
S("M1: api/main.py — RedirectResponse import")

T("RedirectResponse is importable from starlette",
  hasattr(main_mod, "RedirectResponse"),
  "Expected module-level RedirectResponse import")


# --- M1: _security_headers middleware exists ---
S("M1: api/main.py — _security_headers middleware")

T("_security_headers function exists",
  hasattr(main_mod, "_security_headers"),
  "Expected _security_headers middleware function")


# --- M1: Development mode — no HSTS, no redirect ---
S("M1: Development mode — no HSTS header")

client = _get_client()
r = client.get("/api/health")
T("Health endpoint returns 200",
  r.status_code == 200)

T("No HSTS header in development",
  "strict-transport-security" not in r.headers,
  f"headers: {dict(r.headers)}")


# --- M1: Production mode — HSTS present ---
S("M1: Production mode — HSTS header")

# Temporarily patch _ENVIRONMENT to "production"
original_env = main_mod._ENVIRONMENT

main_mod._ENVIRONMENT = "production"
try:
    r = client.get("/api/health")
    T("HSTS header present in production",
      "strict-transport-security" in r.headers,
      f"headers keys: {list(r.headers.keys())}")

    hsts_val = r.headers.get("strict-transport-security", "")
    T("HSTS max-age=3600",
      "max-age=3600" in hsts_val,
      f"got: {hsts_val}")

    T("HSTS includeSubDomains",
      "includeSubDomains" in hsts_val,
      f"got: {hsts_val}")
finally:
    main_mod._ENVIRONMENT = original_env


# --- M1: Production mode — HTTPS redirect ---
S("M1: Production mode — HTTPS redirect")

main_mod._ENVIRONMENT = "production"
try:
    r = client.get(
        "/api/health",
        headers={"x-forwarded-proto": "http"},
        follow_redirects=False,
    )
    T("HTTP→HTTPS redirect returns 301",
      r.status_code == 301,
      f"got: {r.status_code}")

    location = r.headers.get("location", "")
    T("Redirect location uses https scheme",
      location.startswith("https://"),
      f"got: {location}")
finally:
    main_mod._ENVIRONMENT = original_env

# Redirect preserves path + query string
main_mod._ENVIRONMENT = "production"
try:
    r = client.get(
        "/api/narratives?page=2&sort=asc",
        headers={"x-forwarded-proto": "http"},
        follow_redirects=False,
    )
    location = r.headers.get("location", "")
    T("Redirect preserves path and query string",
      "/api/narratives?page=2&sort=asc" in location,
      f"got: {location}")
finally:
    main_mod._ENVIRONMENT = original_env


# --- M1: No redirect when x-forwarded-proto absent ---
S("M1: No redirect without x-forwarded-proto")

main_mod._ENVIRONMENT = "production"
try:
    r = client.get("/api/health")
    T("No redirect when x-forwarded-proto absent",
      r.status_code == 200,
      f"got: {r.status_code}")
finally:
    main_mod._ENVIRONMENT = original_env


# --- M1: No redirect when x-forwarded-proto is https ---
S("M1: No redirect when already HTTPS")

main_mod._ENVIRONMENT = "production"
try:
    r = client.get(
        "/api/health",
        headers={"x-forwarded-proto": "https"},
    )
    T("No redirect when x-forwarded-proto=https",
      r.status_code == 200,
      f"got: {r.status_code}")
finally:
    main_mod._ENVIRONMENT = original_env


# --- H4: CSP middleware in Next.js (file exists) ---
S("H4: Next.js CSP middleware file")

middleware_path = Path(_project_root) / "frontend" / "src" / "middleware.ts"
T("frontend/src/middleware.ts exists",
  middleware_path.exists())

if middleware_path.exists():
    content = middleware_path.read_text(encoding="utf-8")
    T("CSP header set in middleware",
      "Content-Security-Policy" in content,
      "Expected CSP header setting")
    T("Nonce generation present",
      "crypto.randomUUID" in content,
      "Expected nonce generation")
    T("strict-dynamic in CSP",
      "strict-dynamic" in content)
    T("unsafe-eval conditional for dev mode",
      "unsafe-eval" in content and "NODE_ENV" in content)
    T("HSTS conditional for production",
      "Strict-Transport-Security" in content and 'NODE_ENV' in content)


# --- H6: CORS allow_headers whitelist ---
S("H6: CORS allow_headers whitelist")

import inspect
source = inspect.getsource(main_mod)

T("allow_headers is not wildcard",
  'allow_headers=["*"]' not in source,
  "Expected explicit header list, not wildcard")

T("Content-Type in allow_headers",
  "Content-Type" in source)

T("x-auth-token in allow_headers",
  "x-auth-token" in source)


# --- Report ---
_report()
sys.exit(0 if _fail == 0 else 1)
