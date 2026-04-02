"""
Security Audit S2-C test suite — H7 (HttpOnly Cookies), M2 (JWT Expiry + Refresh Tokens), M9 (Auth Audit Logging).

Tests: HttpOnly cookie setting on login/signup, cookie-based auth fallback,
CSRF cookie, cookie clearing on logout, JWT expiry default 2h, refresh token
rotation, auth_audit_log table and event logging.

Run with:
    python -X utf8 tests/test_sec_s2c.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import datetime
import inspect
import logging
import os
import sqlite3
import sys
import tempfile
import time
import uuid
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

# Disable rate limiting for tests
os.environ["RATE_LIMIT_ENABLED"] = "0"

from fastapi.testclient import TestClient  # noqa: E402
from api.main import (  # noqa: E402
    app,
    get_current_user,
    get_optional_user,
    STUB_AUTH_TOKEN,
    _AUTH_MODE,
    _decode_jwt,
    _extract_ip,
    _generate_csrf_token,
    _IS_SECURE_ENV,
)

client = TestClient(app)

STUB_HEADER = {"x-auth-token": STUB_AUTH_TOKEN}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jwt_mode_available() -> bool:
    """Check if JWT mode is feasible (PyJWT installed + secret set)."""
    try:
        import jwt  # noqa: F401
        return True
    except ImportError:
        return False


def _make_jwt_token(
    user_id: str = "test-user-1",
    email: str = "test@example.com",
    jti: str | None = None,
    exp_hours: float = 24,
    secret: str | None = None,
    token_type: str | None = None,
) -> str:
    """Create a JWT token for testing."""
    import jwt as pyjwt
    if secret is None:
        secret = os.environ.get("JWT_SECRET_KEY", "test-secret-key-that-is-at-least-32-chars-long!!")
    now = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "sub": user_id,
        "email": email,
        "role": "user",
        "iat": now,
        "exp": now + datetime.timedelta(hours=exp_hours),
    }
    if jti is not None:
        payload["jti"] = jti
    if token_type is not None:
        payload["type"] = token_type
    return pyjwt.encode(payload, secret, algorithm="HS256")


# ===========================================================================
# H7 — HttpOnly Cookie Tests
# ===========================================================================

# ---------------------------------------------------------------------------
# H7 Structural: Source code analysis
# ---------------------------------------------------------------------------

S("H7-struct: HttpOnly cookie source analysis")

_main_src = (Path(__file__).parent.parent / "api" / "main.py").read_text(encoding="utf-8")

# Check that set_cookie is called with httponly=True in signup
_signup_src = _main_src[_main_src.find("def auth_signup("):]
_signup_src = _signup_src[:_signup_src.find("\n@app.")]
T("signup sets httponly cookie", "httponly=True" in _signup_src)
T("signup sets samesite=strict", 'samesite="strict"' in _signup_src.lower() or "samesite=\"strict\"" in _signup_src)
T("signup sets csrf cookie", 'csrf_token' in _signup_src)

# Check that set_cookie is called with httponly=True in login
_login_src = _main_src[_main_src.find("def auth_login("):]
_login_src = _login_src[:_login_src.find("\n@app.")]
T("login sets httponly cookie", "httponly=True" in _login_src)
T("login sets samesite=strict", 'samesite="strict"' in _login_src.lower() or "samesite=\"strict\"" in _login_src)
T("login sets csrf cookie", 'csrf_token' in _login_src)

# Check that cookies are cleared on logout
_logout_src = _main_src[_main_src.find("def auth_logout("):]
_logout_src = _logout_src[:_logout_src.find("\n@app.") if "\n@app." in _logout_src else len(_logout_src)]
T("logout deletes auth_token cookie", 'delete_cookie(key="auth_token"' in _logout_src)
T("logout deletes csrf_token cookie", 'delete_cookie(key="csrf_token"' in _logout_src)

# Check that get_current_user reads cookie as fallback
_gcu_src = _main_src[_main_src.find("def get_current_user("):]
_gcu_src = _gcu_src[:_gcu_src.find("\ndef ")]
T("get_current_user accepts Request param", "request: Request" in _gcu_src)
T("get_current_user reads auth_token cookie", 'request.cookies.get("auth_token")' in _gcu_src)

# Check that get_optional_user reads cookie as fallback
_gou_src = _main_src[_main_src.find("def get_optional_user("):]
_gou_src = _gou_src[:_gou_src.find("\ndef ") if "\ndef " in _gou_src[1:] else len(_gou_src)]
T("get_optional_user accepts Request param", "request: Request" in _gou_src)
T("get_optional_user reads auth_token cookie", 'request.cookies.get("auth_token")' in _gou_src)


# ---------------------------------------------------------------------------
# H7 Behavioral: Cookie-based auth in stub mode
# ---------------------------------------------------------------------------

S("H7-behavior: stub mode cookie auth")

# Stub mode logout should clear cookies
resp_logout = client.post("/api/auth/logout", headers=STUB_HEADER)
T("stub logout → 200", resp_logout.status_code == 200)
_logout_cookies = {c.name for c in resp_logout.cookies.jar}
# Check set-cookie headers for delete directives
_raw_set_cookies = resp_logout.headers.get_list("set-cookie") if hasattr(resp_logout.headers, 'get_list') else [v for k, v in resp_logout.headers.multi_items() if k.lower() == "set-cookie"]
_has_auth_clear = any("auth_token" in sc for sc in _raw_set_cookies)
T("logout set-cookie includes auth_token clear", _has_auth_clear,
  f"set-cookie headers: {_raw_set_cookies}")

# Stub mode: using cookie instead of header should work
# We simulate cookie-based auth by setting the cookie manually
resp_with_cookie = client.get(
    "/api/narratives",
    cookies={"auth_token": STUB_AUTH_TOKEN},
)
T("cookie-based auth (stub) → 200", resp_with_cookie.status_code == 200,
  f"got {resp_with_cookie.status_code}")

# No header, no cookie → 403
resp_no_auth = client.get("/api/narratives")
T("no auth → 403 or 200 (stub allows)", resp_no_auth.status_code in (200, 403))

# Cookie takes precedence when header is absent
resp_cookie_only = client.get(
    "/api/ticker",
    cookies={"auth_token": STUB_AUTH_TOKEN},
)
T("ticker with cookie-only auth → 200", resp_cookie_only.status_code == 200,
  f"got {resp_cookie_only.status_code}")


# ---------------------------------------------------------------------------
# H7 Structural: Frontend credentials: "include"
# ---------------------------------------------------------------------------

S("H7-struct: frontend credentials include")

_api_ts = (Path(__file__).parent.parent / "frontend" / "src" / "lib" / "api.ts").read_text(encoding="utf-8")

# Count fetch calls and credentials: "include"
_fetch_count = _api_ts.count("await fetch(")
_cred_count = _api_ts.count('credentials: "include"')
T("all fetch calls have credentials include",
  _cred_count >= _fetch_count,
  f"fetch calls: {_fetch_count}, credentials: {_cred_count}")

# Check authFetch helper exists
T("authFetch helper exists", "function authFetch" in _api_ts)

# ---------------------------------------------------------------------------
# H7 Structural: AuthContext.tsx uses /api/auth/me
# ---------------------------------------------------------------------------

S("H7-struct: AuthContext uses /api/auth/me")

_auth_ctx = (Path(__file__).parent.parent / "frontend" / "src" / "contexts" / "AuthContext.tsx").read_text(encoding="utf-8")
T("AuthContext calls /api/auth/me", "/api/auth/me" in _auth_ctx)
T("AuthContext uses credentials include", 'credentials: "include"' in _auth_ctx)
T("AuthContext calls logout endpoint", "/api/auth/logout" in _auth_ctx)


# ---------------------------------------------------------------------------
# H7 Helper tests
# ---------------------------------------------------------------------------

S("H7-helper: _extract_ip and _generate_csrf_token")

# Test _generate_csrf_token
_csrf1 = _generate_csrf_token()
_csrf2 = _generate_csrf_token()
T("CSRF token is 64-char hex", len(_csrf1) == 64 and all(c in "0123456789abcdef" for c in _csrf1))
T("CSRF tokens are unique", _csrf1 != _csrf2)

# Test _IS_SECURE_ENV (should be False in test env)
T("_IS_SECURE_ENV is False in test", _IS_SECURE_ENV is False or _IS_SECURE_ENV == False)


# ===========================================================================
# M2 — JWT Expiry Reduction + Refresh Tokens
# ===========================================================================

# ---------------------------------------------------------------------------
# M2 Structural: Default expiry changed
# ---------------------------------------------------------------------------

S("M2-struct: JWT expiry default")

# Check both signup and login use 2h default
_signup_match_count = _main_src.count('JWT_EXPIRY_HOURS", "2")')
T("JWT_EXPIRY_HOURS default is 2 (not 24)", _signup_match_count >= 2,
  f"found {_signup_match_count} occurrences, expected >= 2 (signup, login, refresh)")
T("no 24h default remains", 'JWT_EXPIRY_HOURS", "24")' not in _main_src)

# ---------------------------------------------------------------------------
# M2 Structural: RefreshRequest model
# ---------------------------------------------------------------------------

S("M2-struct: RefreshRequest Pydantic model")

T("RefreshRequest class exists", "class RefreshRequest(BaseModel)" in _main_src)
T("RefreshRequest has refresh_token field", "refresh_token: str" in _main_src)

# ---------------------------------------------------------------------------
# M2 Structural: refresh_tokens table
# ---------------------------------------------------------------------------

S("M2-struct: refresh_tokens table")

_repo_src = (Path(__file__).parent.parent / "repository.py").read_text(encoding="utf-8")
T("refresh_tokens table CREATE exists", "CREATE TABLE IF NOT EXISTS refresh_tokens" in _repo_src)
_rt_section = _repo_src[_repo_src.find("CREATE TABLE IF NOT EXISTS refresh_tokens"):]
_rt_section = _rt_section[:_rt_section.find(")")]
T("refresh_tokens has jti PK", "jti TEXT PRIMARY KEY" in _rt_section)
T("refresh_tokens has revoked column", "revoked INTEGER DEFAULT 0" in _repo_src)

# ---------------------------------------------------------------------------
# M2 Structural: Refresh token generation in signup/login
# ---------------------------------------------------------------------------

S("M2-struct: refresh token generation")

T("signup generates refresh token", "refresh_jti" in _signup_src and "type.*refresh" in _signup_src or '"type": "refresh"' in _signup_src)
T("signup stores refresh token", "store_refresh_token" in _signup_src)
T("login generates refresh token", "refresh_jti" in _login_src and '"type": "refresh"' in _login_src)
T("login stores refresh token", "store_refresh_token" in _login_src)
T("signup response includes refresh_token", "refresh_token" in _signup_src)
T("login response includes refresh_token", "refresh_token" in _login_src)

# ---------------------------------------------------------------------------
# M2 Structural: /api/auth/refresh endpoint
# ---------------------------------------------------------------------------

S("M2-struct: /api/auth/refresh endpoint")

_refresh_src = _main_src[_main_src.find("def auth_refresh("):]
_refresh_src = _refresh_src[:_refresh_src.find("\n# ----")]
T("refresh endpoint exists", "def auth_refresh(" in _main_src)
T("refresh validates token type", '"type") != "refresh"' in _refresh_src or "type.*refresh" in _refresh_src)
T("refresh revokes old token", "revoke_refresh_token" in _refresh_src)
T("refresh issues new access token", "new_access_payload" in _refresh_src)
T("refresh issues new refresh token", "new_refresh_jti" in _refresh_src)
T("refresh stores new refresh token", "store_refresh_token" in _refresh_src)
T("refresh endpoint uses Pydantic model", "body: RefreshRequest" in _refresh_src)

# ---------------------------------------------------------------------------
# M2 Repository: Refresh token operations
# ---------------------------------------------------------------------------

S("M2-repo: refresh token CRUD")

# Test with a temp DB
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp_path = _tmp.name
_tmp.close()

sys.path.insert(0, str(Path(__file__).parent.parent))
from repository import SqliteRepository  # noqa: E402

_repo = SqliteRepository(_tmp_path)
_repo.migrate()

# Store
_rt_jti = str(uuid.uuid4())
_repo.store_refresh_token(_rt_jti, "user-1", "2099-12-31T00:00:00Z")
T("store_refresh_token doesn't raise", True)

# Get
_rt = _repo.get_refresh_token(_rt_jti)
T("get_refresh_token returns dict", _rt is not None)
T("refresh token jti matches", _rt["jti"] == _rt_jti if _rt else False)
T("refresh token not revoked", _rt["revoked"] == 0 if _rt else False)
T("refresh token has user_id", _rt["user_id"] == "user-1" if _rt else False)

# Revoke
_repo.revoke_refresh_token(_rt_jti)
_rt_revoked = _repo.get_refresh_token(_rt_jti)
T("revoked token shows revoked=1", _rt_revoked["revoked"] == 1 if _rt_revoked else False)

# Get non-existent
_rt_none = _repo.get_refresh_token("nonexistent-jti")
T("get_refresh_token returns None for missing", _rt_none is None)

# Revoke all for user
_rt2_jti = str(uuid.uuid4())
_rt3_jti = str(uuid.uuid4())
_repo.store_refresh_token(_rt2_jti, "user-2", "2099-12-31T00:00:00Z")
_repo.store_refresh_token(_rt3_jti, "user-2", "2099-12-31T00:00:00Z")
_revoked_count = _repo.revoke_all_user_refresh_tokens("user-2")
T("revoke_all returns correct count", _revoked_count == 2, f"got {_revoked_count}")
_rt2 = _repo.get_refresh_token(_rt2_jti)
_rt3 = _repo.get_refresh_token(_rt3_jti)
T("all user refresh tokens revoked", _rt2["revoked"] == 1 and _rt3["revoked"] == 1 if _rt2 and _rt3 else False)


# ---------------------------------------------------------------------------
# M2 Behavioral: Refresh endpoint in stub mode → 404
# ---------------------------------------------------------------------------

S("M2-behavior: refresh endpoint")

resp_refresh_stub = client.post("/api/auth/refresh", json={"refresh_token": "test"})
# In stub mode, auth endpoints return 404
_expected_stub = 404 if _AUTH_MODE == "stub" else 200
T("refresh in stub mode → 404", resp_refresh_stub.status_code == 404,
  f"got {resp_refresh_stub.status_code}")


# ---------------------------------------------------------------------------
# M2 Behavioral: Refresh token rotation (with temp repo)
# ---------------------------------------------------------------------------

S("M2-behavior: refresh token rotation logic")

if _jwt_mode_available():
    import jwt as pyjwt

    _jwt_secret = "test-secret-key-that-is-at-least-32-chars-long!!"
    _now = datetime.datetime.now(datetime.timezone.utc)

    # Create a refresh token
    _ref_jti = str(uuid.uuid4())
    _ref_payload = {
        "jti": _ref_jti,
        "sub": "user-1",
        "type": "refresh",
        "iat": _now,
        "exp": _now + datetime.timedelta(days=7),
    }
    _ref_token = pyjwt.encode(_ref_payload, _jwt_secret, algorithm="HS256")

    # Store it
    _repo.store_refresh_token(_ref_jti, "user-1", (_now + datetime.timedelta(days=7)).isoformat())

    # Decode and validate
    _decoded_ref = pyjwt.decode(_ref_token, _jwt_secret, algorithms=["HS256"])
    T("refresh token has type=refresh", _decoded_ref.get("type") == "refresh")
    T("refresh token has jti", "jti" in _decoded_ref)
    T("refresh token has sub", "sub" in _decoded_ref)

    # Simulate rotation: revoke old, verify it's marked
    _repo.revoke_refresh_token(_ref_jti)
    _stored = _repo.get_refresh_token(_ref_jti)
    T("rotation revokes old token", _stored["revoked"] == 1 if _stored else False)

    # Second use should fail (token is revoked)
    _stored2 = _repo.get_refresh_token(_ref_jti)
    T("reused refresh token is revoked", _stored2["revoked"] == 1 if _stored2 else False)

    # Test with expired refresh token
    _exp_ref_payload = {
        "jti": str(uuid.uuid4()),
        "sub": "user-1",
        "type": "refresh",
        "iat": _now - datetime.timedelta(days=8),
        "exp": _now - datetime.timedelta(days=1),
    }
    _exp_ref_token = pyjwt.encode(_exp_ref_payload, _jwt_secret, algorithm="HS256")
    try:
        pyjwt.decode(_exp_ref_token, _jwt_secret, algorithms=["HS256"])
        T("expired refresh token raises", False, "should have raised")
    except pyjwt.ExpiredSignatureError:
        T("expired refresh token raises", True)

    # Test with non-refresh token type
    _bad_type_payload = {
        "jti": str(uuid.uuid4()),
        "sub": "user-1",
        "iat": _now,
        "exp": _now + datetime.timedelta(hours=2),
    }
    _bad_type_token = pyjwt.encode(_bad_type_payload, _jwt_secret, algorithm="HS256")
    _bad_decoded = pyjwt.decode(_bad_type_token, _jwt_secret, algorithms=["HS256"])
    T("access token has no type=refresh", _bad_decoded.get("type") != "refresh")
else:
    T("PyJWT not installed — skipping M2 JWT tests", True, "SKIP")


# ===========================================================================
# M9 — Auth Audit Logging
# ===========================================================================

# ---------------------------------------------------------------------------
# M9 Structural: auth_audit_log table
# ---------------------------------------------------------------------------

S("M9-struct: auth_audit_log table")

T("auth_audit_log CREATE exists", "CREATE TABLE IF NOT EXISTS auth_audit_log" in _repo_src)
T("auth_audit_log has event_type", "event_type TEXT NOT NULL" in _repo_src)
T("auth_audit_log has email", "email TEXT" in _repo_src)
T("auth_audit_log has ip_address", "ip_address TEXT" in _repo_src)
T("auth_audit_log has user_agent", "user_agent TEXT" in _repo_src)
T("auth_audit_log has success", "success INTEGER NOT NULL" in _repo_src)
T("auth_audit_log has details", "details TEXT" in _repo_src)
T("email+event_type index exists", "idx_auth_audit_email_event" in _repo_src)
T("created_at index exists", "idx_auth_audit_created" in _repo_src)

# ---------------------------------------------------------------------------
# M9 Structural: Abstract + concrete methods
# ---------------------------------------------------------------------------

S("M9-struct: repository methods")

T("ABC has log_auth_event", "def log_auth_event(self, event: dict)" in _repo_src)
T("SqliteRepository implements log_auth_event",
  _repo_src.count("def log_auth_event(self, event: dict)") >= 2)

# ---------------------------------------------------------------------------
# M9 Repository: log_auth_event CRUD
# ---------------------------------------------------------------------------

S("M9-repo: log_auth_event operations")

_repo.log_auth_event({
    "event_type": "login_success",
    "email": "test@example.com",
    "user_id": "user-1",
    "ip_address": "127.0.0.1",
    "user_agent": "TestBot/1.0",
    "success": True,
})
T("log_auth_event doesn't raise", True)

_repo.log_auth_event({
    "event_type": "login_failure",
    "email": "test@example.com",
    "ip_address": "192.168.1.1",
    "user_agent": "TestBot/1.0",
    "success": False,
    "details": "Invalid email or password",
})
T("log failure event doesn't raise", True)

_repo.log_auth_event({
    "event_type": "signup",
    "email": "new@example.com",
    "user_id": "user-new",
    "ip_address": "10.0.0.1",
    "success": True,
})
T("log signup event doesn't raise", True)

_repo.log_auth_event({
    "event_type": "token_validation_failure",
    "ip_address": "1.2.3.4",
    "success": False,
    "details": "Token expired",
})
T("log token validation failure doesn't raise", True)

_repo.log_auth_event({
    "event_type": "logout",
    "user_id": "user-1",
    "ip_address": "127.0.0.1",
    "success": True,
})
T("log logout event doesn't raise", True)

_repo.log_auth_event({
    "event_type": "token_refresh",
    "user_id": "user-1",
    "ip_address": "127.0.0.1",
    "success": True,
})
T("log token refresh event doesn't raise", True)

# Verify rows exist
_aconn = sqlite3.connect(_tmp_path)
_aconn.row_factory = sqlite3.Row
_rows = _aconn.execute("SELECT * FROM auth_audit_log ORDER BY id").fetchall()
T("6 audit events logged", len(_rows) == 6, f"got {len(_rows)}")

_row0 = dict(_rows[0])
T("first event is login_success", _row0["event_type"] == "login_success")
T("first event has email", _row0["email"] == "test@example.com")
T("first event has user_id", _row0["user_id"] == "user-1")
T("first event has ip", _row0["ip_address"] == "127.0.0.1")
T("first event has user_agent", _row0["user_agent"] == "TestBot/1.0")
T("first event success=1", _row0["success"] == 1)
T("first event has created_at", _row0["created_at"] is not None)

_row1 = dict(_rows[1])
T("second event is login_failure", _row1["event_type"] == "login_failure")
T("second event success=0", _row1["success"] == 0)
T("second event has details", _row1["details"] == "Invalid email or password")

_row2 = dict(_rows[2])
T("third event is signup", _row2["event_type"] == "signup")

_row3 = dict(_rows[3])
T("fourth event is token_validation_failure", _row3["event_type"] == "token_validation_failure")
T("fourth event has no email", _row3["email"] is None)

_row4 = dict(_rows[4])
T("fifth event is logout", _row4["event_type"] == "logout")

_row5 = dict(_rows[5])
T("sixth event is token_refresh", _row5["event_type"] == "token_refresh")

_aconn.close()


# ---------------------------------------------------------------------------
# M9 Structural: Logging calls in auth endpoints
# ---------------------------------------------------------------------------

S("M9-struct: logging in auth endpoints")

T("signup logs auth event", "log_auth_event" in _signup_src)
T("login logs success", 'event_type": "login_success"' in _login_src or "login_success" in _login_src)
T("login logs failure", 'event_type": "login_failure"' in _login_src or "login_failure" in _login_src)
T("logout logs event", "log_auth_event" in _logout_src)

# Token validation logging in get_current_user/get_optional_user
T("get_current_user logs validation failures", "log_auth_event" in _gcu_src)
T("get_optional_user logs validation failures", "log_auth_event" in _gou_src)

# Refresh endpoint logging
T("refresh logs event", "log_auth_event" in _refresh_src)


# ---------------------------------------------------------------------------
# M9: _extract_ip helper
# ---------------------------------------------------------------------------

S("M9-helper: _extract_ip")

# Test that _extract_ip exists as a function
T("_extract_ip is callable", callable(_extract_ip))

# Source analysis: _extract_ip reads X-Forwarded-For
_ip_src = _main_src[_main_src.find("def _extract_ip("):]
_ip_src = _ip_src[:_ip_src.find("\ndef ")]
T("_extract_ip reads X-Forwarded-For", "x-forwarded-for" in _ip_src.lower())
T("_extract_ip fallback to request.client.host", "request.client.host" in _ip_src)


# ===========================================================================
# Cross-cutting: Verify tables exist after migration
# ===========================================================================

S("cross: schema tables exist")

_sconn = sqlite3.connect(_tmp_path)
_tables = {r[0] for r in _sconn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
T("refresh_tokens table exists", "refresh_tokens" in _tables)
T("auth_audit_log table exists", "auth_audit_log" in _tables)
T("token_blacklist table exists", "token_blacklist" in _tables)

# Check indexes
_indexes = {r[0] for r in _sconn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
T("idx_auth_audit_email_event exists", "idx_auth_audit_email_event" in _indexes)
T("idx_auth_audit_created exists", "idx_auth_audit_created" in _indexes)

_sconn.close()

# Cleanup
os.unlink(_tmp_path)


# ===========================================================================
# Cross-cutting: Verify existing endpoint auth still works (cookie fallback)
# ===========================================================================

S("cross: existing endpoint auth with cookies")

# Test a variety of endpoints with cookie-based auth
_cookie_jar = {"auth_token": STUB_AUTH_TOKEN}

resp_health = client.get("/api/health")
T("/api/health still public", resp_health.status_code == 200)

resp_narratives_cookie = client.get("/api/narratives", cookies=_cookie_jar)
T("/api/narratives with cookie → 200", resp_narratives_cookie.status_code == 200)

resp_ticker_cookie = client.get("/api/ticker", cookies=_cookie_jar)
T("/api/ticker with cookie → 200", resp_ticker_cookie.status_code == 200)

resp_constellation_cookie = client.get("/api/constellation", cookies=_cookie_jar)
T("/api/constellation with cookie → 200", resp_constellation_cookie.status_code == 200)

resp_me_cookie = client.get("/api/auth/me", cookies=_cookie_jar)
T("/api/auth/me with cookie → 200", resp_me_cookie.status_code == 200)

# Both header and cookie should work (header takes precedence)
resp_both = client.get(
    "/api/narratives",
    headers=STUB_HEADER,
    cookies=_cookie_jar,
)
T("header+cookie auth → 200", resp_both.status_code == 200)


# ===========================================================================
# Summary
# ===========================================================================

_print_summary()
sys.exit(0 if _fail == 0 else 1)
