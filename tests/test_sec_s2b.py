"""
Security Audit S2-B test suite — Token Revocation / Logout (H1).

Tests: jti claim in JWT tokens, token blacklist table, /api/auth/logout endpoint,
_decode_jwt blacklist check, cleanup_expired_blacklist, fail-closed behavior.

Run with:
    python -X utf8 tests/test_sec_s2b.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import datetime
import inspect
import logging
import os
import sqlite3
import sys
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
    return pyjwt.encode(payload, secret, algorithm="HS256")


# ---------------------------------------------------------------------------
# Tests: Structural — jti in JWT payloads
# ---------------------------------------------------------------------------

S("H1-struct: jti in token payloads")


def _test_jti_in_signup_payload():
    """Verify signup JWT payload construction includes jti."""
    src = Path(__file__).parent.parent / "api" / "main.py"
    text = src.read_text(encoding="utf-8")

    # Find the signup function
    signup_idx = text.find("def auth_signup(")
    T("signup function exists", signup_idx > 0)

    # Find payload dict in signup — look for the payload block after signup
    payload_start = text.find('"jti": str(uuid.uuid4())', signup_idx)
    # Also check that it comes before the login function
    login_idx = text.find("def auth_login(", signup_idx)
    T("jti in signup payload", payload_start > signup_idx and payload_start < login_idx,
      f"jti at {payload_start}, signup at {signup_idx}, login at {login_idx}")


def _test_jti_in_login_payload():
    """Verify login JWT payload construction includes jti."""
    src = Path(__file__).parent.parent / "api" / "main.py"
    text = src.read_text(encoding="utf-8")

    login_idx = text.find("def auth_login(")
    # Find the second jti occurrence (after login)
    first_jti = text.find('"jti": str(uuid.uuid4())')
    second_jti = text.find('"jti": str(uuid.uuid4())', first_jti + 1)
    T("jti in login payload", second_jti > login_idx,
      f"second jti at {second_jti}, login at {login_idx}")


_test_jti_in_signup_payload()
_test_jti_in_login_payload()

# Verify uuid import exists
src_text = (Path(__file__).parent.parent / "api" / "main.py").read_text(encoding="utf-8")
T("uuid module imported", "import uuid" in src_text)


# ---------------------------------------------------------------------------
# Tests: Structural — _decode_jwt blacklist check
# ---------------------------------------------------------------------------

S("H1-struct: _decode_jwt blacklist check")

# Read the _decode_jwt source to verify blacklist check is present
_decode_src = inspect.getsource(_decode_jwt)
T("blacklist check in _decode_jwt", "is_token_blacklisted" in _decode_src)
T("fail-closed: 503 on DB unavailable", "503" in _decode_src and "Database unavailable" in _decode_src)
T("401 on revoked token", "Token has been revoked" in _decode_src)
T("jti guard present", 'payload.get("jti")' in _decode_src or "payload.get('jti')" in _decode_src)


# ---------------------------------------------------------------------------
# Tests: Structural — get_current_user passes jti/exp
# ---------------------------------------------------------------------------

S("H1-struct: get_current_user jti/exp passthrough")

_gcu_src = inspect.getsource(get_current_user)
T("jti in get_current_user return", '"jti"' in _gcu_src or "'jti'" in _gcu_src)
T("exp in get_current_user return", '"exp"' in _gcu_src or "'exp'" in _gcu_src)

_gou_src = inspect.getsource(get_optional_user)
T("jti in get_optional_user return", '"jti"' in _gou_src or "'jti'" in _gou_src)
T("exp in get_optional_user return", '"exp"' in _gou_src or "'exp'" in _gou_src)


# ---------------------------------------------------------------------------
# Tests: Structural — /api/auth/logout endpoint exists
# ---------------------------------------------------------------------------

S("H1-struct: logout endpoint")

_logout_route_found = False
_logout_method = None
for route in app.routes:
    if hasattr(route, "path") and route.path == "/api/auth/logout":
        _logout_route_found = True
        _logout_method = list(route.methods) if hasattr(route, "methods") else []

T("logout route registered", _logout_route_found)
T("logout is POST method", _logout_route_found and "POST" in _logout_method,
  f"methods={_logout_method}")

# Check logout uses get_current_user (not get_optional_user — must be authenticated)
_logout_src_text = src_text[src_text.find("def auth_logout("):]
_logout_src_text = _logout_src_text[:_logout_src_text.find("\n\n\n")]
T("logout uses get_current_user", "get_current_user" in _logout_src_text)
T("logout has rate limit", "limiter.limit" in src_text[src_text.find("auth_logout") - 200:src_text.find("auth_logout")])


# ---------------------------------------------------------------------------
# Tests: Structural — token_blacklist table
# ---------------------------------------------------------------------------

S("H1-struct: token_blacklist table")

repo_src = (Path(__file__).parent.parent / "repository.py").read_text(encoding="utf-8")
_create_table_idx = repo_src.find("CREATE TABLE IF NOT EXISTS token_blacklist")
T("token_blacklist CREATE TABLE", _create_table_idx > 0)
_create_block = repo_src[_create_table_idx:_create_table_idx + 500]
T("jti TEXT PRIMARY KEY", "jti TEXT PRIMARY KEY" in _create_block)
T("user_id column", "user_id TEXT NOT NULL" in _create_block)
T("blacklisted_at column", "blacklisted_at TEXT NOT NULL" in _create_block)
T("expires_at column", "expires_at TEXT NOT NULL" in _create_block)


# ---------------------------------------------------------------------------
# Tests: Structural — repository methods
# ---------------------------------------------------------------------------

S("H1-struct: repository methods")

T("blacklist_token method", "def blacklist_token(" in repo_src)
T("is_token_blacklisted method", "def is_token_blacklisted(" in repo_src)
T("cleanup_expired_blacklist method", "def cleanup_expired_blacklist(" in repo_src)

# Verify INSERT OR IGNORE for idempotent blacklisting
T("INSERT OR IGNORE for blacklist_token", "INSERT OR IGNORE INTO token_blacklist" in repo_src)

# Abstract methods in Repository ABC
T("abstract blacklist_token", repo_src.count("def blacklist_token(") >= 2,
  "should be in both ABC and SqliteRepository")
T("abstract is_token_blacklisted", repo_src.count("def is_token_blacklisted(") >= 2)
T("abstract cleanup_expired_blacklist", repo_src.count("def cleanup_expired_blacklist(") >= 2)


# ---------------------------------------------------------------------------
# Tests: Behavioral — stub mode logout
# ---------------------------------------------------------------------------

S("H1-behavior: stub mode logout")

resp = client.post("/api/auth/logout", headers=STUB_HEADER)
T("stub logout returns 200", resp.status_code == 200, f"got {resp.status_code}")
T("stub logout detail message", "stub mode" in resp.json().get("detail", "").lower(),
  f"got {resp.json()}")

# No auth → 403
resp_no_auth = client.post("/api/auth/logout")
T("logout without token → 403", resp_no_auth.status_code == 403, f"got {resp_no_auth.status_code}")


# ---------------------------------------------------------------------------
# Tests: Behavioral — repository blacklist operations (direct DB)
# ---------------------------------------------------------------------------

S("H1-behavior: repository blacklist ops")

from repository import SqliteRepository  # noqa: E402

import tempfile

_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp_db_path = _tmp_db.name
_tmp_db.close()

_test_repo = SqliteRepository(_tmp_db_path)
_test_repo.migrate()

# Verify table was created
_conn = sqlite3.connect(_tmp_db_path)
_tables = [r[0] for r in _conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
T("token_blacklist table created by migrate()", "token_blacklist" in _tables,
  f"tables: {_tables}")

# Test blacklist_token
_test_jti = str(uuid.uuid4())
_test_repo.blacklist_token(
    jti=_test_jti,
    user_id="test-user",
    expires_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
)
T("is_token_blacklisted returns True for blacklisted jti",
  _test_repo.is_token_blacklisted(_test_jti))

# Test non-blacklisted jti
T("is_token_blacklisted returns False for unknown jti",
  not _test_repo.is_token_blacklisted(str(uuid.uuid4())))

# Test idempotent blacklisting (INSERT OR IGNORE — no error on duplicate)
try:
    _test_repo.blacklist_token(
        jti=_test_jti,
        user_id="test-user",
        expires_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    )
    T("duplicate blacklist_token does not error", True)
except Exception as e:
    T("duplicate blacklist_token does not error", False, str(e))

# Test cleanup_expired_blacklist
_expired_jti = str(uuid.uuid4())
_past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)).isoformat()
_test_repo.blacklist_token(jti=_expired_jti, user_id="test-user", expires_at=_past)
T("expired entry exists before cleanup", _test_repo.is_token_blacklisted(_expired_jti))

_deleted = _test_repo.cleanup_expired_blacklist()
T("cleanup returns count of deleted entries", _deleted >= 1, f"deleted={_deleted}")
T("expired entry removed after cleanup", not _test_repo.is_token_blacklisted(_expired_jti))

# Non-expired entry should survive cleanup
_future_jti = str(uuid.uuid4())
_future = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=24)).isoformat()
_test_repo.blacklist_token(jti=_future_jti, user_id="test-user", expires_at=_future)
_test_repo.cleanup_expired_blacklist()
T("non-expired entry survives cleanup", _test_repo.is_token_blacklisted(_future_jti))

# Cleanup
_conn.close()
os.unlink(_tmp_db_path)


# ---------------------------------------------------------------------------
# Tests: Behavioral — JWT mode token revocation (if PyJWT available)
# ---------------------------------------------------------------------------

S("H1-behavior: JWT token revocation flow")

if _jwt_mode_available():
    import jwt as pyjwt

    # Create a temporary DB for JWT mode testing
    _jwt_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    _jwt_db_path = _jwt_tmp.name
    _jwt_tmp.close()

    _jwt_repo = SqliteRepository(_jwt_db_path)
    _jwt_repo.migrate()

    _jwt_secret = "test-secret-key-that-is-at-least-32-chars-long!!"

    # Create a token with jti
    _test_jti2 = str(uuid.uuid4())
    _now = datetime.datetime.now(datetime.timezone.utc)
    _exp = _now + datetime.timedelta(hours=24)
    _token = pyjwt.encode({
        "jti": _test_jti2,
        "sub": "user-1",
        "email": "test@test.com",
        "role": "user",
        "iat": _now,
        "exp": _exp,
    }, _jwt_secret, algorithm="HS256")

    # Decode should work
    _decoded = pyjwt.decode(_token, _jwt_secret, algorithms=["HS256"])
    T("decoded token has jti", "jti" in _decoded)
    T("decoded jti matches", _decoded["jti"] == _test_jti2)

    # Blacklist the token
    _jwt_repo.blacklist_token(
        jti=_test_jti2,
        user_id="user-1",
        expires_at=_exp.isoformat(),
    )
    T("token is blacklisted", _jwt_repo.is_token_blacklisted(_test_jti2))

    # Verify jti is in the database
    _jconn = sqlite3.connect(_jwt_db_path)
    _row = _jconn.execute("SELECT * FROM token_blacklist WHERE jti = ?", (_test_jti2,)).fetchone()
    T("blacklist row exists in DB", _row is not None)
    T("blacklist row has correct user_id", _row[1] == "user-1" if _row else False)
    T("blacklist row has blacklisted_at", _row[2] is not None if _row else False)
    T("blacklist row has expires_at", _row[3] is not None if _row else False)

    # Test token without jti (pre-revocation-support tokens)
    _token_no_jti = pyjwt.encode({
        "sub": "user-2",
        "email": "old@test.com",
        "role": "user",
        "iat": _now,
        "exp": _exp,
    }, _jwt_secret, algorithm="HS256")
    _decoded_no_jti = pyjwt.decode(_token_no_jti, _jwt_secret, algorithms=["HS256"])
    T("token without jti has no jti field", "jti" not in _decoded_no_jti)

    _jconn.close()
    os.unlink(_jwt_db_path)
else:
    T("PyJWT not installed — skipping JWT behavioral tests", True, "SKIP")


# ---------------------------------------------------------------------------
# Tests: Behavioral — logout endpoint in stub mode (functional)
# ---------------------------------------------------------------------------

S("H1-behavior: logout endpoint functional")

# Stub mode: logout should work and return success
resp = client.post("/api/auth/logout", headers=STUB_HEADER)
T("stub logout → 200", resp.status_code == 200)
T("stub logout returns detail", "detail" in resp.json())

# Bad token → 403
resp_bad = client.post("/api/auth/logout", headers={"x-auth-token": "bad-token"})
T("bad token logout → 403", resp_bad.status_code == 403, f"got {resp_bad.status_code}")


# ---------------------------------------------------------------------------
# Tests: Structural — auth_logout source code analysis
# ---------------------------------------------------------------------------

S("H1-struct: auth_logout implementation")

_logout_full_src = src_text[src_text.find("def auth_logout("):src_text.find("# ----", src_text.find("def auth_logout("))]
T("logout checks _AUTH_MODE", "_AUTH_MODE" in _logout_full_src)
T("logout handles no-jti tokens gracefully", "no jti" in _logout_full_src.lower() or "not jti" in _logout_full_src)
T("logout calls blacklist_token", "blacklist_token" in _logout_full_src)
T("logout handles DB unavailable (503)", "503" in _logout_full_src)
T("logout uses datetime.fromtimestamp for expires_at", "fromtimestamp" in _logout_full_src)


# ---------------------------------------------------------------------------
# Tests: Structural — _decode_jwt fail-closed design
# ---------------------------------------------------------------------------

S("H1-struct: fail-closed _decode_jwt design")

# The critical property: if get_repo() returns None, reject the token (503)
# NOT: silently accept it (fail-open)
T("fail-closed: repo None → 503 (not silent accept)",
  "repo is None" in _decode_src and "503" in _decode_src)
T("fail-closed: raises HTTPException on None repo",
  "raise HTTPException" in _decode_src.split("repo is None")[1][:200] if "repo is None" in _decode_src else False)

# Verify the check order: decode first, then blacklist
_decode_idx = _decode_src.find("jwt.decode(")
_blacklist_idx = _decode_src.find("is_token_blacklisted")
T("blacklist check after JWT decode", _decode_idx < _blacklist_idx,
  f"decode at {_decode_idx}, blacklist at {_blacklist_idx}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
_print_summary()
sys.exit(0 if _fail == 0 else 1)
