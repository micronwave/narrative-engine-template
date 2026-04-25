"""
D9 API test suite -- Auth Foundation (Phase 2 Batch 6).

Tests: D9-U1 (schema migration), D9-U2 (repository user operations),
       D9-U3 (stub mode auth), D9-U4 (auth endpoints in stub mode),
       D9-U5 (JWT signup), D9-U6 (JWT login), D9-U7 (JWT token validation),
       D9-U8 (JWT /api/auth/me).

Requires PyJWT and bcrypt for JWT-mode tests (D9-U5 through D9-U8).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 test_d9_api.py

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
            f"  FAIL [{_current_section}] {name}" + (f" -- {details}" if details else ""),
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
# Imports
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import sqlite3
import tempfile
import uuid
from datetime import datetime, timedelta, timezone

from repository import SqliteRepository

# ===========================================================================
# D9-U1: Schema migration -- users table
# ===========================================================================
S("D9-U1: Schema migration")

_tmp_dir = tempfile.mkdtemp()
_test_db = str(Path(_tmp_dir) / "test_d9.db")
repo = SqliteRepository(_test_db)
repo.migrate()

# Verify users table exists and has correct columns
conn = sqlite3.connect(_test_db)
cols = conn.execute("PRAGMA table_info(users)").fetchall()
col_names = [c[1] for c in cols]
conn.close()

T("users table exists", len(cols) > 0, f"columns: {col_names}")
T("id column exists", "id" in col_names)
T("email column exists", "email" in col_names)
T("password_hash column exists", "password_hash" in col_names)
T("created_at column exists", "created_at" in col_names)

# Verify unique index on email
conn = sqlite3.connect(_test_db)
indexes = conn.execute("PRAGMA index_list(users)").fetchall()
idx_names = [i[1] for i in indexes]
conn.close()

T("idx_users_email index exists", "idx_users_email" in idx_names,
  f"indexes: {idx_names}")

# Verify email UNIQUE constraint by checking index info
conn = sqlite3.connect(_test_db)
idx_info = conn.execute("PRAGMA index_info(idx_users_email)").fetchall()
idx_cols = [i[2] for i in idx_info]
conn.close()

T("idx_users_email covers email column", "email" in idx_cols,
  f"indexed columns: {idx_cols}")


# ===========================================================================
# D9-U2: Repository user operations
# ===========================================================================
S("D9-U2: Repository user operations")

user1_id = str(uuid.uuid4())
user1_email = "test@example.com"
user1_hash = "$2b$12$fakehashfortest1234567890abcdefghij"
user1_created = datetime.now(timezone.utc).isoformat()

repo.create_user({
    "id": user1_id,
    "email": user1_email,
    "password_hash": user1_hash,
    "created_at": user1_created,
})

# get_user_by_id
fetched = repo.get_user_by_id(user1_id)
T("get_user_by_id returns user", fetched is not None)
T("user has correct id", fetched["id"] == user1_id)
T("user has correct email", fetched["email"] == user1_email)
T("user has correct password_hash", fetched["password_hash"] == user1_hash)
T("user has correct created_at", fetched["created_at"] == user1_created)

# get_user_by_id nonexistent
T("get_user_by_id returns None for missing",
  repo.get_user_by_id("nonexistent-id") is None)

# get_user_by_email
fetched_by_email = repo.get_user_by_email(user1_email)
T("get_user_by_email returns user", fetched_by_email is not None)
T("email lookup returns correct id", fetched_by_email["id"] == user1_id)

# get_user_by_email nonexistent
T("get_user_by_email returns None for missing",
  repo.get_user_by_email("nobody@example.com") is None)

# create second user
user2_id = str(uuid.uuid4())
repo.create_user({
    "id": user2_id,
    "email": "other@example.com",
    "password_hash": "$2b$12$anotherfakehash",
    "created_at": datetime.now(timezone.utc).isoformat(),
})

T("second user created and retrievable",
  repo.get_user_by_id(user2_id) is not None)

# Duplicate email raises error
duplicate_raised = False
try:
    repo.create_user({
        "id": str(uuid.uuid4()),
        "email": user1_email,  # duplicate
        "password_hash": "$2b$12$whatever",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
except Exception:
    duplicate_raised = True

T("duplicate email raises error", duplicate_raised)


# ===========================================================================
# D9-U3: Stub mode auth (default behavior preserved)
# ===========================================================================
S("D9-U3: Stub mode auth")

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app, STUB_AUTH_TOKEN, _AUTH_MODE  # noqa: E402
import api.main as api_main_module  # noqa: E402

AUTH_HEADER = {"x-auth-token": STUB_AUTH_TOKEN}

# Verify we're in stub mode
T("default AUTH_MODE is stub", _AUTH_MODE == "stub", f"got: {_AUTH_MODE}")

with TestClient(app) as client:
    # get_current_user: valid stub token
    resp = client.get("/api/auth/me", headers=AUTH_HEADER)
    T("stub token accepted on protected endpoint", resp.status_code == 200,
      f"got {resp.status_code}")

    # get_current_user: no token
    resp_noauth = client.get("/api/auth/me")
    T("no token returns 403", resp_noauth.status_code == 403)

    # get_current_user: bad token
    resp_bad = client.get("/api/auth/me", headers={"x-auth-token": "wrong-token"})
    T("bad token returns 403", resp_bad.status_code == 403)

    # get_optional_user: no token returns 200 (local user)
    resp_opt = client.get("/api/activity")
    T("optional auth no token returns 200", resp_opt.status_code == 200,
      f"got {resp_opt.status_code}")

    # get_optional_user: bad token returns 403
    resp_opt_bad = client.get("/api/activity", headers={"x-auth-token": "wrong"})
    T("optional auth bad token returns 403", resp_opt_bad.status_code == 403)

    # get_optional_user: valid stub token returns 200
    resp_opt_good = client.get("/api/activity", headers=AUTH_HEADER)
    T("optional auth valid token returns 200", resp_opt_good.status_code == 200)


# ===========================================================================
# D9-U4: Auth endpoints in stub mode
# ===========================================================================
S("D9-U4: Auth endpoints in stub mode")

with TestClient(app) as client:
    # signup returns 404 in stub mode
    resp_signup = client.post("/api/auth/signup",
                              json={"email": "new@example.com", "password": "password123"})
    T("signup returns 404 in stub mode", resp_signup.status_code == 404,
      f"got {resp_signup.status_code}")

    # login returns 404 in stub mode
    resp_login = client.post("/api/auth/login",
                             json={"email": "new@example.com", "password": "password123"})
    T("login returns 404 in stub mode", resp_login.status_code == 404,
      f"got {resp_login.status_code}")

    # /me returns stub user info
    resp_me = client.get("/api/auth/me", headers=AUTH_HEADER)
    T("/me returns 200 in stub mode", resp_me.status_code == 200,
      f"got {resp_me.status_code}")
    me_data = resp_me.json()
    T("/me auth_mode is stub", me_data.get("auth_mode") == "stub")
    T("/me user_id is local", me_data.get("user_id") == "local")
    T("/me role is user", me_data.get("role") == "user")


# ===========================================================================
# D9-U5 through D9-U8: JWT mode tests
# ===========================================================================
# Switch to JWT mode by patching module-level variable.
# _decode_jwt reads JWT_SECRET_KEY from os.environ per-call.

_JWT_SECRET = "test-secret-key-for-d9-auth-tests-min-32bytes!"
_original_auth_mode = api_main_module._AUTH_MODE
os.environ["JWT_SECRET_KEY"] = _JWT_SECRET
os.environ["JWT_EXPIRY_HOURS"] = "24"
api_main_module._AUTH_MODE = "jwt"

try:
    import jwt as pyjwt
    import bcrypt
    _jwt_available = True
except ImportError:
    _jwt_available = False

# Unique email per test run to avoid 409 from prior runs
_run_id = uuid.uuid4().hex[:8]
_test_email = f"testuser-{_run_id}@example.com"
_test_email_upper = f"TestUser-{_run_id}@Example.com"

if not _jwt_available:
    S("D9-U5: JWT signup")
    T("SKIP: PyJWT/bcrypt not installed", True, "Install PyJWT and bcrypt to run JWT tests")
    S("D9-U6: JWT login")
    T("SKIP: PyJWT/bcrypt not installed", True)
    S("D9-U7: JWT token validation")
    T("SKIP: PyJWT/bcrypt not installed", True)
    S("D9-U8: JWT /api/auth/me")
    T("SKIP: PyJWT/bcrypt not installed", True)
else:

    # ===========================================================================
    # D9-U5: JWT mode -- signup flow
    # ===========================================================================
    S("D9-U5: JWT signup")

    with TestClient(app) as client:
        # Valid signup
        resp_s = client.post("/api/auth/signup",
                             json={"email": _test_email_upper, "password": "securepass123"})
        T("signup returns 200", resp_s.status_code == 200, f"got {resp_s.status_code}")

        s_data = resp_s.json()
        T("signup returns user_id", "user_id" in s_data)
        T("signup returns email", "email" in s_data)
        T("signup returns token", "token" in s_data)
        T("email is lowercased", s_data.get("email") == _test_email,
          f"got: {s_data.get('email')}")

        _jwt_user_id = s_data.get("user_id")
        _jwt_token = s_data.get("token")

        # Token is a valid JWT
        decoded = pyjwt.decode(_jwt_token, _JWT_SECRET, algorithms=["HS256"])
        T("token sub is user_id", decoded.get("sub") == _jwt_user_id)
        T("token has exp claim", "exp" in decoded)
        T("token has role claim", decoded.get("role") == "user")

        # Duplicate email returns 409
        resp_dup = client.post("/api/auth/signup",
                               json={"email": _test_email, "password": "otherpass123"})
        T("duplicate email returns 409", resp_dup.status_code == 409,
          f"got {resp_dup.status_code}")

        # Invalid email returns 422
        resp_bad_email = client.post("/api/auth/signup",
                                     json={"email": "notanemail", "password": "securepass123"})
        T("invalid email returns 422", resp_bad_email.status_code == 422,
          f"got {resp_bad_email.status_code}")

        # Short password returns 422
        resp_short = client.post("/api/auth/signup",
                                  json={"email": "short@test.com", "password": "abc"})
        T("short password returns 422", resp_short.status_code == 422,
          f"got {resp_short.status_code}")

        # Empty email returns 422
        resp_empty = client.post("/api/auth/signup",
                                   json={"email": "", "password": "securepass123"})
        T("empty email returns 422 (or 429 when limiter window exhausted)", resp_empty.status_code in (422, 429),
          f"got {resp_empty.status_code}")

    # ===========================================================================
    # D9-U6: JWT mode -- login flow
    # ===========================================================================
    S("D9-U6: JWT login")

    with TestClient(app) as client:
        # Valid login with the user created in D9-U5
        resp_l = client.post("/api/auth/login",
                             json={"email": _test_email, "password": "securepass123"})
        T("login returns 200", resp_l.status_code == 200, f"got {resp_l.status_code}")

        l_data = resp_l.json()
        T("login returns user_id", "user_id" in l_data)
        T("login returns email", l_data.get("email") == _test_email)
        T("login returns token", "token" in l_data)

        login_token = l_data.get("token")
        login_decoded = pyjwt.decode(login_token, _JWT_SECRET, algorithms=["HS256"])
        T("login token sub matches user_id", login_decoded.get("sub") == l_data.get("user_id"))

        # Wrong password returns 401
        resp_wrong = client.post("/api/auth/login",
                                  json={"email": _test_email, "password": "wrongpassword"})
        T("wrong password returns 401", resp_wrong.status_code == 401,
          f"got {resp_wrong.status_code}")

        # Nonexistent email returns 401
        resp_nouser = client.post("/api/auth/login",
                                   json={"email": "nobody@test.com", "password": "anything123"})
        T("nonexistent email returns 401", resp_nouser.status_code == 401,
          f"got {resp_nouser.status_code}")

    # ===========================================================================
    # D9-U7: JWT mode -- token validation
    # ===========================================================================
    S("D9-U7: JWT token validation")

    with TestClient(app) as client:
        # Valid JWT on protected endpoint
        resp_ok = client.get("/api/auth/me",
                             headers={"x-auth-token": _jwt_token})
        T("valid JWT accepted on protected endpoint", resp_ok.status_code == 200,
          f"got {resp_ok.status_code}")

        # Expired JWT returns 401
        expired_payload = {
            "sub": _jwt_user_id,
            "email": _test_email,
            "role": "user",
            "iat": datetime.now(timezone.utc) - timedelta(hours=48),
            "exp": datetime.now(timezone.utc) - timedelta(hours=24),
        }
        expired_token = pyjwt.encode(expired_payload, _JWT_SECRET, algorithm="HS256")
        resp_expired = client.get("/api/auth/me",
                                   headers={"x-auth-token": expired_token})
        T("expired JWT returns 401", resp_expired.status_code == 401,
          f"got {resp_expired.status_code}")

        # Malformed token returns 403
        resp_malformed = client.get("/api/auth/me",
                                     headers={"x-auth-token": "not.a.jwt"})
        T("malformed JWT returns 403", resp_malformed.status_code == 403,
          f"got {resp_malformed.status_code}")

        # Wrong key returns 403
        wrong_key_token = pyjwt.encode(
            {"sub": _jwt_user_id, "role": "user",
             "exp": datetime.now(timezone.utc) + timedelta(hours=1)},
            "wrong-secret-key",
            algorithm="HS256",
        )
        resp_wrongkey = client.get("/api/auth/me",
                                    headers={"x-auth-token": wrong_key_token})
        T("wrong key JWT returns 403", resp_wrongkey.status_code == 403,
          f"got {resp_wrongkey.status_code}")

        # No token in JWT mode returns 403
        resp_notoken = client.get("/api/auth/me")
        T("no token in JWT mode returns 403", resp_notoken.status_code == 403,
          f"got {resp_notoken.status_code}")

        # get_optional_user in JWT mode requires token
        resp_opt_jwt = client.get("/api/activity")
        T("optional auth in JWT mode requires token (403)",
          resp_opt_jwt.status_code == 403,
          f"got {resp_opt_jwt.status_code}")

    # ===========================================================================
    # D9-U8: JWT mode -- /api/auth/me
    # ===========================================================================
    S("D9-U8: JWT /api/auth/me")

    with TestClient(app) as client:
        resp_me = client.get("/api/auth/me",
                             headers={"x-auth-token": _jwt_token})
        T("/me returns 200", resp_me.status_code == 200)

        me = resp_me.json()
        T("/me auth_mode is jwt", me.get("auth_mode") == "jwt")
        T("/me user_id matches", me.get("user_id") == _jwt_user_id)
        T("/me has email", me.get("email") == _test_email)
        T("/me has role", me.get("role") == "user")
        T("/me has created_at", me.get("created_at") is not None)


# ---------------------------------------------------------------------------
# Restore stub mode
# ---------------------------------------------------------------------------
api_main_module._AUTH_MODE = _original_auth_mode
os.environ.pop("JWT_SECRET_KEY", None)
os.environ.pop("JWT_EXPIRY_HOURS", None)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
_print_summary()
sys.exit(0 if _fail == 0 else 1)
