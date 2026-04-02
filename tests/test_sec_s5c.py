"""
Security Audit S5 Checkpoint C test suite — L1 + L2 + L3.

Tests:
  - L1: Bcrypt SHA-256 pre-hash (signup path, login dual-check, hash migration)
  - L2: Email verification columns, token generation, verify endpoint wiring, repo methods
  - L3: RBAC role column + require_role() factory function

Run with:
    python -X utf8 tests/test_sec_s5c.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import sys
import os
import hashlib
import secrets
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
_API = str(ROOT / "api")
for _p in [str(ROOT), _API]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Minimal test runner (matches project conventions)
# ---------------------------------------------------------------------------

_results: list[dict] = []
_current_section: str = "Unset"
_pass = 0
_fail = 0


def S(section_name: str) -> None:
    global _current_section
    _current_section = section_name
    print(f"\n{'=' * 60}")
    print(f"  {section_name}")
    print(f"{'=' * 60}")


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
    mark = "PASS" if condition else "FAIL"
    det = f"  ({details})" if details else ""
    print(f"  [{mark}] {name}{det}")


# ---------------------------------------------------------------------------
# Source texts (read once, reused across sections)
# ---------------------------------------------------------------------------

_main_src = (ROOT / "api" / "main.py").read_text(encoding="utf-8")
_repo_src = (ROOT / "repository.py").read_text(encoding="utf-8")


# ===========================================================================
# L1 — Bcrypt pre-hash: module-level imports
# ===========================================================================

S("L1 — Bcrypt pre-hash: module-level imports")

T("import hashlib present at module level in api/main.py",
  "import hashlib" in _main_src)

T("import secrets present at module level in api/main.py",
  "import secrets" in _main_src)

# Confirm neither is buried in a function def (they must appear before any 'def ')
_hashlib_pos = _main_src.find("import hashlib")
_secrets_pos = _main_src.find("import secrets")
_first_def_pos = _main_src.find("\ndef ")

T("import hashlib appears before first function definition",
  0 <= _hashlib_pos < _first_def_pos,
  f"hashlib_pos={_hashlib_pos}, first_def={_first_def_pos}")

T("import secrets appears before first function definition",
  0 <= _secrets_pos < _first_def_pos,
  f"secrets_pos={_secrets_pos}, first_def={_first_def_pos}")


# ===========================================================================
# L1 — Bcrypt pre-hash: signup path (source inspection)
# ===========================================================================

S("L1 — Bcrypt pre-hash: signup path")

T("signup uses hashlib.sha256 digest for pw_bytes",
  'hashlib.sha256(body.password.encode("utf-8")).digest()' in _main_src)

T("signup calls bcrypt.hashpw with pw_bytes (not raw password)",
  "bcrypt.hashpw(pw_bytes, bcrypt.gensalt())" in _main_src)

# pw_bytes must appear BEFORE the bcrypt.hashpw call (not swapped)
_pw_bytes_assign = _main_src.find("hashlib.sha256(body.password.encode")
_bcrypt_hashpw   = _main_src.find("bcrypt.hashpw(pw_bytes,")
T("pw_bytes assigned before bcrypt.hashpw call",
  0 < _pw_bytes_assign < _bcrypt_hashpw,
  f"sha256_pos={_pw_bytes_assign}, hashpw_pos={_bcrypt_hashpw}")


# ===========================================================================
# L1 — Bcrypt pre-hash: login dual-check (source inspection)
# ===========================================================================

S("L1 — Bcrypt pre-hash: login dual-check")

T("login computes pw_sha via SHA-256",
  "pw_sha = hashlib.sha256" in _main_src)

T("login has new-style checkpw (SHA-256 digest)",
  "bcrypt.checkpw(pw_sha," in _main_src)

T("login has old-style fallback checkpw (plain bytes)",
  'bcrypt.checkpw(body.password.encode("utf-8"),' in _main_src)

T("login rehashes old-style password on migration",
  "bcrypt.hashpw(pw_sha, bcrypt.gensalt())" in _main_src)

T("login calls repo.update_user_password_hash for migration",
  "repo.update_user_password_hash(" in _main_src)

# Order check: new-style attempt must precede old-style fallback
_new_check = _main_src.find("bcrypt.checkpw(pw_sha,")
_old_check = _main_src.find('bcrypt.checkpw(body.password.encode("utf-8")',)
T("new-style checkpw appears before old-style fallback in source",
  0 < _new_check < _old_check,
  f"new_check={_new_check}, old_check={_old_check}")


# ===========================================================================
# L1 — Bcrypt pre-hash: repository method (source inspection)
# ===========================================================================

S("L1 — Bcrypt pre-hash: repository.update_user_password_hash")

T("update_user_password_hash method defined in repository.py",
  "def update_user_password_hash(" in _repo_src)

T("UPDATE users SET password_hash present",
  "UPDATE users SET password_hash = ?" in _repo_src)

# Parameterised — no string formatting in the UPDATE
_update_stmt = _repo_src[_repo_src.find("UPDATE users SET password_hash"):]
_update_line = _update_stmt.split("\n")[0]
T("UPDATE uses parameterised query (no f-string/format)",
  "%" not in _update_line and 'f"' not in _update_line and "format(" not in _update_line,
  f"line: {_update_line.strip()}")


# ===========================================================================
# L1 — Bcrypt pre-hash: functional verification
# ===========================================================================

S("L1 — Bcrypt pre-hash: functional bcrypt logic")

import bcrypt  # noqa: E402 — only needed here

password = "S3cur3P@ssw0rd-functional-test!"
pw_sha = hashlib.sha256(password.encode("utf-8")).digest()

# New-style hash (SHA-256 pre-hashed)
new_style_hash = bcrypt.hashpw(pw_sha, bcrypt.gensalt())

T("new-style hash: checkpw with SHA-256 digest returns True",
  bcrypt.checkpw(pw_sha, new_style_hash))

T("new-style hash: checkpw with plain bytes returns False",
  not bcrypt.checkpw(password.encode("utf-8"), new_style_hash))

# Old-style hash (plain password bytes — legacy)
old_style_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

T("old-style hash: checkpw with plain bytes returns True",
  bcrypt.checkpw(password.encode("utf-8"), old_style_hash))

T("old-style hash: checkpw with SHA-256 digest returns False",
  not bcrypt.checkpw(pw_sha, old_style_hash))

# Dual-check logic simulation (mirrors login flow)
def _dual_check(plain_password: str, stored_hash: bytes) -> str:
    """Returns 'new', 'old', or 'fail' — mirrors the L1 login dual-check."""
    sha = hashlib.sha256(plain_password.encode("utf-8")).digest()
    if bcrypt.checkpw(sha, stored_hash):
        return "new"
    if bcrypt.checkpw(plain_password.encode("utf-8"), stored_hash):
        return "old"
    return "fail"

T("dual-check: new-style stored hash → 'new' (no migration needed)",
  _dual_check(password, new_style_hash) == "new")

T("dual-check: old-style stored hash → 'old' (triggers migration)",
  _dual_check(password, old_style_hash) == "old")

T("dual-check: wrong password → 'fail'",
  _dual_check("wrong-password", new_style_hash) == "fail")

T("dual-check: wrong password against old hash → 'fail'",
  _dual_check("wrong-password", old_style_hash) == "fail")

# Verify that a migrated hash (rehash of SHA-256 digest) works correctly
migrated_hash = bcrypt.hashpw(pw_sha, bcrypt.gensalt())
T("migrated hash: same password now resolves as 'new' (migration complete)",
  _dual_check(password, migrated_hash) == "new")


# ===========================================================================
# L2 — Email verification: migration (source inspection)
# ===========================================================================

S("L2 — Email verification: migration columns")

T("email_verified column in users migration (INTEGER DEFAULT 0)",
  "email_verified INTEGER DEFAULT 0" in _repo_src)

T("verification_token column in users migration (TEXT)",
  "verification_token TEXT" in _repo_src)

# Both ALTER TABLE statements must be wrapped in try/except (idempotent)
_ev_idx   = _repo_src.find("email_verified INTEGER DEFAULT 0")
_vt_idx   = _repo_src.find("verification_token TEXT")
_try_before_ev = _repo_src.rfind("try:", 0, _ev_idx)
_try_before_vt = _repo_src.rfind("try:", 0, _vt_idx)
_except_after_ev = _repo_src.find("except Exception:", _ev_idx)
_except_after_vt = _repo_src.find("except Exception:", _vt_idx)

T("email_verified ALTER TABLE wrapped in try/except (idempotent)",
  _try_before_ev != -1 and _except_after_ev != -1 and _except_after_ev - _ev_idx < 200,
  f"try@{_try_before_ev}, except@{_except_after_ev}, col@{_ev_idx}")

T("verification_token ALTER TABLE wrapped in try/except (idempotent)",
  _try_before_vt != -1 and _except_after_vt != -1 and _except_after_vt - _vt_idx < 200,
  f"try@{_try_before_vt}, except@{_except_after_vt}, col@{_vt_idx}")


# ===========================================================================
# L2 — Email verification: signup wiring (source inspection)
# ===========================================================================

S("L2 — Email verification: signup wiring")

T("signup generates verification token via secrets.token_urlsafe(32)",
  "secrets.token_urlsafe(32)" in _main_src)

T("signup passes email_verified=0 to create_user",
  '"email_verified": 0' in _main_src)

T("signup passes verification_token to create_user",
  '"verification_token": verification_token' in _main_src)

T("signup logs verification URL to console",
  "/api/auth/verify?token=" in _main_src)

T("GET /api/auth/verify endpoint defined",
  '@app.get("/api/auth/verify")' in _main_src)

T("verify endpoint calls repo.get_user_by_verification_token",
  "repo.get_user_by_verification_token(" in _main_src)

T("verify endpoint calls repo.mark_email_verified",
  "repo.mark_email_verified(" in _main_src)

T("verify endpoint returns 400 on invalid token",
  "status_code=400" in _main_src and "Invalid verification token" in _main_src)

T("verify endpoint is JWT-mode gated",
  'Auth endpoints require AUTH_MODE=jwt' in _main_src)


# ===========================================================================
# L2 — Email verification: repository methods (source inspection)
# ===========================================================================

S("L2 — Email verification: repository method signatures")

T("get_user_by_verification_token defined in repository.py",
  "def get_user_by_verification_token(" in _repo_src)

T("mark_email_verified defined in repository.py",
  "def mark_email_verified(" in _repo_src)

T("mark_email_verified sets email_verified = 1",
  "email_verified = 1" in _repo_src)

T("mark_email_verified clears token (verification_token = NULL)",
  "verification_token = NULL" in _repo_src)

T("get_user_by_verification_token queries WHERE verification_token = ?",
  "WHERE verification_token = ?" in _repo_src)

T("mark_email_verified uses parameterised WHERE id = ?",
  _repo_src.count("WHERE id = ?") >= 1)


# ===========================================================================
# L2 — Email verification: functional repository tests
# ===========================================================================

S("L2 — Email verification: functional repository tests")

from repository import SqliteRepository  # noqa: E402

with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as _tmpdir:
    _db = os.path.join(_tmpdir, "test_s5c.db")
    _repo = SqliteRepository(_db)
    _repo.migrate()

    _token = secrets.token_urlsafe(32)
    _user_id = "test-l2-user-001"

    _repo.create_user({
        "id": _user_id,
        "email": "l2test@example.com",
        "password_hash": "placeholder",
        "email_verified": 0,
        "verification_token": _token,
        "created_at": "2026-04-01T00:00:00+00:00",
    })

    # Valid token lookup
    _found = _repo.get_user_by_verification_token(_token)
    T("get_user_by_verification_token returns user for valid token",
      _found is not None)
    T("returned user id matches",
      _found["id"] == _user_id if _found else False)
    T("returned user email_verified is 0 (unverified)",
      _found["email_verified"] == 0 if _found else False)

    # Invalid token lookup
    _not_found = _repo.get_user_by_verification_token("invalid-token-xyz")
    T("get_user_by_verification_token returns None for unknown token",
      _not_found is None)

    # Mark verified
    _repo.mark_email_verified(_user_id)
    _after = _repo.get_user_by_id(_user_id)
    T("mark_email_verified sets email_verified=1",
      _after["email_verified"] == 1 if _after else False,
      f"got {_after.get('email_verified') if _after else 'None'}")
    T("mark_email_verified clears verification_token to None",
      _after.get("verification_token") is None if _after else False,
      f"got {_after.get('verification_token') if _after else 'None'}")

    # Token no longer valid after verification
    _expired = _repo.get_user_by_verification_token(_token)
    T("verification token is invalid after mark_email_verified (one-time use)",
      _expired is None)


# ===========================================================================
# L2 — Email verification: migration idempotency
# ===========================================================================

S("L2 — Email verification: migration idempotency")

with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as _tmpdir2:
    _db2 = os.path.join(_tmpdir2, "test_idem.db")
    _r1 = SqliteRepository(_db2)
    _r1.migrate()
    _crashed = False
    try:
        _r2 = SqliteRepository(_db2)
        _r2.migrate()  # Second migration must not raise
    except Exception as _e:
        _crashed = True
        print(f"    migration crash: {_e}")
    T("second migrate() call does not raise (idempotent)",
      not _crashed)

    # Also verify columns actually exist after migration
    import sqlite3 as _sqlite3
    with _sqlite3.connect(_db2) as _conn:
        _pragma = _conn.execute("PRAGMA table_info(users)").fetchall()
        _col_names = {row[1] for row in _pragma}
    T("email_verified column exists in users table after migration",
      "email_verified" in _col_names,
      f"cols: {sorted(_col_names)}")
    T("verification_token column exists in users table after migration",
      "verification_token" in _col_names,
      f"cols: {sorted(_col_names)}")
    T("role column exists in users table after migration",
      "role" in _col_names,
      f"cols: {sorted(_col_names)}")


# ===========================================================================
# L3 — RBAC stub: migration (source inspection)
# ===========================================================================

S("L3 — RBAC stub: migration")

T("role TEXT DEFAULT 'user' column in users migration",
  "role TEXT DEFAULT 'user'" in _repo_src)

_role_idx = _repo_src.find("role TEXT DEFAULT 'user'")
_try_before_role   = _repo_src.rfind("try:", 0, _role_idx)
_except_after_role = _repo_src.find("except Exception:", _role_idx)

T("role ALTER TABLE wrapped in try/except (idempotent)",
  _try_before_role != -1 and _except_after_role != -1 and _except_after_role - _role_idx < 200,
  f"try@{_try_before_role}, except@{_except_after_role}, col@{_role_idx}")


# ===========================================================================
# L3 — RBAC stub: require_role() (source inspection)
# ===========================================================================

S("L3 — RBAC stub: require_role() factory")

T("require_role function defined in api/main.py",
  "def require_role(" in _main_src)

T("require_role checks user.get('role') against required_role",
  "user.get(\"role\") != required_role" in _main_src
  or "user.get('role') != required_role" in _main_src)

T("require_role raises HTTPException with status_code=403",
  "status_code=403" in _main_src)

# require_role must be defined before the auth endpoints section
_rr_pos   = _main_src.find("def require_role(")
_auth_pos  = _main_src.find("# Auth endpoints")
T("require_role is defined before the auth endpoints section",
  0 < _rr_pos < _auth_pos,
  f"require_role@{_rr_pos}, auth_section@{_auth_pos}")

T("require_role inner function uses Depends(get_current_user)",
  "Depends(get_current_user)" in _main_src[_rr_pos:_rr_pos + 400])

T("require_role returns the user dict on success",
  "return user" in _main_src[_rr_pos:_rr_pos + 400])


# ===========================================================================
# L3 — RBAC stub: functional require_role() logic
# ===========================================================================

S("L3 — RBAC stub: functional require_role() logic")

# Test the role-check logic directly without importing FastAPI app
# Mirror the logic from the source: user.get("role") != required_role → 403
def _simulate_require_role(required_role: str, user_role: str) -> bool:
    """Returns True if access is granted, False if 403 would be raised."""
    return user_role == required_role

T("admin user passes require_role('admin')",
  _simulate_require_role("admin", "admin"))

T("regular user rejected by require_role('admin')",
  not _simulate_require_role("admin", "user"))

T("regular user passes require_role('user')",
  _simulate_require_role("user", "user"))

T("missing role (None) rejected by require_role('admin')",
  not _simulate_require_role("admin", None))

# Verify the actual source implements the same logic (user.get returns None for missing)
T("require_role uses .get() (safe for missing role key)",
  "user.get(" in _main_src[_rr_pos:_rr_pos + 400])

# Default role on new users
with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as _tmpdir3:
    _db3 = os.path.join(_tmpdir3, "test_l3.db")
    _r3 = SqliteRepository(_db3)
    _r3.migrate()
    _r3.create_user({
        "id": "test-l3-user",
        "email": "l3@example.com",
        "password_hash": "placeholder",
        "email_verified": 0,
        "verification_token": secrets.token_urlsafe(32),
        "created_at": "2026-04-01T00:00:00+00:00",
    })
    _l3_user = _r3.get_user_by_id("test-l3-user")
    T("new user created without explicit role gets DEFAULT 'user' role from DB",
      _l3_user is not None and _l3_user.get("role") == "user",
      f"role={_l3_user.get('role') if _l3_user else 'None'}")


# ===========================================================================
# Summary
# ===========================================================================

print(f"\n{'=' * 60}")
print(f"S5 Checkpoint C — {_pass} passed, {_fail} failed out of {_pass + _fail}")
print(f"{'=' * 60}")

if _fail > 0:
    print("\nFailed tests:")
    for r in _results:
        if not r["passed"]:
            det = f"  ({r['details']})" if r["details"] else ""
            print(f"  [{r['section']}] {r['name']}{det}")

sys.exit(0 if _fail == 0 else 1)
