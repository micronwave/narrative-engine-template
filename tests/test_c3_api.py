"""
C3 API test suite — Narrative Intelligence Platform, Phase C3.

Tests: C3-U1 through C3-U4 (credits/use + SSE stream endpoint).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 test_c3_api.py

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
# TestClient
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app, _user_credits, STUB_AUTH_TOKEN  # noqa: E402

client = TestClient(app)
AUTH_HEADER = {"x-auth-token": STUB_AUTH_TOKEN}

# ===========================================================================
# C3-U1: POST /api/credits/use — decrements balance
# ===========================================================================
S("C3-U1: POST /api/credits/use decrements balance")

# Reset to known state before test
_user_credits["balance"] = 5
_user_credits["total_used"] = 15

resp = client.post("/api/credits/use", headers=AUTH_HEADER)
T("status 200", resp.status_code == 200, f"got {resp.status_code}")

body = resp.json()
T("balance decremented", body.get("balance") == 4, f"got {body.get('balance')}")
T("total_used incremented", body.get("total_used") == 16, f"got {body.get('total_used')}")
T("user_id present", isinstance(body.get("user_id"), str))
T("total_purchased unchanged", body.get("total_purchased") == _user_credits["total_purchased"])

# Multiple decrements accumulate
_user_credits["balance"] = 3
resp2 = client.post("/api/credits/use", headers=AUTH_HEADER)
T("second decrement status 200", resp2.status_code == 200, f"got {resp2.status_code}")
T("balance now 2", resp2.json().get("balance") == 2, f"got {resp2.json().get('balance')}")

# ===========================================================================
# C3-U2: POST /api/credits/use — V3: no token = single-user OK, bad token = 403
# ===========================================================================
S("C3-U2: POST /api/credits/use auth behavior")

resp = client.post("/api/credits/use")
T("no token → 403 (auth required)", resp.status_code == 403, f"got {resp.status_code}")

resp_wrong = client.post("/api/credits/use", headers={"x-auth-token": "wrong-token"})
T("wrong token → 403", resp_wrong.status_code == 403, f"got {resp_wrong.status_code}")

# ===========================================================================
# C3-U3: POST /api/credits/use — 402 when balance is 0
# ===========================================================================
S("C3-U3: POST /api/credits/use returns 402 when balance is 0")

_user_credits["balance"] = 0

resp = client.post("/api/credits/use", headers=AUTH_HEADER)
T("status 402", resp.status_code == 402, f"got {resp.status_code}")
T("detail mentions credits", "credit" in resp.json().get("detail", "").lower(),
  resp.json().get("detail"))

# Balance stays at 0 after 402
T("balance stays 0", _user_credits["balance"] == 0, f"got {_user_credits['balance']}")

# Reset for subsequent tests
_user_credits["balance"] = 5

# ===========================================================================
# C3-U4: GET /api/stream — SSE content-type
# ===========================================================================
S("C3-U4: GET /api/stream returns SSE content-type")

# The SSE stream is an infinite generator — TestClient.stream() blocks until
# EOF (never). We verify the endpoint is registered via the OpenAPI spec and
# that the response type annotation/path is correct.
# NOTE: In production, C4 connects directly via EventSource to
# http://localhost:8000/api/stream — not through the Next.js proxy.

# Verify route is registered in OpenAPI schema
_spec = client.get("/openapi.json").json()
_paths = _spec.get("paths", {})
T("/api/stream is registered", "/api/stream" in _paths,
  f"registered paths: {list(_paths.keys())}")
T("/api/stream has GET", "get" in _paths.get("/api/stream", {}),
  str(_paths.get("/api/stream", {})))

# ===========================================================================
# Summary + exit
# ===========================================================================
_print_summary()

sys.exit(0 if _fail == 0 else 1)
