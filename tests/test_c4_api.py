"""
C4 API test suite — Narrative Intelligence Platform, Phase C4.

Tests: C4-U1 through C4-U8 (subscription, export, signals, ticker id).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 test_c4_api.py

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
from api.main import app, _subscription, _user_credits, STUB_AUTH_TOKEN  # noqa: E402

client = TestClient(app)
AUTH_HEADER = {"x-auth-token": STUB_AUTH_TOKEN}

# Reset subscription to known state before tests
_subscription["subscribed"] = False
_user_credits["balance"] = 5

# ===========================================================================
# C4-U1: GET /api/subscription — returns status, requires auth
# ===========================================================================
S("C4-U1: GET /api/subscription")

resp = client.get("/api/subscription", headers=AUTH_HEADER)
T("status 200", resp.status_code == 200, f"got {resp.status_code}")

body = resp.json()
T("has user_id", isinstance(body.get("user_id"), str))
T("has subscribed bool", isinstance(body.get("subscribed"), bool))
T("initial subscribed=False", body.get("subscribed") is False)

# Guest gets 403
resp_guest = client.get("/api/subscription")
T("guest gets 403", resp_guest.status_code == 403, f"got {resp_guest.status_code}")

# ===========================================================================
# C4-U2: POST /api/subscription/toggle — toggles and returns status
# ===========================================================================
S("C4-U2: POST /api/subscription/toggle")

# Toggle → subscribed=True
resp = client.post("/api/subscription/toggle", headers=AUTH_HEADER)
T("status 200", resp.status_code == 200, f"got {resp.status_code}")
T("now subscribed", resp.json().get("subscribed") is True, str(resp.json()))

# Toggle again → subscribed=False
resp2 = client.post("/api/subscription/toggle", headers=AUTH_HEADER)
T("toggled back to False", resp2.json().get("subscribed") is False)

# Guest gets 403
resp_guest = client.post("/api/subscription/toggle")
T("guest gets 403", resp_guest.status_code == 403, f"got {resp_guest.status_code}")

# ===========================================================================
# C4-U3: POST /api/narratives/{id}/export — subscriber gets CSV
# ===========================================================================
S("C4-U3: POST /api/narratives/{id}/export subscriber flow")

# Subscribe first
_subscription["subscribed"] = True

# Get a real narrative ID
narr_resp = client.get("/api/narratives")
visible = [n for n in narr_resp.json() if not n.get("blurred", True)]
real_id = visible[0]["id"] if visible else None
T("have a real narrative id", real_id is not None)

if real_id:
    resp = client.post(f"/api/narratives/{real_id}/export", headers=AUTH_HEADER)
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")
    ct = resp.headers.get("content-type", "")
    T("content-type is text/csv", "text/csv" in ct, f"got '{ct}'")
    cd = resp.headers.get("content-disposition", "")
    T("content-disposition has filename", "filename" in cd, f"got '{cd}'")
    T("body is non-empty CSV", len(resp.content) > 0)
    # CSV should start with a header row
    text = resp.content.decode("utf-8", errors="replace")
    T("CSV has header row", "type" in text.lower() or "id" in text.lower(), text[:100])

# 404 for non-existent narrative
resp_404 = client.post("/api/narratives/nonexistent-id-xyz/export", headers=AUTH_HEADER)
T("non-existent narrative \u2192 404", resp_404.status_code == 404, f"got {resp_404.status_code}")

# ===========================================================================
# C4-U4: POST /api/narratives/{id}/export — non-subscriber gets 403
# ===========================================================================
S("C4-U4: POST /api/narratives/{id}/export non-subscriber")

_subscription["subscribed"] = False

if real_id:
    resp = client.post(f"/api/narratives/{real_id}/export", headers=AUTH_HEADER)
    T("non-subscriber gets 403", resp.status_code == 403, f"got {resp.status_code}")
    T("detail mentions subscription", "subscription" in resp.json().get("detail", "").lower(),
      resp.json().get("detail"))

# Guest gets 403 too
if real_id:
    resp_guest = client.post(f"/api/narratives/{real_id}/export")
    T("guest gets 403", resp_guest.status_code == 403, f"got {resp_guest.status_code}")

# Re-subscribe for remaining tests
_subscription["subscribed"] = True

# ===========================================================================
# C4-U5: GET /api/signals — returns signal list with coordination flags
# ===========================================================================
S("C4-U5: GET /api/signals returns signals with coordination_flag")

resp = client.get("/api/signals")
T("status 200", resp.status_code == 200, f"got {resp.status_code}")

sigs = resp.json()
T("returns list", isinstance(sigs, list))
T("at least 1 signal", len(sigs) >= 1, f"got {len(sigs)}")

if sigs:
    sig = sigs[0]
    T("signal has id", "id" in sig)
    T("signal has headline str", isinstance(sig.get("headline"), str))
    T("signal has coordination_flag bool", isinstance(sig.get("coordination_flag"), bool))
    T("signal has source", isinstance(sig.get("source"), dict))
    T("signal has timestamp", "timestamp" in sig)

# Coordination flags come from real adversarial data (no longer demo-seeded)
has_flag_field = all("coordination_flag" in s for s in sigs)
T("all signals have coordination_flag field", has_flag_field, "missing coordination_flag field")

# ===========================================================================
# C4-U6: GET /api/ticker — items include id field
# ===========================================================================
S("C4-U6: GET /api/ticker items include id field")

resp = client.get("/api/ticker")
T("status 200", resp.status_code == 200, f"got {resp.status_code}")

tickers = resp.json()
T("returns list", isinstance(tickers, list))
T("at least 5 items", len(tickers) >= 5, f"got {len(tickers)}")

if tickers:
    T("first item has id key", "id" in tickers[0], str(tickers[0].keys()))

# ===========================================================================
# C4-U7: GET /api/narratives — subscriber sees all 9 as visible
# ===========================================================================
S("C4-U7: GET /api/narratives all visible (monetization removed)")

# All narratives visible regardless of subscription — monetization removed in D4
resp_sub = client.get("/api/narratives")
T("status 200", resp_sub.status_code == 200, f"got {resp_sub.status_code}")

sub_data = resp_sub.json()
sub_visible = [n for n in sub_data if not n.get("blurred", True)]
T("all narratives visible", len(sub_visible) == len(sub_data), f"visible={len(sub_visible)}, total={len(sub_data)}")
T("no blurred items", not any(n.get("blurred") for n in sub_data))

# ===========================================================================
# C4-U8: Regression — previous C2/C3 endpoints still work
# ===========================================================================
S("C4-U8: Regression \u2014 C2/C3 endpoints still functional")

T("GET /api/health ok", client.get("/api/health").json() == {"status": "ok"})
T("GET /api/credits ok", client.get("/api/credits", headers=AUTH_HEADER).status_code == 200)
T("POST /api/credits/use ok", client.post("/api/credits/use", headers=AUTH_HEADER).status_code in (200, 402))
T("GET /api/constellation ok", client.get("/api/constellation").status_code == 200)

# ===========================================================================
# Summary + exit
# ===========================================================================
_print_summary()

sys.exit(0 if _fail == 0 else 1)
