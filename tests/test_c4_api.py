"""
C4 API test suite — Narrative Intelligence Platform, Phase C4.

Tests: C4-U1 through C4-U8 (subscription, export, signals, ticker id).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 test_c4_api.py

Exit code 0 if all tests pass, 1 if any fail.
On all-pass, appends a line to frontend_build_log.
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
from api.main import app, STUB_AUTH_TOKEN  # noqa: E402

client = TestClient(app)
AUTH_HEADER = {"x-auth-token": STUB_AUTH_TOKEN}

# ===========================================================================
# C4-U1: Monetization endpoints removed
# ===========================================================================
S("C4-U1: monetization endpoints removed")

T("GET /api/subscription returns 404", client.get("/api/subscription", headers=AUTH_HEADER).status_code == 404)
T("POST /api/subscription/toggle returns 404", client.post("/api/subscription/toggle", headers=AUTH_HEADER).status_code == 404)
T("GET /api/credits returns 404", client.get("/api/credits", headers=AUTH_HEADER).status_code == 404)
T("POST /api/credits/use returns 404", client.post("/api/credits/use", headers=AUTH_HEADER).status_code == 404)

# ===========================================================================
# C4-U2: Export endpoint retained and local-safe
# ===========================================================================
S("C4-U2: POST /api/narratives/{id}/export local-safe")

# ===========================================================================
# C4-U3: POST /api/narratives/{id}/export returns CSV
# ===========================================================================
S("C4-U3: POST /api/narratives/{id}/export CSV response")

# Get a real narrative ID
narr_resp = client.get("/api/narratives")
visible = [n for n in narr_resp.json() if not n.get("blurred", True)]
real_id = visible[0]["id"] if visible else None
T("have a real narrative id", real_id is not None)

if real_id:
    resp = client.post(f"/api/narratives/{real_id}/export")
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
T("non-existent narrative → 404", resp_404.status_code == 404, f"got {resp_404.status_code}")

# ===========================================================================
# C4-U4: POST /api/narratives/{id}/export invalid token rejected
# ===========================================================================
S("C4-U4: POST /api/narratives/{id}/export invalid token")

if real_id:
    resp_bad = client.post(
        f"/api/narratives/{real_id}/export",
        headers={"x-auth-token": "wrong-token"},
    )
    T("bad token gets 403", resp_bad.status_code == 403, f"got {resp_bad.status_code}")

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
S("C4-U8: Regression — C2/C3 endpoints still functional")

T("GET /api/health ok", client.get("/api/health").json().get("status") == "ok")
T("GET /api/credits removed", client.get("/api/credits", headers=AUTH_HEADER).status_code == 404)
T("POST /api/credits/use removed", client.post("/api/credits/use", headers=AUTH_HEADER).status_code == 404)
T("GET /api/constellation ok", client.get("/api/constellation").status_code == 200)

# ===========================================================================
# Summary + exit
# ===========================================================================
_print_summary()

passed = _fail == 0

if passed:
    log_path = Path(__file__).parent.parent / "frontend_build_log"
    try:
        from datetime import date as _date
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{_date.today()}] C4 backend tests: {_pass}/{_pass + _fail} passed\n")
    except Exception:
        pass

sys.exit(0 if passed else 1)
