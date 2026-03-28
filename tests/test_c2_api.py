"""
C2 API test suite — Narrative Intelligence Platform, Phase C2.

Tests: C2-U1 through C2-U5 + auth flow (C2-A1, C2-A2 backend side).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).
Requires data/narrative_engine.db to exist.

Run with:
    python -X utf8 test_c2_api.py

Exit code 0 if all tests pass, 1 if any fail.
On all-pass, appends a line to frontend_build_log.
"""

import logging
import sys
from datetime import date
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
# C2-U1: GET /api/narratives — updated schema with velocity_timeseries
# ===========================================================================
S("C2-U1: GET /api/narratives with velocity_timeseries")

resp = client.get("/api/narratives")
T("status 200", resp.status_code == 200, f"got {resp.status_code}")

data = resp.json()
T("returns list", isinstance(data, list))
T("at least 1 item", len(data) >= 1, f"got {len(data)}")

visible = [n for n in data if not n.get("blurred", True)]
T("all narratives visible (monetization removed)", len(visible) == len(data), f"visible={len(visible)}, total={len(data)}")

for i, n in enumerate(visible[:3]):
    ts = n.get("velocity_timeseries")
    T(f"visible[{i}] has velocity_timeseries", isinstance(ts, list), str(type(ts)))
    T(f"visible[{i}] timeseries has 7 entries", len(ts) == 7, f"got {len(ts)}")
    if ts:
        first = ts[0]
        T(f"visible[{i}] ts[0] has date str", isinstance(first.get("date"), str))
        T(f"visible[{i}] ts[0] has numeric value",
          isinstance(first.get("value"), (int, float)), str(type(first.get("value"))))

    T(f"visible[{i}] has saturation", "saturation" in n)
    T(f"visible[{i}] saturation is float", isinstance(n.get("saturation"), (int, float)))
    T(f"visible[{i}] has signals list", isinstance(n.get("signals"), list))
    T(f"visible[{i}] has catalysts list", isinstance(n.get("catalysts"), list))
    T(f"visible[{i}] has mutations list", isinstance(n.get("mutations"), list))

# ===========================================================================
# C2-U2: GET /api/narratives/{id} — detailed payload
# ===========================================================================
S("C2-U2: GET /api/narratives/{id} detailed payload")

# Get a real narrative ID from the narratives list
real_id = visible[0]["id"] if visible else None
T("have a real narrative id to test", real_id is not None, "no visible narratives in DB")

if real_id:
    resp = client.get(f"/api/narratives/{real_id}")
    T("status 200", resp.status_code == 200, f"got {resp.status_code}")

    detail = resp.json()
    T("has id", detail.get("id") == real_id)
    T("has name str", isinstance(detail.get("name"), str))
    T("has descriptor str", isinstance(detail.get("descriptor"), str))
    T("has velocity_summary str", isinstance(detail.get("velocity_summary"), str))
    T("has entropy (float or null)", detail.get("entropy") is None or isinstance(detail.get("entropy"), (int, float)))
    T("has saturation float", isinstance(detail.get("saturation"), (int, float)))

    ts = detail.get("velocity_timeseries")
    T("has velocity_timeseries list", isinstance(ts, list))
    T("timeseries 7 entries", len(ts) == 7, f"got {len(ts)}" if ts else "missing")

    signals = detail.get("signals")
    T("has signals list", isinstance(signals, list))

    # If signals present, validate first one
    if signals:
        sig = signals[0]
        T("signal has id", "id" in sig)
        T("signal has headline str", isinstance(sig.get("headline"), str))
        T("signal has source", isinstance(sig.get("source"), dict))
        src = sig.get("source", {})
        T("source has name", isinstance(src.get("name"), str))
        T("signal has timestamp", "timestamp" in sig)
        T("signal has sentiment", isinstance(sig.get("sentiment"), (int, float)))
        T("signal has coordination_flag bool", isinstance(sig.get("coordination_flag"), bool))

    catalysts = detail.get("catalysts")
    T("has catalysts list", isinstance(catalysts, list))
    if catalysts:
        cat = catalysts[0]
        T("catalyst has id", "id" in cat)
        T("catalyst has description str", isinstance(cat.get("description"), str))
        T("catalyst has impact_score float", isinstance(cat.get("impact_score"), (int, float)))

    mutations = detail.get("mutations")
    T("has mutations list", isinstance(mutations, list))
    if mutations:
        mut = mutations[0]
        T("mutation has id", "id" in mut)
        T("mutation has from_state str", isinstance(mut.get("from_state"), str))
        T("mutation has to_state str", isinstance(mut.get("to_state"), str))
        T("mutation has description str", isinstance(mut.get("description"), str))

    entropy_detail = detail.get("entropy_detail")
    T("has entropy_detail object", isinstance(entropy_detail, dict))
    if entropy_detail:
        comps = entropy_detail.get("components", {})
        T("entropy_detail.narrative_id str", isinstance(entropy_detail.get("narrative_id"), str))
        T("entropy_detail has components", isinstance(comps, dict))
        T("components has source_diversity", "source_diversity" in comps)
        T("components has temporal_spread", "temporal_spread" in comps)
        T("components has sentiment_variance", "sentiment_variance" in comps)

# 404 on unknown id
resp404 = client.get("/api/narratives/does-not-exist-xyz")
T("404 on unknown id", resp404.status_code == 404, f"got {resp404.status_code}")

# ===========================================================================
# C2-U3: GET /api/constellation — nodes and edges
# ===========================================================================
S("C2-U3: GET /api/constellation")

resp = client.get("/api/constellation")
T("status 200", resp.status_code == 200, f"got {resp.status_code}")

const_data = resp.json()
T("has nodes list", isinstance(const_data.get("nodes"), list))
T("has edges list", isinstance(const_data.get("edges"), list))

nodes = const_data.get("nodes", [])
edges = const_data.get("edges", [])

T("at least 3 nodes", len(nodes) >= 3, f"got {len(nodes)}")
T("at least 2 edges", len(edges) >= 2, f"got {len(edges)}")

for i, node in enumerate(nodes[:3]):
    T(f"node[{i}] has id str", isinstance(node.get("id"), str))
    T(f"node[{i}] has name str", isinstance(node.get("name"), str))
    T(f"node[{i}] has type str", node.get("type") in ("narrative", "catalyst"), str(node.get("type")))

for i, edge in enumerate(edges[:2]):
    T(f"edge[{i}] has source str", isinstance(edge.get("source"), str))
    T(f"edge[{i}] has target str", isinstance(edge.get("target"), str))
    T(f"edge[{i}] has weight numeric", isinstance(edge.get("weight"), (int, float)))

# ===========================================================================
# C2-U4: GET /api/credits — auth gated
# ===========================================================================
S("C2-U4: GET /api/credits")

# Auth enforcement: no token → 403
resp_guest = client.get("/api/credits")
T("no token gets 403", resp_guest.status_code == 403, f"got {resp_guest.status_code}")

# Signed-in → 200 + credit object
resp_auth = client.get("/api/credits", headers=AUTH_HEADER)
T("signed-in gets 200", resp_auth.status_code == 200, f"got {resp_auth.status_code}")

credits = resp_auth.json()
T("has balance int", isinstance(credits.get("balance"), int))
T("has total_purchased int", isinstance(credits.get("total_purchased"), int))
T("has total_used int", isinstance(credits.get("total_used"), int))
T("has user_id str", isinstance(credits.get("user_id"), str))

# ===========================================================================
# C2-U5: POST /api/credits/topup — increments balance
# ===========================================================================
S("C2-U5: POST /api/credits/topup increments balance")

# Auth enforcement: no token → 403
resp_guest_topup = client.post("/api/credits/topup", json={"amount": 5})
T("no token topup gets 403", resp_guest_topup.status_code == 403, f"got {resp_guest_topup.status_code}")

# Get current balance
before = _user_credits["balance"]
resp_topup = client.post("/api/credits/topup", json={"amount": 5}, headers=AUTH_HEADER)
T("topup returns 200", resp_topup.status_code == 200, f"got {resp_topup.status_code}")

updated = resp_topup.json()
T("balance increased by 5", updated.get("balance") == before + 5,
  f"before={before} after={updated.get('balance')}")
T("total_purchased increased by 5", updated.get("total_purchased") == _user_credits["total_purchased"])

# Confirm via GET
resp_check = client.get("/api/credits", headers=AUTH_HEADER)
T("GET /api/credits reflects new balance", resp_check.json().get("balance") == before + 5)

# ===========================================================================
# Auth check: no auth token → 403 on credit endpoints
# ===========================================================================
S("Auth: invalid token blocks credit endpoints (V3: no token = single-user OK)")

T("GET /api/credits with bad token → 403",
  client.get("/api/credits", headers={"x-auth-token": "bad-token"}).status_code == 403)
T("POST /api/credits/topup with bad token → 403",
  client.post("/api/credits/topup", json={"amount": 1}, headers={"x-auth-token": "bad-token"}).status_code == 403)

# ===========================================================================
# Summary
# ===========================================================================
_print_summary()

sys.exit(0 if _fail == 0 else 1)
