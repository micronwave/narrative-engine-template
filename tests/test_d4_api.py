"""
D4 — Manipulation/Coordination Detection Tests

Unit:
  D4-U1: GET /api/manipulation returns list with 3+ narrative objects
  D4-U2: Each item has manipulation_indicators array with at least one indicator
  D4-U3: ManipulationIndicator has all required fields: id, narrative_id, indicator_type,
         confidence, detected_at, evidence_summary, flagged_signals, status
  D4-U4: GET /api/manipulation?indicator_type=coordinated_amplification returns only items with that type
  D4-U5: GET /api/manipulation?min_confidence=0.7 excludes indicators with confidence < 0.7
  D4-U6: GET /api/manipulation?status=active returns only items with active status indicators
  D4-U7: GET /api/narratives/{id}/manipulation returns indicators for that narrative
  D4-U8: GET /api/credits still returns valid response (backend preserved)
  D4-U9: POST /api/credits/use still returns valid response (backend preserved)
  D4-U10: GET /api/subscription still returns valid response (backend preserved)
"""

import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app  # noqa: E402

STUB_TOKEN = "stub-auth-token"

_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    status = "PASS" if condition else "FAIL"
    _results.append((name, condition))
    marker = "✓" if condition else "✗"
    msg = f"  [{marker}] {name}"
    if details and not condition:
        msg += f"\n      details: {details}"
    elif details and condition:
        msg += f"  ({details})"
    print(msg)


# ===========================================================================
# D4-U1: GET /api/manipulation returns 3+ narrative objects
# ===========================================================================
S("D4-U1: GET /api/manipulation returns 3+ narrative objects")
with TestClient(app) as client:
    resp = client.get("/api/manipulation")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("returns a list", isinstance(data, list))
    T("has 3+ narrative objects", len(data) >= 3, f"len={len(data)}")

# ===========================================================================
# D4-U2: Each item has manipulation_indicators array with at least one indicator
# ===========================================================================
S("D4-U2: Each item has manipulation_indicators with at least one indicator")
with TestClient(app) as client:
    resp = client.get("/api/manipulation")
    data = resp.json()
    for item in data:
        T(
            f"narrative {item['id']} has manipulation_indicators list",
            isinstance(item.get("manipulation_indicators"), list),
        )
        T(
            f"narrative {item['id']} has at least one indicator",
            len(item.get("manipulation_indicators", [])) >= 1,
        )

# ===========================================================================
# D4-U3: ManipulationIndicator has all required fields
# ===========================================================================
S("D4-U3: ManipulationIndicator has all required fields")
REQUIRED_FIELDS = [
    "id", "narrative_id", "indicator_type", "confidence",
    "detected_at", "evidence_summary", "flagged_signals", "status"
]
with TestClient(app) as client:
    resp = client.get("/api/manipulation")
    data = resp.json()
    for item in data:
        for mi in item.get("manipulation_indicators", []):
            for field in REQUIRED_FIELDS:
                T(
                    f"mi-{mi.get('id', '?')} has field '{field}'",
                    field in mi,
                    f"fields={list(mi.keys())}",
                )
            break  # check first indicator per narrative only
        break  # check first narrative only

# ===========================================================================
# D4-U4: GET /api/manipulation?indicator_type=coordinated_amplification
# ===========================================================================
S("D4-U4: filter by indicator_type=coordinated_amplification")
with TestClient(app) as client:
    resp = client.get("/api/manipulation?indicator_type=coordinated_amplification")
    T("status 200", resp.status_code == 200)
    data = resp.json()
    T("returns a list", isinstance(data, list))
    T("returns at least one result", len(data) >= 1, f"len={len(data)}")
    for item in data:
        for mi in item.get("manipulation_indicators", []):
            T(
                "all indicators are coordinated_amplification",
                mi["indicator_type"] == "coordinated_amplification",
                f"type={mi['indicator_type']}",
            )

# ===========================================================================
# D4-U5: GET /api/manipulation?min_confidence=0.7 excludes low-confidence
# ===========================================================================
S("D4-U5: filter by min_confidence=0.7")
with TestClient(app) as client:
    resp = client.get("/api/manipulation?min_confidence=0.7")
    T("status 200", resp.status_code == 200)
    data = resp.json()
    for item in data:
        for mi in item.get("manipulation_indicators", []):
            T(
                f"mi-{mi['id']} confidence >= 0.7",
                mi["confidence"] >= 0.7,
                f"confidence={mi['confidence']}",
            )

# ===========================================================================
# D4-U6: GET /api/manipulation?status=active returns only active indicators
# ===========================================================================
S("D4-U6: filter by status=active")
with TestClient(app) as client:
    resp = client.get("/api/manipulation?status=active")
    T("status 200", resp.status_code == 200)
    data = resp.json()
    T("returns at least one result", len(data) >= 1, f"len={len(data)}")
    for item in data:
        for mi in item.get("manipulation_indicators", []):
            T(
                f"mi-{mi['id']} status == active",
                mi["status"] == "active",
                f"status={mi['status']}",
            )

# ===========================================================================
# D4-U7: GET /api/narratives/{id}/manipulation returns indicators for narrative
# ===========================================================================
S("D4-U7: GET /api/narratives/{id}/manipulation")
with TestClient(app) as client:
    # Use nar-002 which has 2 indicators
    resp = client.get("/api/narratives/nar-002/manipulation")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("returns a list", isinstance(data, list))
    T("returns at least one indicator", len(data) >= 1, f"len={len(data)}")
    T(
        "all indicators belong to nar-002",
        all(mi["narrative_id"] == "nar-002" for mi in data),
        f"ids={[mi['narrative_id'] for mi in data]}",
    )

    # Unknown narrative returns []
    resp2 = client.get("/api/narratives/unknown-narrative/manipulation")
    T("unknown narrative returns 200", resp2.status_code == 200)
    T("unknown narrative returns []", resp2.json() == [])

# ===========================================================================
# D4-U8: GET /api/credits still returns valid response (backend preserved)
# ===========================================================================
S("D4-U8: GET /api/credits still works")
with TestClient(app) as client:
    resp = client.get("/api/credits", headers={"x-auth-token": STUB_TOKEN})
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("has balance field", "balance" in data, f"fields={list(data.keys())}")
    T("has user_id field", "user_id" in data)

# ===========================================================================
# D4-U9: POST /api/credits/use still returns valid response (backend preserved)
# ===========================================================================
S("D4-U9: POST /api/credits/use still works")
with TestClient(app) as client:
    import api.main as main_module
    original_balance = main_module._user_credits["balance"]
    # Ensure there's balance to use
    main_module._user_credits["balance"] = 10
    resp = client.post("/api/credits/use", headers={"x-auth-token": STUB_TOKEN})
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("has balance field", "balance" in data)
    T("balance decremented", data["balance"] == 9, f"balance={data['balance']}")
    main_module._user_credits["balance"] = original_balance

# ===========================================================================
# D4-U10: GET /api/subscription still returns valid response (backend preserved)
# ===========================================================================
S("D4-U10: GET /api/subscription still works")
with TestClient(app) as client:
    resp = client.get("/api/subscription", headers={"x-auth-token": STUB_TOKEN})
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("has subscribed field", "subscribed" in data)
    T("has user_id field", "user_id" in data)

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"D4 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All D4 tests passed.")
