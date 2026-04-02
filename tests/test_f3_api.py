"""
F3 — Pre-Earnings Intelligence Brief Tests

Unit:
  F3-U1: GET /api/brief/TSM returns 200 with ticker field
  F3-U2: Response includes narratives array with at least 1 entry
  F3-U3: Each narrative has entropy_interpretation string
  F3-U4: risk_summary has all required fields
  F3-U5: GET /api/brief/INVALID returns 404
  F3-U6: Entropy interpretation matches expected ranges
  F3-U7: coordination_flags count matches MANIPULATION_INDICATORS data
"""

import sys
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from fastapi.testclient import TestClient
from api.main import app, MANIPULATION_INDICATORS

# ---------------------------------------------------------------------------
_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    _results.append((name, condition))
    marker = "✓" if condition else "✗"
    msg = f"  [{marker}] {name}"
    if details and not condition:
        msg += f"\n      details: {details}"
    elif details and condition:
        msg += f"  ({details})"
    print(msg)


# ===========================================================================
# F3-U1: GET /api/brief/TSM returns 200 with ticker field
# ===========================================================================
S("F3-U1: GET /api/brief/TSM returns 200")
with TestClient(app) as client:
    resp = client.get("/api/brief/TSM")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("has ticker field", "ticker" in data, f"keys={list(data.keys())}")
    T("ticker is TSM", data.get("ticker") == "TSM", f"ticker={data.get('ticker')}")
    T("has security field", "security" in data)
    T("has narratives field", "narratives" in data)
    T("has risk_summary field", "risk_summary" in data)
    T("has generated_at field", "generated_at" in data)

# ===========================================================================
# F3-U2: Response includes narratives array with at least 1 entry
# ===========================================================================
S("F3-U2: narratives array")
with TestClient(app) as client:
    data = client.get("/api/brief/TSM").json()
    narratives = data.get("narratives", [])
    T("narratives is a list", isinstance(narratives, list))
    T("at least 1 narrative", len(narratives) >= 1, f"len={len(narratives)}")

# ===========================================================================
# F3-U3: Each narrative has entropy_interpretation string
# ===========================================================================
S("F3-U3: entropy_interpretation")
with TestClient(app) as client:
    data = client.get("/api/brief/TSM").json()
    narratives = data.get("narratives", [])
    for nar in narratives:
        interp = nar.get("entropy_interpretation", "")
        T(f"narrative {nar['id']} has entropy_interpretation",
          isinstance(interp, str) and len(interp) > 0,
          f"interp='{interp}'")

# ===========================================================================
# F3-U4: risk_summary has all required fields
# ===========================================================================
S("F3-U4: risk_summary fields")
with TestClient(app) as client:
    data = client.get("/api/brief/TSM").json()
    rs = data.get("risk_summary", {})
    required = ["coordination_detected", "highest_burst_ratio", "dominant_direction",
                "narrative_count", "avg_entropy", "entropy_assessment"]
    for field in required:
        T(f"risk_summary has '{field}'", field in rs, f"keys={list(rs.keys())}")

# ===========================================================================
# F3-U5: GET /api/brief/INVALID returns 404
# ===========================================================================
S("F3-U5: Invalid ticker returns 404")
with TestClient(app) as client:
    resp = client.get("/api/brief/INVALIDTICKER")
    T("status 404", resp.status_code == 404, f"status={resp.status_code}")

# ===========================================================================
# F3-U6: Entropy interpretation matches expected ranges
# ===========================================================================
S("F3-U6: Entropy interpretation ranges")
from api.main import _interpret_entropy

T("None → 'Insufficient data'", _interpret_entropy(None) == "Insufficient data")
T("0.3 → 'Narrow sourcing'", "Narrow" in _interpret_entropy(0.3))
T("0.7 → 'Limited diversity'", "Limited" in _interpret_entropy(0.7))
T("1.5 → 'Multi-source'", "Multi-source" in _interpret_entropy(1.5))
T("2.5 → 'Broad coverage'", "Broad" in _interpret_entropy(2.5))

# ===========================================================================
# F3-U7: coordination_flags from MANIPULATION_INDICATORS
# ===========================================================================
S("F3-U7: coordination_flags")
with TestClient(app) as client:
    # TSM is in asset_class ac-001, linked to narratives via NARRATIVE_ASSETS
    data = client.get("/api/brief/TSM").json()
    narratives = data.get("narratives", [])
    for nar in narratives:
        expected_flags = sum(1 for mi in MANIPULATION_INDICATORS
                           if mi["narrative_id"] == nar["id"])
        T(f"narrative {nar['id']} coordination_flags={nar.get('coordination_flags')}",
          nar.get("coordination_flags") == expected_flags,
          f"expected={expected_flags}, got={nar.get('coordination_flags')}")

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"F3 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All F3 tests passed.")
