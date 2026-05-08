"""
V3 Phase 2 — Core Features Tests

  V3-TL-1: GET /api/narratives/{id}/timeline returns timeline
  V3-TL-2: Timeline entries have expected fields
  V3-TL-3: GET /api/narratives/{id}/compare returns comparison
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
_API_DIR = str(Path(__file__).parent.parent / "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app  # noqa: E402

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


client = TestClient(app)


# ===========================================================================
# Timeline Tests
# ===========================================================================
S("V3-TL: Story Timeline")

narratives_resp = client.get("/api/narratives")
narratives = narratives_resp.json()
has_narratives = isinstance(narratives, list) and len(narratives) > 0

if has_narratives:
    test_id = narratives[0]["id"]

    resp = client.get(f"/api/narratives/{test_id}/timeline?days=7")
    T("TL-1: timeline → 200", resp.status_code == 200)
    tl_data = resp.json()
    T("TL-1a: has timeline list", isinstance(tl_data.get("timeline"), list))

    timeline = tl_data.get("timeline", [])
    if timeline:
        entry = timeline[0]
        T("TL-2a: entry has date", "date" in entry)
        T("TL-2b: entry has ns_score", "ns_score" in entry)
        T("TL-2c: entry has velocity", "velocity" in entry)
        T("TL-2d: entry has mutations", isinstance(entry.get("mutations"), list))

    # Compare endpoint
    _today = datetime.now().strftime("%Y-%m-%d")
    _three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    resp = client.get(f"/api/narratives/{test_id}/compare?date1={_three_days_ago}&date2={_today}")
    T("TL-3: compare → 200", resp.status_code == 200)
    cmp = resp.json()
    T("TL-3a: has differences list", isinstance(cmp.get("differences"), list))
    T("TL-3b: has narrative_name", "narrative_name" in cmp)


# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"V3 Phase 2 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All V3 Phase 2 tests passed.")
