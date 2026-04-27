"""
V3 Phase 2 — Core Features Tests

  V3-PORT-1: GET /api/portfolio returns portfolio (or null)
  V3-PORT-2: POST /api/portfolio/holdings creates holding and auto-creates portfolio
  V3-PORT-3: GET /api/portfolio returns the newly added holding
  V3-PORT-4: GET /api/portfolio/exposure returns exposure list
  V3-PORT-5: DELETE /api/portfolio/holdings/{id} removes holding
  V3-PORT-6: Duplicate holding returns already_exists
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
# Portfolio Tests
# ===========================================================================
S("V3-PORT: Portfolio CRUD")

resp = client.get("/api/portfolio")
T("PORT-1: GET /api/portfolio → 200", resp.status_code == 200)
data = resp.json()
T("PORT-1a: has holdings list", isinstance(data.get("holdings"), list))

# Add a holding
resp = client.post("/api/portfolio/holdings", json={"ticker": "AAPL", "shares": 10})
T("PORT-2: POST holding → 200", resp.status_code == 200, f"body={resp.json()}")
add_data = resp.json()
T("PORT-2a: status is added", add_data.get("status") == "added")
holding_id = add_data.get("holding_id", "")

# Verify it appears
resp = client.get("/api/portfolio")
data = resp.json()
tickers = [h.get("ticker") for h in data.get("holdings", [])]
T("PORT-3: AAPL in holdings", "AAPL" in tickers, f"tickers={tickers}")

# Duplicate
resp = client.post("/api/portfolio/holdings", json={"ticker": "AAPL"})
T("PORT-6: Duplicate → already_exists", resp.json().get("status") == "already_exists")

# Exposure
resp = client.get("/api/portfolio/exposure")
T("PORT-4: GET exposure → 200", resp.status_code == 200)
exp_data = resp.json()
T("PORT-4a: has exposures list", isinstance(exp_data.get("exposures"), list))

# Remove
if holding_id:
    resp = client.delete(f"/api/portfolio/holdings/{holding_id}")
    T("PORT-5: DELETE holding → 200", resp.status_code == 200)
    T("PORT-5a: status removed", resp.json().get("status") == "removed")

# Verify removal
resp = client.get("/api/portfolio")
data = resp.json()
tickers_after = [h.get("ticker") for h in data.get("holdings", [])]
T("PORT-5b: AAPL removed", "AAPL" not in tickers_after)


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
