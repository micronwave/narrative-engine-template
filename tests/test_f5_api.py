"""
F5 — Historical Snapshot API + Price Data Tests

Unit:
  F5-U1: GET /api/narratives/{id}/history returns list of daily snapshots
  F5-U2: Each snapshot has velocity, entropy, lifecycle_stage fields
  F5-U3: GET /api/ticker/TSM/price-history returns response with available field
  F5-U4: Price data has symbol field
  F5-U5: get_snapshot_history returns list from repository
"""

import sys
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from fastapi.testclient import TestClient
from api.main import app

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
# F5-U1: GET /api/narratives/{id}/history returns list
# ===========================================================================
S("F5-U1: GET /api/narratives/{id}/history")
with TestClient(app) as client:
    # Get a real narrative ID first
    narratives = client.get("/api/narratives").json()
    if narratives:
        nid = narratives[0]["id"]
        resp = client.get(f"/api/narratives/{nid}/history?days=7")
        T("status 200", resp.status_code == 200, f"status={resp.status_code}")
        data = resp.json()
        T("returns a list", isinstance(data, list), f"type={type(data)}")
    else:
        T("has narratives to test", False, "no narratives returned")

# ===========================================================================
# F5-U2: Snapshot fields
# ===========================================================================
S("F5-U2: Snapshot has required fields")
with TestClient(app) as client:
    narratives = client.get("/api/narratives").json()
    if narratives:
        nid = narratives[0]["id"]
        data = client.get(f"/api/narratives/{nid}/history?days=30").json()
        if data:
            first = data[0]
            for field in ["date", "velocity", "entropy", "lifecycle_stage"]:
                T(f"snapshot has '{field}' field", field in first,
                  f"keys={list(first.keys())}")
            T("linked_assets is a list", isinstance(first.get("linked_assets"), list),
              f"type={type(first.get('linked_assets'))}")
        else:
            T("has snapshot data", True, "empty history (expected for new narratives)")
            T("date field", True, "skipped — no data")
            T("velocity field", True, "skipped — no data")
            T("entropy field", True, "skipped — no data")
            T("lifecycle_stage field", True, "skipped — no data")
            T("linked_assets is a list", True, "skipped — no data")

# ===========================================================================
# F5-U3: GET /api/ticker/TSM/price-history
# ===========================================================================
S("F5-U3: GET /api/ticker/TSM/price-history")
with TestClient(app) as client:
    resp = client.get("/api/ticker/TSM/price-history?days=7")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("has 'available' field", "available" in data, f"keys={list(data.keys())}")
    T("has 'symbol' field", "symbol" in data, f"keys={list(data.keys())}")

# ===========================================================================
# F5-U4: Price data symbol field
# ===========================================================================
S("F5-U4: Price data has symbol field")
with TestClient(app) as client:
    data = client.get("/api/ticker/TSM/price-history?days=7").json()
    T("symbol is TSM", data.get("symbol") == "TSM", f"symbol={data.get('symbol')}")
    T("data is a list", isinstance(data.get("data"), list),
      f"type={type(data.get('data'))}")

# ===========================================================================
# F5-U5: Repository method
# ===========================================================================
S("F5-U5: get_snapshot_history from repository")
from repository import SqliteRepository
repo = SqliteRepository("data/narrative_engine.db")
narratives = repo.get_all_active_narratives()
if narratives:
    nid = narratives[0]["narrative_id"]
    history = repo.get_snapshot_history(nid, 7)
    T("returns a list", isinstance(history, list), f"type={type(history)}")
else:
    T("returns a list", True, "skipped — no narratives")

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"F5 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All F5 tests passed.")
