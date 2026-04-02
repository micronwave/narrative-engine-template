"""
D3 — Narrative Impact Score + Stocks API Tests

Unit:
  D3-U1: calculate_narrative_impact_scores returns dict with one entry per security
  D3-U2: All returned scores are integers in range 1–100
  D3-U3: With the D1 stub data, at least 3 different score values exist (not all same)
  D3-U4: GET /api/stocks returns list with narrative_impact_score populated (> 0 for securities with associations)
  D3-U5: GET /api/stocks?sort_by=impact&sort_order=desc — items in descending score order
  D3-U6: GET /api/stocks?sort_by=symbol&sort_order=asc — items in ascending alphabetical order
  D3-U7: GET /api/stocks?asset_class=ac-001 — only returns securities with asset_class_id == "ac-001"
  D3-U8: GET /api/stocks?min_impact=50 — excludes securities with narrative_impact_score < 50
  D3-U9: GET /api/stocks/{symbol} returns security with "narratives" field (list)
  D3-U10: GET /api/stocks/INVALID returns 404

Integration:
  D3-I1: GET /api/stocks with combined filters returns correctly filtered, sorted list
"""

import sys
from pathlib import Path

# Add project root to sys.path
_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Add api/ to sys.path for finnhub_service
_API_DIR = str(Path(__file__).parent.parent / "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

from fastapi.testclient import TestClient
from api.main import app, calculate_narrative_impact_scores, TRACKED_SECURITIES, NARRATIVE_ASSETS

# ---------------------------------------------------------------------------
# Test runner helpers
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Pre-compute impact scores using stub narratives (entropy=0.5)
# This ensures narrative_impact_score > 0 for securities with associations
# ---------------------------------------------------------------------------
import api.main as main_module

stub_nar_ids = list({na["narrative_id"] for na in NARRATIVE_ASSETS})
stub_narratives = [{"narrative_id": nid, "entropy": 0.5} for nid in stub_nar_ids]
precomputed_scores = calculate_narrative_impact_scores(
    TRACKED_SECURITIES, NARRATIVE_ASSETS, stub_narratives
)
# Apply scores to module-level TRACKED_SECURITIES so API endpoints return them
for sec in main_module.TRACKED_SECURITIES:
    sec["narrative_impact_score"] = precomputed_scores.get(sec["id"], 0)


# ===========================================================================
# D3-U1: calculate_narrative_impact_scores returns dict with one entry per security
# ===========================================================================
S("D3-U1: calculate_narrative_impact_scores — one entry per security")
scores = calculate_narrative_impact_scores(TRACKED_SECURITIES, NARRATIVE_ASSETS, stub_narratives)
T("returns a dict", isinstance(scores, dict))
T(
    "one entry per security",
    len(scores) == len(TRACKED_SECURITIES),
    f"len(scores)={len(scores)}, len(securities)={len(TRACKED_SECURITIES)}",
)
T(
    "all security ids are keys",
    all(sec["id"] in scores for sec in TRACKED_SECURITIES),
    f"missing={[s['id'] for s in TRACKED_SECURITIES if s['id'] not in scores]}",
)

# ===========================================================================
# D3-U2: All returned scores are integers in range 1–100
# ===========================================================================
S("D3-U2: All scores are integers in 1–100")
T("all values are int", all(isinstance(v, int) for v in scores.values()), f"types={set(type(v).__name__ for v in scores.values())}")
T("all scores >= 1", all(v >= 1 for v in scores.values()), f"min={min(scores.values())}")
T("all scores <= 100", all(v <= 100 for v in scores.values()), f"max={max(scores.values())}")

# ===========================================================================
# D3-U3: At least 3 different score values exist with stub data
# ===========================================================================
S("D3-U3: At least 3 distinct score values with stub data")
distinct_scores = set(scores.values())
T(
    "at least 3 distinct score values",
    len(distinct_scores) >= 3,
    f"distinct={sorted(distinct_scores)}",
)

# ===========================================================================
# D3-U4: GET /api/stocks returns list with narrative_impact_score populated
# ===========================================================================
S("D3-U4: GET /api/stocks — narrative_impact_score populated")
with TestClient(app) as client:
    resp = client.get("/api/stocks")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("returns a list", isinstance(data, list))
    T("list is non-empty", len(data) > 0, f"len={len(data)}")
    if data:
        first = data[0]
        T("narrative_impact_score field exists", "narrative_impact_score" in first, f"fields={list(first.keys())}")
        # Securities with asset associations should have score > 0
        associated_ids = {na["asset_class_id"] for na in NARRATIVE_ASSETS}
        associated_secs = [s for s in data if s["asset_class_id"] in associated_ids]
        T(
            "securities with associations have score > 0",
            all(s["narrative_impact_score"] > 0 for s in associated_secs),
            f"zero-scores={[s['symbol'] for s in associated_secs if s['narrative_impact_score'] == 0]}",
        )

# ===========================================================================
# D3-U5: GET /api/stocks?sort_by=impact&sort_order=desc — descending order
# ===========================================================================
S("D3-U5: GET /api/stocks?sort_by=impact&sort_order=desc")
with TestClient(app) as client:
    resp = client.get("/api/stocks?sort_by=impact&sort_order=desc")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    scores_list = [s["narrative_impact_score"] for s in data]
    T(
        "scores are in descending order",
        scores_list == sorted(scores_list, reverse=True),
        f"scores={scores_list[:5]}...",
    )

# ===========================================================================
# D3-U6: GET /api/stocks?sort_by=symbol&sort_order=asc — ascending alpha order
# ===========================================================================
S("D3-U6: GET /api/stocks?sort_by=symbol&sort_order=asc")
with TestClient(app) as client:
    resp = client.get("/api/stocks?sort_by=symbol&sort_order=asc")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    symbols = [s["symbol"] for s in data]
    T(
        "symbols are in ascending alphabetical order",
        symbols == sorted(symbols),
        f"symbols={symbols[:5]}...",
    )

# ===========================================================================
# D3-U7: GET /api/stocks?asset_class=ac-001 — only returns ac-001 securities
# ===========================================================================
S("D3-U7: GET /api/stocks?asset_class=ac-001 — filter by asset class")
with TestClient(app) as client:
    resp = client.get("/api/stocks?asset_class=ac-001")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("returns a list", isinstance(data, list))
    T("at least 1 result", len(data) > 0, f"len={len(data)}")
    T(
        "all items have asset_class_id == 'ac-001'",
        all(s["asset_class_id"] == "ac-001" for s in data),
        f"ids={set(s['asset_class_id'] for s in data)}",
    )
    T(
        "no items from other asset classes",
        not any(s["asset_class_id"] != "ac-001" for s in data),
    )

# ===========================================================================
# D3-U8: GET /api/stocks?min_impact=50 — excludes securities with score < 50
# ===========================================================================
S("D3-U8: GET /api/stocks?min_impact=50 — filter by min impact")
with TestClient(app) as client:
    resp = client.get("/api/stocks?min_impact=50")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T(
        "all items have narrative_impact_score >= 50",
        all(s["narrative_impact_score"] >= 50 for s in data),
        f"below_50={[s['symbol'] for s in data if s['narrative_impact_score'] < 50]}",
    )
    # Verify items with score < 50 were excluded
    all_resp = client.get("/api/stocks")
    all_data = all_resp.json()
    below_50 = [s for s in all_data if s["narrative_impact_score"] < 50]
    T(
        "items with score < 50 are excluded",
        len(data) == len(all_data) - len(below_50),
        f"expected={len(all_data) - len(below_50)}, got={len(data)}",
    )

# ===========================================================================
# D3-U9: GET /api/stocks/{symbol} returns security with "narratives" field
# ===========================================================================
S("D3-U9: GET /api/stocks/{symbol} — returns security with narratives field")
with TestClient(app) as client:
    resp = client.get("/api/stocks/TSM")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("symbol is TSM", data.get("symbol") == "TSM", f"symbol={data.get('symbol')}")
    T("narratives field exists", "narratives" in data, f"fields={list(data.keys())}")
    T("narratives is a list", isinstance(data.get("narratives"), list))
    if data.get("narratives"):
        nar = data["narratives"][0]
        T("narrative_id field present", "narrative_id" in nar, f"nar_keys={list(nar.keys())}")
        T("narrative_name field present", "narrative_name" in nar)
        T("exposure_score field present", "exposure_score" in nar)
        T("direction field present", "direction" in nar)

# ===========================================================================
# D3-U10: GET /api/stocks/INVALID returns 404
# ===========================================================================
S("D3-U10: GET /api/stocks/INVALID returns 404")
with TestClient(app) as client:
    resp = client.get("/api/stocks/INVALIDSYMBOL")
    T("status 404", resp.status_code == 404, f"status={resp.status_code}")

# ===========================================================================
# D3-I1: Combined filters (asset_class + min_impact + sort) work correctly
# ===========================================================================
S("D3-I1: Combined filters — asset_class + min_impact + sort_by=symbol")
with TestClient(app) as client:
    # All securities, to understand what to expect
    all_resp = client.get("/api/stocks")
    all_data = all_resp.json()
    ac001_above_50 = [s for s in all_data if s["asset_class_id"] == "ac-001" and s["narrative_impact_score"] >= 50]

    resp = client.get("/api/stocks?asset_class=ac-001&min_impact=50&sort_by=symbol&sort_order=asc")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T(
        "all items have asset_class_id == 'ac-001'",
        all(s["asset_class_id"] == "ac-001" for s in data),
    )
    T(
        "all items have narrative_impact_score >= 50",
        all(s["narrative_impact_score"] >= 50 for s in data),
    )
    symbols = [s["symbol"] for s in data]
    T("symbols are sorted ascending", symbols == sorted(symbols), f"symbols={symbols}")
    T(
        "result count matches expected",
        len(data) == len(ac001_above_50),
        f"expected={len(ac001_above_50)}, got={len(data)}",
    )

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"D3 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All D3 tests passed.")
