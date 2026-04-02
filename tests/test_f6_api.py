"""
F6 — Velocity-Price Correlation Tests

Unit:
  F6-U1: compute_velocity_price_correlation returns valid correlation dict
  F6-U2: Correlation is between -1 and 1
  F6-U3: Returns is_significant=false when n_observations < 30
  F6-U4: GET /api/correlations/{id}/{ticker} returns 200
  F6-U5: Interpretation string matches correlation magnitude
"""

import sys
from pathlib import Path

_API_DIR = str(Path(__file__).parent.parent / "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from api.correlation_service import compute_velocity_price_correlation
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
# Test data
# ===========================================================================
VELOCITY_HIST = [
    {"date": f"2026-03-{d:02d}", "velocity": 0.01 * d}
    for d in range(1, 32)
]
PRICE_HIST = [
    {"date": f"2026-03-{d:02d}", "close": 100 + d, "change_pct": 0.5 * d}
    for d in range(1, 32)
]

SMALL_VELOCITY = [{"date": "2026-03-01", "velocity": 0.1}]
SMALL_PRICE = [{"date": "2026-03-01", "close": 100, "change_pct": 1.0}]

# ===========================================================================
# F6-U1: Returns valid correlation dict
# ===========================================================================
S("F6-U1: compute_velocity_price_correlation returns valid dict")
result = compute_velocity_price_correlation(VELOCITY_HIST, PRICE_HIST, lead_days=1)
T("has correlation field", "correlation" in result, f"keys={list(result.keys())}")
T("has p_value field", "p_value" in result)
T("has n_observations field", "n_observations" in result)
T("has is_significant field", "is_significant" in result)
T("has lead_days field", "lead_days" in result)
T("has interpretation field", "interpretation" in result)
T("lead_days matches input", result["lead_days"] == 1, f"lead_days={result['lead_days']}")

# ===========================================================================
# F6-U2: Correlation is between -1 and 1
# ===========================================================================
S("F6-U2: Correlation is between -1 and 1")
r = result["correlation"]
T("correlation >= -1", r >= -1.0, f"r={r}")
T("correlation <= 1", r <= 1.0, f"r={r}")
T("n_observations > 0", result["n_observations"] > 0,
  f"n={result['n_observations']}")

# ===========================================================================
# F6-U3: is_significant=false when n_observations < 30
# ===========================================================================
S("F6-U3: is_significant=false with insufficient data")
small_result = compute_velocity_price_correlation(SMALL_VELOCITY, SMALL_PRICE, lead_days=0)
T("is_significant is False", small_result["is_significant"] is False,
  f"is_significant={small_result['is_significant']}")
T("interpretation mentions insufficient or collecting",
  "Insufficient" in small_result["interpretation"] or "Collecting" in small_result["interpretation"],
  f"interp='{small_result['interpretation']}'")

# ===========================================================================
# F6-U4: GET /api/correlations/{id}/{ticker}
# ===========================================================================
S("F6-U4: GET /api/correlation endpoint")
with TestClient(app) as client:
    narratives = client.get("/api/narratives").json()
    if narratives:
        nid = narratives[0]["id"]
        resp = client.get(f"/api/correlations/{nid}/TSM?lead_days=1")
        T("status 200", resp.status_code == 200, f"status={resp.status_code}")
        data = resp.json()
        T("has narrative_id", "narrative_id" in data, f"keys={list(data.keys())}")
        T("has ticker", "ticker" in data)
        T("has correlation", "correlation" in data)
        T("ticker is TSM", data.get("ticker") == "TSM", f"ticker={data.get('ticker')}")
    else:
        T("has narratives", False, "no narratives")

# ===========================================================================
# F6-U5: Interpretation matches correlation magnitude
# ===========================================================================
S("F6-U5: Interpretation string")
T("30-day result has interpretation",
  isinstance(result.get("interpretation"), str) and len(result["interpretation"]) > 0,
  f"interp='{result.get('interpretation')}'")

# Test with zero-correlation data
zero_vel = [{"date": f"2026-03-{d:02d}", "velocity": 0.1} for d in range(1, 32)]
zero_price = [{"date": f"2026-03-{d:02d}", "close": 100, "change_pct": (-1)**d * 0.5} for d in range(1, 32)]
zero_result = compute_velocity_price_correlation(zero_vel, zero_price, lead_days=0)
T("near-zero correlation gives 'No meaningful' or 'Weak'",
  "No meaningful" in zero_result["interpretation"] or "Weak" in zero_result["interpretation"]
  or "Collecting" in zero_result["interpretation"],
  f"interp='{zero_result['interpretation']}'")

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"F6 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All F6 tests passed.")
