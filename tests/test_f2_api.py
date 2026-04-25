"""
F2 — Pipeline Frequency + Burst Velocity Tests

Unit:
  F2-U1: compute_burst_velocity returns ratio 1.0 when rate equals baseline
  F2-U2: compute_burst_velocity returns is_burst=true when ratio >= 3.0
  F2-U3: compute_burst_velocity handles zero baseline gracefully (returns ratio 0)
  F2-U4: Settings have PIPELINE_FREQUENCY_HOURS and BURST_VELOCITY_ALERT_RATIO
  F2-U5: GET /api/narratives response includes burst_velocity field
"""

import sys
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from signals import compute_burst_velocity
from settings import Settings
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
# F2-U1: ratio 1.0 when rate equals baseline
# ===========================================================================
S("F2-U1: ratio 1.0 when rate equals baseline")
result = compute_burst_velocity(recent_doc_count=10, baseline_docs_per_window=10.0)
T("ratio is 1.0", result["ratio"] == 1.0, f"ratio={result['ratio']}")
T("is_burst is False", result["is_burst"] is False)
T("rate is 10", result["rate"] == 10.0, f"rate={result['rate']}")
T("baseline is 10", result["baseline"] == 10.0, f"baseline={result['baseline']}")

# ===========================================================================
# F2-U2: is_burst=true when ratio >= 3.0
# ===========================================================================
S("F2-U2: is_burst when ratio >= 3.0")
result = compute_burst_velocity(recent_doc_count=30, baseline_docs_per_window=10.0)
T("ratio is 3.0", result["ratio"] == 3.0, f"ratio={result['ratio']}")
T("is_burst is True", result["is_burst"] is True)

result2 = compute_burst_velocity(recent_doc_count=50, baseline_docs_per_window=10.0)
T("ratio 5.0 is_burst True", result2["is_burst"] is True, f"ratio={result2['ratio']}")

result3 = compute_burst_velocity(recent_doc_count=20, baseline_docs_per_window=10.0)
T("ratio 2.0 is_burst False", result3["is_burst"] is False, f"ratio={result3['ratio']}")

# ===========================================================================
# F2-U3: zero baseline → graceful degradation
# ===========================================================================
S("F2-U3: zero baseline graceful degradation")
result = compute_burst_velocity(recent_doc_count=15, baseline_docs_per_window=0.0)
T("ratio is 0 with zero baseline", result["ratio"] == 0.0, f"ratio={result['ratio']}")
T("is_burst is False with zero baseline", result["is_burst"] is False)
T("baseline is 0", result["baseline"] == 0.0, f"baseline={result['baseline']}")
T("rate still populated", result["rate"] == 15.0, f"rate={result['rate']}")

result_neg = compute_burst_velocity(recent_doc_count=5, baseline_docs_per_window=-1.0)
T("negative baseline also safe", result_neg["ratio"] == 0.0, f"ratio={result_neg['ratio']}")

# ===========================================================================
# F2-U4: Settings exist
# ===========================================================================
S("F2-U4: Settings have burst velocity fields")
s = Settings()
T("PIPELINE_FREQUENCY_HOURS exists", hasattr(s, "PIPELINE_FREQUENCY_HOURS"),
  f"value={getattr(s, 'PIPELINE_FREQUENCY_HOURS', 'MISSING')}")
T("BURST_VELOCITY_ALERT_RATIO exists", hasattr(s, "BURST_VELOCITY_ALERT_RATIO"),
  f"value={getattr(s, 'BURST_VELOCITY_ALERT_RATIO', 'MISSING')}")
T("PIPELINE_FREQUENCY_HOURS default is 4", s.PIPELINE_FREQUENCY_HOURS == 4,
  f"value={s.PIPELINE_FREQUENCY_HOURS}")

# ===========================================================================
# F2-U5: API response includes burst_velocity field
# ===========================================================================
S("F2-U5: GET /api/narratives includes burst_velocity")
with TestClient(app) as client:
    resp = client.get("/api/narratives")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("returns a list", isinstance(data, list))
    if data:
        first = data[0]
        T("first item has burst_velocity field", "burst_velocity" in first,
          f"keys={list(first.keys())}")

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"F2 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All F2 tests passed.")
