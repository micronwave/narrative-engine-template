"""
Health Fix 8 — Remaining partial issues

Section 1: Burst velocity cleanup
  T1: zero baseline returns ratio 0.0 and is_burst False
  T2: spike above threshold triggers burst
  T3: below threshold does not trigger burst

Section 2: Velocity behavior documentation
  T4: single centroid history still returns 0.0 as expected
"""

import sys
from pathlib import Path

import numpy as np

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from signals import compute_burst_velocity, compute_velocity_windowed

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


S("Section 1: Burst velocity cleanup")

result = compute_burst_velocity(recent_doc_count=5, baseline_docs_per_window=0.0)
T(
    "T1: zero baseline returns ratio 0.0",
    result["ratio"] == 0.0 and result["is_burst"] is False,
    f"got {result}",
)

result = compute_burst_velocity(
    recent_doc_count=9,
    baseline_docs_per_window=2.0,
    alert_ratio=3.0,
)
T(
    "T2: spike above threshold triggers burst",
    result["is_burst"] is True and result["ratio"] == 4.5,
    f"got {result}",
)

result = compute_burst_velocity(
    recent_doc_count=5,
    baseline_docs_per_window=2.0,
    alert_ratio=3.0,
)
T(
    "T3: below threshold does not trigger burst",
    result["is_burst"] is False and result["ratio"] == 2.5,
    f"got {result}",
)


S("Section 2: Velocity behavior documentation")

single_vec = np.ones(4, dtype=np.float32)
T(
    "T4: single centroid history returns 0.0",
    compute_velocity_windowed([single_vec], window_days=7) == 0.0,
    "single-entry history returns 0.0",
)


print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Health Fix 8 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All Health Fix 8 tests passed.")
