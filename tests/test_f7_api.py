"""
F7 — Audit Fix Verification Tests

Unit:
  F7-U1: compute_ns_score clamped to [0, 1] with extreme entropy
  F7-U2: compute_ns_score clamped to [0, 1] with all-max inputs
  F7-U3: compute_ns_score correct for normal inputs (non-regression)
  F7-U4: compute_lifecycle_stage returns input unchanged for invalid stage
  F7-U5: get_narrative_age_days returns 0 for future date
  F7-U6: get_narrative_age_days returns 0 for today
  F7-U7: Settings has HDBSCAN_MIN_CLUSTER_SIZE and HDBSCAN_MIN_SAMPLES
  F7-U8: HDBSCAN settings validator rejects values < 2
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from signals import compute_ns_score, compute_lifecycle_stage, get_narrative_age_days
from settings import Settings

# ---------------------------------------------------------------------------
_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    _results.append((name, condition))
    marker = "\u2713" if condition else "\u2717"
    msg = f"  [{marker}] {name}"
    if details and not condition:
        msg += f"\n      details: {details}"
    elif details and condition:
        msg += f"  ({details})"
    print(msg)


# ===========================================================================
# F7-U1: ns_score clamped with extreme entropy
# ===========================================================================
S("F7-U1: ns_score clamped with extreme entropy")
ns = compute_ns_score(
    velocity=0.5, intent_weight=1.0, cross_source_score=1.0,
    cohesion=1.0, polarization=0.5, centrality=1.0,
    entropy=50.0, entropy_vocab_window=10,
)
T("ns_score <= 1.0 with entropy=50", ns <= 1.0, f"ns={ns}")
T("ns_score > 0.0", ns > 0.0, f"ns={ns}")

# ===========================================================================
# F7-U2: ns_score clamped with all-max inputs
# ===========================================================================
S("F7-U2: ns_score clamped with all-max inputs")
ns_max = compute_ns_score(
    velocity=10.0, intent_weight=1.0, cross_source_score=1.0,
    cohesion=1.0, polarization=10.0, centrality=1.0,
    entropy=100.0, entropy_vocab_window=2,
)
T("ns_score <= 1.0 with all-max", ns_max <= 1.0, f"ns={ns_max}")

# ===========================================================================
# F7-U3: ns_score correct for normal inputs (non-regression)
# ===========================================================================
S("F7-U3: ns_score normal inputs non-regression")
ns_normal = compute_ns_score(
    velocity=0.3, intent_weight=0.6, cross_source_score=0.5,
    cohesion=0.8, polarization=0.4, centrality=0.2,
    entropy=1.5, entropy_vocab_window=10,
)
T("ns_score in [0, 1]", 0.0 <= ns_normal <= 1.0, f"ns={ns_normal}")
T("ns_score > 0", ns_normal > 0.0, f"ns={ns_normal}")

# ===========================================================================
# F7-U4: invalid lifecycle stage returns input unchanged
# ===========================================================================
S("F7-U4: invalid lifecycle stage passthrough")
result = compute_lifecycle_stage(
    current_stage="InvalidStage", document_count=10,
    velocity_windowed=0.05, entropy=1.0,
    consecutive_declining_cycles=0, days_since_creation=5,
)
T("returns 'InvalidStage' unchanged", result == "InvalidStage", f"got {result}")

# Valid stages still work (non-regression)
result2 = compute_lifecycle_stage(
    current_stage="Emerging", document_count=3,
    velocity_windowed=0.02, entropy=None,
    consecutive_declining_cycles=0, days_since_creation=1,
)
T("Emerging still works", result2 == "Emerging", f"got {result2}")

# ===========================================================================
# F7-U5: get_narrative_age_days returns 0 for future date
# ===========================================================================
S("F7-U5: get_narrative_age_days future date")
future = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
age_future = get_narrative_age_days(future)
T("returns 0 for future date", age_future == 0, f"age={age_future}")

# ===========================================================================
# F7-U6: get_narrative_age_days returns 0 for today
# ===========================================================================
S("F7-U6: get_narrative_age_days today")
today = datetime.now(timezone.utc).isoformat()
age_today = get_narrative_age_days(today)
T("returns 0 for today", age_today == 0, f"age={age_today}")

# ===========================================================================
# F7-U7: Settings has HDBSCAN fields
# ===========================================================================
S("F7-U7: Settings HDBSCAN fields")
s = Settings()
T("HDBSCAN_MIN_CLUSTER_SIZE exists", hasattr(s, "HDBSCAN_MIN_CLUSTER_SIZE"),
  f"value={getattr(s, 'HDBSCAN_MIN_CLUSTER_SIZE', 'MISSING')}")
T("HDBSCAN_MIN_SAMPLES exists", hasattr(s, "HDBSCAN_MIN_SAMPLES"),
  f"value={getattr(s, 'HDBSCAN_MIN_SAMPLES', 'MISSING')}")
T("HDBSCAN_MIN_CLUSTER_SIZE default is 8", s.HDBSCAN_MIN_CLUSTER_SIZE == 8,
  f"value={s.HDBSCAN_MIN_CLUSTER_SIZE}")
T("HDBSCAN_MIN_SAMPLES default is 5", s.HDBSCAN_MIN_SAMPLES == 5,
  f"value={s.HDBSCAN_MIN_SAMPLES}")

# ===========================================================================
# F7-U8: HDBSCAN settings validator rejects < 2
# ===========================================================================
S("F7-U8: HDBSCAN settings validation")
try:
    Settings(ANTHROPIC_API_KEY="test-key", HDBSCAN_MIN_CLUSTER_SIZE=1)
    T("rejects HDBSCAN_MIN_CLUSTER_SIZE=1", False, "no error raised")
except Exception:
    T("rejects HDBSCAN_MIN_CLUSTER_SIZE=1", True)

try:
    Settings(ANTHROPIC_API_KEY="test-key", HDBSCAN_MIN_SAMPLES=0)
    T("rejects HDBSCAN_MIN_SAMPLES=0", False, "no error raised")
except Exception:
    T("rejects HDBSCAN_MIN_SAMPLES=0", True)

try:
    Settings(ANTHROPIC_API_KEY="test-key", HDBSCAN_MIN_CLUSTER_SIZE=5, HDBSCAN_MIN_SAMPLES=3)
    T("accepts valid HDBSCAN params", True)
except Exception as e:
    T("accepts valid HDBSCAN params", False, str(e))

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"F7 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  \u2717 {name}")
    sys.exit(1)
else:
    print("All F7 tests passed.")
