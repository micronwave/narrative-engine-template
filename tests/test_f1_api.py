"""
F1 — Lifecycle Stage Progression Tests

Unit:
  F1-U1: compute_lifecycle_stage returns "Emerging" for brand-new narrative
  F1-U2: compute_lifecycle_stage returns "Growing" when doc_count >= 8 and velocity > 0.02
  F1-U3: compute_lifecycle_stage returns "Mature" when days >= 5, entropy >= 1.2, docs >= 15
  F1-U4: compute_lifecycle_stage returns "Declining" when consecutive_declining >= 30 (or >= 18 with velocity < 0.008)
  F1-U5: compute_lifecycle_stage returns "Dormant" when declining >= 42 and velocity < 0.01
  F1-U6: Revival — Declining + velocity > 0.10 returns "Growing"
  F1-U7: Cannot skip stages — Emerging with high entropy still returns "Growing" not "Mature"
  F1-U8: GET /api/narratives includes "stage" field in response
"""

import sys
from pathlib import Path
from unittest.mock import patch

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from signals import compute_lifecycle_stage, get_narrative_age_days
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
# F1-U1: Emerging for brand-new narrative
# ===========================================================================
S("F1-U1: Emerging for brand-new narrative")
result = compute_lifecycle_stage(
    current_stage="Emerging", document_count=3, velocity_windowed=0.02,
    entropy=None, consecutive_declining_cycles=0, days_since_creation=1,
)
T("returns Emerging", result == "Emerging", f"got {result}")

# ===========================================================================
# F1-U2: Growing when doc_count >= 8 and velocity > 0.02
# ===========================================================================
S("F1-U2: Emerging → Growing")
result = compute_lifecycle_stage(
    current_stage="Emerging", document_count=9, velocity_windowed=0.08,
    entropy=0.5, consecutive_declining_cycles=0, days_since_creation=1,
    cycles_in_current_stage=3,
)
T("returns Growing", result == "Growing", f"got {result}")

# Still Emerging if velocity too low
result2 = compute_lifecycle_stage(
    current_stage="Emerging", document_count=9, velocity_windowed=0.015,
    entropy=0.5, consecutive_declining_cycles=0, days_since_creation=1,
    cycles_in_current_stage=3,
)
T("stays Emerging with low velocity", result2 == "Emerging", f"got {result2}")

# Still Emerging if doc count too low
result3 = compute_lifecycle_stage(
    current_stage="Emerging", document_count=5, velocity_windowed=0.08,
    entropy=0.5, consecutive_declining_cycles=0, days_since_creation=3,
)
T("stays Emerging with low doc count", result3 == "Emerging", f"got {result3}")

# ===========================================================================
# F1-U3: Mature when days >= 5, entropy >= 1.2, docs >= 15
# ===========================================================================
S("F1-U3: Growing → Mature")
result = compute_lifecycle_stage(
    current_stage="Growing", document_count=20, velocity_windowed=0.06,
    entropy=2.0, consecutive_declining_cycles=0, days_since_creation=7,
    cycles_in_current_stage=3,
)
T("returns Mature", result == "Mature", f"got {result}")

# Stays Growing if entropy too low
result2 = compute_lifecycle_stage(
    current_stage="Growing", document_count=20, velocity_windowed=0.06,
    entropy=1.0, consecutive_declining_cycles=0, days_since_creation=7,
)
T("stays Growing with low entropy", result2 == "Growing", f"got {result2}")

# ===========================================================================
# F1-U4: Declining when consecutive_declining >= 30 (or >= 18 with low velocity)
# ===========================================================================
S("F1-U4: Mature → Declining")
result = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.05,
    entropy=2.0, consecutive_declining_cycles=31, days_since_creation=10,
    cycles_in_current_stage=3,
)
T("returns Declining (consecutive >= 30)", result == "Declining", f"got {result}")

result2 = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=19, days_since_creation=10,
    cycles_in_current_stage=3,
)
T("returns Declining (consecutive >= 18 and velocity < 0.008)", result2 == "Declining", f"got {result2}")

# ===========================================================================
# F1-U5: Dormant when declining >= 42 and velocity < 0.01
# ===========================================================================
S("F1-U5: Declining → Dormant")
result = compute_lifecycle_stage(
    current_stage="Declining", document_count=20, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=43, days_since_creation=20,
    cycles_in_current_stage=3,
)
T("returns Dormant", result == "Dormant", f"got {result}")

# Stays Declining if velocity not low enough
result2 = compute_lifecycle_stage(
    current_stage="Declining", document_count=20, velocity_windowed=0.05,
    entropy=2.0, consecutive_declining_cycles=43, days_since_creation=20,
)
T("stays Declining with higher velocity", result2 == "Declining", f"got {result2}")

# ===========================================================================
# F1-U6: Revival — Declining + velocity > 0.10 → Growing
# ===========================================================================
S("F1-U6: Revival")
result = compute_lifecycle_stage(
    current_stage="Declining", document_count=20, velocity_windowed=0.15,
    entropy=2.0, consecutive_declining_cycles=5, days_since_creation=15,
)
T("Declining revives to Growing", result == "Growing", f"got {result}")

result2 = compute_lifecycle_stage(
    current_stage="Dormant", document_count=20, velocity_windowed=0.12,
    entropy=2.0, consecutive_declining_cycles=10, days_since_creation=30,
)
T("Dormant revives to Growing", result2 == "Growing", f"got {result2}")

# ===========================================================================
# F1-U7: Cannot skip stages
# ===========================================================================
S("F1-U7: Cannot skip stages")
result = compute_lifecycle_stage(
    current_stage="Emerging", document_count=50, velocity_windowed=0.20,
    entropy=3.0, consecutive_declining_cycles=0, days_since_creation=30,
    cycles_in_current_stage=3,
)
T("Emerging → Growing (not Mature)", result == "Growing", f"got {result}")

# ===========================================================================
# F1-U8: GET /api/narratives includes stage field
# ===========================================================================
S("F1-U8: GET /api/narratives includes stage field")
with TestClient(app) as client:
    resp = client.get("/api/narratives")
    T("status 200", resp.status_code == 200, f"status={resp.status_code}")
    data = resp.json()
    T("returns a list", isinstance(data, list))
    T("has at least 1 item", len(data) >= 1, f"len={len(data)}")
    if data:
        first = data[0]
        T("first item has stage field", "stage" in first, f"keys={list(first.keys())}")
        stage_val = first.get("stage", "")
        valid_stages = {"Emerging", "Growing", "Mature", "Declining", "Dormant"}
        T("stage is a valid value", stage_val in valid_stages, f"stage={stage_val}")

# ===========================================================================
# get_narrative_age_days utility
# ===========================================================================
S("F1 utility: get_narrative_age_days")
age = get_narrative_age_days("2026-03-15T05:49:39.618361+00:00")
T("returns positive int for past date", age >= 0, f"age={age}")
with patch("signals.logger.warning") as _warn:
    age_bad = get_narrative_age_days("invalid")
T("returns 0 for invalid date", age_bad == 0, f"age={age_bad}")
T("logs warning for invalid date", _warn.called, "expected logger.warning call")
if _warn.call_args:
    _warn_args, _warn_kwargs = _warn.call_args
    T(
        "warning includes parse failure context",
        "Could not parse created_at" in str(_warn_args[0]) and _warn_args[1] == "invalid",
        f"args={_warn_args}, kwargs={_warn_kwargs}",
    )

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"F1 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All F1 tests passed.")
