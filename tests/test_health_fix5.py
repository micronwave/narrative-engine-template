"""
Health Fix 5 — Stage Transitions, Cohesion, and Asset Mapping Thresholds

Section A: Stage transition fallback paths
  T1: Emerging -> Growing via age+doc fallback (doc>=10, age>=2, cycles>=3)
  T2: Emerging stays Emerging when age fallback conditions not met (doc<10)
  T3: Emerging stays Emerging when age fallback conditions not met (age<2)
  T4: Growing -> Mature via volume fallback (doc>=50, age>=7, cycles>=3)
  T5: Growing stays Growing when volume fallback conditions not met (doc<50)
  T6: Growing stays Growing when volume fallback conditions not met (age<7)
  T7: Full lifecycle: Emerging -> Growing -> Mature -> Declining -> Dormant
  T8: Fallback blocked by hysteresis (cycles<3)
  T9: Original velocity-based Emerging->Growing still works
  T10: Original entropy-based Growing->Mature still works
  T11: Declining is reachable from Mature via low velocity

Section B: Cohesion edge cases
  T12: Cohesion returns 0.0 for empty list
  T13: Cohesion returns 0.0 for single embedding
  T14: Cohesion returns 1.0 for two identical embeddings
  T15: Cohesion returns intermediate value for different embeddings

Section C: Asset mapping threshold
  T16: ASSET_MAPPING_MIN_SIMILARITY defaults to 0.60
  T17: AssetMapper.map_narrative default min_similarity is 0.60
  T18: Weak asset link cleanup migration removes entries below 0.60
  T19: Cleanup migration preserves entries at or above 0.60
  T20: Cleanup migration is idempotent
"""

import inspect
import json
import os
import sys
import sqlite3
import tempfile
import uuid
from pathlib import Path

import numpy as np

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-unit-tests")

from signals import compute_lifecycle_stage, compute_cohesion
from settings import Settings
from repository import SqliteRepository

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
# Section A: Stage transition fallback paths
# ===========================================================================

S("T1: Emerging -> Growing via age+doc fallback")
result = compute_lifecycle_stage(
    current_stage="Emerging", document_count=12, velocity_windowed=0.0,
    entropy=None, consecutive_declining_cycles=0, days_since_creation=5,
    cycles_in_current_stage=3,
)
T("T1: age+doc fallback triggers Growing", result == "Growing", f"got {result}")

S("T2: Emerging stays Emerging (doc < 10)")
result = compute_lifecycle_stage(
    current_stage="Emerging", document_count=8, velocity_windowed=0.0,
    entropy=None, consecutive_declining_cycles=0, days_since_creation=5,
    cycles_in_current_stage=10,
)
T("T2: doc=8 not enough for fallback", result == "Emerging", f"got {result}")

S("T3: Emerging stays Emerging (age < 2)")
result = compute_lifecycle_stage(
    current_stage="Emerging", document_count=20, velocity_windowed=0.0,
    entropy=None, consecutive_declining_cycles=0, days_since_creation=1,
    cycles_in_current_stage=10,
)
T("T3: age=1 not enough for fallback", result == "Emerging", f"got {result}")

S("T4: Growing -> Mature via volume fallback")
result = compute_lifecycle_stage(
    current_stage="Growing", document_count=55, velocity_windowed=0.0,
    entropy=0.5, consecutive_declining_cycles=0, days_since_creation=10,
    cycles_in_current_stage=3,
)
T("T4: volume fallback triggers Mature", result == "Mature", f"got {result}")

S("T5: Growing stays Growing (doc < 50)")
result = compute_lifecycle_stage(
    current_stage="Growing", document_count=40, velocity_windowed=0.0,
    entropy=0.5, consecutive_declining_cycles=0, days_since_creation=10,
    cycles_in_current_stage=10,
)
T("T5: doc=40 not enough for fallback", result == "Growing", f"got {result}")

S("T6: Growing stays Growing (age < 7)")
result = compute_lifecycle_stage(
    current_stage="Growing", document_count=60, velocity_windowed=0.0,
    entropy=0.5, consecutive_declining_cycles=0, days_since_creation=5,
    cycles_in_current_stage=10,
)
T("T6: age=5 not enough for volume fallback", result == "Growing", f"got {result}")

S("T7: Full lifecycle Emerging -> Dormant")
stage = "Emerging"
# Step 1: Emerging -> Growing via fallback
stage = compute_lifecycle_stage(
    current_stage=stage, document_count=15, velocity_windowed=0.0,
    entropy=None, consecutive_declining_cycles=0, days_since_creation=3,
    cycles_in_current_stage=5,
)
T("T7a: Emerging -> Growing", stage == "Growing", f"got {stage}")
# Step 2: Growing -> Mature via volume fallback
stage = compute_lifecycle_stage(
    current_stage=stage, document_count=60, velocity_windowed=0.0,
    entropy=None, consecutive_declining_cycles=0, days_since_creation=10,
    cycles_in_current_stage=5,
)
T("T7b: Growing -> Mature", stage == "Mature", f"got {stage}")
# Step 3: Mature -> Declining (needs 18+ cycles with low velocity, or 30+ cycles outright)
stage = compute_lifecycle_stage(
    current_stage=stage, document_count=60, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=20, days_since_creation=15,
    cycles_in_current_stage=5,
)
T("T7c: Mature -> Declining", stage == "Declining", f"got {stage}")
# Step 4: Declining -> Dormant (needs 42+ cycles with low velocity)
stage = compute_lifecycle_stage(
    current_stage=stage, document_count=60, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=45, days_since_creation=25,
    cycles_in_current_stage=5,
)
T("T7d: Declining -> Dormant", stage == "Dormant", f"got {stage}")

S("T8: Fallback blocked by hysteresis (cycles<3)")
result = compute_lifecycle_stage(
    current_stage="Emerging", document_count=15, velocity_windowed=0.0,
    entropy=None, consecutive_declining_cycles=0, days_since_creation=5,
    cycles_in_current_stage=2,
)
T("T8: hysteresis blocks fallback at cycles=2", result == "Emerging", f"got {result}")

S("T9: Original velocity-based Emerging->Growing still works")
result = compute_lifecycle_stage(
    current_stage="Emerging", document_count=10, velocity_windowed=0.08,
    entropy=0.5, consecutive_declining_cycles=0, days_since_creation=1,
    cycles_in_current_stage=3,
)
T("T9: velocity path still works", result == "Growing", f"got {result}")

S("T10: Original entropy-based Growing->Mature still works")
result = compute_lifecycle_stage(
    current_stage="Growing", document_count=20, velocity_windowed=0.06,
    entropy=2.0, consecutive_declining_cycles=0, days_since_creation=7,
    cycles_in_current_stage=3,
)
T("T10: entropy path still works", result == "Mature", f"got {result}")

S("T11: Declining reachable from Mature via sustained low velocity (18+ cycles)")
result = compute_lifecycle_stage(
    current_stage="Mature", document_count=30, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=20, days_since_creation=15,
    cycles_in_current_stage=5,
)
T("T11: low velocity triggers Declining", result == "Declining", f"got {result}")

# ===========================================================================
# Section B: Cohesion edge cases
# ===========================================================================

S("T12: Cohesion returns 0.0 for empty list")
T("T12: empty list", compute_cohesion([]) == 0.0, f"got {compute_cohesion([])}")

S("T13: Cohesion returns 0.0 for single embedding")
single = [np.ones(768, dtype=np.float32)]
T("T13: single embedding", compute_cohesion(single) == 0.0, f"got {compute_cohesion(single)}")

S("T14: Cohesion returns ~1.0 for two identical embeddings")
vec = np.random.randn(768).astype(np.float32)
vec = vec / np.linalg.norm(vec)  # L2-normalize
coh = compute_cohesion([vec, vec.copy()])
T("T14: identical embeddings", abs(coh - 1.0) < 0.01, f"got {coh:.4f}")

S("T15: Cohesion intermediate for different embeddings")
v1 = np.zeros(768, dtype=np.float32); v1[0] = 1.0
v2 = np.zeros(768, dtype=np.float32); v2[1] = 1.0
v3 = np.zeros(768, dtype=np.float32); v3[0] = 0.707; v3[1] = 0.707
coh2 = compute_cohesion([v1, v2, v3])
T("T15: intermediate cohesion", 0.0 < coh2 < 1.0, f"got {coh2:.4f}")

# ===========================================================================
# Section C: Asset mapping threshold
# ===========================================================================

S("T16: ASSET_MAPPING_MIN_SIMILARITY defaults to 0.60")
_settings = Settings()
T("T16: setting is 0.60",
  _settings.ASSET_MAPPING_MIN_SIMILARITY == 0.60,
  f"got {_settings.ASSET_MAPPING_MIN_SIMILARITY}")

S("T17: AssetMapper.map_narrative default min_similarity is 0.60")
from asset_mapper import AssetMapper
sig = inspect.signature(AssetMapper.map_narrative)
default_sim = sig.parameters["min_similarity"].default
T("T17: default is 0.60", default_sim == 0.60, f"got {default_sim}")

S("T18: Weak asset link cleanup removes entries below 0.60")
_tf = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tf.close()
_repo = SqliteRepository(_tf.name)
_repo.migrate()
nid = str(uuid.uuid4())
# Insert narrative with weak + strong asset links
weak_assets = json.dumps([
    {"ticker": "AIG", "asset_name": "American International Group", "similarity_score": 0.55},
    {"ticker": "AAPL", "asset_name": "Apple Inc", "similarity_score": 0.72},
    {"ticker": "IDXX", "asset_name": "IDEXX Laboratories", "similarity_score": 0.53},
])
_repo.insert_narrative({
    "narrative_id": nid,
    "name": "Test Narrative",
    "stage": "Emerging",
    "linked_assets": weak_assets,
    "suppressed": 0,
})
# Re-run migrate to trigger cleanup
_repo2 = SqliteRepository(_tf.name)
_repo2.migrate()
result = _repo2.get_narrative(nid)
assets_after = json.loads(result["linked_assets"])
T("T18: weak links removed", len(assets_after) == 1, f"got {len(assets_after)} assets: {assets_after}")
T("T18b: AAPL preserved", assets_after[0]["ticker"] == "AAPL" if assets_after else False,
  f"got {assets_after}")

S("T19: Cleanup preserves entries at or above 0.60")
nid2 = str(uuid.uuid4())
strong_assets = json.dumps([
    {"ticker": "MSFT", "asset_name": "Microsoft", "similarity_score": 0.60},
    {"ticker": "NVDA", "asset_name": "NVIDIA", "similarity_score": 0.85},
])
_repo2.insert_narrative({
    "narrative_id": nid2,
    "name": "Strong Links",
    "stage": "Emerging",
    "linked_assets": strong_assets,
    "suppressed": 0,
})
_repo3 = SqliteRepository(_tf.name)
_repo3.migrate()
result2 = _repo3.get_narrative(nid2)
assets_after2 = json.loads(result2["linked_assets"])
T("T19: both strong links preserved", len(assets_after2) == 2, f"got {len(assets_after2)}")

S("T20: Cleanup migration is idempotent")
_repo4 = SqliteRepository(_tf.name)
_repo4.migrate()
result3 = _repo4.get_narrative(nid)
assets_after3 = json.loads(result3["linked_assets"])
T("T20: still 1 asset after re-run", len(assets_after3) == 1, f"got {len(assets_after3)}")

S("T21: Legacy declining-days column migrates into declining-cycles")
_tf2 = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tf2.close()
_repo_legacy = SqliteRepository(_tf2.name)
_repo_legacy.migrate()
legacy_id = str(uuid.uuid4())
with sqlite3.connect(_tf2.name) as _conn:
    _conn.row_factory = sqlite3.Row
    _conn.execute(
        "ALTER TABLE narratives ADD COLUMN consecutive_declining_days INTEGER DEFAULT 0"
    )
    _conn.execute(
        "INSERT INTO narratives (narrative_id, name, stage, suppressed, consecutive_declining_days) "
        "VALUES (?, ?, ?, ?, ?)",
        (legacy_id, "Legacy Decline", "Mature", 0, 11),
    )
    _conn.commit()
_repo_legacy2 = SqliteRepository(_tf2.name)
_repo_legacy2.migrate()
legacy_row = _repo_legacy2.get_narrative(legacy_id)
T(
    "T21: legacy value copied to new column",
    legacy_row is not None and int(legacy_row.get("consecutive_declining_cycles") or 0) == 11,
    f"got {legacy_row}",
)
with sqlite3.connect(_tf2.name) as _conn:
    _legacy_cols = [c[1] for c in _conn.execute("PRAGMA table_info(narratives)").fetchall()]
T(
    "T21b: new column remains present",
    "consecutive_declining_cycles" in _legacy_cols,
    f"columns={_legacy_cols}",
)

# Cleanup temp file
try:
    os.unlink(_tf.name)
except OSError:
    pass
try:
    os.unlink(_tf2.name)
except OSError:
    pass

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Health Fix 5 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  \u2717 {name}")
    sys.exit(1)
else:
    print("All Health Fix 5 tests passed.")
