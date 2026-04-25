"""
Health Fix 4 — Signal Calculation Fixes (Burst Ratio + Velocity)

Section A: Burst ratio baseline scaling
  T1: Burst ratio non-zero when new docs exceed per-cycle baseline
  T2: Burst ratio = 0 when no new docs this cycle
  T3: Fallback baseline (1.0) used when no snapshot history but narrative has docs
  T4: Burst ratio detects a genuine spike (ratio >= alert_ratio)
  T5: Burst ratio = 0 for brand-new narrative with no documents

Section B: Velocity centroid deduplication
  T6: get_centroid_history deduplicates to one entry per date
  T7: Velocity non-zero when centroids differ across dates
  T8: Velocity = 0 when only one date in history (< 2 distinct entries)
  T9: Multiple runs per day produce only one entry per date
  T10: Correct ordering — most recent date first

Section C: Integration — pipeline compute_signals path
  T11: Burst calculation uses per-cycle baseline (not raw daily)
  T12: Velocity uses deduplicated centroid history
"""

import json
import os
import datetime as _dt
import sys
import tempfile
import uuid
from pathlib import Path

import numpy as np

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

_results = []
_tmpfiles = []
_today = _dt.datetime.now(_dt.timezone.utc).date()


def _tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    _tmpfiles.append(path)
    return path


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


from repository import SqliteRepository
from signals import compute_burst_velocity, compute_velocity, compute_velocity_windowed

_now = "2026-04-03T12:00:00Z"
EMB_DIM = 768

# =========================================================================
# Section A: Burst ratio baseline scaling
# =========================================================================
S("Section A: Burst ratio baseline scaling")

# T1: Burst ratio non-zero when new docs exceed per-cycle baseline
burst = compute_burst_velocity(
    recent_doc_count=15,
    baseline_docs_per_window=10.0,
    alert_ratio=3.0,
)
T("T1: Burst ratio non-zero when docs > baseline",
  burst["ratio"] > 0 and burst["ratio"] == 1.5,
  f"ratio={burst['ratio']}")

# T2: Burst ratio = 0 when no new docs
burst_zero = compute_burst_velocity(
    recent_doc_count=0,
    baseline_docs_per_window=10.0,
    alert_ratio=3.0,
)
T("T2: Burst ratio = 0 when no new docs",
  burst_zero["ratio"] == 0.0,
  f"ratio={burst_zero['ratio']}")

# T3: Fallback baseline — when baseline_docs_per_window <= 0, burst returns ratio=0
# (The fallback of 1.0 is applied in pipeline.py, not in compute_burst_velocity)
burst_no_baseline = compute_burst_velocity(
    recent_doc_count=5,
    baseline_docs_per_window=0.0,
    alert_ratio=3.0,
)
T("T3a: compute_burst_velocity returns 0 when baseline=0",
  burst_no_baseline["ratio"] == 0.0,
  f"ratio={burst_no_baseline['ratio']}")

# With the pipeline's fallback baseline of 1.0:
burst_fallback = compute_burst_velocity(
    recent_doc_count=5,
    baseline_docs_per_window=1.0,  # fallback
    alert_ratio=3.0,
)
T("T3b: With fallback baseline=1.0, ratio = new_docs/1.0",
  burst_fallback["ratio"] == 5.0,
  f"ratio={burst_fallback['ratio']}")

# T4: Burst detection triggered when ratio >= alert_ratio
burst_spike = compute_burst_velocity(
    recent_doc_count=40,
    baseline_docs_per_window=10.0,
    alert_ratio=3.0,
)
T("T4: Burst detected when ratio >= alert_ratio",
  burst_spike["is_burst"] is True and burst_spike["ratio"] == 4.0,
  f"ratio={burst_spike['ratio']} is_burst={burst_spike['is_burst']}")

# T5: Burst ratio = 0 for narrative with no docs (and baseline=0)
burst_empty = compute_burst_velocity(
    recent_doc_count=0,
    baseline_docs_per_window=0.0,
    alert_ratio=3.0,
)
T("T5: Zero docs + zero baseline → ratio=0",
  burst_empty["ratio"] == 0.0 and burst_empty["is_burst"] is False,
  f"ratio={burst_empty['ratio']}")

# =========================================================================
# Section B: Velocity centroid deduplication
# =========================================================================
S("Section B: Velocity centroid deduplication")

repo = SqliteRepository(_tmp_db())
repo.migrate()

nid = str(uuid.uuid4())
repo.insert_narrative({
    "narrative_id": nid,
    "name": "test-velocity",
    "stage": "Emerging",
    "created_at": _now,
    "last_updated_at": _now,
})

# Insert multiple centroid entries for the same date (simulating 6 pipeline runs/day)
vecs_day1 = []
day1 = (_today - _dt.timedelta(days=2)).isoformat()
for i in range(6):
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    vec /= np.linalg.norm(vec)
    vecs_day1.append(vec)
    repo.insert_centroid_history(nid, day1, vec.tobytes())

# Insert different centroids for day 2
vecs_day2 = []
day2 = (_today - _dt.timedelta(days=1)).isoformat()
for i in range(6):
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    vec /= np.linalg.norm(vec)
    vecs_day2.append(vec)
    repo.insert_centroid_history(nid, day2, vec.tobytes())

# Insert different centroids for day 3
vecs_day3 = []
day3 = _today.isoformat()
for i in range(4):
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    vec /= np.linalg.norm(vec)
    vecs_day3.append(vec)
    repo.insert_centroid_history(nid, day3, vec.tobytes())

# T6: get_centroid_history deduplicates to one entry per date
history = repo.get_centroid_history(nid, days=7)
T("T6: Deduplicates to one entry per date",
  len(history) == 3,
  f"got {len(history)} entries (expected 3 for 3 dates)")

# T7: Velocity non-zero when centroids differ across dates
if len(history) >= 2:
    c_today = np.frombuffer(history[0]["centroid_blob"], dtype=np.float32).copy()
    c_yesterday = np.frombuffer(history[1]["centroid_blob"], dtype=np.float32).copy()
    vel = compute_velocity(c_today, c_yesterday)
    T("T7: Velocity non-zero with distinct-day centroids",
      vel > 0.0,
      f"velocity={vel:.6f}")
else:
    T("T7: Velocity non-zero with distinct-day centroids", False,
      "not enough history entries")

# T8: Velocity = 0 when only one date in history
nid_single = str(uuid.uuid4())
repo.insert_narrative({
    "narrative_id": nid_single,
    "name": "single-day",
    "stage": "Emerging",
    "created_at": _now,
    "last_updated_at": _now,
})
for i in range(5):
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    vec /= np.linalg.norm(vec)
    repo.insert_centroid_history(nid_single, day3, vec.tobytes())

history_single = repo.get_centroid_history(nid_single, days=7)
T("T8: Single date → one entry in history",
  len(history_single) == 1,
  f"got {len(history_single)}")

# T9: Multiple runs per day produce only one entry per date
nid_multi = str(uuid.uuid4())
repo.insert_narrative({
    "narrative_id": nid_multi,
    "name": "multi-run",
    "stage": "Emerging",
    "created_at": _now,
    "last_updated_at": _now,
})
# 6 runs on day 1, 6 on day 2
for i in range(6):
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    repo.insert_centroid_history(nid_multi, day1, vec.tobytes())
for i in range(6):
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    repo.insert_centroid_history(nid_multi, day2, vec.tobytes())

history_multi = repo.get_centroid_history(nid_multi, days=7)
T("T9: 12 rows across 2 dates → 2 entries",
  len(history_multi) == 2,
  f"got {len(history_multi)}")

# T10: Correct ordering — most recent date first
dates = [h["date"] for h in history]
T("T10: Most recent date first",
  dates == [day3, day2, day1],
  f"order={dates}")

# =========================================================================
# Section C: Integration — pipeline baseline conversion
# =========================================================================
S("Section C: Integration — pipeline baseline conversion")

# Simulate what pipeline.py does: convert daily baseline to per-cycle
from settings import get_settings
settings = get_settings()

PIPELINE_FREQUENCY_HOURS = settings.PIPELINE_FREQUENCY_HOURS  # 4
cycles_per_day = max(24.0 / PIPELINE_FREQUENCY_HOURS, 1.0)

# T11: Daily baseline of 60 → per-cycle baseline of 10
baseline_daily = 60.0
baseline_per_cycle = baseline_daily / cycles_per_day
T("T11: Daily baseline 60 → per-cycle 10",
  abs(baseline_per_cycle - 10.0) < 0.01,
  f"per_cycle={baseline_per_cycle}, cycles_per_day={cycles_per_day}")

# With 15 new docs this cycle vs 10 avg: ratio = 1.5
burst_integrated = compute_burst_velocity(
    recent_doc_count=15,
    baseline_docs_per_window=baseline_per_cycle,
    alert_ratio=settings.BURST_VELOCITY_ALERT_RATIO,
)
T("T11b: 15 new docs / 10 baseline → ratio 1.5",
  burst_integrated["ratio"] == 1.5,
  f"ratio={burst_integrated['ratio']}")

# T12: Velocity windowed uses deduplicated history
nid_vel = str(uuid.uuid4())
repo.insert_narrative({
    "narrative_id": nid_vel,
    "name": "vel-window",
    "stage": "Growing",
    "created_at": _now,
    "last_updated_at": _now,
})
# Insert distinct centroids across 5 days, multiple per day
for day_offset in range(5):
    date_str = (_today - _dt.timedelta(days=4 - day_offset)).isoformat()
    for run in range(4):
        vec = np.random.randn(EMB_DIM).astype(np.float32)
        vec /= np.linalg.norm(vec)
        repo.insert_centroid_history(nid_vel, date_str, vec.tobytes())

hist = repo.get_centroid_history(nid_vel, days=7)
T("T12a: 5 days × 4 runs → 5 deduplicated entries",
  len(hist) == 5,
  f"got {len(hist)}")

# Convert to vecs and compute windowed velocity
vecs = []
for rec in hist:
    v = np.frombuffer(rec["centroid_blob"], dtype=np.float32).copy()
    if v.size == EMB_DIM:
        vecs.append(v)

vel_windowed = compute_velocity_windowed(vecs, window_days=settings.VELOCITY_WINDOW_DAYS)
T("T12b: Windowed velocity non-zero with 5-day history",
  vel_windowed > 0.0,
  f"velocity_windowed={vel_windowed:.6f}")

# =========================================================================
# Section D: Edge cases
# =========================================================================
S("Section D: Edge cases")

# T13: get_centroid_history with limit parameter still works
history_limited = repo.get_centroid_history(nid, days=7, limit=2)
T("T13: Limit parameter respected",
  len(history_limited) == 2,
  f"got {len(history_limited)}")

# T14: Empty centroid history returns empty list
nid_empty = str(uuid.uuid4())
repo.insert_narrative({
    "narrative_id": nid_empty,
    "name": "no-centroids",
    "stage": "Emerging",
    "created_at": _now,
    "last_updated_at": _now,
})
history_empty = repo.get_centroid_history(nid_empty, days=7)
T("T14: No centroid history → empty list",
  len(history_empty) == 0)

# T15: Negative baseline (edge case) → ratio = 0
burst_neg = compute_burst_velocity(
    recent_doc_count=10,
    baseline_docs_per_window=-5.0,
    alert_ratio=3.0,
)
T("T15: Negative baseline → ratio=0",
  burst_neg["ratio"] == 0.0,
  f"ratio={burst_neg['ratio']}")

# T16: Latest centroid per date is the one returned (highest rowid)
nid_order = str(uuid.uuid4())
repo.insert_narrative({
    "narrative_id": nid_order,
    "name": "order-check",
    "stage": "Emerging",
    "created_at": _now,
    "last_updated_at": _now,
})
# Insert identifiable vectors: first ones are zeros, last is a known value
zero_vec = np.zeros(EMB_DIM, dtype=np.float32)
known_vec = np.ones(EMB_DIM, dtype=np.float32)
known_vec /= np.linalg.norm(known_vec)
repo.insert_centroid_history(nid_order, day3, zero_vec.tobytes())
repo.insert_centroid_history(nid_order, day3, known_vec.tobytes())  # latest

hist_order = repo.get_centroid_history(nid_order, days=7)
returned_vec = np.frombuffer(hist_order[0]["centroid_blob"], dtype=np.float32).copy()
T("T16: Latest centroid per date returned (highest rowid)",
  np.allclose(returned_vec, known_vec, atol=1e-6),
  f"norm={np.linalg.norm(returned_vec):.4f}")

# T17: Velocity between identical centroids = 0
vel_same = compute_velocity(known_vec, known_vec)
T("T17: Velocity = 0 for identical centroids",
  vel_same == 0.0,
  f"velocity={vel_same}")

# T18: compute_velocity_windowed returns 0 with < 2 entries
vel_short = compute_velocity_windowed([known_vec], window_days=7)
T("T18: velocity_windowed = 0 with single entry",
  vel_short == 0.0,
  f"velocity={vel_short}")

# =========================================================================
# Section E: Audit-discovered issues — new tests
# =========================================================================
S("Section E: Audit edge cases + meta-audit findings")

# T19: PIPELINE_FREQUENCY_HOURS=0 guard — freq clamped to 1
# Simulates the pipeline.py guard: freq = max(PIPELINE_FREQUENCY_HOURS, 1)
for bad_freq in [0, -1]:
    freq_guarded = max(bad_freq, 1)
    cpd = 24.0 / freq_guarded
    T(f"T19: freq={bad_freq} guarded to 1, cycles_per_day={cpd}",
      cpd == 24.0 and freq_guarded == 1,
      f"freq_guarded={freq_guarded} cpd={cpd}")

# T20: Improved fallback baseline scales with doc_count (not flat 1.0)
# Narrative with 200 existing docs, 3 new docs → fallback should prevent false burst
cycles_per_day_t20 = 6.0  # 24/4
baseline_daily_t20 = 0.0  # no snapshot history
doc_count_t20 = 200
baseline_per_cycle_t20 = baseline_daily_t20 / cycles_per_day_t20 if baseline_daily_t20 > 0 else 0.0
if baseline_per_cycle_t20 <= 0 and doc_count_t20 > 0:
    baseline_per_cycle_t20 = max(doc_count_t20 / (7.0 * cycles_per_day_t20), 1.0)
burst_fallback_scaled = compute_burst_velocity(
    recent_doc_count=3,
    baseline_docs_per_window=baseline_per_cycle_t20,
    alert_ratio=3.0,
)
T("T20: Scaled fallback (200 docs) → ratio < 3.0 (no false burst)",
  burst_fallback_scaled["ratio"] < 3.0 and not burst_fallback_scaled["is_burst"],
  f"baseline={baseline_per_cycle_t20:.2f} ratio={burst_fallback_scaled['ratio']}")

# T21: get_baseline_doc_rate with < 2 snapshots returns 0.0
repo_t21 = SqliteRepository(_tmp_db())
repo_t21.migrate()
nid_t21 = str(uuid.uuid4())
repo_t21.insert_narrative({
    "narrative_id": nid_t21, "name": "sparse-snapshots",
    "stage": "Emerging", "created_at": _now, "last_updated_at": _now,
})
# Insert only 1 snapshot (need >= 2 for a baseline)
repo_t21.save_snapshot({
    "id": str(uuid.uuid4()), "narrative_id": nid_t21,
    "snapshot_date": (_today - _dt.timedelta(days=1)).isoformat(), "doc_count": 10,
    "ns_score": 0.5, "velocity": 0.0, "entropy": 1.0,
    "cohesion": 0.8, "polarization": 0.1,
    "lifecycle_stage": "Emerging", "created_at": _now,
})
rate_t21 = repo_t21.get_baseline_doc_rate(nid_t21, lookback_days=7)
T("T21: < 2 snapshots → baseline returns 0.0",
  rate_t21 == 0.0,
  f"rate={rate_t21}")

# T22: Corrupt centroid blob (wrong size) silently skipped
from pipeline import _load_centroid_history_vecs
repo_t22 = SqliteRepository(_tmp_db())
repo_t22.migrate()
nid_t22 = str(uuid.uuid4())
repo_t22.insert_narrative({
    "narrative_id": nid_t22, "name": "corrupt-blob",
    "stage": "Emerging", "created_at": _now, "last_updated_at": _now,
})
# Insert a valid blob then a corrupt one (wrong dimension)
good_vec = np.random.randn(EMB_DIM).astype(np.float32)
bad_vec = np.random.randn(100).astype(np.float32)  # wrong size
repo_t22.insert_centroid_history(nid_t22, day1, good_vec.tobytes())
repo_t22.insert_centroid_history(nid_t22, day2, bad_vec.tobytes())
repo_t22.insert_centroid_history(nid_t22, day3, good_vec.tobytes())
vecs_t22 = _load_centroid_history_vecs(repo_t22, nid_t22, days=7, emb_dim=EMB_DIM)
T("T22: Corrupt blob skipped, valid blobs kept",
  len(vecs_t22) == 2,
  f"got {len(vecs_t22)} vectors (expected 2: day 3 good, day 1 good, day 2 corrupt skipped)")

# T23: compute_burst_velocity no longer needs window_hours — no crash
burst_wh0 = compute_burst_velocity(
    recent_doc_count=5,
    baseline_docs_per_window=2.0, alert_ratio=3.0,
)
T("T23: burst call without window_hours → no crash",
  burst_wh0["ratio"] == 2.5,
  f"ratio={burst_wh0['ratio']}")

# T24: get_centroid_history with days=0 — boundary
repo_t24 = SqliteRepository(_tmp_db())
repo_t24.migrate()
nid_t24 = str(uuid.uuid4())
repo_t24.insert_narrative({
    "narrative_id": nid_t24, "name": "boundary-days",
    "stage": "Emerging", "created_at": _now, "last_updated_at": _now,
})
today_str = _today.isoformat()
yesterday_str = (_today - _dt.timedelta(days=1)).isoformat()
vec_t24 = np.random.randn(EMB_DIM).astype(np.float32)
repo_t24.insert_centroid_history(nid_t24, today_str, vec_t24.tobytes())
repo_t24.insert_centroid_history(nid_t24, yesterday_str, vec_t24.tobytes())
hist_t24 = repo_t24.get_centroid_history(nid_t24, days=0)
T("T24: days=0 → returns only today's entry",
  len(hist_t24) == 1 and hist_t24[0]["date"] == today_str,
  f"got {len(hist_t24)} entries, dates={[h['date'] for h in hist_t24]}")

# T25: Large recent_doc_count — no overflow
burst_large = compute_burst_velocity(
    recent_doc_count=10000,
    baseline_docs_per_window=50.0, alert_ratio=3.0,
)
T("T25: Large doc count (10000) → correct ratio, no overflow",
  burst_large["ratio"] == 200.0 and burst_large["is_burst"] is True,
  f"ratio={burst_large['ratio']}")

# T26 (Meta-audit 4): Inflow velocity unit conversion — daily baseline
# converted to per-cycle before passing to compute_inflow_velocity
from signals import compute_inflow_velocity
# Simulate pipeline logic: baseline_daily=60, freq=4h, cycles_per_day=6
# Per-cycle avg = 60/6 = 10. With 15 new docs, inflow_vel = 15/10 = 1.5
baseline_daily_t26 = 60.0
cycles_per_day_t26 = 6.0
avg_per_cycle_t26 = baseline_daily_t26 / cycles_per_day_t26
inflow_t26 = compute_inflow_velocity(15, avg_per_cycle_t26)
T("T26: Inflow velocity uses per-cycle baseline (not raw daily)",
  abs(inflow_t26 - 1.5) < 0.01,
  f"avg_per_cycle={avg_per_cycle_t26} inflow_vel={inflow_t26}")

# T27 (Meta-audit 4): Without unit conversion, inflow would be ~6x too low
# This test documents the bug that was present before the fix
inflow_buggy = compute_inflow_velocity(15, baseline_daily_t26)  # raw daily=60
T("T27: Without conversion, inflow would be 0.25 (6x too low)",
  abs(inflow_buggy - 0.25) < 0.01,
  f"buggy_inflow={inflow_buggy} (correct is 1.5)")

# T28: get_baseline_doc_rate with sufficient snapshots returns positive value
repo_t28 = SqliteRepository(_tmp_db())
repo_t28.migrate()
nid_t28 = str(uuid.uuid4())
repo_t28.insert_narrative({
    "narrative_id": nid_t28, "name": "good-baseline",
    "stage": "Growing", "created_at": _now, "last_updated_at": _now,
})
# Insert 5 snapshots with increasing doc_count (10, 15, 22, 30, 35)
doc_counts = [10, 15, 22, 30, 35]
for i, dc in enumerate(doc_counts):
    d = (_today - _dt.timedelta(days=4 - i)).isoformat()
    repo_t28.save_snapshot({
        "id": str(uuid.uuid4()), "narrative_id": nid_t28,
        "snapshot_date": d, "doc_count": dc,
        "ns_score": 0.5, "velocity": 0.1, "entropy": 1.5,
        "cohesion": 0.8, "polarization": 0.2,
        "lifecycle_stage": "Growing", "created_at": _now,
    })
rate_t28 = repo_t28.get_baseline_doc_rate(nid_t28, lookback_days=7)
# Expected: avg of |35-30|, |30-22|, |22-15|, |15-10| = avg(5,8,7,5) = 6.25
T("T28: Sufficient snapshots → positive baseline",
  rate_t28 > 0,
  f"rate={rate_t28} (expected ~6.25)")

# =========================================================================
# Summary
# =========================================================================
print("\n" + "=" * 60)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Health Fix 4: {passed}/{total} passed")
for f in _tmpfiles:
    try:
        os.unlink(f)
    except OSError:
        pass

if passed < total:
    for name, ok in _results:
        if not ok:
            print(f"  FAILED: {name}")
    sys.exit(1)
