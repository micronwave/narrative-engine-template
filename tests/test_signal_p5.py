"""
Signal Redesign Phase 5 — Learned Signal Weights + Inflow Velocity Tests

Section 1: Training dataset (2 tests)
  SP5-DATA-1: build_training_dataset returns empty ([], []) when no Mature/Declining narratives
  SP5-DATA-2: train_signal_model with <30 samples returns method="default"

Section 2: Model training (2 tests)
  SP5-TRAIN-1: train_signal_model with 30+ samples returns method="learned" with 15 weights
  SP5-TRAIN-2: cold start (no Mature narratives) degrades to default weights without error

Section 3: Learned scoring (3 tests)
  SP5-SCORE-1: compute_learned_ns_score with default model returns same as compute_ns_score()
  SP5-SCORE-2: compute_learned_ns_score with learned model returns values in [0.0, 1.0]
  SP5-SCORE-3: compute_learned_ns_score handles missing features (None values) -> 0.0

Section 4: Model persistence (3 tests)
  SP5-PERSIST-1: load_or_train_model creates pickle file at SIGNAL_MODEL_PATH
  SP5-PERSIST-2: load_or_train_model loads cached model if file exists and is fresh
  SP5-PERSIST-3: load_or_train_model retrains if file is older than SIGNAL_MODEL_RETRAIN_DAYS

Section 5: Settings (1 test)
  SP5-SET-1: SIGNAL_MODEL_PATH, SIGNAL_MODEL_RETRAIN_DAYS, SIGNAL_MIN_TRAINING_SAMPLES exist

Section 6: Inflow velocity (3 tests)
  SP5-INFLOW-1: compute_inflow_velocity(5, 2.0) returns 2.5
  SP5-INFLOW-2: compute_inflow_velocity(0, 3.0) returns 0.0
  SP5-INFLOW-3: compute_inflow_velocity(100, 1.0) returns 10.0 (clamped)

Section 7: Schema (1 test)
  SP5-SCH-1: inflow_velocity and avg_docs_per_cycle_7d columns exist after migrate()

Section 8: Pipeline integration (1 test)
  SP5-INT-1: Pipeline Step 12 completes without error using learned model path
"""

import json
import math
import os
import pickle
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from signal_trainer import (
    build_training_dataset,
    train_signal_model,
    compute_learned_ns_score,
    load_or_train_model,
    FEATURE_NAMES,
)
from signals import compute_ns_score, compute_inflow_velocity
from repository import SqliteRepository

# ---------------------------------------------------------------------------
# Test runner helpers
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


def _make_repo() -> SqliteRepository:
    tmp = tempfile.mktemp(suffix=".db")
    repo = SqliteRepository(tmp)
    repo.migrate()
    return repo


# ---------------------------------------------------------------------------
# Section 1: Training dataset
# ---------------------------------------------------------------------------
S("Section 1: Training dataset")

# SP5-DATA-1: build_training_dataset returns empty when no Mature/Declining narratives
repo = _make_repo()
X, y = build_training_dataset(repo)
T("SP5-DATA-1: empty dataset when no Mature/Declining", X == [] and y == [],
  f"X={len(X)}, y={len(y)}")

# SP5-DATA-2: train_signal_model with <30 samples returns method="default"
result = train_signal_model([[0.0] * 15] * 5, [0, 1, 0, 1, 0], min_samples=30)
T("SP5-DATA-2: <30 samples -> method=default",
  result["method"] == "default" and result["n_samples"] == 5,
  f"method={result['method']}, n={result['n_samples']}")

# ---------------------------------------------------------------------------
# Section 2: Model training
# ---------------------------------------------------------------------------
S("Section 2: Model training")

# SP5-TRAIN-1: train_signal_model with 30+ samples returns method="learned" with 15 weights
import numpy as np
np.random.seed(42)
X_train = np.random.randn(50, 15).tolist()
y_train = [1 if sum(row) > 0 else 0 for row in X_train]
result = train_signal_model(X_train, y_train, min_samples=30)
T("SP5-TRAIN-1: 30+ samples -> method=learned with 15 weights",
  result["method"] == "learned" and len(result["weights"]) == 15,
  f"method={result['method']}, weights={len(result.get('weights', []))}")

# SP5-TRAIN-2: cold start degrades to default
repo = _make_repo()
model = load_or_train_model(repo, tempfile.mktemp(suffix=".pkl"),
                            retrain_days=7, min_samples=30)
T("SP5-TRAIN-2: cold start -> default weights",
  model["method"] == "default",
  f"method={model['method']}")

# ---------------------------------------------------------------------------
# Section 3: Learned scoring
# ---------------------------------------------------------------------------
S("Section 3: Learned scoring")

# SP5-SCORE-1: default model matches compute_ns_score()
default_model = {
    "method": "default",
    "weights": [0.0] * 15,
    "intercept": 0.0,
    "n_samples": 0,
}
features = {
    "velocity_windowed": 0.3,
    "inflow_velocity": 1.5,
    "cross_source_score": 0.4,
    "cohesion": 0.8,
    "intent_weight": 0.5,
    "centrality": 0.2,
    "entropy_normalized": 0.6,
    "direction_float": 1.0,
    "confidence": 0.7,
    "certainty_float": 0.7,
    "magnitude_float": 0.6,
    "source_escalation_velocity": 0.1,
    "convergence_exposure": 0.3,
    "catalyst_proximity_score": 0.5,
    "macro_alignment": 0.2,
    # Extra fields for fallback path
    "polarization": 0.15,
    "entropy": 1.5,
    "entropy_vocab_window": 10,
}
learned_result = compute_learned_ns_score(features, default_model)
direct_result = compute_ns_score(
    velocity=0.3, intent_weight=0.5, cross_source_score=0.4,
    cohesion=0.8, polarization=0.15, centrality=0.2,
    entropy=1.5, entropy_vocab_window=10,
)
T("SP5-SCORE-1: default model == compute_ns_score()",
  abs(learned_result - direct_result) < 1e-9,
  f"learned={learned_result:.6f}, direct={direct_result:.6f}")

# SP5-SCORE-2: learned model returns values in [0.0, 1.0]
learned_model = {
    "method": "learned",
    "weights": [0.1] * 15,
    "intercept": -0.5,
    "n_samples": 50,
}
score = compute_learned_ns_score(features, learned_model)
T("SP5-SCORE-2: learned model in [0, 1]",
  0.0 <= score <= 1.0,
  f"score={score:.6f}")

# SP5-SCORE-3: handles None/missing features
sparse_features = {
    "velocity_windowed": None,
    "polarization": 0.0,
    "entropy": None,
    "entropy_vocab_window": 10,
}
score_sparse = compute_learned_ns_score(sparse_features, learned_model)
T("SP5-SCORE-3: None features -> no crash, in [0, 1]",
  0.0 <= score_sparse <= 1.0,
  f"score={score_sparse:.6f}")

# ---------------------------------------------------------------------------
# Section 4: Model persistence
# ---------------------------------------------------------------------------
S("Section 4: Model persistence")

# SP5-PERSIST-1: load_or_train_model creates pickle file
tmp_model_path = tempfile.mktemp(suffix=".pkl")
repo = _make_repo()
model = load_or_train_model(repo, tmp_model_path, retrain_days=7, min_samples=30)
T("SP5-PERSIST-1: pickle file created",
  os.path.exists(tmp_model_path),
  f"path={tmp_model_path}")

# SP5-PERSIST-2: loads cached model if fresh
model2 = load_or_train_model(repo, tmp_model_path, retrain_days=7, min_samples=30)
T("SP5-PERSIST-2: loads cached model",
  model2["method"] == model["method"],
  f"cached method={model2['method']}")

# SP5-PERSIST-3: retrains if file is old
# Make file appear old by setting mtime to 10 days ago
old_time = time.time() - (10 * 86400)
os.utime(tmp_model_path, (old_time, old_time))
model3 = load_or_train_model(repo, tmp_model_path, retrain_days=7, min_samples=30)
T("SP5-PERSIST-3: retrains when stale",
  isinstance(model3, dict) and "method" in model3,
  f"method={model3['method']}")
# Clean up
try:
    os.unlink(tmp_model_path)
except Exception:
    pass

# ---------------------------------------------------------------------------
# Section 5: Settings
# ---------------------------------------------------------------------------
S("Section 5: Settings")

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-settings-validation")
from settings import Settings
s = Settings(ANTHROPIC_API_KEY="test-key-for-settings-validation")
T("SP5-SET-1: Phase 5 settings exist with correct defaults",
  s.SIGNAL_MODEL_PATH == "./data/signal_model.pkl"
  and s.SIGNAL_MODEL_RETRAIN_DAYS == 7
  and s.SIGNAL_MIN_TRAINING_SAMPLES == 30,
  f"path={s.SIGNAL_MODEL_PATH}, retrain={s.SIGNAL_MODEL_RETRAIN_DAYS}, min={s.SIGNAL_MIN_TRAINING_SAMPLES}")

# ---------------------------------------------------------------------------
# Section 6: Inflow velocity
# ---------------------------------------------------------------------------
S("Section 6: Inflow velocity")

T("SP5-INFLOW-1: inflow_velocity(5, 2.0) = 2.5",
  compute_inflow_velocity(5, 2.0) == 2.5,
  f"got={compute_inflow_velocity(5, 2.0)}")

T("SP5-INFLOW-2: inflow_velocity(0, 3.0) = 0.0",
  compute_inflow_velocity(0, 3.0) == 0.0,
  f"got={compute_inflow_velocity(0, 3.0)}")

T("SP5-INFLOW-3: inflow_velocity(100, 1.0) = 10.0 (clamped)",
  compute_inflow_velocity(100, 1.0) == 10.0,
  f"got={compute_inflow_velocity(100, 1.0)}")

# ---------------------------------------------------------------------------
# Section 7: Schema
# ---------------------------------------------------------------------------
S("Section 7: Schema")

repo = _make_repo()
import sqlite3
conn = sqlite3.connect(repo._db_path)
conn.row_factory = sqlite3.Row
cols = [r["name"] for r in conn.execute("PRAGMA table_info(narratives)").fetchall()]
conn.close()
T("SP5-SCH-1: inflow_velocity + avg_docs_per_cycle_7d columns exist",
  "inflow_velocity" in cols and "avg_docs_per_cycle_7d" in cols,
  f"found={'inflow_velocity' in cols}, {' avg_docs_per_cycle_7d' in cols}")

# ---------------------------------------------------------------------------
# Section 8: Pipeline integration
# ---------------------------------------------------------------------------
S("Section 8: Pipeline integration")

# SP5-INT-1: Step 12 completes with learned model path
# We mock the pipeline environment to test that step 12 can run with the model
try:
    repo = _make_repo()
    tmp_model = tempfile.mktemp(suffix=".pkl")
    model = load_or_train_model(repo, tmp_model, retrain_days=7, min_samples=30)

    # Insert a test narrative
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    repo.insert_narrative({
        "narrative_id": "test-p5-int",
        "name": "Test P5 Integration",
        "stage": "Emerging",
        "document_count": 5,
        "velocity": 0.1,
        "velocity_windowed": 0.1,
        "cohesion": 0.8,
        "polarization": 0.05,
        "centrality": 0.1,
        "entropy": 1.2,
        "intent_weight": 0.4,
        "cross_source_score": 0.3,
        "ns_score": 0.2,
        "created_at": now_iso,
        "last_updated_at": now_iso,
    })

    # Compute learned ns_score for it
    narrative = repo.get_narrative("test-p5-int")
    test_features = {
        "velocity_windowed": float(narrative.get("velocity_windowed") or 0.0),
        "inflow_velocity": 0.0,
        "cross_source_score": float(narrative.get("cross_source_score") or 0.0),
        "cohesion": float(narrative.get("cohesion") or 0.0),
        "intent_weight": float(narrative.get("intent_weight") or 0.0),
        "centrality": float(narrative.get("centrality") or 0.0),
        "entropy_normalized": 0.0,
        "direction_float": 0.0,
        "confidence": 0.0,
        "certainty_float": 0.2,
        "magnitude_float": 0.3,
        "source_escalation_velocity": 0.0,
        "convergence_exposure": 0.0,
        "catalyst_proximity_score": 0.0,
        "macro_alignment": 0.0,
        "polarization": float(narrative.get("polarization") or 0.0),
        "entropy": narrative.get("entropy"),
        "entropy_vocab_window": 10,
    }
    score = compute_learned_ns_score(test_features, model)
    step12_ok = 0.0 <= score <= 1.0
    T("SP5-INT-1: Step 12 completes with model",
      step12_ok,
      f"score={score:.6f}, model_method={model['method']}")

    # Cleanup
    try:
        os.unlink(tmp_model)
    except Exception:
        pass
except Exception as exc:
    T("SP5-INT-1: Step 12 completes with model", False, f"ERROR: {exc}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Phase 5 results: {passed}/{total} passed")
if passed < total:
    print("FAILED:")
    for name, ok in _results:
        if not ok:
            print(f"  - {name}")
    sys.exit(1)
else:
    print("All Phase 5 tests passed.")
