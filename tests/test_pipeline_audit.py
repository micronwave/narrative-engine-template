"""
Pipeline audit tests — targets edge cases and failure scenarios
identified during code review of pipeline.py, settings.py,
quick_refresh.py, and run_pipeline.bat.

Run: python -X utf8 tests/test_pipeline_audit.py
"""

import json
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ─── Minimal test harness ───────────────────────────────────────────────

_results = {"pass": 0, "fail": 0, "errors": []}


def S(section: str):
    print(f"\n{'='*60}\n  {section}\n{'='*60}")


def T(name: str, condition: bool, details: str = ""):
    if condition:
        _results["pass"] += 1
        print(f"  ✓ {name}")
    else:
        _results["fail"] += 1
        _results["errors"].append(f"{name}: {details}")
        print(f"  ✗ {name} — {details}")


def report():
    print(f"\n{'─'*60}")
    total = _results["pass"] + _results["fail"]
    print(f"  {_results['pass']}/{total} passed, {_results['fail']} failed")
    if _results["errors"]:
        print("  Failures:")
        for e in _results["errors"]:
            print(f"    - {e}")
    return _results["fail"] == 0


# ─── Tests ──────────────────────────────────────────────────────────────

S("CRITICAL-1: _log_step signature accepts run_id parameter")

import inspect
from pipeline import _log_step

sig = inspect.signature(_log_step)
params = list(sig.parameters.keys())
T("_log_step has run_id as second parameter",
  len(params) >= 2 and params[1] == "run_id",
  f"params={params}")
T("_log_step has 7+ parameters (repo, run_id, step_number, step_name, status, duration_ms, error_message)",
  len(params) >= 7,
  f"got {len(params)} params: {params}")


S("CRITICAL-2: run_light deduplicator gets RawDocument, not str")

# Verify the source code does NOT contain the broken pattern
import pipeline
source = inspect.getsource(pipeline.run_light)
T("run_light does not call is_duplicate with text variable",
  "is_duplicate(text)" not in source,
  "Found is_duplicate(text) — should be is_duplicate(doc)")
T("run_light does not call deduplicator.add(text)",
  "deduplicator.add(text)" not in source,
  "Found deduplicator.add(text) — should be deduplicator.add(doc)")


S("HIGH-1: Centroid decay must NOT renormalize (would be a no-op)")

# Verify the decay code does not renormalize
run_source = inspect.getsource(pipeline.run)
# Find the centroid_decay section
decay_section_start = run_source.find("Step 8: Centroid Decay")
decay_section_end = run_source.find("Step 9:", decay_section_start)
decay_code = run_source[decay_section_start:decay_section_end]

T("Centroid decay section does not contain renormalization",
  "decayed / norm" not in decay_code and "decayed /= norm" not in decay_code,
  "Found normalization after decay scaling — this makes decay a no-op on unit vectors")


S("HIGH-3: _classify_lifecycle removed — Step 14 uses Step 10 stage")

# Verify _classify_lifecycle no longer exists (was removed)
import pipeline
T("_classify_lifecycle function removed from pipeline module",
  not hasattr(pipeline, '_classify_lifecycle'),
  "Function still exists — Step 14 would overwrite Step 10's stage")

# Verify Step 14 source does NOT call _classify_lifecycle
run_source = inspect.getsource(pipeline.run)
T("Step 14 does not call _classify_lifecycle",
  "_classify_lifecycle" not in run_source,
  "Step 14 still calls _classify_lifecycle, overwriting Step 10's Dormant-aware stage")

# Verify Step 14 reads stage from fresh narrative (set by Step 10)
step14_start = run_source.find("Step 14:")
step14_end = run_source.find("Step 15:", step14_start)
step14_code = run_source[step14_start:step14_end]
T("Step 14 reads stage from DB (fresh.get('stage'))",
  "fresh.get(\"stage\")" in step14_code or "fresh.get('stage')" in step14_code,
  "Step 14 doesn't read stage from DB — may use stale data")


S("HIGH-5: JWT validation rejects whitespace-only secrets")

from settings import Settings

try:
    s = Settings(
        ANTHROPIC_API_KEY="test-key",
        AUTH_MODE="jwt",
        JWT_SECRET_KEY="                                ",  # 32 spaces
    )
    T("Whitespace-only JWT secret rejected in jwt mode",
      False, "Accepted 32 spaces as JWT secret")
except Exception:
    T("Whitespace-only JWT secret rejected in jwt mode", True)

# Valid key should still work
try:
    s = Settings(
        ANTHROPIC_API_KEY="test-key",
        AUTH_MODE="jwt",
        JWT_SECRET_KEY="a" * 32,
    )
    T("Valid 32-char JWT secret accepted",
      True)
except Exception as exc:
    T("Valid 32-char JWT secret accepted",
      False, str(exc))

# stub mode should not require JWT secret
try:
    s = Settings(
        ANTHROPIC_API_KEY="test-key",
        AUTH_MODE="stub",
        JWT_SECRET_KEY="",
    )
    T("Empty JWT secret allowed in stub mode",
      True)
except Exception as exc:
    T("Empty JWT secret allowed in stub mode",
      False, str(exc))


S("Settings: Validation edge cases")

# CENTROID_ALPHA boundary: 0.0 and 1.0 should be rejected
for val, label in [(0.0, "zero"), (1.0, "one"), (-0.1, "negative")]:
    try:
        Settings(ANTHROPIC_API_KEY="test", CENTROID_ALPHA=val)
        T(f"CENTROID_ALPHA={label} rejected", False, "accepted invalid value")
    except Exception:
        T(f"CENTROID_ALPHA={label} rejected", True)

# LSH_NUM_PERM minimum
try:
    Settings(ANTHROPIC_API_KEY="test", LSH_NUM_PERM=32)
    T("LSH_NUM_PERM=32 rejected (min 64)", False, "accepted < 64")
except Exception:
    T("LSH_NUM_PERM=32 rejected (min 64)", True)

# EMBEDDING_MODE validation
try:
    Settings(ANTHROPIC_API_KEY="test", EMBEDDING_MODE="sparse")
    T("EMBEDDING_MODE=sparse rejected", False, "accepted invalid mode")
except Exception:
    T("EMBEDDING_MODE=sparse rejected", True)


S("QuickRefresh: cosine similarity edge cases")

from quick_refresh import QuickRefresh

# Zero vector handling
qr = QuickRefresh.__new__(QuickRefresh)  # bypass __init__
zero = np.zeros(768, dtype=np.float32)
normal = np.random.randn(768).astype(np.float32)
normal /= np.linalg.norm(normal)

T("Cosine sim with zero vector returns 0.0",
  qr._cosine_similarity(zero, normal) == 0.0)
T("Cosine sim with both zero vectors returns 0.0",
  qr._cosine_similarity(zero, zero) == 0.0)

# Identical vectors
T("Cosine sim of identical vectors ≈ 1.0",
  abs(qr._cosine_similarity(normal, normal) - 1.0) < 1e-6)

# Opposite vectors
T("Cosine sim of opposite vectors ≈ -1.0",
  abs(qr._cosine_similarity(normal, -normal) - (-1.0)) < 1e-6)


S("Pipeline: _load_centroid_history_vecs edge cases")

from pipeline import _load_centroid_history_vecs

# Verify it handles empty blob gracefully
class MockRepo:
    def get_centroid_history(self, narrative_id, days):
        return [
            {"centroid_blob": None},
            {"centroid_blob": b""},
            {"centroid_blob": np.zeros(768, dtype=np.float32).tobytes()},
        ]

repo = MockRepo()
vecs = _load_centroid_history_vecs(repo, "test-id", days=7, emb_dim=768)
T("None blob skipped", len(vecs) <= 2)
T("Valid blob deserialized", len(vecs) >= 1)

# Wrong dimension blob
class MockRepoBadDim:
    def get_centroid_history(self, narrative_id, days):
        return [{"centroid_blob": np.zeros(512, dtype=np.float32).tobytes()}]

vecs2 = _load_centroid_history_vecs(MockRepoBadDim(), "test-id", days=7, emb_dim=768)
T("Wrong-dimension blob rejected", len(vecs2) == 0)


S("Settings: ensure_data_dirs creates directories")

from settings import ensure_data_dirs
import tempfile

with tempfile.TemporaryDirectory() as tmpdir:
    s = Settings(
        ANTHROPIC_API_KEY="test",
        DB_PATH=f"{tmpdir}/sub/deep/test.db",
        LSH_INDEX_PATH=f"{tmpdir}/sub/lsh.pkl",
        FAISS_INDEX_PATH=f"{tmpdir}/sub/faiss.pkl",
        ASSET_LIBRARY_PATH=f"{tmpdir}/sub/assets.pkl",
    )
    ensure_data_dirs(s)
    T("ensure_data_dirs creates nested directories",
      os.path.isdir(f"{tmpdir}/sub/deep"))


# ─── Batch FAISS search (P12 Batch 2.4) ────────────────────────────────

S("VectorStore: batch_search correctness")

from vector_store import FaissVectorStore
import tempfile as _tempfile

def _make_vs(dim: int = 4):
    tmp = _tempfile.mktemp(suffix=".pkl")
    vs = FaissVectorStore(tmp)
    vs.initialize(dim)
    return vs

_vs = _make_vs(4)
_v1 = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
_v2 = np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32)
_v3 = np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32)
_vs.add(np.stack([_v1, _v2, _v3]), ["id1", "id2", "id3"])

# batch_search on empty store
_empty_vs = _make_vs(4)
_empty_res = _empty_vs.batch_search(np.stack([_v1]))
T("batch_search on empty store returns (0.0, None)",
  len(_empty_res) == 1 and _empty_res[0] == (0.0, None),
  f"got {_empty_res}")

# batch results match single search
_q = np.array([0.9, 0.1, 0.0, 0.0], dtype=np.float32)  # closest to id1
_batch = _vs.batch_search(np.stack([_q]))
_single_dist, _single_ids = _vs.search(_q, k=1)
T("batch_search nearest matches single search result",
  len(_batch) == 1 and _batch[0][1] == (_single_ids[0] if _single_ids else None),
  f"batch={_batch[0]}, single=({_single_dist}, {_single_ids})")

# multiple queries in one call
_queries = np.stack([_v1, _v2, _v3])
_multi = _vs.batch_search(_queries)
T("batch_search returns one result per query row",
  len(_multi) == 3,
  f"got {len(_multi)} results")
T("batch_search per-row nearest: id1→id1, id2→id2, id3→id3",
  _multi[0][1] == "id1" and _multi[1][1] == "id2" and _multi[2][1] == "id3",
  f"got {[r[1] for r in _multi]}")


# ─── Report ─────────────────────────────────────────────────────────────

success = report()
sys.exit(0 if success else 1)
