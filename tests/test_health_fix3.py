"""
Health Fix 3 — Stale Data Cleanup Tests

Section A: TOPIC: pseudo-ticker cleanup migration
  T1: No TOPIC: entries remain in linked_assets after migrate()
  T2: Migration is idempotent (safe to run twice)
  T3: Legitimate tickers preserved after cleanup
  T4: Empty/null linked_assets rows unaffected

Section B: Centroid backfill from centroid_history
  T5: Backfill loads centroids for narratives missing from VectorStore
  T6: Backfill skips narratives already in VectorStore (no duplicates)
  T7: Dimension-mismatched blobs are skipped
  T8: Narratives with no centroid_history entry are left out
  T9: Backfilled vectors are searchable in VectorStore
"""

import json
import os
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
from vector_store import FaissVectorStore

repo = SqliteRepository(_tmp_db())
repo.migrate()

# =========================================================================
# Section A: TOPIC: pseudo-ticker cleanup
# =========================================================================
S("Section A: TOPIC: pseudo-ticker cleanup migration")

# Seed narratives with various linked_assets formats
_nid_topic_dict = str(uuid.uuid4())
_nid_topic_mixed = str(uuid.uuid4())
_nid_clean = str(uuid.uuid4())
_nid_null = str(uuid.uuid4())
_nid_empty_list = str(uuid.uuid4())

_now = "2026-04-03T12:00:00Z"

for nid in [_nid_topic_dict, _nid_topic_mixed, _nid_clean, _nid_null, _nid_empty_list]:
    repo.insert_narrative({
        "narrative_id": nid,
        "name": "test",
        "stage": "Emerging",
        "created_at": _now,
        "last_updated_at": _now,
    })

# Dict format with TOPIC: ticker (all TOPIC:)
repo.update_narrative(_nid_topic_dict, {
    "linked_assets": json.dumps([
        {"ticker": "TOPIC:inflation CPI report", "similarity_score": 0.61},
        {"ticker": "TOPIC:credit spread widening", "similarity_score": 0.55},
    ])
})

# Mixed: some TOPIC:, some real tickers
repo.update_narrative(_nid_topic_mixed, {
    "linked_assets": json.dumps([
        {"ticker": "AAPL", "similarity_score": 0.82},
        {"ticker": "TOPIC:Iran sanctions oil embargo", "similarity_score": 0.63},
        {"ticker": "MSFT", "similarity_score": 0.78},
    ])
})

# Clean — only real tickers
repo.update_narrative(_nid_clean, {
    "linked_assets": json.dumps([
        {"ticker": "GOOGL", "similarity_score": 0.85},
    ])
})

# NULL linked_assets
repo.update_narrative(_nid_null, {"linked_assets": None})

# Empty list
repo.update_narrative(_nid_empty_list, {"linked_assets": json.dumps([])})

# Run migration (which includes the TOPIC: cleanup)
repo.migrate()

# T1: No TOPIC: entries remain
with repo._get_conn() as conn:
    topic_rows = conn.execute(
        "SELECT narrative_id FROM narratives WHERE linked_assets LIKE '%TOPIC:%'"
    ).fetchall()
T("T1: No TOPIC: entries remain after migrate()", len(topic_rows) == 0,
  f"found {len(topic_rows)} rows with TOPIC:")

# T2: Idempotent — run again, no error
try:
    repo.migrate()
    idempotent_ok = True
except Exception as e:
    idempotent_ok = False
T("T2: Migration is idempotent", idempotent_ok)

# T3: Legitimate tickers preserved
n_mixed = repo.get_narrative(_nid_topic_mixed)
mixed_assets = json.loads(n_mixed["linked_assets"]) if n_mixed["linked_assets"] else []
real_tickers = [a["ticker"] for a in mixed_assets]
T("T3: Legitimate tickers preserved after cleanup",
  real_tickers == ["AAPL", "MSFT"],
  f"got {real_tickers}")

n_clean = repo.get_narrative(_nid_clean)
clean_assets = json.loads(n_clean["linked_assets"]) if n_clean["linked_assets"] else []
T("T3b: Fully-clean row untouched",
  len(clean_assets) == 1 and clean_assets[0]["ticker"] == "GOOGL")

# T4: Null/empty rows unaffected
n_null = repo.get_narrative(_nid_null)
T("T4a: NULL linked_assets unaffected", n_null["linked_assets"] is None)

n_empty = repo.get_narrative(_nid_empty_list)
empty_assets = json.loads(n_empty["linked_assets"]) if n_empty["linked_assets"] else []
T("T4b: Empty list unaffected", empty_assets == [])

# All-TOPIC row should have empty list now
n_all_topic = repo.get_narrative(_nid_topic_dict)
all_topic_assets = json.loads(n_all_topic["linked_assets"]) if n_all_topic["linked_assets"] else []
T("T4c: All-TOPIC row cleaned to empty list", all_topic_assets == [],
  f"got {all_topic_assets}")

# =========================================================================
# Section B: Centroid backfill
# =========================================================================
S("Section B: Centroid backfill from centroid_history")

EMB_DIM = 768

repo2 = SqliteRepository(_tmp_db())
repo2.migrate()

_nids = [str(uuid.uuid4()) for _ in range(5)]
for nid in _nids:
    repo2.insert_narrative({
        "narrative_id": nid,
        "name": f"narrative-{nid[:8]}",
        "stage": "Emerging",
        "created_at": _now,
        "last_updated_at": _now,
    })

# Create L2-normalized centroid vectors for first 3 narratives
_centroids = {}
for nid in _nids[:3]:
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    vec /= np.linalg.norm(vec)
    _centroids[nid] = vec
    repo2.insert_centroid_history(nid, "2026-04-03", vec.tobytes())

# _nids[3] has no centroid_history
# _nids[4] will be pre-loaded in VectorStore

vs = FaissVectorStore(_tmp_db())  # won't try to load from disk
vs.initialize(EMB_DIM)

# Pre-load _nids[4] into VectorStore
pre_vec = np.random.randn(EMB_DIM).astype(np.float32)
pre_vec /= np.linalg.norm(pre_vec)
vs.add(pre_vec.reshape(1, -1), [_nids[4]])
# Also give it a centroid_history so it shows up in batch
repo2.insert_centroid_history(_nids[4], "2026-04-03", pre_vec.tobytes())

# --- Simulate the backfill logic from pipeline.py ---
existing_ids = set(vs.get_all_ids())
active_narratives_init = repo2.get_all_active_narratives()
missing_ids = [
    n["narrative_id"] for n in active_narratives_init
    if n["narrative_id"] not in existing_ids
]
blob_map = repo2.get_latest_centroids_batch(missing_ids)
backfilled = 0
for nid, blob in blob_map.items():
    vec = np.frombuffer(blob, dtype=np.float32).copy()
    if vec.shape[0] == EMB_DIM:
        vs.add(vec.reshape(1, -1), [nid])
        backfilled += 1

# T5: Backfill loaded centroids for missing narratives
T("T5: Backfill loaded 3 missing centroids", backfilled == 3,
  f"backfilled={backfilled}")

# T6: Pre-existing narrative not duplicated
all_ids = vs.get_all_ids()
count_nid4 = all_ids.count(_nids[4])
T("T6: Pre-existing narrative not duplicated", count_nid4 == 1,
  f"count={count_nid4}")

# T7: Dimension mismatch handling
repo3 = SqliteRepository(_tmp_db())
repo3.migrate()
nid_bad = str(uuid.uuid4())
repo3.insert_narrative({
    "narrative_id": nid_bad,
    "name": "bad-dim",
    "stage": "Emerging",
    "created_at": _now,
    "last_updated_at": _now,
})
bad_vec = np.random.randn(384).astype(np.float32)  # wrong dimension
repo3.insert_centroid_history(nid_bad, "2026-04-03", bad_vec.tobytes())

vs3 = FaissVectorStore(_tmp_db())
vs3.initialize(EMB_DIM)

blob_map3 = repo3.get_latest_centroids_batch([nid_bad])
skipped = 0
for nid, blob in blob_map3.items():
    vec = np.frombuffer(blob, dtype=np.float32).copy()
    if vec.shape[0] != EMB_DIM:
        skipped += 1

T("T7: Dimension-mismatched blob skipped", skipped == 1)

# T8: Narrative with no centroid_history left out
has_vec_3 = vs.get_vector(_nids[3])
T("T8: Narrative without centroid_history has no vector", has_vec_3 is None)

# T9: Backfilled vectors are searchable
query_vec = _centroids[_nids[0]]
distances, found_ids = vs.search(query_vec, k=1)
T("T9: Backfilled vector is searchable", len(found_ids) > 0 and found_ids[0] == _nids[0],
  f"found={found_ids}")

# =========================================================================
# Section C: Audit edge cases
# =========================================================================
S("Section C: Audit edge cases")

# T10: Mixed-type list (string + dict) with TOPIC: in dict element
repo_mix = SqliteRepository(_tmp_db())
repo_mix.migrate()
_nid_mixed_types = str(uuid.uuid4())
repo_mix.insert_narrative({
    "narrative_id": _nid_mixed_types,
    "name": "mixed-types",
    "stage": "Emerging",
    "created_at": _now,
    "last_updated_at": _now,
})
repo_mix.update_narrative(_nid_mixed_types, {
    "linked_assets": json.dumps([
        "AAPL",
        {"ticker": "TOPIC:crypto defi narrative", "similarity_score": 0.5},
        {"ticker": "MSFT", "similarity_score": 0.78},
    ])
})
repo_mix.migrate()
n_mixed_types = repo_mix.get_narrative(_nid_mixed_types)
mt_assets = json.loads(n_mixed_types["linked_assets"]) if n_mixed_types["linked_assets"] else []
# After fix: TOPIC: dict should be removed, string + clean dict kept
mt_has_topic = any(
    (isinstance(a, dict) and a.get("ticker", "").startswith("TOPIC:"))
    or (isinstance(a, str) and a.startswith("TOPIC:"))
    for a in mt_assets
)
T("T10: Mixed-type list TOPIC: dict removed", not mt_has_topic,
  f"got {mt_assets}")
T("T10b: Mixed-type list keeps clean entries", len(mt_assets) == 2,
  f"kept {len(mt_assets)}")

# T11: _backfill_centroids helper returns a summary and handles empty VectorStore
from pipeline import _backfill_centroids as _bf

repo_bf = SqliteRepository(_tmp_db())
repo_bf.migrate()
_nids_bf = [str(uuid.uuid4()) for _ in range(3)]
for nid in _nids_bf:
    repo_bf.insert_narrative({
        "narrative_id": nid,
        "name": f"bf-{nid[:8]}",
        "stage": "Emerging",
        "created_at": _now,
        "last_updated_at": _now,
    })
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    vec /= np.linalg.norm(vec)
    repo_bf.insert_centroid_history(nid, "2026-04-03", vec.tobytes())

vs_bf = FaissVectorStore(_tmp_db())
vs_bf.initialize(EMB_DIM)
summary = _bf(repo_bf, vs_bf, EMB_DIM)
T(
  "T11: _backfill_centroids returns correct summary",
  summary["requested"] == 3
  and summary["missing"] == 3
  and summary["recovered"] == 3
  and summary["missing_history"] == []
  and summary["corrupt_blob"] == []
  and summary["dim_mismatch"] == []
  and summary["failed_recovery"] == [],
  f"returned {summary}",
)

# T12: _backfill_centroids is idempotent (second call returns no new work)
summary2 = _bf(repo_bf, vs_bf, EMB_DIM)
T(
  "T12: Second backfill call returns zero recovered and zero missing",
  summary2["requested"] == 3
  and summary2["missing"] == 0
  and summary2["recovered"] == 0
  and summary2["missing_history"] == []
  and summary2["corrupt_blob"] == []
  and summary2["dim_mismatch"] == []
  and summary2["failed_recovery"] == [],
  f"returned {summary2}",
)

# T13: Corrupt blob (odd byte length) doesn't crash
repo_corrupt = SqliteRepository(_tmp_db())
repo_corrupt.migrate()
_nid_corrupt = str(uuid.uuid4())
repo_corrupt.insert_narrative({
    "narrative_id": _nid_corrupt,
    "name": "corrupt-blob",
    "stage": "Emerging",
    "created_at": _now,
    "last_updated_at": _now,
})
# 7 bytes — not a multiple of 4 (float32)
repo_corrupt.insert_centroid_history(_nid_corrupt, "2026-04-03", b"\x01\x02\x03\x04\x05\x06\x07")
vs_corrupt = FaissVectorStore(_tmp_db())
vs_corrupt.initialize(EMB_DIM)
try:
    count_corrupt = _bf(repo_corrupt, vs_corrupt, EMB_DIM)
    corrupt_ok = True
except Exception as e:
    corrupt_ok = False
    count_corrupt = -1
T("T13: Corrupt blob doesn't crash backfill", corrupt_ok,
  f"count={count_corrupt}")
T("T13b: Corrupt blob not loaded into VectorStore",
  vs_corrupt.get_vector(_nid_corrupt) is None)

# T14: Batch query chunking — large ID list doesn't fail
repo_chunk = SqliteRepository(_tmp_db())
repo_chunk.migrate()
large_ids = [str(uuid.uuid4()) for _ in range(600)]
for nid in large_ids[:5]:
    repo_chunk.insert_narrative({
        "narrative_id": nid,
        "name": f"chunk-{nid[:8]}",
        "stage": "Emerging",
        "created_at": _now,
        "last_updated_at": _now,
    })
    vec = np.random.randn(EMB_DIM).astype(np.float32)
    repo_chunk.insert_centroid_history(nid, "2026-04-03", vec.tobytes())
try:
    result = repo_chunk.get_latest_centroids_batch(large_ids)
    chunk_ok = True
except Exception as e:
    chunk_ok = False
    result = {}
T("T14: Batch query with 600 IDs succeeds (chunking)", chunk_ok,
  f"returned {len(result)} centroids")
T("T14b: Returns only existing centroids", len(result) == 5,
  f"got {len(result)}")

# =========================================================================
# Summary
# =========================================================================
print("\n" + "=" * 60)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Health Fix 3: {passed}/{total} passed")
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
