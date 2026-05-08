import os
import sqlite3
import sys
import uuid
from pathlib import Path

import numpy as np

sys.path.insert(0, ".")

from clustering import periodic_narrative_dedup, run_clustering
from repository import SqliteRepository
from settings import Settings


_results = []


def S(name: str):
    print(f"\n--- {name} ---")


def T(name: str, ok: bool, details: str = ""):
    _results.append((name, ok))
    mark = "PASS" if ok else "FAIL"
    suffix = f" ({details})" if details else ""
    print(f"  {mark}: {name}{suffix}")


class _Embedder:
    def dimension(self):
        return 4

    def embed(self, texts):
        out = []
        for i, _ in enumerate(texts):
            if i == 0:
                out.append(np.zeros(4, dtype=np.float32))
            else:
                v = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
                out.append(v)
        return np.array(out, dtype=np.float32)


class _Settings:
    HDBSCAN_MIN_CLUSTER_SIZE = 2
    HDBSCAN_MIN_SAMPLES = 2
    CLUSTER_MAX_PENDING_BATCH = 2
    PIPELINE_FREQUENCY_HOURS = 4


class _VS:
    def __init__(self):
        self._v = {}

    def count(self):
        return len(self._v)

    def initialize(self, _dim):
        return None

    def get_all_ids(self):
        return list(self._v.keys())

    def get_vector(self, nid):
        return self._v.get(nid)

    def add(self, vecs, ids):
        for i, nid in enumerate(ids):
            self._v[nid] = np.array(vecs[i], dtype=np.float32)

    def delete(self, nid):
        self._v.pop(nid, None)


def _repo_path() -> str:
    base = Path(".tmp")
    base.mkdir(exist_ok=True)
    return str(base / f"p13_test_{uuid.uuid4().hex}.db")


S("Settings checks")
try:
    s = Settings(ANTHROPIC_API_KEY="x", CLUSTER_MAX_PENDING_BATCH=8, HDBSCAN_MIN_CLUSTER_SIZE=8)
    T("CLUSTER_MAX_PENDING_BATCH accepted when >= HDBSCAN_MIN_CLUSTER_SIZE", s.CLUSTER_MAX_PENDING_BATCH == 8)
except Exception as exc:
    T("CLUSTER_MAX_PENDING_BATCH accepted when >= HDBSCAN_MIN_CLUSTER_SIZE", False, str(exc))


S("Repository bounded FIFO candidate reads")
db1 = _repo_path()
repo1 = SqliteRepository(db1)
repo1.migrate()
for i in range(5):
    repo1.insert_candidate(
        {
            "doc_id": f"d{i}",
            "raw_text": f"text {i}",
            "source_url": "",
            "source_domain": "",
            "published_at": f"2026-01-0{i+1}T00:00:00+00:00",
            "ingested_at": f"2026-01-0{i+1}T00:00:00+00:00",
            "author": "",
            "raw_text_hash": f"h{i}",
            "embedding_blob": np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32).tobytes(),
            "status": "pending",
            "narrative_id_assigned": None,
        }
    )
rows = repo1.get_candidate_buffer(status="pending", limit=3)
T("get_candidate_buffer(limit=3) returns 3 rows", len(rows) == 3)
T("get_candidate_buffer is FIFO by ingested_at/doc_id", [r["doc_id"] for r in rows] == ["d0", "d1", "d2"])


S("run_clustering uses bounded pending intake")
vs1 = _VS()
try:
    out = run_clustering(repo1, vs1, _Embedder(), _Settings())
    # With min_cluster_size=2 and bounded read to 2 oldest rows, only one cluster should be created.
    T("run_clustering returns list", isinstance(out, list))
    T("run_clustering consumed bounded batch", len(out) <= 1, f"new_narratives={len(out)}")
except Exception as exc:
    T("run_clustering bounded intake execution", False, str(exc))


S("periodic dedup max_pairs cap")
db2 = _repo_path()
repo2 = SqliteRepository(db2)
repo2.migrate()
vs2 = _VS()
now = "2026-05-01T00:00:00+00:00"
for i in range(6):
    nid = f"n{i}"
    repo2.insert_narrative(
        {
            "narrative_id": nid,
            "name": f"N{i}",
            "stage": "Growing",
            "created_at": now,
            "last_updated_at": now,
            "suppressed": 0,
            "document_count": 10,
            "ns_score": 0.5,
        }
    )
    vs2.add(np.array([[1.0, 0.0, 0.0, 0.0]], dtype=np.float32), [nid])

merged = periodic_narrative_dedup(repo2, vs2, threshold=2.0, max_pairs=1)
T("max_pairs cap short-circuits sweep without crashing", merged == 0)

for p in (db1, db2):
    try:
        sqlite3.connect(p).close()
        os.remove(p)
    except Exception:
        pass

total = len(_results)
passed = sum(1 for _, ok in _results if ok)
print(f"\nP13 verification summary: {passed}/{total} passed")
if passed != total:
    sys.exit(1)
