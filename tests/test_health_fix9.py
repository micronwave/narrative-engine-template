"""
Health Fix 9 — Centrality backfill, cycle-slot velocity, and threshold recalibration

Section 1: Backfill behavior
  T1: missing centroid history is backfilled before graph construction
  T2: unrecoverable centroid blobs stay missing and surface one warning summary

Section 2: Cycle-slot velocity
  T3: distinct same-day slots are preserved in centroid history
  T4: non-identical same-day slots produce non-zero velocity_windowed

Section 3: Lifecycle thresholds
  T5: Emerging -> Growing uses the recalibrated velocity threshold
  T6: Mature -> Declining uses the recalibrated velocity threshold
"""

import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-unit-tests")

from centrality import build_narrative_graph  # noqa: E402
from pipeline import _backfill_centroids  # noqa: E402
from repository import SqliteRepository  # noqa: E402
from signals import compute_lifecycle_stage, compute_velocity_windowed  # noqa: E402

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


class StubVectorStore:
    def __init__(self):
        self.vectors: dict[str, np.ndarray] = {}

    def add(self, vectors: np.ndarray, ids: list[str]) -> None:
        rows = np.asarray(vectors, dtype=np.float32)
        if rows.ndim == 1:
            rows = rows.reshape(1, -1)
        for idx, nid in enumerate(ids):
            self.vectors[nid] = rows[idx].copy()

    def get_vector(self, doc_id: str) -> np.ndarray | None:
        return self.vectors.get(doc_id)

    def get_all_ids(self) -> list[str]:
        return list(self.vectors.keys())


def _insert_narrative(repo: SqliteRepository, narrative_id: str, *, stage: str = "Growing") -> None:
    repo.insert_narrative({
        "narrative_id": narrative_id,
        "name": f"Narrative {narrative_id[:8]}",
        "description": "test narrative",
        "stage": stage,
        "created_at": "2026-04-24T00:00:00+00:00",
        "last_updated_at": "2026-04-24T00:00:00+00:00",
        "suppressed": 0,
        "human_review_required": 0,
        "document_count": 10,
        "ns_score": 0.3,
        "cycles_in_current_stage": 3,
    })


# ======================================================================
# Section 1: Backfill behavior
# ======================================================================
S("Section 1: Backfill behavior")

tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
tmp.close()
repo = SqliteRepository(tmp.name)
repo.migrate()
store = StubVectorStore()

backfill_id = str(uuid.uuid4())
other_id = str(uuid.uuid4())
_insert_narrative(repo, backfill_id)
_insert_narrative(repo, other_id)

store.add(np.array([[1.0, 0.0, 0.0]], dtype=np.float32), [other_id])
repo.insert_centroid_history(backfill_id, "2026-04-24T04", np.array([1.0, 0.0, 0.0], dtype=np.float32).tobytes())
repo.insert_centroid_history(other_id, "2026-04-24T04", np.array([1.0, 0.0, 0.0], dtype=np.float32).tobytes())

summary = _backfill_centroids(
    repo,
    store,
    3,
    target_narrative_ids=[backfill_id, other_id],
)
graph = build_narrative_graph(
    [{"narrative_id": backfill_id}, {"narrative_id": other_id}],
    store,
    unrecoverable_missing_ids=set(),
)
T(
    "T1: missing centroid history is backfilled before graph construction",
    int(summary["recovered"]) == 1
    and store.get_vector(backfill_id) is not None
    and graph.number_of_nodes() == 2
    and graph.number_of_edges() == 1,
    f"summary={summary}, nodes={graph.number_of_nodes()}, edges={graph.number_of_edges()}",
)

bad_id = str(uuid.uuid4())
_insert_narrative(repo, bad_id)
repo.insert_centroid_history(bad_id, "2026-04-24T08", np.array([1.0, 2.0], dtype=np.float32).tobytes())
summary_bad = _backfill_centroids(
    repo,
    store,
    3,
    target_narrative_ids=[bad_id],
)
with patch("centrality.logger.warning") as warn_mock:
    graph_bad = build_narrative_graph(
        [{"narrative_id": bad_id}, {"narrative_id": other_id}],
        store,
        unrecoverable_missing_ids={bad_id},
    )
T(
    "T2: unrecoverable centroid blobs stay missing and surface one warning summary",
    int(summary_bad["recovered"]) == 0
    and bad_id in summary_bad["dim_mismatch"]
    and store.get_vector(bad_id) is None
    and warn_mock.call_count == 1
    and graph_bad.number_of_nodes() == 2,
    f"summary={summary_bad}, warnings={warn_mock.call_count}",
)

try:
    os.unlink(tmp.name)
except OSError:
    pass


# ======================================================================
# Section 2: Cycle-slot velocity
# ======================================================================
S("Section 2: Cycle-slot velocity")

today = datetime.now(timezone.utc).date().isoformat()
slot_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
slot_tmp.close()
slot_repo = SqliteRepository(slot_tmp.name)
slot_repo.migrate()
slot_id = str(uuid.uuid4())
_insert_narrative(slot_repo, slot_id, stage="Emerging")

slot_repo.insert_centroid_history(slot_id, f"{today}T04", np.array([1.0, 0.0, 0.0], dtype=np.float32).tobytes())
slot_repo.insert_centroid_history(slot_id, f"{today}T08", np.array([0.0, 1.0, 0.0], dtype=np.float32).tobytes())

history = slot_repo.get_centroid_history(slot_id, days=7)
history_vecs = [np.frombuffer(row["centroid_blob"], dtype=np.float32).copy() for row in history]
velocity = compute_velocity_windowed(history_vecs, window_days=7)

T(
    "T3: distinct same-day slots are preserved in centroid history",
    len(history) == 2 and history[0]["date"].endswith("T08") and history[1]["date"].endswith("T04"),
    f"history={[row['date'] for row in history]}",
)

T(
    "T4: non-identical same-day slots produce non-zero velocity_windowed",
    velocity > 0.0,
    f"velocity={velocity}",
)

try:
    os.unlink(slot_tmp.name)
except OSError:
    pass


# ======================================================================
# Section 3: Lifecycle thresholds
# ======================================================================
S("Section 3: Lifecycle thresholds")

growing = compute_lifecycle_stage(
    current_stage="Emerging",
    document_count=9,
    velocity_windowed=0.021,
    entropy=0.5,
    consecutive_declining_cycles=0,
    days_since_creation=1,
    cycles_in_current_stage=3,
)
T("T5: Emerging -> Growing uses recalibrated threshold", growing == "Growing", f"got {growing}")

stays_emerging = compute_lifecycle_stage(
    current_stage="Emerging",
    document_count=9,
    velocity_windowed=0.019,
    entropy=0.5,
    consecutive_declining_cycles=0,
    days_since_creation=1,
    cycles_in_current_stage=3,
)
T("T5b: Emerging stays Emerging below recalibrated threshold", stays_emerging == "Emerging", f"got {stays_emerging}")

declining = compute_lifecycle_stage(
    current_stage="Mature",
    document_count=20,
    velocity_windowed=0.007,
    entropy=2.0,
    consecutive_declining_cycles=19,
    days_since_creation=10,
    cycles_in_current_stage=3,
)
T("T6: Mature -> Declining uses recalibrated threshold", declining == "Declining", f"got {declining}")

stays_mature = compute_lifecycle_stage(
    current_stage="Mature",
    document_count=20,
    velocity_windowed=0.008,
    entropy=2.0,
    consecutive_declining_cycles=19,
    days_since_creation=10,
    cycles_in_current_stage=3,
)
T("T6b: Mature stays Mature at the boundary", stays_mature == "Mature", f"got {stays_mature}")


# ======================================================================
# Summary
# ======================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Health Fix 9 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All Health Fix 9 tests passed.")
