"""
Health Fix 7v3 — Periodic Narrative Dedup Sweep

Section 1: Source integration checks
  T1: clustering.py exposes periodic_narrative_dedup
  T2: pipeline.py imports periodic_narrative_dedup
  T3: Step 9.6 counts pipeline cycles from step_number = 0
  T4: Step 9.6 logs periodic_dedup with step_number 96

Section 2: Behavior checks
  T5: Recent duplicate narratives merge
  T6: Established large narratives are skipped by the age/doc gate
  T7: Vector store save is not called by the sweep function itself
"""

import os
import sqlite3
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-unit-tests")

from clustering import periodic_narrative_dedup
from repository import SqliteRepository

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


def _iso_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


@dataclass
class StubVectorStore:
    vectors: dict[str, object]
    save_calls: int = 0
    delete_calls: list[str] | None = None

    def __post_init__(self):
        if self.delete_calls is None:
            self.delete_calls = []

    def get_vector(self, narrative_id: str):
        return self.vectors.get(narrative_id)

    def delete(self, narrative_id: str) -> None:
        self.delete_calls.append(narrative_id)
        self.vectors.pop(narrative_id, None)

    def save(self) -> None:
        self.save_calls += 1


def _insert_narrative(repo: SqliteRepository, *, narrative_id: str, name: str, created_at: str,
                      document_count: int, ns_score: float = 0.5) -> None:
    repo.insert_narrative({
        "narrative_id": narrative_id,
        "name": name,
        "description": f"Description for {name}",
        "stage": "Growing",
        "created_at": created_at,
        "last_updated_at": created_at,
        "suppressed": 0,
        "human_review_required": 0,
        "document_count": document_count,
        "ns_score": ns_score,
    })
    for idx in range(document_count):
        doc_id = f"{narrative_id}-doc-{idx}"
        repo.insert_document_evidence({
            "doc_id": doc_id,
            "narrative_id": narrative_id,
            "source_url": f"https://example.com/{doc_id}",
            "source_domain": "example.com",
            "published_at": created_at,
            "author": "tester",
            "excerpt": f"Excerpt {idx}",
        })


# ======================================================================
# Section 1: Source integration checks
# ======================================================================
S("Section 1: Source integration checks")

clustering_src = Path(_ROOT, "clustering.py").read_text(encoding="utf-8")
pipeline_src = Path(_ROOT, "pipeline.py").read_text(encoding="utf-8")

T(
    "T1: clustering.py exposes periodic_narrative_dedup",
    "def periodic_narrative_dedup(" in clustering_src,
)

T(
    "T2: pipeline.py imports periodic_narrative_dedup",
    "from clustering import deduplicate_new_narratives, periodic_narrative_dedup, run_clustering" in pipeline_src,
)

T(
    "T3: Step 9.6 counts cycles from step_number = 0",
    "SELECT COUNT(*) FROM pipeline_run_log WHERE step_number = 0" in pipeline_src,
)

T(
    "T4: Step 9.6 logs periodic_dedup with step_number 96",
    '"periodic_dedup"' in pipeline_src
    and "run_count={run_count} merged={sweep_merged}" in pipeline_src
    and "96," in pipeline_src,
)


# ======================================================================
# Section 2: Behavior checks
# ======================================================================
S("Section 2: Behavior checks")

tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
tmp.close()
repo = SqliteRepository(tmp.name)
repo.migrate()

recent_a = str(uuid.uuid4())
recent_b = str(uuid.uuid4())
old_c = str(uuid.uuid4())
old_d = str(uuid.uuid4())

recent_vec = [1.0, 0.0, 0.0]
old_vec = [0.0, 1.0, 0.0]

_insert_narrative(repo, narrative_id=recent_a, name="Recent Alpha", created_at=_iso_days_ago(1), document_count=5)
_insert_narrative(repo, narrative_id=recent_b, name="Recent Beta", created_at=_iso_days_ago(2), document_count=3)
_insert_narrative(repo, narrative_id=old_c, name="Old Gamma", created_at=_iso_days_ago(12), document_count=150)
_insert_narrative(repo, narrative_id=old_d, name="Old Delta", created_at=_iso_days_ago(11), document_count=120)

vector_store = StubVectorStore({
    recent_a: recent_vec,
    recent_b: recent_vec,
    old_c: old_vec,
    old_d: old_vec,
})

merged = periodic_narrative_dedup(repo, vector_store)

recent_a_row = repo.get_narrative(recent_a)
recent_b_row = repo.get_narrative(recent_b)
old_c_row = repo.get_narrative(old_c)
old_d_row = repo.get_narrative(old_d)

T(
    "T5: recent duplicate narratives merge",
    merged == 1
    and recent_a_row is not None
    and recent_b_row is not None
    and recent_a_row["stage"] != "Dormant"
    and recent_b_row["stage"] == "Dormant"
    and "Merged into" in (recent_b_row.get("description") or "")
    and int(recent_a_row.get("document_count") or 0) == 8,
    f"merged={merged}, survivor={recent_a_row}, absorbed={recent_b_row}",
)

T(
    "T6: established large narratives are skipped",
    old_c_row is not None
    and old_d_row is not None
    and old_c_row["stage"] != "Dormant"
    and old_d_row["stage"] != "Dormant"
    and int(old_c_row.get("document_count") or 0) == 150
    and int(old_d_row.get("document_count") or 0) == 120,
    f"old_c={old_c_row}, old_d={old_d_row}",
)

T(
    "T7: sweep function does not call save() itself",
    vector_store.save_calls == 0,
    f"save_calls={vector_store.save_calls}",
)

try:
    os.unlink(tmp.name)
except OSError:
    pass


# ======================================================================
# Summary
# ======================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"Health Fix 7v3 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  \u2717 {name}")
    sys.exit(1)
else:
    print("All Health Fix 7v3 tests passed.")
