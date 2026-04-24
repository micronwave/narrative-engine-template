"""
Health Fix 7v4 — Labeling Retry Limit

Section 1: Source/schema checks
  T1: repository migration adds labeling_attempts
  T2: pipeline exposes the failed-labeling helper

Section 2: Behavior checks
  T3: failed labeling increments attempts
  T4: successful labeling does not increment attempts
  T5: third failed attempt retires narrative to Dormant
  T6: retirement deletes the centroid and does not save inline
"""

import os
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-unit-tests")

from pipeline import _handle_failed_labeling_attempt  # noqa: E402
from repository import SqliteRepository  # noqa: E402

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


@dataclass
class StubVectorStore:
    delete_calls: list[str] | None = None
    save_calls: int = 0

    def __post_init__(self):
        if self.delete_calls is None:
            self.delete_calls = []

    def delete(self, narrative_id: str) -> None:
        self.delete_calls.append(narrative_id)

    def save(self) -> None:
        self.save_calls += 1


def _insert_narrative(repo: SqliteRepository, *, narrative_id: str, attempts: int = 0) -> None:
    repo.insert_narrative({
        "narrative_id": narrative_id,
        "name": None,
        "description": None,
        "stage": "Emerging",
        "created_at": "2026-04-23T00:00:00+00:00",
        "last_updated_at": "2026-04-23T00:00:00+00:00",
        "suppressed": 0,
        "human_review_required": 0,
        "document_count": 6,
        "ns_score": 0.0,
        "labeling_attempts": attempts,
    })


# ======================================================================
# Section 1: Source/schema checks
# ======================================================================
S("Section 1: Source/schema checks")

pipeline_src = Path(_ROOT, "pipeline.py").read_text(encoding="utf-8")
repo_src = Path(_ROOT, "repository.py").read_text(encoding="utf-8")

T(
    "T1: repository migration adds labeling_attempts",
    "ALTER TABLE narratives ADD COLUMN labeling_attempts INTEGER DEFAULT 0" in repo_src,
)
T(
    "T2: pipeline exposes the failed-labeling helper",
    "def _handle_failed_labeling_attempt(" in pipeline_src,
)


# ======================================================================
# Section 2: Behavior checks
# ======================================================================
S("Section 2: Behavior checks")

tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
tmp.close()
repo = SqliteRepository(tmp.name)
repo.migrate()
with repo._get_conn() as conn:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(narratives)")}
T("T1b: labeling_attempts exists in schema", "labeling_attempts" in columns, f"columns={sorted(columns)}")

failed_id = str(uuid.uuid4())
success_id = str(uuid.uuid4())
retire_id = str(uuid.uuid4())
_insert_narrative(repo, narrative_id=failed_id, attempts=0)
_insert_narrative(repo, narrative_id=success_id, attempts=0)
_insert_narrative(repo, narrative_id=retire_id, attempts=2)

vector_store = StubVectorStore()

failed_row = repo.get_narrative(failed_id)
retired = _handle_failed_labeling_attempt(
    repo,
    vector_store,
    failed_row,
    needs_label=True,
    label_persisted=False,
    now_iso="2026-04-23T01:00:00+00:00",
)
failed_row_after = repo.get_narrative(failed_id)
T(
    "T3: failed labeling increments attempts",
    retired is False
    and int(failed_row_after.get("labeling_attempts") or 0) == 1
    and failed_row_after.get("stage") == "Emerging",
    f"row={failed_row_after}",
)

success_row = repo.get_narrative(success_id)
retired = _handle_failed_labeling_attempt(
    repo,
    vector_store,
    success_row,
    needs_label=True,
    label_persisted=True,
    now_iso="2026-04-23T01:00:00+00:00",
)
success_row_after = repo.get_narrative(success_id)
T(
    "T4: successful labeling does not increment attempts",
    retired is False
    and int(success_row_after.get("labeling_attempts") or 0) == 0
    and success_row_after.get("stage") == "Emerging",
    f"row={success_row_after}",
)

retire_row = repo.get_narrative(retire_id)
retired = _handle_failed_labeling_attempt(
    repo,
    vector_store,
    retire_row,
    needs_label=True,
    label_persisted=False,
    now_iso="2026-04-23T01:00:00+00:00",
)
retire_row_after = repo.get_narrative(retire_id)
T(
    "T5: third failed attempt retires narrative",
    retired is True
    and retire_row_after.get("stage") == "Dormant"
    and int(retire_row_after.get("labeling_attempts") or 0) == 3
    and "Auto-retired: labeling failed after 3 attempts" in (retire_row_after.get("description") or ""),
    f"row={retire_row_after}",
)

T(
    "T6: retirement deletes centroid and does not save inline",
    vector_store.delete_calls == [retire_id]
    and vector_store.save_calls == 0,
    f"delete_calls={vector_store.delete_calls}, save_calls={vector_store.save_calls}",
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
print(f"Health Fix 7v4 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  \u2717 {name}")
    sys.exit(1)
else:
    print("All Health Fix 7v4 tests passed.")
