"""
F8 — Dormant Narrative Filter Tests

Unit:
  F8-U1: get_all_active_narratives excludes Dormant by default
  F8-U2: get_all_active_narratives includes Dormant when stage='Dormant'
  F8-U3: get_all_active_narratives includes all stages when stage is set explicitly (non-Dormant)
  F8-U4: count_active_narratives excludes Dormant by default
  F8-U5: count_active_narratives includes Dormant when stage='Dormant'
  F8-U6: suppressed=1 narratives always excluded regardless of stage
  F8-U7: topic filter combined with default exclusion still excludes Dormant
  F8-U8: stage + topic combined filter works correctly
"""

import sqlite3
import sys
import tempfile
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo() -> SqliteRepository:
    """Return a SqliteRepository backed by a fresh in-memory SQLite database."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    repo = SqliteRepository(tmp.name)
    repo.migrate()
    return repo


def _insert_narrative(repo: SqliteRepository, narrative_id: str, stage: str,
                       suppressed: int = 0, ns_score: float = 0.5,
                       topic_tags: str | None = None) -> None:
    with repo._get_conn() as conn:
        conn.execute(
            """INSERT INTO narratives
               (narrative_id, name, stage, suppressed, ns_score, topic_tags,
                document_count, velocity_windowed, created_at, last_updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 1, 0.0, '2026-01-01', '2026-01-01')""",
            (narrative_id, f"Narrative {narrative_id}", stage, suppressed,
             ns_score, topic_tags),
        )


# ---------------------------------------------------------------------------
# Fixture: one narrative per stage + a suppressed dormant + an active dormant
# ---------------------------------------------------------------------------

repo = _make_repo()
_insert_narrative(repo, "n-emerging", "Emerging", ns_score=0.9)
_insert_narrative(repo, "n-growing",  "Growing",  ns_score=0.8)
_insert_narrative(repo, "n-mature",   "Mature",   ns_score=0.7)
_insert_narrative(repo, "n-declining","Declining", ns_score=0.6)
_insert_narrative(repo, "n-dormant",  "Dormant",  ns_score=0.4)
_insert_narrative(repo, "n-dormant-suppressed", "Dormant", suppressed=1, ns_score=0.3)
_insert_narrative(repo, "n-topic-dormant", "Dormant", ns_score=0.2,
                  topic_tags='["regulatory"]')
_insert_narrative(repo, "n-topic-growing", "Growing", ns_score=0.5,
                  topic_tags='["regulatory"]')


# ===========================================================================
S("F8-U1: default listing excludes Dormant")
# ===========================================================================
rows = repo.get_all_active_narratives()
ids = {r["narrative_id"] for r in rows}
T("Dormant narrative absent",
  "n-dormant" not in ids,
  f"ids={ids}")
T("Dormant-suppressed absent",
  "n-dormant-suppressed" not in ids,
  f"ids={ids}")
T("non-Dormant narratives present",
  {"n-emerging", "n-growing", "n-mature", "n-declining"}.issubset(ids),
  f"ids={ids}")

# ===========================================================================
S("F8-U2: stage='Dormant' returns only Dormant unsuppressed rows")
# ===========================================================================
dormant_rows = repo.get_all_active_narratives(stage="Dormant")
dormant_ids = {r["narrative_id"] for r in dormant_rows}
T("active Dormant returned",
  "n-dormant" in dormant_ids,
  f"ids={dormant_ids}")
T("suppressed Dormant excluded",
  "n-dormant-suppressed" not in dormant_ids,
  f"ids={dormant_ids}")
T("non-Dormant not returned",
  not {"n-emerging", "n-growing"} & dormant_ids,
  f"ids={dormant_ids}")

# ===========================================================================
S("F8-U3: stage='Growing' returns only Growing rows")
# ===========================================================================
growing_rows = repo.get_all_active_narratives(stage="Growing")
growing_ids = {r["narrative_id"] for r in growing_rows}
T("Growing returned",
  "n-growing" in growing_ids and "n-topic-growing" in growing_ids,
  f"ids={growing_ids}")
T("non-Growing absent",
  "n-emerging" not in growing_ids and "n-dormant" not in growing_ids,
  f"ids={growing_ids}")

# ===========================================================================
S("F8-U4: count_active_narratives excludes Dormant by default")
# ===========================================================================
count_default = repo.count_active_narratives()
# Expect: Emerging, Growing, Growing-topic, Mature, Declining = 4 non-dormant unsuppressed
# n-topic-growing is Growing so included; n-topic-dormant is Dormant so excluded
expected_default = 5  # n-emerging, n-growing, n-mature, n-declining, n-topic-growing
T("count excludes Dormant",
  count_default == expected_default,
  f"count={count_default}, expected={expected_default}")

# ===========================================================================
S("F8-U5: count_active_narratives stage='Dormant' counts Dormant unsuppressed")
# ===========================================================================
count_dormant = repo.count_active_narratives(stage="Dormant")
# n-dormant + n-topic-dormant (both unsuppressed Dormant) = 2
T("count includes both unsuppressed Dormant",
  count_dormant == 2,
  f"count={count_dormant}, expected=2")

# ===========================================================================
S("F8-U6: suppressed=1 excluded regardless of stage filter")
# ===========================================================================
all_rows = repo.get_all_active_narratives(stage="Dormant")
all_ids = {r["narrative_id"] for r in all_rows}
T("suppressed Dormant not returned even with explicit stage filter",
  "n-dormant-suppressed" not in all_ids,
  f"ids={all_ids}")

# ===========================================================================
S("F8-U7: topic filter combined with default excludes Dormant")
# ===========================================================================
topic_rows = repo.get_all_active_narratives(topic="regulatory")
topic_ids = {r["narrative_id"] for r in topic_rows}
T("regulatory topic includes Growing narrative",
  "n-topic-growing" in topic_ids,
  f"ids={topic_ids}")
T("regulatory topic Dormant narrative excluded by default",
  "n-topic-dormant" not in topic_ids,
  f"ids={topic_ids}")

# ===========================================================================
S("F8-U8: stage='Dormant' + topic combined filter")
# ===========================================================================
combo_rows = repo.get_all_active_narratives(stage="Dormant", topic="regulatory")
combo_ids = {r["narrative_id"] for r in combo_rows}
T("Dormant + regulatory returns topic-dormant narrative",
  "n-topic-dormant" in combo_ids,
  f"ids={combo_ids}")
T("Dormant + regulatory excludes non-regulatory Dormant",
  "n-dormant" not in combo_ids,
  f"ids={combo_ids}")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
total = len(_results)
passed = sum(1 for _, ok in _results if ok)
print(f"\n{'='*50}")
print(f"Results: {passed}/{total} passed")
if passed < total:
    print("FAILED:")
    for name, ok in _results:
        if not ok:
            print(f"  - {name}")
    sys.exit(1)
else:
    print("All tests passed.")
