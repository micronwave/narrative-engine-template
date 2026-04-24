"""
Health Fix 7v5 — Relevance Gating (Spam Prevention)

Section 1: Source checks
  T1: check_financial_relevance uses AND logic
  T2: Step 14 includes the single-source review gate

Section 2: Function checks
  T3: Tags alone do not pass
  T4: Keyword + tag passes
  T5: Keyword without tag does not pass

Section 3: Review flag checks
  T6: Non-financial narrative is flagged for review
  T7: Single-source narrative is flagged for review
  T8: Multi-source financial narrative is not flagged
"""

import os
import sys
import tempfile
import uuid
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-unit-tests")

from pipeline import _flag_post_label_review, check_financial_relevance  # noqa: E402
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


def _insert_narrative(repo: SqliteRepository, *, narrative_id: str, name: str, description: str,
                      topic_tags: str | None, source_count: int, human_review_required: int = 0) -> None:
    repo.insert_narrative({
        "narrative_id": narrative_id,
        "name": name,
        "description": description,
        "stage": "Growing",
        "created_at": "2026-04-23T00:00:00+00:00",
        "last_updated_at": "2026-04-23T00:00:00+00:00",
        "suppressed": 0,
        "human_review_required": human_review_required,
        "document_count": 12,
        "ns_score": 0.0,
        "topic_tags": topic_tags,
        "source_count": source_count,
    })


# ======================================================================
# Section 1: Source checks
# ======================================================================
S("Section 1: Source checks")

pipeline_src = Path(_ROOT, "pipeline.py").read_text(encoding="utf-8")

T(
    "T1: check_financial_relevance uses AND logic",
    "return has_financial and has_investable_tag" in pipeline_src,
)

T(
    "T2: Step 14 includes the single-source review gate",
    "_flag_post_label_review(" in pipeline_src and "source_count == 1" in pipeline_src,
)


# ======================================================================
# Section 2: Function checks
# ======================================================================
S("Section 2: Function checks")

T(
    "T3: Tags alone do not pass",
    check_financial_relevance("Danish Coalition Politics", "Party leaders disagree", '["geopolitical"]') is False,
)

T(
    "T4: Keyword + tag passes",
    check_financial_relevance("Stock Market Rally", "Prices surge on earnings", '["macro"]') is True,
)

T(
    "T5: Keyword without tag does not pass",
    check_financial_relevance("Stock Market Rally", "Prices surge on earnings", "[]") is False,
)


# ======================================================================
# Section 3: Review flag checks
# ======================================================================
S("Section 3: Review flag checks")

tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
tmp.close()
repo = SqliteRepository(tmp.name)
repo.migrate()

non_financial_id = str(uuid.uuid4())
single_source_id = str(uuid.uuid4())
multi_source_id = str(uuid.uuid4())

_insert_narrative(
    repo,
    narrative_id=non_financial_id,
    name="Laptop Reviews",
    description="tech products",
    topic_tags='["macro"]',
    source_count=4,
)
_insert_narrative(
    repo,
    narrative_id=single_source_id,
    name="Stock Market Rally",
    description="Prices surge on earnings",
    topic_tags='["macro"]',
    source_count=1,
)
_insert_narrative(
    repo,
    narrative_id=multi_source_id,
    name="Stock Market Rally",
    description="Prices surge on earnings",
    topic_tags='["macro"]',
    source_count=3,
)

row_non_financial = repo.get_narrative(non_financial_id)
row_single_source = repo.get_narrative(single_source_id)
row_multi_source = repo.get_narrative(multi_source_id)

_flag_post_label_review(
    repo,
    non_financial_id,
    row_non_financial,
    row_non_financial.get("name") or "",
    row_non_financial.get("description") or "",
    row_non_financial.get("topic_tags"),
)
_flag_post_label_review(
    repo,
    single_source_id,
    row_single_source,
    row_single_source.get("name") or "",
    row_single_source.get("description") or "",
    row_single_source.get("topic_tags"),
)
_flag_post_label_review(
    repo,
    multi_source_id,
    row_multi_source,
    row_multi_source.get("name") or "",
    row_multi_source.get("description") or "",
    row_multi_source.get("topic_tags"),
)

row_non_financial_after = repo.get_narrative(non_financial_id)
row_single_source_after = repo.get_narrative(single_source_id)
row_multi_source_after = repo.get_narrative(multi_source_id)

T(
    "T6: Non-financial narrative is flagged for review",
    int(row_non_financial_after.get("human_review_required") or 0) == 1,
    f"row={row_non_financial_after}",
)

T(
    "T7: Single-source narrative is flagged for review",
    int(row_single_source_after.get("human_review_required") or 0) == 1,
    f"row={row_single_source_after}",
)

T(
    "T8: Multi-source financial narrative is not flagged",
    int(row_multi_source_after.get("human_review_required") or 0) == 0,
    f"row={row_multi_source_after}",
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
print(f"Health Fix 7v5 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  \u2717 {name}")
    sys.exit(1)
else:
    print("All Health Fix 7v5 tests passed.")
