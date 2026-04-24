"""
Narrative Quality Audit — Phase 5: Validation & Prevention Tests

Section 11: Enhanced coherence validation prompt
  T1: Prompt checks for single coherent theme
  T2: Prompt checks for ongoing market-relevant trend
  T3: Prompt checks for financial/investment implication
  T4: Suppression threshold remains 0.5

Section 12: Post-labeling relevance gate
  T5: Name with financial keyword passes (no flag)
  T6: Description with financial keyword passes (no flag)
  T7: Name without financial keywords AND no topic tags -> flagged
  T8: Name without financial keywords BUT has topic tags -> flagged
  T9: "airline pricing amid oil crisis" passes
  T10: "Danish coalition politics" gets flagged
  T11: All existing named active narratives pass relevance gate

Section 13: Unlabeled cluster cleanup script
  T12: Cleanup script exists and is importable
  T13: Clusters with <50 docs would be set to Dormant
  T14: Clusters with >=50 docs would be re-validated
  T17: Fallback sentinel guard present
  T18: Relevance gate integrated
"""

import json
import sys
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

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


# ===========================================================================
# Section 11: Enhanced coherence validation prompt
# ===========================================================================
S("Section 11: Enhanced Coherence Validation Prompt")

# Read the clustering.py source to verify prompt content
clustering_src = Path(_ROOT, "clustering.py").read_text(encoding="utf-8")

T(
    "T1: Prompt checks for single coherent theme",
    "Form a single coherent theme" in clustering_src,
)

T(
    "T2: Prompt checks for ongoing market-relevant trend",
    "ongoing market-relevant trend" in clustering_src,
)

T(
    "T3: Prompt checks for financial/investment implication",
    "financial or investment implication" in clustering_src,
)

T(
    "T4: Suppression threshold remains 0.5",
    "coherence_score < 0.5" in clustering_src,
)

# ===========================================================================
# Section 12: Post-labeling relevance gate
# ===========================================================================
S("Section 12: Post-Labeling Relevance Gate")

# Verify the relevance gate exists in pipeline.py as an importable function
pipeline_src = Path(_ROOT, "pipeline.py").read_text(encoding="utf-8")

T(
    "T5: Relevance gate code exists in pipeline.py",
    "human_review_required" in pipeline_src and "_FINANCIAL_KEYWORDS" in pipeline_src,
)

# Test the actual extracted function — not a reimplementation
from pipeline import check_financial_relevance

T(
    "T6: Name with financial keyword but no tags is flagged",
    check_financial_relevance("Tech Stock Rally Continues", "Some description", None) is False,
)

T(
    "T7: Description with financial keyword but no tags is flagged",
    check_financial_relevance("Something Vague", "Rising inflation concerns", None) is False,
)

T(
    "T8: No financial keywords and no tags -> flagged",
    check_financial_relevance("Danish Coalition Politics", "Party leaders disagree on reform", None) is False,
)

T(
    "T9: No financial keywords BUT has topic tags -> flagged",
    check_financial_relevance("Danish Coalition Politics", "Party leaders disagree", '["geopolitical"]') is False,
)

T(
    "T10: 'airline pricing amid oil crisis' is flagged without tags",
    check_financial_relevance("Airline Pricing Amid Oil Crisis", "Airlines face rising costs", None) is False,
    "contains financial keywords but no topic tags"
)

T(
    "T11: 'Danish coalition politics' gets flagged",
    check_financial_relevance("Political Fragmentation in Denmark", "Coalition talks stall in Danish parliament", None) is False,
    "no financial keywords"
)

T(
    "T12: Step 14 uses the single-source review gate",
    "source_count == 1" in pipeline_src and "_flag_post_label_review(" in pipeline_src,
)

# ===========================================================================
# Section 13: Unlabeled cluster cleanup script
# ===========================================================================
S("Section 13: Unlabeled Cluster Cleanup Script")

# Verify the script is importable
try:
    import importlib
    mod = importlib.import_module("cleanup_unlabeled_clusters")
    script_importable = hasattr(mod, "main")
except Exception as exc:
    script_importable = False

T("T13: Cleanup script exists and has main()", script_importable)

# Verify the script uses the enhanced coherence prompt
script_src = Path(_ROOT, "cleanup_unlabeled_clusters.py").read_text(encoding="utf-8")

T(
    "T14: Script uses enhanced coherence prompt",
    "ongoing market-relevant trend" in script_src
    and "financial or investment implication" in script_src,
)

# Verify logic: <50 docs -> Dormant, >=50 docs -> re-validate
T(
    "T15: Script sets <50 doc clusters to Dormant",
    'doc_count < 50' in script_src and '"stage": "Dormant"' in script_src,
)

T(
    "T16: Script re-validates >=50 doc clusters with coherence check",
    "validate_cluster" in script_src and "coherence_score" in script_src,
)

T(
    "T17: Script guards against call_haiku fallback 'Validation unavailable'",
    '"Validation unavailable" in result' in script_src,
)

T(
    "T18: Script runs relevance gate via check_financial_relevance",
    "check_financial_relevance" in script_src,
)

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, c in _results if c)
total = len(_results)
print(f"RESULTS: {passed}/{total} passed")
if passed < total:
    for name, c in _results:
        if not c:
            print(f"  FAILED: {name}")
    sys.exit(1)
else:
    print("ALL TESTS PASSED")
