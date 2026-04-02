"""
V3 Phase 3 — Data Enrichment Tests

  V3-RED-1: RedditIngester can be instantiated
  V3-RED-2: RedditIngester.is_enabled() returns False without credentials
  V3-PI-1: compute_public_interest returns 0-1 float
  V3-PI-2: compute_public_interest with zero inputs returns 0
  V3-PI-3: compute_public_interest with high Reddit signal is boosted
  V3-EARN-1: GET /api/earnings/upcoming returns 200
  V3-EARN-2: earnings_service can be imported
  V3-SENT-3: compute_sentiment_scores handles single doc
"""

import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
_API_DIR = str(Path(__file__).parent.parent / "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app  # noqa: E402

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


client = TestClient(app)

# ===========================================================================
# Reddit Ingester Tests
# ===========================================================================
S("V3-RED: Reddit Ingester")

from reddit_ingester import RedditIngester  # noqa: E402


class FakeSettings:
    REDDIT_CLIENT_ID = ""
    REDDIT_CLIENT_SECRET = ""
    REDDIT_USER_AGENT = "test/1.0"
    REDDIT_SUBREDDITS = ["stocks"]
    REDDIT_POSTS_PER_SUB = 10


ingester = RedditIngester(repository=None, settings=FakeSettings())
T("RED-1: RedditIngester can be instantiated", ingester is not None)
T("RED-2: is_enabled() returns False without credentials", not ingester.is_enabled())

# With fake credentials
FakeSettings.REDDIT_CLIENT_ID = "test"
FakeSettings.REDDIT_CLIENT_SECRET = "test"
ingester2 = RedditIngester(repository=None, settings=FakeSettings())
T("RED-2b: is_enabled() returns True with credentials", ingester2.is_enabled())


# ===========================================================================
# Public Interest Signal Tests
# ===========================================================================
S("V3-PI: Public Interest")

from signals import compute_public_interest  # noqa: E402

result = compute_public_interest(
    cross_source_score=0.5,
    cross_source_prev=0.3,
    doc_count=50,
    doc_count_prev=30,
    reddit_doc_count=5,
)
T("PI-1: returns float between 0-1", 0.0 <= result <= 1.0, f"result={result}")

result_zero = compute_public_interest(
    cross_source_score=0.0,
    cross_source_prev=0.0,
    doc_count=0,
    doc_count_prev=0,
    reddit_doc_count=0,
)
T("PI-2: zero inputs returns 0", result_zero == 0.0, f"result={result_zero}")

result_reddit = compute_public_interest(
    cross_source_score=0.5,
    cross_source_prev=0.5,
    doc_count=50,
    doc_count_prev=50,
    reddit_doc_count=20,
)
result_no_reddit = compute_public_interest(
    cross_source_score=0.5,
    cross_source_prev=0.5,
    doc_count=50,
    doc_count_prev=50,
    reddit_doc_count=0,
)
T("PI-3: Reddit signal boosts score", result_reddit > result_no_reddit,
  f"with_reddit={result_reddit}, without={result_no_reddit}")


# ===========================================================================
# Earnings Tests
# ===========================================================================
S("V3-EARN: Earnings")

try:
    from earnings_service import get_upcoming_earnings
    T("EARN-2: earnings_service importable", True)
except Exception as e:
    T("EARN-2: earnings_service importable", False, str(e))

resp = client.get("/api/earnings/upcoming?days=30")
T("EARN-1: GET /api/earnings/upcoming → 200", resp.status_code == 200, f"status={resp.status_code}")
data = resp.json()
T("EARN-1a: returns list", isinstance(data, list))


# ===========================================================================
# Sentiment additional test
# ===========================================================================
S("V3-SENT: Additional sentiment tests")

from signals import compute_sentiment_scores  # noqa: E402

single = compute_sentiment_scores(["Markets rallied strongly today"])
T("SENT-3: single doc returns count=1", single.get("count") == 1)
T("SENT-3a: std is 0 for single doc", single.get("std") == 0.0)


# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"V3 Phase 3 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All V3 Phase 3 tests passed.")
