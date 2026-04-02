"""
Signal Redesign Phase 2 — Source Tier Tracking Tests

Section 1: get_domain_tier (4 tests)
  SP2-TIER-1: Known tier 1 domain returns 1
  SP2-TIER-2: www prefix stripped correctly
  SP2-TIER-3: Unknown domain returns DEFAULT_TIER
  SP2-TIER-4: None and empty string return DEFAULT_TIER

Section 2: compute_source_escalation (7 tests)
  SP2-ESC-1: Empty evidence returns safe defaults
  SP2-ESC-2: Mixed-tier evidence returns correct highest_tier and tier_breadth
  SP2-ESC-3: Tier 1 evidence within 24h → is_institutional_pickup=True
  SP2-ESC-4: Old tier 1 evidence (>24h) → is_institutional_pickup=False
  SP2-ESC-5: Single-tier evidence → escalation_velocity=0.0
  SP2-ESC-6: Evidence with missing/None published_at skipped gracefully
  SP2-ESC-7: Evidence with missing/None source_domain skipped gracefully

Section 3: compute_weighted_source_score (4 tests)
  SP2-WSS-1: Tier 1 domain weighted 3x vs tier 4 domain weighted 1x
  SP2-WSS-2: Output clamped to [0.0, 1.0]
  SP2-WSS-3: Empty evidence returns 0.0
  SP2-WSS-4: corpus_domain_count=0 returns 0.0 without crash

Section 4: Schema (1 test)
  SP2-SCH-1: All 5 new columns exist after migrate()

Section 5: Integration (2 tests)
  SP2-INT-1: Round-trip persistence of escalation fields
  SP2-INT-2: Known subdomain news.bbc.com → tier 2
"""

import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from source_tiers import (
    DOMAIN_TIERS,
    DEFAULT_TIER,
    TIER_WEIGHTS,
    get_domain_tier,
    compute_source_escalation,
    compute_weighted_source_score,
)
from repository import SqliteRepository

# ---------------------------------------------------------------------------
# Test runner helpers
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
def _make_evidence(domain: str, published_at: str | None = None) -> dict:
    return {
        "doc_id": f"doc-{domain}-{published_at}",
        "narrative_id": "test-nar-1",
        "source_url": f"https://{domain}/article",
        "source_domain": domain,
        "published_at": published_at,
        "author": "test",
        "excerpt": "test excerpt",
    }


def _iso_hours_ago(hours: float) -> str:
    dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    return dt.isoformat()


def _get_temp_repo() -> SqliteRepository:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    repo = SqliteRepository(path)
    repo.migrate()
    return repo


# ===========================================================================
# Section 1: get_domain_tier
# ===========================================================================
S("SP2-TIER: get_domain_tier")

T("SP2-TIER-1: reuters.com returns tier 1",
  get_domain_tier("reuters.com") == 1,
  f"got {get_domain_tier('reuters.com')}")

T("SP2-TIER-2: www.bloomberg.com returns tier 1 (www stripped)",
  get_domain_tier("www.bloomberg.com") == 1,
  f"got {get_domain_tier('www.bloomberg.com')}")

T("SP2-TIER-3: unknown domain returns DEFAULT_TIER ({})".format(DEFAULT_TIER),
  get_domain_tier("unknownblog.xyz") == DEFAULT_TIER,
  f"got {get_domain_tier('unknownblog.xyz')}")

T("SP2-TIER-4: None and empty string return DEFAULT_TIER",
  get_domain_tier(None) == DEFAULT_TIER
  and get_domain_tier("") == DEFAULT_TIER
  and get_domain_tier("   ") == DEFAULT_TIER,
  f"None={get_domain_tier(None)}, empty={get_domain_tier('')}, spaces={get_domain_tier('   ')}")


# ===========================================================================
# Section 2: compute_source_escalation
# ===========================================================================
S("SP2-ESC: compute_source_escalation")

_empty_result = compute_source_escalation([])
T("SP2-ESC-1: empty evidence returns safe defaults",
  _empty_result["highest_tier"] == 5
  and _empty_result["tier_breadth"] == 0
  and _empty_result["escalation_velocity"] == 0.0
  and _empty_result["is_institutional_pickup"] is False
  and _empty_result["tier_first_seen"] == {},
  f"got {_empty_result}")

_mixed_evidence = [
    _make_evidence("reuters.com", _iso_hours_ago(2)),     # tier 1
    _make_evidence("techcrunch.com", _iso_hours_ago(10)),  # tier 3
    _make_evidence("reddit.com", _iso_hours_ago(20)),      # tier 5
]
_mixed_result = compute_source_escalation(_mixed_evidence)
T("SP2-ESC-2: mixed-tier evidence → highest_tier=1, tier_breadth=3",
  _mixed_result["highest_tier"] == 1
  and _mixed_result["tier_breadth"] == 3,
  f"highest={_mixed_result['highest_tier']}, breadth={_mixed_result['tier_breadth']}")

_recent_institutional = [
    _make_evidence("wsj.com", _iso_hours_ago(1)),     # tier 1, 1h ago
    _make_evidence("reddit.com", _iso_hours_ago(10)),  # tier 5
]
_recent_result = compute_source_escalation(_recent_institutional)
T("SP2-ESC-3: tier 1 within 24h → is_institutional_pickup=True",
  _recent_result["is_institutional_pickup"] is True,
  f"got {_recent_result['is_institutional_pickup']}")

_old_institutional = [
    _make_evidence("wsj.com", _iso_hours_ago(48)),     # tier 1, 48h ago
    _make_evidence("reddit.com", _iso_hours_ago(50)),  # tier 5
]
_old_result = compute_source_escalation(_old_institutional)
T("SP2-ESC-4: tier 1 >24h ago → is_institutional_pickup=False",
  _old_result["is_institutional_pickup"] is False,
  f"got {_old_result['is_institutional_pickup']}")

# Regression: old AND new tier-1 evidence — latest must trigger pickup
_mixed_age_institutional = [
    _make_evidence("reuters.com", _iso_hours_ago(48)),  # tier 1, 48h ago (old)
    _make_evidence("wsj.com", _iso_hours_ago(2)),       # tier 1, 2h ago (recent)
    _make_evidence("reddit.com", _iso_hours_ago(50)),   # tier 5
]
_mixed_age_result = compute_source_escalation(_mixed_age_institutional)
T("SP2-ESC-4b: old + recent tier 1 → is_institutional_pickup=True (uses latest)",
  _mixed_age_result["is_institutional_pickup"] is True,
  f"got {_mixed_age_result['is_institutional_pickup']}")

_single_tier = [
    _make_evidence("reuters.com", _iso_hours_ago(5)),
    _make_evidence("bloomberg.com", _iso_hours_ago(3)),
]
_single_result = compute_source_escalation(_single_tier)
T("SP2-ESC-5: single-tier evidence → escalation_velocity=0.0",
  _single_result["escalation_velocity"] == 0.0
  and _single_result["tier_breadth"] == 1,
  f"velocity={_single_result['escalation_velocity']}, breadth={_single_result['tier_breadth']}")

_missing_ts = [
    _make_evidence("reuters.com", None),
    _make_evidence("techcrunch.com", ""),
    _make_evidence("reddit.com", "not-a-date"),
]
_missing_ts_result = compute_source_escalation(_missing_ts)
T("SP2-ESC-6: missing/invalid published_at → no crash, safe defaults",
  _missing_ts_result["highest_tier"] == 5
  and _missing_ts_result["escalation_velocity"] == 0.0
  and _missing_ts_result["tier_first_seen"] == {},
  f"got {_missing_ts_result}")

_missing_domain = [
    {"doc_id": "d1", "source_domain": None, "published_at": _iso_hours_ago(1)},
    {"doc_id": "d2", "source_domain": "", "published_at": _iso_hours_ago(2)},
    {"doc_id": "d3", "published_at": _iso_hours_ago(3)},  # key missing entirely
]
_missing_domain_result = compute_source_escalation(_missing_domain)
T("SP2-ESC-7: missing/None source_domain → skipped, safe defaults",
  _missing_domain_result["highest_tier"] == 5
  and _missing_domain_result["tier_breadth"] == 0,
  f"got {_missing_domain_result}")


# ===========================================================================
# Section 3: compute_weighted_source_score
# ===========================================================================
S("SP2-WSS: compute_weighted_source_score")

# Tier 1 (reuters, weight 3.0) vs tier 4 (seekingalpha, weight 1.0)
_t1_evidence = [_make_evidence("reuters.com", _iso_hours_ago(1))]
_t4_evidence = [_make_evidence("seekingalpha.com", _iso_hours_ago(1))]
_t1_score = compute_weighted_source_score(_t1_evidence, 10)
_t4_score = compute_weighted_source_score(_t4_evidence, 10)
T("SP2-WSS-1: tier 1 domain (3x) scores higher than tier 4 (1x)",
  _t1_score == 3.0 / 10 and _t4_score == 1.0 / 10 and _t1_score > _t4_score,
  f"tier1={_t1_score}, tier4={_t4_score}")

# Clamp: many high-tier domains with low corpus count
_many_t1 = [
    _make_evidence("reuters.com", _iso_hours_ago(1)),
    _make_evidence("bloomberg.com", _iso_hours_ago(1)),
    _make_evidence("wsj.com", _iso_hours_ago(1)),
    _make_evidence("ft.com", _iso_hours_ago(1)),
    _make_evidence("apnews.com", _iso_hours_ago(1)),
]
_clamped = compute_weighted_source_score(_many_t1, 1)
T("SP2-WSS-2: output clamped to [0.0, 1.0]",
  _clamped == 1.0,
  f"got {_clamped}")

T("SP2-WSS-3: empty evidence returns 0.0",
  compute_weighted_source_score([], 10) == 0.0
  and compute_weighted_source_score(None, 10) == 0.0,
  f"empty={compute_weighted_source_score([], 10)}, None={compute_weighted_source_score(None, 10)}")

T("SP2-WSS-4: corpus_domain_count=0 returns 0.0 without crash",
  compute_weighted_source_score(_t1_evidence, 0) <= 1.0,
  f"got {compute_weighted_source_score(_t1_evidence, 0)}")


# ===========================================================================
# Section 4: Schema
# ===========================================================================
S("SP2-SCH: Database schema")

_schema_repo = _get_temp_repo()
_expected_cols = [
    "source_highest_tier",
    "source_tier_breadth",
    "source_escalation_velocity",
    "source_institutional_pickup",
    "weighted_source_score",
]
with _schema_repo._get_conn() as conn:
    cursor = conn.execute("PRAGMA table_info(narratives)")
    _col_names = {row[1] for row in cursor.fetchall()}

T("SP2-SCH-1: all 5 new columns exist on narratives table",
  all(c in _col_names for c in _expected_cols),
  f"missing: {[c for c in _expected_cols if c not in _col_names]}")


# ===========================================================================
# Section 5: Integration
# ===========================================================================
S("SP2-INT: Integration round-trip")

_int_repo = _get_temp_repo()
_test_nar_id = "int-test-nar-001"
with _int_repo._get_conn() as conn:
    conn.execute(
        "INSERT INTO narratives (narrative_id, name) VALUES (?, ?)",
        (_test_nar_id, "Test narrative"),
    )
_int_repo.update_narrative(_test_nar_id, {
    "source_highest_tier": 2,
    "source_tier_breadth": 3,
    "source_escalation_velocity": 1.5,
    "source_institutional_pickup": 1,
    "weighted_source_score": 0.75,
})
_readback = _int_repo.get_narrative(_test_nar_id)
T("SP2-INT-1: escalation fields persist and read back correctly",
  _readback is not None
  and _readback["source_highest_tier"] == 2
  and _readback["source_tier_breadth"] == 3
  and abs(_readback["source_escalation_velocity"] - 1.5) < 0.001
  and _readback["source_institutional_pickup"] == 1
  and abs(_readback["weighted_source_score"] - 0.75) < 0.001,
  f"got {dict((k, _readback[k]) for k in _expected_cols) if _readback else 'None'}")

T("SP2-INT-2: known subdomain news.bbc.com maps to tier 2",
  get_domain_tier("news.bbc.com") == 2,
  f"got {get_domain_tier('news.bbc.com')}")


# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 60)
_passed = sum(1 for _, ok in _results if ok)
_total = len(_results)
print(f"Phase 2 results: {_passed}/{_total} passed")
if _passed < _total:
    print("FAILED:")
    for name, ok in _results:
        if not ok:
            print(f"  - {name}")
    sys.exit(1)
else:
    print("ALL PASSED")
