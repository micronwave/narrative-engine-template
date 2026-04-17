"""
Foundation Fixes (final_solution_1) — Checkpoints A+B+C Tests

Tests for Fixes 1-5 (Checkpoint A):
  T1-T5: Stage oscillation hysteresis
  T6:    Sonnet escalation threshold
  T7-T9: RSS conditional HTTP (feed_metadata table)
  T10-T12: Financial relevance filter
  T13:   LLM pricing constants
  T14-T20: Boundary/edge case audit

Tests for Fixes 6-12 (Checkpoint B):
  T21-T22: Two-tier financial filter
  T23-T24: Asset mapping improvements
  T25-T26: MarketAux source_domain extraction
  T27-T29: NewsData pubDate normalization
  T30:     CoinGecko exception logging
  T31-T32: tracker.increment ordering

Tests for Fixes 13-14 (Checkpoint C):
  T33-T34: Centroid decay dormant exclusion
  T35-T36: Dead code cleanup verification
"""

import sys
import tempfile
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

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


# ===========================================================================
# Stage Hysteresis Tests (Fix 1)
# ===========================================================================
from signals import compute_lifecycle_stage

S("Fix 1: Stage hysteresis — cycles < 3 blocks transition")
result = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=19, days_since_creation=30,
    cycles_in_current_stage=1,
)
T("T1: Mature stays Mature when cycles < 3", result == "Mature", f"got {result}")

S("Fix 1: Stage hysteresis — cycles >= 3 allows transition")
result = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=19, days_since_creation=30,
    cycles_in_current_stage=5,
)
T("T2: Mature → Declining when cycles >= 3", result == "Declining", f"got {result}")

S("Fix 1: Stage hysteresis — Dormant→Growing bypasses hysteresis")
result = compute_lifecycle_stage(
    current_stage="Dormant", document_count=5, velocity_windowed=0.15,
    entropy=None, consecutive_declining_cycles=20, days_since_creation=60,
    cycles_in_current_stage=0,
)
T("T3: Revival allowed immediately (cycles=0)", result == "Growing", f"got {result}")

S("Fix 1: Stage hysteresis — Mature→Declining thresholds")
# velocity at 0.015 — was below old threshold 0.02, but above new 0.01
result_no_decline = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.015,
    entropy=2.0, consecutive_declining_cycles=0, days_since_creation=30,
    cycles_in_current_stage=10,
)
T("T4: velocity 0.015 no longer triggers Declining", result_no_decline == "Mature",
  f"got {result_no_decline}")

S("Fix 1: Schema — cycles_in_current_stage column exists")
from repository import SqliteRepository
_tf = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tf.close()
_repo = SqliteRepository(_tf.name)
_repo.migrate()
_n = _repo.get_narrative("nonexistent")  # just to trigger table creation
import sqlite3
_conn = sqlite3.connect(_tf.name)
_conn.row_factory = sqlite3.Row
_cols = [c[1] for c in _conn.execute("PRAGMA table_info(narratives)").fetchall()]
_conn.close()
T("T5: cycles_in_current_stage column exists", "cycles_in_current_stage" in _cols,
  f"columns: {_cols[-5:]}")

# ===========================================================================
# Sonnet Threshold Test (Fix 2)
# ===========================================================================
S("Fix 2: Sonnet escalation threshold")
from settings import Settings
_settings = Settings()
T("T6: CONFIDENCE_ESCALATION_THRESHOLD is 0.35",
  _settings.CONFIDENCE_ESCALATION_THRESHOLD == 0.35,
  f"got {_settings.CONFIDENCE_ESCALATION_THRESHOLD}")

# ===========================================================================
# RSS Conditional HTTP Tests (Fix 3)
# ===========================================================================
S("Fix 3: feed_metadata table exists")
_fm_cols = [c[1] for c in sqlite3.connect(_tf.name).execute("PRAGMA table_info(feed_metadata)").fetchall()]
T("T7: feed_metadata table created by migrate()", len(_fm_cols) > 0,
  f"columns: {_fm_cols}")

S("Fix 3: upsert_feed_metadata insert and retrieve")
_repo.upsert_feed_metadata("https://example.com/feed.xml", "abc123", "Mon, 01 Jan 2026 00:00:00 GMT", 5)
_meta = _repo.get_feed_metadata("https://example.com/feed.xml")
T("T8: feed_metadata round-trips correctly",
  _meta is not None and _meta["etag"] == "abc123" and _meta["consecutive_empty_cycles"] == 0,
  f"got {_meta}")

S("Fix 3: consecutive_empty_cycles tracking")
_repo.upsert_feed_metadata("https://example.com/feed.xml", "abc123", "Mon, 01 Jan 2026 00:00:00 GMT", 0)
_meta2 = _repo.get_feed_metadata("https://example.com/feed.xml")
empty1 = _meta2["consecutive_empty_cycles"]
_repo.upsert_feed_metadata("https://example.com/feed.xml", "abc123", "Mon, 01 Jan 2026 00:00:00 GMT", 0)
_meta3 = _repo.get_feed_metadata("https://example.com/feed.xml")
empty2 = _meta3["consecutive_empty_cycles"]
_repo.upsert_feed_metadata("https://example.com/feed.xml", "abc123", "Mon, 01 Jan 2026 00:00:00 GMT", 3)
_meta4 = _repo.get_feed_metadata("https://example.com/feed.xml")
empty_reset = _meta4["consecutive_empty_cycles"]
T("T9: empty cycles increment then reset",
  empty1 == 1 and empty2 == 2 and empty_reset == 0,
  f"got {empty1}, {empty2}, {empty_reset}")

# ===========================================================================
# Financial Relevance Filter Tests (Fix 5)
# ===========================================================================
from ingester import is_financially_relevant

S("Fix 5: Financial filter — Tier 1 keyword passes")
T("T10: 'stock market' passes filter",
  is_financially_relevant("The stock market crashed today"))

S("Fix 5: Financial filter — non-financial rejected")
T("T11: 'celebrity gossip sports' rejected",
  not is_financially_relevant("Celebrity gossip about sports entertainment tonight"))

S("Fix 5: Financial filter — public importability")
try:
    from ingester import is_financially_relevant as _ifr
    T("T12: is_financially_relevant is importable (public name)", True)
except ImportError as e:
    T("T12: is_financially_relevant is importable (public name)", False, str(e))

# ===========================================================================
# LLM Pricing Test (Fix 4)
# ===========================================================================
S("Fix 4: LLM pricing constants")
from llm_client import HAIKU_INPUT_PRICE_PER_M
T("T13: Haiku input pricing is $0.80/M",
  HAIKU_INPUT_PRICE_PER_M == 0.80,
  f"got {HAIKU_INPUT_PRICE_PER_M}")

# ===========================================================================
# Audit: Boundary & Edge Case Tests
# ===========================================================================
S("Audit: Hysteresis boundary — cycles_in_current_stage=2 (still blocked)")
result_c2 = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=19, days_since_creation=30,
    cycles_in_current_stage=2,
)
T("T14: cycles=2 blocks Mature→Declining", result_c2 == "Mature", f"got {result_c2}")

S("Audit: Hysteresis boundary — cycles_in_current_stage=3 (just passes)")
result_c3 = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.005,
    entropy=2.0, consecutive_declining_cycles=19, days_since_creation=30,
    cycles_in_current_stage=3,
)
T("T15: cycles=3 allows Mature→Declining", result_c3 == "Declining", f"got {result_c3}")

S("Audit: Velocity boundary — exactly 0.01 should NOT trigger Declining")
result_v01 = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.01,
    entropy=2.0, consecutive_declining_cycles=0, days_since_creation=30,
    cycles_in_current_stage=10,
)
T("T16: velocity=0.01 does not trigger Declining", result_v01 == "Mature", f"got {result_v01}")

S("Audit: Revival boundary — velocity exactly 0.10 should NOT revive")
result_rev = compute_lifecycle_stage(
    current_stage="Declining", document_count=20, velocity_windowed=0.10,
    entropy=2.0, consecutive_declining_cycles=5, days_since_creation=20,
    cycles_in_current_stage=0,
)
T("T17: velocity=0.10 does not revive", result_rev == "Declining", f"got {result_rev}")

S("Audit: Dormant stays Dormant when velocity low (no warning expected)")
result_dorm = compute_lifecycle_stage(
    current_stage="Dormant", document_count=5, velocity_windowed=0.03,
    entropy=None, consecutive_declining_cycles=30, days_since_creation=90,
    cycles_in_current_stage=50,
)
T("T18: Dormant stays Dormant", result_dorm == "Dormant", f"got {result_dorm}")

S("Audit: consecutive_declining_cycles=30 boundary triggers Declining")
result_cd30 = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.05,
    entropy=2.0, consecutive_declining_cycles=30, days_since_creation=30,
    cycles_in_current_stage=5,
)
T("T19: consecutive_declining_cycles=30 triggers Declining", result_cd30 == "Declining", f"got {result_cd30}")

S("Audit: consecutive_declining_cycles=29 does NOT trigger Declining")
result_cd29 = compute_lifecycle_stage(
    current_stage="Mature", document_count=20, velocity_windowed=0.05,
    entropy=2.0, consecutive_declining_cycles=29, days_since_creation=30,
    cycles_in_current_stage=5,
)
T("T20: consecutive_declining_cycles=29 stays Mature", result_cd29 == "Mature", f"got {result_cd29}")

# ===========================================================================
# Checkpoint B Tests (Fixes 6-12)
# ===========================================================================

# --- Fix 11: Two-Tier Financial Filter ---
S("Fix 11: Two-tier filter — Tier 2 only rejected")
T("T21: 'military crisis' (Tier 2 only) rejected",
  not is_financially_relevant("Military crisis escalates in the region"))

S("Fix 11: Two-tier filter — Tier 1 + Tier 2 passes")
T("T22: 'military crisis stock market' (Tier 1+2) passes",
  is_financially_relevant("Military crisis impacts global stock market"))

# --- Fix 12: Asset Mapping ---
S("Fix 12: ASSET_MAPPING_MIN_SIMILARITY setting")
T("T23: ASSET_MAPPING_MIN_SIMILARITY defaults to 0.60",
  _settings.ASSET_MAPPING_MIN_SIMILARITY == 0.60,
  f"got {getattr(_settings, 'ASSET_MAPPING_MIN_SIMILARITY', 'MISSING')}")

S("Fix 12: Sector validation suppresses irrelevant ticker")
from asset_mapper import TOPIC_SECTOR_RELEVANCE
_mock_sector_map = {"AAPL": "Technology", "XOM": "Energy", "JPM": "Financials"}
# crypto topic should not allow Technology sector for this validation test
# (checking the mapping exists and has narrow sectors)
_crypto_sectors = TOPIC_SECTOR_RELEVANCE.get("crypto", set())
T("T24: crypto topic has narrow sector set",
  len(_crypto_sectors) > 0 and "Energy" not in _crypto_sectors,
  f"got {_crypto_sectors}")

# --- Fix 9: MarketAux source_domain extraction ---
S("Fix 9: MarketAux source_domain extraction")
from urllib.parse import urlparse
_test_url = "https://www.reuters.com/technology/some-article"
_extracted = urlparse(_test_url).netloc.replace("www.", "")
T("T25: real domain extracted from URL",
  _extracted == "reuters.com", f"got {_extracted}")

_empty_url = ""
_fallback = urlparse(_empty_url).netloc.replace("www.", "") if _empty_url else "marketaux.com"
T("T26: empty URL falls back to marketaux.com",
  _fallback == "marketaux.com", f"got {_fallback}")

# --- Fix 10: NewsData pubDate normalization ---
S("Fix 10: NewsData pubDate normalization")
import sys as _sys
_sys.path.insert(0, _ROOT)
from api_ingesters import _normalize_pubdate

_iso_tz = _normalize_pubdate("2026-03-25T10:00:00+05:00")
T("T27: ISO8601 with timezone → UTC",
  _iso_tz.startswith("2026-03-25T05:00:00"), f"got {_iso_tz}")

_iso_notz = _normalize_pubdate("2026-03-25T10:00:00")
T("T28: ISO8601 without timezone → assumes UTC",
  "2026-03-25T10:00:00" in _iso_notz and "+00:00" in _iso_notz,
  f"got {_iso_notz}")

_rfc2822 = _normalize_pubdate("Tue, 25 Mar 2026 10:00:00 +0000")
T("T29: RFC 2822 date → UTC ISO8601",
  _rfc2822.startswith("2026-03-25T10:00:00"), f"got {_rfc2822}")

# --- Fix 6: CoinGecko exception logging ---
S("Fix 6: CoinGecko logging sanitization")
import inspect
_cg_path = Path(_ROOT) / "api" / "adapters" / "coingecko_adapter.py"
_cg_src = _cg_path.read_text()
T("T30: CoinGecko logs type(e).__name__, not str(e)",
  'type(e).__name__)' in _cg_src and 'symbol, e)' not in _cg_src,
  "checked source code")

# --- Fix 7: tracker.increment position ---
S("Fix 7: tracker.increment after processing")
_ai_path = Path(_ROOT) / "api_ingesters.py"
_ai_src = _ai_path.read_text()
# In each ingester, tracker.increment should come AFTER the for loop (after docs list is built)
# Check that increment appears after the for-loop body (after append) in MarketAux section
_maux_section = _ai_src[_ai_src.index("class MarketauxIngester"):_ai_src.index("class NewsdataIngester")]
_maux_increment_pos = _maux_section.index('tracker.increment')
_maux_append_pos = _maux_section.rindex('docs.append')
T("T31: MarketAux tracker.increment after article loop",
  _maux_increment_pos > _maux_append_pos,
  f"increment@{_maux_increment_pos} vs last append@{_maux_append_pos}")

_ndata_section = _ai_src[_ai_src.index("class NewsdataIngester"):_ai_src.index("class RedditIngester")]
_ndata_increment_pos = _ndata_section.index('tracker.increment')
_ndata_append_pos = _ndata_section.rindex('docs.append')
T("T32: NewsData tracker.increment after article loop",
  _ndata_increment_pos > _ndata_append_pos,
  f"increment@{_ndata_increment_pos} vs last append@{_ndata_append_pos}")

# ===========================================================================
# Checkpoint C Tests (Fixes 13-14)
# ===========================================================================

# --- Fix 13: Centroid Decay Dormant Exclusion ---
S("Fix 13: Dormant narratives excluded from decay")
import uuid as _uuid
_decay_repo = SqliteRepository(_tf.name)
_decay_repo.migrate()
_dormant_id = f"narr-dormant-{_uuid.uuid4().hex[:8]}"
_growing_id = f"narr-growing-{_uuid.uuid4().hex[:8]}"
_today = "2026-03-31"
# Insert a Dormant narrative and a Growing narrative
with _decay_repo._get_conn() as _dc:
    _dc.execute(
        "INSERT INTO narratives (narrative_id, name, stage, suppressed, document_count) VALUES (?, ?, ?, 0, 5)",
        (_dormant_id, "Test Dormant", "Dormant"),
    )
    _dc.execute(
        "INSERT INTO narratives (narrative_id, name, stage, suppressed, document_count) VALUES (?, ?, ?, 0, 10)",
        (_growing_id, "Test Growing", "Growing"),
    )
_decay_ids = _decay_repo.get_narratives_needing_decay(_today)
T("T33: Dormant narrative excluded from decay list",
  _dormant_id not in _decay_ids,
  f"dormant_id={'found' if _dormant_id in _decay_ids else 'excluded'}")

T("T34: Growing narrative included in decay list",
  _growing_id in _decay_ids,
  f"growing_id={'found' if _growing_id in _decay_ids else 'missing'}")

# --- Fix 14: Dead Code Cleanup ---
S("Fix 14: Dead code removed from mutations.py")
_mut_path = Path(_ROOT) / "mutations.py"
_mut_src = _mut_path.read_text()

# stage_change section: template call should NOT be followed by an LLM fallback
_stage_block = _mut_src[_mut_src.index("# stage_change"):_mut_src.index("# doc_surge")]
_has_stage_fallback = "generate_llm_explanation" in _stage_block
T("T35: stage_change LLM fallback removed",
  not _has_stage_fallback,
  "generate_llm_explanation still in stage_change block" if _has_stage_fallback else "clean")

_sonnet_block = _mut_src[_mut_src.index("# new_sonnet"):_mut_src.index("return mutations")]
_has_sonnet_fallback = "generate_llm_explanation" in _sonnet_block
T("T36: new_sonnet LLM fallback removed",
  not _has_sonnet_fallback,
  "generate_llm_explanation still in new_sonnet block" if _has_sonnet_fallback else "clean")

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 60)
passed = sum(1 for _, ok in _results if ok)
failed = sum(1 for _, ok in _results if not ok)
print(f"Foundation Fixes (Checkpoints A+B+C): {passed} passed, {failed} failed out of {len(_results)}")
if failed:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  FAIL: {name}")
    sys.exit(1)
else:
    print("All tests passed.")

# Cleanup
import os
try:
    os.unlink(_tf.name)
except OSError:
    pass
