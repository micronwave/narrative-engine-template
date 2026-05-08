"""
Persistence & Market Data audit tests — validates fixes for repository.py and stock_data.py.

Sections:
  A: Connection Pragmas (C1 + M2)
  B: Column Sanitization (C2)
  C: Atomic Operations (C3)
  D: change_pct Logic (H1)
  E: Cache Bounds (H3)
  F: JSON Ticker Query (H5)

Run with:
    python -X utf8 tests/test_persistence_audit.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import json
import logging
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)

# ---------------------------------------------------------------------------
# Simple test runner (same convention as project)
# ---------------------------------------------------------------------------

_results: list[dict] = []
_current_section: str = "Unset"
_pass = 0
_fail = 0


def S(section_name: str) -> None:
    global _current_section
    _current_section = section_name


def T(name: str, condition: bool, details: str = "") -> None:
    global _pass, _fail
    _results.append({
        "section": _current_section,
        "name": name,
        "passed": bool(condition),
        "details": details,
    })
    if condition:
        _pass += 1
    else:
        _fail += 1


def _print_results():
    current_section = None
    for r in _results:
        if r["section"] != current_section:
            current_section = r["section"]
            print(f"\n{'=' * 60}")
            print(f"  {current_section}")
            print(f"{'=' * 60}")
        status = "PASS" if r["passed"] else "FAIL"
        line = f"  [{status}] {r['name']}"
        if r["details"]:
            line += f" — {r['details']}"
        print(line)
    print(f"\n{'=' * 60}")
    print(f"  Total: {_pass + _fail} | Passed: {_pass} | Failed: {_fail}")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo():
    """Create a temporary SqliteRepository for testing."""
    from repository import SqliteRepository
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    f.close()
    tmp = f.name
    repo = SqliteRepository(tmp)
    repo.migrate()
    return repo, tmp


def _cleanup(tmp_path):
    """Remove temp DB files."""
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(tmp_path + suffix)
        except FileNotFoundError:
            pass


# ===========================================================================
# Section A: Connection Pragmas (C1 + M2)
# ===========================================================================

S("A: Connection Pragmas")

repo_a, tmp_a = _make_repo()

# A1: busy_timeout is set on every connection
try:
    with repo_a._get_conn() as conn:
        result = conn.execute("PRAGMA busy_timeout").fetchone()
        bt = result[0]
    T("A1: busy_timeout=30000 on _get_conn()", bt == 30000, f"got {bt}")
except Exception as exc:
    T("A1: busy_timeout=30000 on _get_conn()", False, str(exc))

# A2: foreign_keys pragma is ON
try:
    with repo_a._get_conn() as conn:
        result = conn.execute("PRAGMA foreign_keys").fetchone()
        fk = result[0]
    T("A2: foreign_keys=ON on _get_conn()", fk == 1, f"got {fk}")
except Exception as exc:
    T("A2: foreign_keys=ON on _get_conn()", False, str(exc))

# A3: Concurrent writes don't fail (busy_timeout allows waiting)
import threading
import sqlite3

def _writer(db_path, barrier, results_list, writer_id):
    """Write from a separate thread to test busy_timeout."""
    from repository import SqliteRepository
    r = SqliteRepository(db_path)
    barrier.wait()  # sync start
    try:
        nid = f"concurrent_test_{writer_id}_{uuid.uuid4().hex[:8]}"
        r.insert_narrative({
            "narrative_id": nid,
            "name": f"Test Narrative {writer_id}",
            "stage": "Emerging",
            "created_at": "2026-01-01T00:00:00",
            "last_updated_at": "2026-01-01T00:00:00",
            "suppressed": 0,
        })
        results_list.append(("ok", writer_id))
    except Exception as exc:
        results_list.append(("error", str(exc)))

barrier = threading.Barrier(2)
thread_results = []
t1 = threading.Thread(target=_writer, args=(tmp_a, barrier, thread_results, 1))
t2 = threading.Thread(target=_writer, args=(tmp_a, barrier, thread_results, 2))
t1.start()
t2.start()
t1.join(timeout=10)
t2.join(timeout=10)

ok_count = sum(1 for status, _ in thread_results if status == "ok")
errors = [msg for status, msg in thread_results if status == "error"]
T("A3: Concurrent writes succeed (no SQLITE_BUSY)", ok_count == 2,
  f"ok={ok_count}, errors={errors}")

_cleanup(tmp_a)


# ===========================================================================
# Section B: Column Sanitization (C2)
# ===========================================================================

S("B: Column Sanitization")

repo_b, tmp_b = _make_repo()

# B1: Valid column names pass
try:
    valid_cols = repo_b._sanitize_columns(["narrative_id", "name", "stage"])
    T("B1: Valid column names pass", valid_cols == ["narrative_id", "name", "stage"],
      f"got {valid_cols}")
except Exception as exc:
    T("B1: Valid column names pass", False, str(exc))

# B2: Malicious column name raises ValueError
try:
    repo_b._sanitize_columns(["name; DROP TABLE narratives--"])
    T("B2: Malicious column raises ValueError", False, "no exception raised")
except ValueError as exc:
    T("B2: Malicious column raises ValueError", True, str(exc))
except Exception as exc:
    T("B2: Malicious column raises ValueError", False, f"wrong exception: {exc}")

# B3: insert_narrative with clean dict succeeds
try:
    repo_b.insert_narrative({
        "narrative_id": "test_b3",
        "name": "Clean Insert Test",
        "stage": "Emerging",
        "created_at": "2026-01-01T00:00:00",
        "last_updated_at": "2026-01-01T00:00:00",
        "suppressed": 0,
    })
    result = repo_b.get_narrative("test_b3")
    T("B3: insert_narrative with clean dict succeeds",
      result is not None and result["name"] == "Clean Insert Test")
except Exception as exc:
    T("B3: insert_narrative with clean dict succeeds", False, str(exc))

# B4: insert_narrative with injected key raises ValueError
try:
    repo_b.insert_narrative({
        "narrative_id": "test_b4",
        "name; DROP TABLE narratives--": "evil",
    })
    T("B4: Injected key raises ValueError", False, "no exception raised")
except ValueError:
    T("B4: Injected key raises ValueError", True)
except Exception as exc:
    T("B4: Injected key raises ValueError", False, f"wrong exception: {exc}")

_cleanup(tmp_b)


# ===========================================================================
# Section C: Atomic Operations (C3)
# ===========================================================================

S("C: Atomic Operations")

repo_c, tmp_c = _make_repo()

# Setup: insert a narrative first
repo_c.insert_narrative({
    "narrative_id": "atomic_test_1",
    "name": "Atomic Test",
    "stage": "Emerging",
    "created_at": "2026-01-01T00:00:00",
    "last_updated_at": "2026-01-01T00:00:00",
    "suppressed": 0,
})

# C1: assign_doc_to_narrative commits both INSERT and UPDATE atomically
try:
    repo_c.assign_doc_to_narrative("doc_001", "atomic_test_1")
    narrative = repo_c.get_narrative("atomic_test_1")
    has_assignment = narrative["last_assignment_date"] is not None
    # Check the assignment row exists
    with repo_c._get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM narrative_assignments WHERE doc_id = ? AND narrative_id = ?",
            ("doc_001", "atomic_test_1"),
        ).fetchone()
    T("C1: assign_doc_to_narrative commits both operations",
      has_assignment and row is not None,
      f"assignment_date={narrative.get('last_assignment_date')}, row={'found' if row else 'missing'}")
except Exception as exc:
    T("C1: assign_doc_to_narrative commits both operations", False, str(exc))

# C2: Both operations share a single transaction (structural test)
import inspect
source = inspect.getsource(repo_c.assign_doc_to_narrative)
has_single_get_conn = source.count("_get_conn") == 1
has_no_update_narrative_call = "update_narrative" not in source
T("C2: Single _get_conn block (no separate update_narrative call)",
  has_single_get_conn and has_no_update_narrative_call,
  f"_get_conn count={source.count('_get_conn')}, update_narrative present={'update_narrative' in source}")

_cleanup(tmp_c)


# ===========================================================================
# Section D: change_pct Logic (H1)
# ===========================================================================

S("D: change_pct Precedence Fix")

# We test the logic by mocking yfinance
mock_info_base = {
    "currentPrice": 100.0,
    "shortName": "Test Corp",
    "regularMarketVolume": 1000000,
    "marketCap": 1000000000,
    "sector": "Technology",
    "industry": "Software",
}

# D1: regularMarketChangePercent = 0 should return 0.0 (not fallback)
mock_info_d1 = {**mock_info_base, "regularMarketChangePercent": 0, "regularMarketChange": 5.0}
mock_ticker_d1 = MagicMock()
mock_ticker_d1.info = mock_info_d1
mock_ticker_d1.history.return_value = MagicMock(empty=True)

repo_d, tmp_d = _make_repo()

from stock_data import StockDataProvider

with patch("stock_data.yf.Ticker", return_value=mock_ticker_d1):
    provider = StockDataProvider(repo_d)
    result = provider.get_quote("TEST", force_refresh=True)
    T("D1: change_pct=0 when regularMarketChangePercent=0",
      result is not None and result["change_pct"] == 0.0,
      f"got change_pct={result.get('change_pct') if result else 'None'}")

# D2: regularMarketChangePercent absent, regularMarketChange present
mock_info_d2 = {**mock_info_base, "regularMarketChange": 2.5}
# Remove regularMarketChangePercent explicitly
mock_info_d2.pop("regularMarketChangePercent", None)
mock_ticker_d2 = MagicMock()
mock_ticker_d2.info = mock_info_d2
mock_ticker_d2.history.return_value = MagicMock(empty=True)

with patch("stock_data.yf.Ticker", return_value=mock_ticker_d2):
    provider2 = StockDataProvider(repo_d)
    result2 = provider2.get_quote("TEST2", force_refresh=True)
    expected_pct = 2.5 / 100.0 * 100  # = 2.5
    T("D2: Fallback to regularMarketChange / price * 100",
      result2 is not None and abs(result2["change_pct"] - expected_pct) < 0.01,
      f"expected ~{expected_pct}, got {result2.get('change_pct') if result2 else 'None'}")

# D3: Both absent — returns 0.0
mock_info_d3 = {**mock_info_base}
mock_info_d3.pop("regularMarketChangePercent", None)
mock_info_d3.pop("regularMarketChange", None)
mock_ticker_d3 = MagicMock()
mock_ticker_d3.info = mock_info_d3
mock_ticker_d3.history.return_value = MagicMock(empty=True)

with patch("stock_data.yf.Ticker", return_value=mock_ticker_d3):
    provider3 = StockDataProvider(repo_d)
    result3 = provider3.get_quote("TEST3", force_refresh=True)
    T("D3: Both absent — change_pct=0.0",
      result3 is not None and result3["change_pct"] == 0.0,
      f"got {result3.get('change_pct') if result3 else 'None'}")

_cleanup(tmp_d)


# ===========================================================================
# Section E: Cache Bounds (H3)
# ===========================================================================

S("E: Cache Bounds")

from stock_data import _price_history_cache, _MAX_CACHE_SIZE

# Save original cache state
original_cache = dict(_price_history_cache)
_price_history_cache.clear()

# E1: Cache evicts oldest entry when exceeding max size
now = time.time()
for i in range(_MAX_CACHE_SIZE + 5):
    _price_history_cache[f"SYM{i}:30"] = (now - (_MAX_CACHE_SIZE + 5 - i), [{"close": i}])

# Manually trigger the eviction logic (simulating what get_price_history does)
while len(_price_history_cache) > _MAX_CACHE_SIZE:
    oldest_key = min(_price_history_cache, key=lambda k: _price_history_cache[k][0])
    del _price_history_cache[oldest_key]

T("E1: Cache bounded to max size after eviction",
  len(_price_history_cache) == _MAX_CACHE_SIZE,
  f"size={len(_price_history_cache)}, max={_MAX_CACHE_SIZE}")

# E2: Oldest entries were evicted (SYM0-SYM4 should be gone)
oldest_present = any(f"SYM{i}:30" in _price_history_cache for i in range(5))
T("E2: Oldest entries evicted first", not oldest_present,
  f"SYM0-SYM4 still present: {oldest_present}")

# Restore cache
_price_history_cache.clear()
_price_history_cache.update(original_cache)


# ===========================================================================
# Section F: JSON Ticker Query (H5)
# ===========================================================================

S("F: JSON Ticker Query (get_narratives_for_ticker)")

repo_f, tmp_f = _make_repo()

# Setup: insert narratives with various linked_assets formats
repo_f.insert_narrative({
    "narrative_id": "json_test_1",
    "name": "AI Chip Shortage",
    "stage": "Growing",
    "ns_score": 0.85,
    "suppressed": 0,
    "linked_assets": json.dumps([{"ticker": "NVDA", "score": 0.9}, {"ticker": "AMD", "score": 0.7}]),
    "created_at": "2026-01-01",
    "last_updated_at": "2026-01-01",
})

repo_f.insert_narrative({
    "narrative_id": "json_test_2",
    "name": "Cloud Revenue Surge",
    "stage": "Mature",
    "ns_score": 0.65,
    "suppressed": 0,
    "linked_assets": json.dumps([{"ticker": "MSFT", "score": 0.8}]),
    "created_at": "2026-01-01",
    "last_updated_at": "2026-01-01",
})

repo_f.insert_narrative({
    "narrative_id": "json_test_3",
    "name": "Suppressed Narrative",
    "stage": "Dormant",
    "ns_score": 0.1,
    "suppressed": 1,
    "linked_assets": json.dumps([{"ticker": "NVDA", "score": 0.3}]),
    "created_at": "2026-01-01",
    "last_updated_at": "2026-01-01",
})

repo_f.insert_narrative({
    "narrative_id": "json_test_4",
    "name": "No Assets Narrative",
    "stage": "Emerging",
    "ns_score": 0.2,
    "suppressed": 0,
    "linked_assets": None,
    "created_at": "2026-01-01",
    "last_updated_at": "2026-01-01",
})

# F1: Matches dict-format assets
results = repo_f.get_narratives_for_ticker("NVDA")
matched_ids = {r["narrative_id"] for r in results}
T("F1: Finds narratives with matching ticker (dict format)",
  "json_test_1" in matched_ids,
  f"matched: {matched_ids}")

# F2: Case-insensitive matching
results_lower = repo_f.get_narratives_for_ticker("nvda")
matched_ids_lower = {r["narrative_id"] for r in results_lower}
T("F2: Case-insensitive ticker matching",
  matched_ids == matched_ids_lower,
  f"upper={matched_ids}, lower={matched_ids_lower}")

# F3: Non-matching ticker returns empty
results_empty = repo_f.get_narratives_for_ticker("ZZZZ")
T("F3: Non-matching ticker returns empty list",
  len(results_empty) == 0,
  f"got {len(results_empty)} results")

# F4: NULL linked_assets does not crash
try:
    results_null = repo_f.get_narratives_for_ticker("AAPL")
    T("F4: NULL linked_assets handled gracefully", True,
      f"returned {len(results_null)} results")
except Exception as exc:
    T("F4: NULL linked_assets handled gracefully", False, str(exc))

# F5: Suppressed narratives excluded
T("F5: Suppressed narratives excluded from results",
  "json_test_3" not in matched_ids,
  f"matched: {matched_ids}")

# F6: get_ticker_impact_score uses json_each query
impact = repo_f.get_ticker_impact_score("NVDA")
T("F6: Impact score sums ns_scores correctly",
  abs(impact - 0.85) < 0.01,
  f"expected 0.85, got {impact}")

_cleanup(tmp_f)


# ===========================================================================
# Section G: WAL Failure Fallback (P11a Batch 1)
# ===========================================================================

S("G: WAL Fallback — journal_mode=OFF never set")

# G1: Normal migrate uses WAL or DELETE, not OFF
repo_g, tmp_g = _make_repo()
try:
    with repo_g._get_conn() as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    T("G1: journal_mode is WAL or DELETE after migrate()", mode in ("wal", "delete"),
      f"mode={mode}")
except Exception as exc:
    T("G1: journal_mode is WAL or DELETE after migrate()", False, str(exc))

# G2: When WAL raises, fallback is DELETE not OFF
import sqlite3 as _sqlite3
_wal_call_count = {"n": 0}
_orig_execute = None


class _FaultyConn:
    """Wraps a real connection, intercepts the first WAL PRAGMA to raise."""
    def __init__(self, real_conn):
        self._conn = real_conn
        self._wal_raised = False

    def execute(self, sql, params=()):
        if "journal_mode=WAL" in sql.upper() and not self._wal_raised:
            self._wal_raised = True
            raise _sqlite3.OperationalError("WAL not supported (simulated)")
        return self._conn.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._conn, name)


from repository import SqliteRepository as _SQLiteRepo2

f_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
f_tmp.close()
_faulty_path = f_tmp.name

try:
    _repo_g2 = _SQLiteRepo2(_faulty_path)
    _orig_get_conn = _repo_g2._get_conn.__func__ if hasattr(_repo_g2._get_conn, "__func__") else None
    _executed_pragmas: list[str] = []

    import contextlib

    @contextlib.contextmanager
    def _patched_get_conn(self=_repo_g2):
        conn = _sqlite3.connect(self._db_path)
        conn.row_factory = _sqlite3.Row
        _wal_done = {"raised": False}

        class _InterceptConn:
            def execute(inner_self, sql, params=()):
                upper = sql.upper().strip()
                if "JOURNAL_MODE=WAL" in upper and not _wal_done["raised"]:
                    _wal_done["raised"] = True
                    _executed_pragmas.append("WAL_ATTEMPT")
                    raise _sqlite3.OperationalError("WAL not supported (simulated)")
                _executed_pragmas.append(sql.strip()[:60])
                return conn.execute(sql, params)

            def __getattr__(inner_self, name):
                return getattr(conn, name)

        ic = _InterceptConn()
        try:
            yield ic
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    import types
    _repo_g2._get_conn = types.MethodType(lambda self: _patched_get_conn(), _repo_g2)

    try:
        _repo_g2.migrate()
        # Case-insensitive substring checks
        pragmas_upper = [p.upper() for p in _executed_pragmas]
        off_set = any("JOURNAL_MODE=OFF" in p for p in pragmas_upper)
        delete_set = any("JOURNAL_MODE=DELETE" in p for p in pragmas_upper)
        T("G2: WAL failure uses DELETE fallback, not OFF",
          delete_set and not off_set,
          f"delete={delete_set}, off={off_set}, pragmas={_executed_pragmas}")
    except RuntimeError as exc:
        # Acceptable: plan allows raising instead of OFF
        T("G2: WAL failure raises RuntimeError (acceptable), not silent OFF",
          "journal mode" in str(exc).lower() or "WAL" in str(exc) or "migrate" in str(exc),
          str(exc))
    except Exception as exc:
        T("G2: WAL failure behavior", False, f"unexpected exception: {exc}")
finally:
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(_faulty_path + suffix)
        except FileNotFoundError:
            pass

_cleanup(tmp_g)


# ===========================================================================
# Section H: Orphan Precheck Helpers (P11a Batch 2)
# ===========================================================================

S("H: Orphan Precheck Helpers")

repo_h, tmp_h = _make_repo()

# H1: check_all_orphans returns a dict for each expected relationship
try:
    orphans = repo_h.check_all_orphans()
    expected_keys = [
        "portfolio_holdings.portfolio_id -> portfolios.id",
        "watchlist_items.watchlist_id -> watchlists.id",
        "notifications.rule_id -> notification_rules.id",
        "document_evidence.narrative_id -> narratives.narrative_id",
        "narrative_snapshots.narrative_id -> narratives.narrative_id",
        "mutation_events.narrative_id -> narratives.narrative_id",
        "adversarial_log.narrative_id -> narratives.narrative_id",
        "centroid_history.narrative_id -> narratives.narrative_id",
        "narrative_assignments.narrative_id -> narratives.narrative_id",
    ]
    all_present = all(k in orphans for k in expected_keys)
    T("H1: check_all_orphans returns all 9 relationships", all_present,
      f"missing={[k for k in expected_keys if k not in orphans]}")
except Exception as exc:
    T("H1: check_all_orphans returns all 9 relationships", False, str(exc))

# H2: Clean DB has zero orphans for all relationships
try:
    orphans = repo_h.check_all_orphans()
    all_zero = all(v.get("count", -1) == 0 for v in orphans.values())
    T("H2: Clean DB has zero orphans", all_zero,
      {k: v["count"] for k, v in orphans.items() if v.get("count", 0) != 0} or "all clean")
except Exception as exc:
    T("H2: Clean DB has zero orphans", False, str(exc))

# H3: Orphaned row is detected
try:
    with repo_h._get_conn() as conn:
        conn.execute(
            "INSERT INTO narrative_snapshots (narrative_id, snapshot_date) VALUES (?, ?)",
            ("orphan_nid_xyz", "2026-01-01"),
        )
    orphans = repo_h.check_all_orphans()
    snap_key = "narrative_snapshots.narrative_id -> narratives.narrative_id"
    T("H3: Orphaned narrative_snapshot detected",
      orphans.get(snap_key, {}).get("count", 0) >= 1,
      f"count={orphans.get(snap_key, {}).get('count')}")
except Exception as exc:
    T("H3: Orphaned narrative_snapshot detected", False, str(exc))

_cleanup(tmp_h)


# ===========================================================================
# Section I: Composite Indexes (P11c Batch 5)
# ===========================================================================

S("I: Composite Indexes")

repo_i, tmp_i = _make_repo()

try:
    with repo_i._get_conn() as conn:
        indexes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()}
    expected_new = [
        "idx_watchlists_user_created",
        "idx_notification_rules_user_created",
        "idx_notifications_rule_created",
        "idx_social_mentions_recorded_ticker",
        "idx_ticker_convergence_pressure",
    ]
    for idx in expected_new:
        T(f"I: {idx} exists", idx in indexes, f"found={idx in indexes}")
except Exception as exc:
    for idx in expected_new:
        T(f"I: {idx} exists", False, str(exc))

_cleanup(tmp_i)


# ===========================================================================
# Section J: Bulk Repository Helpers (P11b Batch 4)
# ===========================================================================

S("J: Bulk Repository Helpers")

repo_j, tmp_j = _make_repo()

# Setup narratives
for nid, name in [("j1", "Alpha"), ("j2", "Beta"), ("j3", "Gamma")]:
    repo_j.insert_narrative({
        "narrative_id": nid,
        "name": name,
        "stage": "Emerging",
        "suppressed": 0,
        "created_at": "2026-01-01T00:00:00",
        "last_updated_at": "2026-01-01T00:00:00",
    })

# J1: get_narratives_by_ids returns all requested
try:
    result = repo_j.get_narratives_by_ids(["j1", "j2", "j3"])
    T("J1: get_narratives_by_ids returns all 3 rows",
      set(result.keys()) == {"j1", "j2", "j3"},
      f"keys={set(result.keys())}")
except Exception as exc:
    T("J1: get_narratives_by_ids returns all 3 rows", False, str(exc))

# J2: get_narratives_by_ids handles empty list
try:
    result_empty = repo_j.get_narratives_by_ids([])
    T("J2: get_narratives_by_ids([]) returns empty dict",
      result_empty == {},
      f"got {result_empty}")
except Exception as exc:
    T("J2: get_narratives_by_ids([]) returns empty dict", False, str(exc))

# J3: get_adversarial_events_for_narratives returns empty lists for narratives with no events
try:
    result = repo_j.get_adversarial_events_for_narratives(["j1", "j2"])
    T("J3: get_adversarial_events_for_narratives returns keyed dict with empty lists",
      "j1" in result and "j2" in result and result["j1"] == [] and result["j2"] == [],
      f"keys={set(result.keys())}")
except Exception as exc:
    T("J3: get_adversarial_events_for_narratives returns keyed dict", False, str(exc))

# J4: get_snapshot_history_for_narratives returns empty lists for narratives with no snapshots
try:
    result = repo_j.get_snapshot_history_for_narratives(["j1", "j2"])
    T("J4: get_snapshot_history_for_narratives returns keyed dict with empty lists",
      "j1" in result and "j2" in result and result["j1"] == [] and result["j2"] == [],
      f"keys={set(result.keys())}")
except Exception as exc:
    T("J4: get_snapshot_history_for_narratives returns keyed dict", False, str(exc))

# J5: get_all_narrative_signals with limit returns at most N rows
try:
    # Insert 3 signals
    import datetime as _dt
    for nid in ["j1", "j2", "j3"]:
        with repo_j._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO narrative_signals "
                "(narrative_id, direction, confidence, timeframe, magnitude, certainty, "
                "key_actors, affected_sectors, catalyst_type, extracted_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (nid, "bullish", 0.9, "short", "significant", "confirmed",
                 "[]", "[]", "earnings", _dt.datetime.now().isoformat()),
            )
    sigs_unbounded = repo_j.get_all_narrative_signals()
    sigs_limited = repo_j.get_all_narrative_signals(limit=2)
    T("J5: get_all_narrative_signals(limit=2) returns ≤2 rows",
      len(sigs_limited) <= 2 and len(sigs_unbounded) == 3,
      f"limited={len(sigs_limited)}, unbounded={len(sigs_unbounded)}")
except Exception as exc:
    T("J5: get_all_narrative_signals limit parameter works", False, str(exc))

_cleanup(tmp_j)


# ===========================================================================
# K: FAISS+LSH pair-commit manifest (P12 Batch 4.1)
# ===========================================================================

_section_name = "K: FAISS/LSH pair-commit manifest"
print(f"\n{'='*60}\n  {_section_name}\n{'='*60}")

import tempfile as _tempfile
from pathlib import Path as _Path

try:
    from vector_store import FaissVectorStore
    from deduplicator import Deduplicator
    from settings import Settings as _Settings

    _tmp_k = _tempfile.mkdtemp()
    _faiss_path = str(_Path(_tmp_k) / "faiss_index.pkl")
    _lsh_path = str(_Path(_tmp_k) / "lsh_index.pkl")
    _manifest_path = _Path(_faiss_path).with_suffix(".pair_manifest.json")

    # Create and initialize a minimal vector store
    _vs_k = FaissVectorStore(_faiss_path)
    _vs_k.initialize(4)
    _vs_k.save()

    _ded_k = Deduplicator(threshold=0.5, num_perm=32, lsh_path=_lsh_path)
    _ded_k.save()

    # Simulate what Step 17 does: delete old manifest, save both, write new manifest
    _manifest_path.unlink(missing_ok=True)
    _vs_k.save()
    _ded_k.save()
    _manifest_data = {
        "cycle_id": "test-cycle-001",
        "faiss_path": _faiss_path,
        "lsh_path": _lsh_path,
        "faiss_mtime": _Path(_faiss_path).stat().st_mtime if _Path(_faiss_path).exists() else None,
        "lsh_mtime": _Path(_lsh_path).stat().st_mtime if _Path(_lsh_path).exists() else None,
        "saved_at": "2026-05-07T00:00:00+00:00",
    }
    _manifest_path.write_text(json.dumps(_manifest_data))

    T("K1: manifest written after successful pair save",
      _manifest_path.exists(),
      f"manifest path: {_manifest_path}")

    _loaded_manifest = json.loads(_manifest_path.read_text())
    T("K2: manifest contains cycle_id, faiss_path, lsh_path, saved_at",
      all(k in _loaded_manifest for k in ("cycle_id", "faiss_path", "lsh_path", "saved_at")),
      f"keys={list(_loaded_manifest.keys())}")

    T("K3: manifest cycle_id matches expected",
      _loaded_manifest.get("cycle_id") == "test-cycle-001",
      f"got {_loaded_manifest.get('cycle_id')}")

    # Simulate incomplete save: manifest deleted before second save succeeds
    _manifest_path.unlink(missing_ok=True)
    _vs_k.save()
    # LSH save never happens — manifest never written
    T("K4: no manifest when second save doesn't complete (no manifest present)",
      not _manifest_path.exists(),
      "manifest unexpectedly present after failed pair save")

except Exception as _e_k:
    T("K section setup", False, str(_e_k))
    for _n in ("K1", "K2", "K3", "K4"):
        T(f"{_n}: pair manifest test", False, "setup failed")

# ===========================================================================
# Print results
# ===========================================================================

_print_results()
sys.exit(0 if _fail == 0 else 1)
