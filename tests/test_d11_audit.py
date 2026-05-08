"""
D11 Audit regression test suite -- adversarial.py, mutations.py, llm_client.py

Tests critical fixes from the production audit:
  - Duplicate coordination penalty (Fix 1)
  - Stale model pricing (Fix 2)
  - Gate 2 dead code (Fix 3)
  - UTC date consistency (Fix 4)
  - Sonnet snapshot population (Fix 5)
  - Error detail leak (Fix 6)
  - datetime.utcnow deprecation (Fix 7)
  - N+1 query in generate_mutation_summary (Fix 8)
  - Unknown mutation type warning (Fix 9)
  - Adversarial happy path + temporal filtering
  - Mutation detection: velocity_reversal, doc_surge, new_sonnet
  - LLM client retry/fallback paths

Run with:
    python -X utf8 tests/test_d11_audit.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)

# ---------------------------------------------------------------------------
# Custom test runner
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
        print(
            f"  FAIL [{_current_section}] {name}" + (f" -- {details}" if details else ""),
            file=sys.stderr,
        )


def _print_summary() -> None:
    sections: dict[str, dict] = {}
    for r in _results:
        sec = r["section"]
        if sec not in sections:
            sections[sec] = {"pass": 0, "fail": 0}
        if r["passed"]:
            sections[sec]["pass"] += 1
        else:
            sections[sec]["fail"] += 1

    print("\n" + "=" * 60)
    print(f"{'Section':<35} {'Pass':>5} {'Fail':>5}")
    print("-" * 60)
    for sec, counts in sections.items():
        marker = "" if counts["fail"] == 0 else " <--"
        print(f"  {sec:<33} {counts['pass']:>5} {counts['fail']:>5}{marker}")
    print("=" * 60)
    print(f"  TOTAL: {_pass} passed, {_fail} failed out of {_pass + _fail} tests")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import sqlite3
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

from repository import SqliteRepository
from mutations import MutationDetector

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_tmp_db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
_tmp_db_file.close()
_test_db = _tmp_db_file.name
repo = SqliteRepository(_test_db)
repo.migrate()


class _FakeLlm:
    def call_haiku(self, task_type, narrative_id, prompt):
        return "LLM fallback explanation"


_fake_llm = _FakeLlm()


def _insert_narrative(conn, nid, name="Test Narrative", ns_score=0.65,
                      velocity=0.15, doc_count=12, stage="Growing",
                      created_at=None):
    if created_at is None:
        created_at = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO narratives "
        "(narrative_id, name, ns_score, velocity, document_count, stage, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (nid, name, ns_score, velocity, doc_count, stage, created_at),
    )
    conn.commit()


def _insert_snapshot(conn, snap_id, nid, snap_date, ns_score=0.5, velocity=0.1,
                     doc_count=10, stage="Growing", sonnet_analysis=None):
    conn.execute(
        "INSERT OR REPLACE INTO narrative_snapshots "
        "(id, narrative_id, snapshot_date, ns_score, velocity, doc_count, "
        "lifecycle_stage, sonnet_analysis, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (snap_id, nid, snap_date, ns_score, velocity, doc_count, stage,
         sonnet_analysis, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


# ===========================================================================
# D11-U1: Pipeline no longer writes redundant is_coordinated
# ===========================================================================
S("D11-U1: Duplicate penalty removed")

# Verify by inspecting pipeline.py source for the specific pattern
_pipeline_path = Path(__file__).parent.parent / "pipeline.py"
_pipeline_src = _pipeline_path.read_text(encoding="utf-8")

# The penalty block should NOT contain "is_coordinated" alongside ns_score
# Look for the specific adversarial penalty update block
_penalty_block_start = _pipeline_src.find("max(0.0, current_ns - 0.25)")
T("penalty block found in pipeline.py", _penalty_block_start != -1)

# Extract surrounding context (the update_narrative call)
if _penalty_block_start != -1:
    _block = _pipeline_src[max(0, _penalty_block_start - 100):_penalty_block_start + 100]
    T("penalty block does not write is_coordinated",
      "is_coordinated" not in _block,
      f"block: {_block!r}")
else:
    T("penalty block does not write is_coordinated", False, "penalty block not found")


# ===========================================================================
# D11-U2: Pricing constants match model names
# ===========================================================================
S("D11-U2: Pricing constants")

from llm_client import (
    HAIKU_INPUT_PRICE_PER_M,
    HAIKU_OUTPUT_PRICE_PER_M,
    SONNET_INPUT_PRICE_PER_M,
    SONNET_OUTPUT_PRICE_PER_M,
)

T("Haiku input price is 1.00", HAIKU_INPUT_PRICE_PER_M == 1.00,
  f"got {HAIKU_INPUT_PRICE_PER_M}")
T("Haiku output price is 5.00", HAIKU_OUTPUT_PRICE_PER_M == 5.00,
  f"got {HAIKU_OUTPUT_PRICE_PER_M}")
T("Sonnet input price correct", SONNET_INPUT_PRICE_PER_M == 3.00,
  f"got {SONNET_INPUT_PRICE_PER_M}")
T("Sonnet output price correct", SONNET_OUTPUT_PRICE_PER_M == 15.00,
  f"got {SONNET_OUTPUT_PRICE_PER_M}")

# Verify comment matches actual model names
_llm_path = Path(__file__).parent.parent / "llm_client.py"
_llm_src = _llm_path.read_text(encoding="utf-8")
T("comment references haiku-4-5", "claude-haiku-4-5" in _llm_src)
T("comment references sonnet-4-6", "claude-sonnet-4-6" in _llm_src)
T("no stale 3-5-haiku reference", "claude-3-5-haiku" not in _llm_src)
T("no stale 3-5-sonnet reference", "claude-3-5-sonnet" not in _llm_src)


# ===========================================================================
# D11-U3: Gate 2 -- age logic
# ===========================================================================
S("D11-U3: Gate 2 age logic")

from signals import get_narrative_age_days

# age=0 should be rejected
_age_0 = get_narrative_age_days(datetime.now(timezone.utc).isoformat())
T("age 0 for today's narrative", _age_0 == 0, f"got {_age_0}")

# age=2 should be accepted
_two_days_ago = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
_age_2 = get_narrative_age_days(_two_days_ago)
T("age 2 for 2-day-old narrative", _age_2 >= 2, f"got {_age_2}")

# parse failure returns 0 (fails safe)
_age_bad = get_narrative_age_days("not-a-date")
T("parse failure returns 0", _age_bad == 0, f"got {_age_bad}")

from llm_client import LlmClient

_mock_repo_gate2 = MagicMock()
_mock_repo_gate2.get_narrative.return_value = {"narrative_id": "n-gate2", "ns_score": 0.95}
_mock_repo_gate2.get_sonnet_calls_last_24h.return_value = []
_mock_repo_gate2.get_sonnet_daily_spend.return_value = {"total_tokens_used": 0}
_mock_settings_gate2 = MagicMock()
_mock_settings_gate2.CONFIDENCE_ESCALATION_THRESHOLD = 0.60
_mock_settings_gate2.SONNET_DAILY_TOKEN_BUDGET = 200000

_llm_gate2 = LlmClient.__new__(LlmClient)
_llm_gate2._repository = _mock_repo_gate2
_llm_gate2._settings = _mock_settings_gate2

with patch("signals.get_narrative_age_days", return_value=3):
    _gate_ok, _gate_reason = _llm_gate2.check_sonnet_gates(
        "n-gate2",
        datetime.now(timezone.utc).isoformat(),
        estimated_tokens=250,
    )
T("gate 2 path executes without dead-code side effects",
  isinstance(_gate_ok, bool) and isinstance(_gate_reason, str),
  f"got=({_gate_ok!r}, {_gate_reason!r})")
T("gates pass on healthy inputs", _gate_ok is True, _gate_reason)

with patch("signals.get_narrative_age_days", return_value=0):
    _gate_fail, _gate_fail_reason = _llm_gate2.check_sonnet_gates(
        "n-gate2",
        datetime.now(timezone.utc).isoformat(),
        estimated_tokens=250,
    )
T("age<2 fails via gate_2 path", _gate_fail is False and "gate_2_age" in _gate_fail_reason,
  f"reason={_gate_fail_reason!r}")


# ===========================================================================
# D11-U4: UTC dates in mutations.py
# ===========================================================================
S("D11-U4: UTC date consistency")

_mutations_path = Path(__file__).parent.parent / "mutations.py"
_mutations_src = _mutations_path.read_text(encoding="utf-8")
T("no date.today() in mutations.py", "date.today()" not in _mutations_src)
T("uses datetime.now(timezone.utc).date()",
  "datetime.now(timezone.utc).date()" in _mutations_src)


# ===========================================================================
# D11-U5: Sonnet snapshot populated from mutation_analyses
# ===========================================================================
S("D11-U5: Sonnet snapshot")

_nid_sonnet = "n-sonnet-test-" + str(uuid.uuid4())[:8]
_conn = sqlite3.connect(_test_db)
_insert_narrative(_conn, _nid_sonnet, name="Sonnet Test Narrative")
_conn.close()

_sonnet_text = "Deep analysis: market momentum driven by institutional buyers."
_det_sonnet = MutationDetector(
    settings=None, repository=repo, llm_client=_fake_llm,
    mutation_analyses={_nid_sonnet: _sonnet_text},
)

_snap_id = _det_sonnet.take_daily_snapshot(_nid_sonnet)
T("snapshot created", _snap_id != "")

_today = datetime.now(timezone.utc).date().isoformat()
_snap = repo.get_snapshot(_nid_sonnet, _today)
T("snapshot retrieved", _snap is not None)
T("sonnet_analysis populated in snapshot",
  _snap.get("sonnet_analysis") == _sonnet_text,
  f"got: {_snap.get('sonnet_analysis')!r}")

# Without mutation_analyses, sonnet_analysis should be None
_nid_no_sonnet = "n-no-sonnet-" + str(uuid.uuid4())[:8]
_conn = sqlite3.connect(_test_db)
_insert_narrative(_conn, _nid_no_sonnet, name="No Sonnet Narrative")
_conn.close()

_det_no_sonnet = MutationDetector(
    settings=None, repository=repo, llm_client=_fake_llm,
)
_det_no_sonnet.take_daily_snapshot(_nid_no_sonnet)
_snap_no = repo.get_snapshot(_nid_no_sonnet, _today)
T("sonnet_analysis is None when no analyses provided",
  _snap_no.get("sonnet_analysis") is None)


# ===========================================================================
# D11-U6: call_haiku_chat does not leak error details
# ===========================================================================
S("D11-U6: Error detail leak")

from llm_client import LlmClient

_llm_src_lines = _llm_src.split("\n")
# Find the call_haiku_chat error return
_found_generic = False
for line in _llm_src_lines:
    if "unable to process" in line.lower():
        _found_generic = True
        break

T("call_haiku_chat returns generic error message", _found_generic)
T("no f-string Error: in call_haiku_chat",
  'f"Error: {str(exc)}"' not in _llm_src and "f'Error: {str(exc)}'" not in _llm_src)


# ===========================================================================
# D11-U7: datetime.utcnow() removed from repository hot paths
# ===========================================================================
S("D11-U7: utcnow deprecation")

_repo_path = Path(__file__).parent.parent / "repository.py"
_repo_src = _repo_path.read_text(encoding="utf-8")
_utcnow_count = _repo_src.count("utcnow()")
T("no datetime.utcnow() in repository.py", _utcnow_count == 0,
  f"found {_utcnow_count} occurrences")


# ===========================================================================
# D11-U8: N+1 query eliminated in generate_mutation_summary
# ===========================================================================
S("D11-U8: N+1 query cache")

_nid_n1 = "n-n1-test-" + str(uuid.uuid4())[:8]
_conn = sqlite3.connect(_test_db)
_insert_narrative(_conn, _nid_n1, name="N+1 Test")

# Insert 3 mutations for the SAME narrative
for i in range(3):
    _conn.execute(
        "INSERT INTO mutation_events "
        "(id, narrative_id, detected_at, mutation_type, previous_value, new_value, magnitude, haiku_explanation) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), _nid_n1, datetime.now(timezone.utc).isoformat(),
         "score_spike", "0.3", "0.6", 0.3 + i * 0.1, f"Explanation {i}"),
    )
_conn.commit()
_conn.close()

# Wrap get_narrative to count calls
_original_get = repo.get_narrative
_call_count = [0]


def _counting_get(nid):
    _call_count[0] += 1
    return _original_get(nid)


repo.get_narrative = _counting_get

_det_n1 = MutationDetector(settings=None, repository=repo, llm_client=_fake_llm)
_summary = _det_n1.generate_mutation_summary()

T("summary has 3 mutations", _summary["mutations_today"] >= 3,
  f"got {_summary['mutations_today']}")
T("get_narrative called at most once per narrative",
  _call_count[0] <= 1,
  f"called {_call_count[0]} times for 1 narrative with 3 mutations")

# Restore original
repo.get_narrative = _original_get


# ===========================================================================
# D11-U9: Unknown mutation type logs warning
# ===========================================================================
S("D11-U9: Unknown mutation type warning")

_det_warn = MutationDetector(
    settings=None, repository=repo, llm_client=_fake_llm,
)

with patch("mutations.logger") as _mock_logger:
    _result_unknown = _det_warn.generate_template_explanation(
        "n-any", "totally_unknown_type", "a", "b"
    )
    T("unknown type returns None", _result_unknown is None)
    T("warning logged for unknown type",
      _mock_logger.warning.called,
      f"warning calls: {_mock_logger.warning.call_args_list}")


# ===========================================================================
# D11-U10: Adversarial happy path -- coordination detected
# ===========================================================================
S("D11-U10: Adversarial happy path")

from dataclasses import dataclass
from adversarial import check_coordination, AdversarialEvent
from ingester import RawDocument
from deduplicator import Deduplicator
from datasketch import MinHash

# Build fake documents from 3 different untrusted domains, same content, tight timestamps
_now_ts = datetime.now(timezone.utc)
_adv_docs = []
_adv_sigs: dict[str, MinHash] = {}

_common_text = "Federal Reserve announces emergency rate cut amid banking concerns and market volatility"

for i, domain in enumerate(["spam1.com", "spam2.com", "spam3.com"]):
    did = f"adv-doc-{i}"
    doc = RawDocument(
        doc_id=did,
        raw_text=_common_text,
        source_url=f"https://{domain}/article",
        source_domain=domain,
        published_at=(_now_ts + timedelta(seconds=i * 10)).isoformat(),
        ingested_at=_now_ts.isoformat(),
    )
    _adv_docs.append(doc)
    # Create identical MinHash signatures (high Jaccard similarity)
    mh = MinHash(num_perm=128)
    for word in _common_text.lower().split():
        mh.update(word.encode("utf-8"))
    _adv_sigs[did] = mh

# Mock the deduplicator
_mock_dedup = MagicMock(spec=Deduplicator)
_mock_dedup.get_batch_signatures.return_value = _adv_sigs

# Mock settings
_mock_settings = MagicMock()
_mock_settings.LSH_THRESHOLD = 0.85
_mock_settings.SYNC_BURST_MIN_SOURCES = 3
_mock_settings.SYNC_BURST_WINDOW_SECONDS = 600

# Mock repository for adversarial
_mock_repo_adv = MagicMock()
_mock_repo_adv.get_candidate_buffer.return_value = []
_mock_repo_adv.get_narrative.return_value = {"narrative_id": "n1", "coordination_flag_count": 0}
_mock_repo_adv.get_coordination_flags_rolling_window.return_value = 0

events = check_coordination(
    batch_documents=_adv_docs,
    deduplicator=_mock_dedup,
    trusted_domains=["reuters.com", "apnews.com"],
    settings=_mock_settings,
    repository=_mock_repo_adv,
)

T("coordination events detected", len(events) >= 1, f"got {len(events)}")
if events:
    T("event is AdversarialEvent", isinstance(events[0], AdversarialEvent))
    T("event has 3 source domains", len(events[0].source_domains) == 3,
      f"got {events[0].source_domains}")
    T("similarity score > threshold", events[0].similarity_score >= 0.85,
      f"got {events[0].similarity_score:.3f}")
    T("log_adversarial_event called", _mock_repo_adv.log_adversarial_event.called)
else:
    T("event is AdversarialEvent", False, "no events")
    T("event has 3 source domains", False, "no events")
    T("similarity score > threshold", False, "no events")
    T("log_adversarial_event called", False, "no events")


# ===========================================================================
# D11-U11: Temporal filtering -- spread-out docs NOT flagged
# ===========================================================================
S("D11-U11: Temporal filtering")

_spread_docs = []
_spread_sigs: dict[str, MinHash] = {}

for i, domain in enumerate(["spam1.com", "spam2.com", "spam3.com"]):
    did = f"spread-doc-{i}"
    # Spread timestamps beyond 600 second window
    doc = RawDocument(
        doc_id=did,
        raw_text=_common_text,
        source_url=f"https://{domain}/article",
        source_domain=domain,
        published_at=(_now_ts + timedelta(seconds=i * 400)).isoformat(),
        ingested_at=_now_ts.isoformat(),
    )
    _spread_docs.append(doc)
    mh = MinHash(num_perm=128)
    for word in _common_text.lower().split():
        mh.update(word.encode("utf-8"))
    _spread_sigs[did] = mh

_mock_dedup_spread = MagicMock(spec=Deduplicator)
_mock_dedup_spread.get_batch_signatures.return_value = _spread_sigs

_mock_repo_spread = MagicMock()
_mock_repo_spread.get_candidate_buffer.return_value = []

events_spread = check_coordination(
    batch_documents=_spread_docs,
    deduplicator=_mock_dedup_spread,
    trusted_domains=[],
    settings=_mock_settings,
    repository=_mock_repo_spread,
)

T("spread-out docs NOT flagged", len(events_spread) == 0,
  f"got {len(events_spread)} events")


# ===========================================================================
# D11-U12: Velocity reversal mutation
# ===========================================================================
S("D11-U12: Velocity reversal")

_nid_vel = "n-vel-" + str(uuid.uuid4())[:8]
_conn = sqlite3.connect(_test_db)
_insert_narrative(_conn, _nid_vel, name="Velocity Test", velocity=-0.05)

_yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
_insert_snapshot(_conn, str(uuid.uuid4()), _nid_vel, _yesterday,
                 velocity=-0.100, ns_score=0.5, stage="Growing")
_insert_snapshot(_conn, str(uuid.uuid4()), _nid_vel, _today,
                 velocity=0.200, ns_score=0.5, stage="Growing")
_conn.close()

_det_vel = MutationDetector(
    settings=None, repository=repo, llm_client=_fake_llm,
)
_muts_vel = _det_vel.detect_mutations(_nid_vel)
_vel_types = [m["mutation_type"] for m in _muts_vel]
T("velocity_reversal detected", "velocity_reversal" in _vel_types,
  f"types: {_vel_types}")


# ===========================================================================
# D11-U13: Doc surge mutation
# ===========================================================================
S("D11-U13: Doc surge")

_nid_surge = "n-surge-" + str(uuid.uuid4())[:8]
_conn = sqlite3.connect(_test_db)
_insert_narrative(_conn, _nid_surge, name="Surge Test", doc_count=30)

_insert_snapshot(_conn, str(uuid.uuid4()), _nid_surge, _yesterday,
                 doc_count=10, ns_score=0.5, stage="Growing")
_insert_snapshot(_conn, str(uuid.uuid4()), _nid_surge, _today,
                 doc_count=25, ns_score=0.5, stage="Growing")
_conn.close()

_det_surge = MutationDetector(
    settings=None, repository=repo, llm_client=_fake_llm,
)
_muts_surge = _det_surge.detect_mutations(_nid_surge)
_surge_types = [m["mutation_type"] for m in _muts_surge]
T("doc_surge detected (25 > 10*2.0)", "doc_surge" in _surge_types,
  f"types: {_surge_types}")


# ===========================================================================
# D11-U14: New sonnet mutation
# ===========================================================================
S("D11-U14: New sonnet mutation")

_nid_new_sonnet = "n-newsonnet-" + str(uuid.uuid4())[:8]
_conn = sqlite3.connect(_test_db)
_insert_narrative(_conn, _nid_new_sonnet, name="New Sonnet Test")

# Yesterday: no sonnet
_insert_snapshot(_conn, str(uuid.uuid4()), _nid_new_sonnet, _yesterday,
                 ns_score=0.5, stage="Growing", sonnet_analysis=None)
# Today: has sonnet (created by take_daily_snapshot with mutation_analyses)
_insert_snapshot(_conn, str(uuid.uuid4()), _nid_new_sonnet, _today,
                 ns_score=0.5, stage="Growing",
                 sonnet_analysis="Deep analysis of market trends")
_conn.close()

_det_new_sonnet = MutationDetector(
    settings=None, repository=repo, llm_client=_fake_llm,
)
_muts_new_sonnet = _det_new_sonnet.detect_mutations(_nid_new_sonnet)
_new_sonnet_types = [m["mutation_type"] for m in _muts_new_sonnet]
T("new_sonnet detected", "new_sonnet" in _new_sonnet_types,
  f"types: {_new_sonnet_types}")


# ===========================================================================
# D11-U15: compare_snapshots returns correct diff
# ===========================================================================
S("D11-U15: compare_snapshots")

_nid_cmp = "n-cmp-" + str(uuid.uuid4())[:8]
_conn = sqlite3.connect(_test_db)
_insert_narrative(_conn, _nid_cmp, name="Compare Test")

_date1 = (datetime.now(timezone.utc).date() - timedelta(days=3)).isoformat()
_date2 = (datetime.now(timezone.utc).date() - timedelta(days=2)).isoformat()
_insert_snapshot(_conn, str(uuid.uuid4()), _nid_cmp, _date1,
                 ns_score=0.30, velocity=0.05, stage="Emerging")
_insert_snapshot(_conn, str(uuid.uuid4()), _nid_cmp, _date2,
                 ns_score=0.65, velocity=0.20, stage="Growing")
_conn.close()

_det_cmp = MutationDetector(settings=None, repository=repo, llm_client=_fake_llm)
_cmp = _det_cmp.compare_snapshots(_nid_cmp, _date1, _date2)

T("compare returns narrative_name", _cmp["narrative_name"] == "Compare Test",
  f"got {_cmp['narrative_name']}")
T("compare has date1_data", _cmp["date1_data"] is not None)
T("compare has date2_data", _cmp["date2_data"] is not None)
T("compare has differences", len(_cmp["differences"]) > 0,
  f"got {_cmp['differences']}")

_diff_fields = [d["field"] for d in _cmp["differences"]]
T("ns_score in differences", "ns_score" in _diff_fields, f"fields: {_diff_fields}")
T("lifecycle_stage in differences", "lifecycle_stage" in _diff_fields,
  f"fields: {_diff_fields}")


# ===========================================================================
# D11-U16: get_story_timeline returns snapshots
# ===========================================================================
S("D11-U16: get_story_timeline")

# Use the narrative from D11-U15 which has 2 snapshots
_det_timeline = MutationDetector(settings=None, repository=repo, llm_client=_fake_llm)
_timeline = _det_timeline.get_story_timeline(_nid_cmp, days=7)
T("timeline returns snapshots", len(_timeline) >= 2,
  f"got {len(_timeline)} snapshots")

# Verify ordering (DESC by snapshot_date)
if len(_timeline) >= 2:
    T("timeline ordered DESC", _timeline[0]["snapshot_date"] >= _timeline[1]["snapshot_date"],
      f"dates: {_timeline[0]['snapshot_date']}, {_timeline[1]['snapshot_date']}")


# ===========================================================================
# D11-U17: generate_mutation_summary aggregation
# ===========================================================================
S("D11-U17: Mutation summary")

# We already have mutations from earlier tests (D11-U12, D11-U13, D11-U14, D11-U8)
_det_summ = MutationDetector(settings=None, repository=repo, llm_client=_fake_llm)
_summ = _det_summ.generate_mutation_summary()

T("summary has mutations_today", _summ["mutations_today"] >= 1,
  f"got {_summ['mutations_today']}")
T("summary has narratives_mutated list", isinstance(_summ["narratives_mutated"], list))
T("summary has most_significant", _summ["most_significant"] is not None)
if _summ["most_significant"]:
    T("most_significant has narrative_name",
      "narrative_name" in _summ["most_significant"])
    T("most_significant has magnitude",
      "magnitude" in _summ["most_significant"])


# ===========================================================================
# D11-U18: Haiku retry exhaustion returns fallback
# ===========================================================================
S("D11-U18: Haiku retry fallback")

from llm_client import _HAIKU_FALLBACKS

_mock_client = MagicMock()
_mock_client.messages.create.side_effect = TimeoutError("API timeout")

_mock_settings_llm = MagicMock()
_mock_settings_llm.HAIKU_MODEL = "claude-haiku-4-5-20251001"
_mock_settings_llm.HAIKU_MAX_TOKENS = 512
_mock_settings_llm.LLM_DAILY_BUDGET_USD = 5.0

_mock_repo_llm = MagicMock()
_mock_repo_llm.get_daily_llm_spend.return_value = 0.0

_llm_inst = LlmClient.__new__(LlmClient)
_llm_inst._settings = _mock_settings_llm
_llm_inst._repository = _mock_repo_llm
_llm_inst._client = _mock_client

with patch("time.sleep"):  # skip retry delays
    _haiku_result = _llm_inst.call_haiku("label_narrative", "n-test", "Label this narrative")

T("haiku returns fallback after 3 failures",
  _haiku_result == _HAIKU_FALLBACKS["label_narrative"],
  f"got: {_haiku_result!r}")
T("API called 3 times (1 + 2 retries)",
  _mock_client.messages.create.call_count == 3,
  f"called {_mock_client.messages.create.call_count} times")
T("pipeline error logged",
  _mock_repo_llm.log_pipeline_run.called)


# ===========================================================================
# D11-U19: Sonnet gate 4 budget fallback
# ===========================================================================
S("D11-U19: Sonnet budget fallback")

_mock_repo_gate4 = MagicMock()
_mock_repo_gate4.get_narrative.return_value = {
    "narrative_id": "n-gate4",
    "ns_score": 0.90,
    "created_at": (datetime.now(timezone.utc) - timedelta(days=10)).isoformat(),
}
_mock_repo_gate4.get_sonnet_calls_last_24h.return_value = []
# Budget nearly exhausted
_mock_repo_gate4.get_sonnet_daily_spend.return_value = {
    "total_tokens_used": 199000,
}

_mock_settings_gate4 = MagicMock()
_mock_settings_gate4.CONFIDENCE_ESCALATION_THRESHOLD = 0.60
_mock_settings_gate4.SONNET_DAILY_TOKEN_BUDGET = 200000
_mock_settings_gate4.SONNET_MAX_TOKENS = 2048
_mock_settings_gate4.HAIKU_MODEL = "claude-haiku-4-5-20251001"
_mock_settings_gate4.HAIKU_MAX_TOKENS = 512
_mock_settings_gate4.LLM_DAILY_BUDGET_USD = 5.0

_mock_client_gate4 = MagicMock()
# Haiku fallback response
_haiku_resp = MagicMock()
_haiku_resp.content = [MagicMock(text="Haiku fallback analysis")]
_haiku_resp.usage = MagicMock(input_tokens=100, output_tokens=50)
_mock_client_gate4.messages.create.return_value = _haiku_resp

_llm_gate4 = LlmClient.__new__(LlmClient)
_llm_gate4._settings = _mock_settings_gate4
_llm_gate4._repository = _mock_repo_gate4
_llm_gate4._client = _mock_client_gate4
_mock_repo_gate4.get_daily_llm_spend.return_value = 0.0

with patch("signals.get_narrative_age_days", return_value=10):
    _sonnet_result = _llm_gate4.call_sonnet("n-gate4", "Analyze this narrative")

T("gate 4 triggers Haiku fallback (not None)",
  _sonnet_result is not None,
  f"got: {_sonnet_result!r}")
T("fallback result is from Haiku",
  _sonnet_result == "Haiku fallback analysis",
  f"got: {_sonnet_result!r}")


# ===========================================================================
# D11-U20: _log_pipeline_error never raises
# ===========================================================================
S("D11-U20: _log_pipeline_error safety")

_mock_repo_err = MagicMock()
_mock_repo_err.log_pipeline_run.side_effect = sqlite3.Error("DB locked")

_llm_err = LlmClient.__new__(LlmClient)
_llm_err._settings = MagicMock()
_llm_err._repository = _mock_repo_err

_raised = False
try:
    _llm_err._log_pipeline_error("test_step", "test error message")
except Exception:
    _raised = True

T("_log_pipeline_error does not raise on repo failure", not _raised)


# ===========================================================================
# P20-B1.1: ANTHROPIC_TIMEOUT_SECONDS wired into Anthropic constructor
# ===========================================================================
S("P20-B1.1: LLM transport timeout wired")

_mock_settings_timeout = MagicMock()
_mock_settings_timeout.ANTHROPIC_API_KEY = "test-key"
_mock_settings_timeout.ANTHROPIC_TIMEOUT_SECONDS = 30

_mock_repo_timeout = MagicMock()

_captured_kwargs: dict = {}

def _mock_anthropic_ctor(**kwargs):
    _captured_kwargs.update(kwargs)
    return MagicMock()

with patch("anthropic.Anthropic", side_effect=_mock_anthropic_ctor):
    _timeout_inst = LlmClient.__new__(LlmClient)
    _timeout_inst._settings = _mock_settings_timeout
    _timeout_inst._repository = _mock_repo_timeout
    _timeout_inst._consecutive_transport_errors = 0
    _timeout_inst._init_client()

T("ANTHROPIC_TIMEOUT_SECONDS passed to Anthropic constructor",
  _captured_kwargs.get("timeout") == 30.0,
  f"kwargs: {_captured_kwargs}")


# ===========================================================================
# Summary
# ===========================================================================
_print_summary()
sys.exit(1 if _fail else 0)
