"""
D8 API test suite -- Changelog System (Phase 2 Batch 5).

Tests: D8-U1 (schema migration), D8-U2 (template explanations),
       D8-U3 (contributing documents JSON), D8-U4 (changelog endpoint),
       D8-U5 (enrichment integration), D8-U6 (get_changelog_for_narrative).

Uses the project's custom S/T runner + FastAPI TestClient (in-process).

Run with:
    python -X utf8 test_d8_api.py

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
from datetime import date, datetime, timedelta, timezone

from repository import SqliteRepository

# ===========================================================================
# D8-U1: Schema migration -- contributing_documents + pipeline_run_id
# ===========================================================================
S("D8-U1: Schema migration")

_tmp_dir = tempfile.mkdtemp()
_test_db = str(Path(_tmp_dir) / "test_d8.db")
repo = SqliteRepository(_test_db)
repo.migrate()

# Verify columns exist via PRAGMA
conn = sqlite3.connect(_test_db)
cols = conn.execute("PRAGMA table_info(mutation_events)").fetchall()
col_names = [c[1] for c in cols]
conn.close()

T("contributing_documents column exists",
  "contributing_documents" in col_names,
  f"columns: {col_names}")
T("pipeline_run_id column exists",
  "pipeline_run_id" in col_names,
  f"columns: {col_names}")

# Test save_mutation with new columns round-trips correctly
test_mut_id = str(uuid.uuid4())
test_run_id = str(uuid.uuid4())
contrib_json = json.dumps([{"doc_id": "d1", "source_domain": "reuters.com",
                            "excerpt_title": "Test headline", "published_at": "2026-03-21T12:00:00Z"}])
repo.save_mutation({
    "id": test_mut_id,
    "narrative_id": "n-test-1",
    "detected_at": datetime.now(timezone.utc).isoformat(),
    "mutation_type": "score_spike",
    "previous_value": "0.3",
    "new_value": "0.6",
    "magnitude": 0.3,
    "haiku_explanation": "Test explanation",
    "contributing_documents": contrib_json,
    "pipeline_run_id": test_run_id,
})

retrieved = repo.get_mutations_for_narrative("n-test-1", limit=1)
T("mutation saved and retrieved", len(retrieved) == 1)
T("contributing_documents round-trips",
  retrieved[0].get("contributing_documents") == contrib_json)
T("pipeline_run_id round-trips",
  retrieved[0].get("pipeline_run_id") == test_run_id)

# Test save_mutation WITHOUT new columns (backward compat)
old_style_id = str(uuid.uuid4())
repo.save_mutation({
    "id": old_style_id,
    "narrative_id": "n-test-2",
    "detected_at": datetime.now(timezone.utc).isoformat(),
    "mutation_type": "stage_change",
    "previous_value": "Emerging",
    "new_value": "Growing",
    "magnitude": 1.0,
    "haiku_explanation": "Old style",
})
old_ret = repo.get_mutations_for_narrative("n-test-2", limit=1)
T("old-style mutation saves without new cols", len(old_ret) == 1)
T("contributing_documents is None for old-style",
  old_ret[0].get("contributing_documents") is None)

# ===========================================================================
# D8-U2: Template explanation generation
# ===========================================================================
S("D8-U2: Template explanations")

# Set up document evidence for template tests
doc_id_1 = str(uuid.uuid4())
doc_id_2 = str(uuid.uuid4())
doc_id_3 = str(uuid.uuid4())
narrative_id = "n-templ-1"

# Insert a narrative for template tests
_conn = sqlite3.connect(_test_db)
_conn.execute(
    "INSERT OR REPLACE INTO narratives (narrative_id, name, velocity, document_count, stage, ns_score) "
    "VALUES (?, ?, ?, ?, ?, ?)",
    (narrative_id, "Test Narrative", 0.15, 12, "Growing", 0.65),
)
_conn.commit()

# Insert document evidence
for did, domain in [(doc_id_1, "reuters.com"), (doc_id_2, "reuters.com"), (doc_id_3, "bloomberg.com")]:
    repo.insert_document_evidence({
        "doc_id": did,
        "narrative_id": narrative_id,
        "source_url": f"https://{domain}/article",
        "source_domain": domain,
        "published_at": "2026-03-21T10:00:00Z",
        "author": "Test",
        "excerpt": "Test article excerpt for testing template explanations in the changelog system",
    })
_conn.close()

# Construct MutationDetector with mock data
from mutations import MutationDetector

class _FakeLlm:
    def call_haiku(self, task_type, narrative_id, prompt):
        return "LLM fallback"

_fake_llm = _FakeLlm()
assigned = {narrative_id: [doc_id_1, doc_id_2, doc_id_3]}
detector = MutationDetector(
    settings=None,  # not used by templates
    repository=repo,
    llm_client=_fake_llm,
    narrative_assigned_docs=assigned,
    pipeline_run_id="test-run-123",
)

# score_spike template
expl = detector.generate_template_explanation(narrative_id, "score_spike", "0.300", "0.600")
T("score_spike template not None", expl is not None)
T("score_spike includes 'increased'", "increased" in expl, f"got: {expl}")
T("score_spike includes percentage", "%" in expl, f"got: {expl}")
T("score_spike includes doc count", "3 new documents" in expl, f"got: {expl}")
T("score_spike includes source breakdown", "reuters.com" in expl, f"got: {expl}")

# velocity_reversal template
expl_v = detector.generate_template_explanation(narrative_id, "velocity_reversal", "-0.100", "0.200")
T("velocity_reversal template not None", expl_v is not None)
T("velocity_reversal includes 'reversed'", "reversed" in expl_v, f"got: {expl_v}")
T("velocity_reversal includes direction", "positive" in expl_v, f"got: {expl_v}")

# stage_change template
expl_s = detector.generate_template_explanation(narrative_id, "stage_change", "Emerging", "Growing")
T("stage_change template not None", expl_s is not None)
T("stage_change includes 'transitioned'", "transitioned" in expl_s, f"got: {expl_s}")
T("stage_change includes doc count", "12" in expl_s, f"got: {expl_s}")
T("stage_change includes velocity", "0.15" in expl_s, f"got: {expl_s}")

# doc_surge template
expl_d = detector.generate_template_explanation(narrative_id, "doc_surge", "5", "12")
T("doc_surge template not None", expl_d is not None)
T("doc_surge includes 'surged'", "surged" in expl_d, f"got: {expl_d}")
T("doc_surge includes ratio", "2.4x" in expl_d, f"got: {expl_d}")

# new_sonnet template
expl_n = detector.generate_template_explanation(narrative_id, "new_sonnet", "none", "generated")
T("new_sonnet template not None", expl_n is not None)
T("new_sonnet includes 'Sonnet'", "Sonnet" in expl_n, f"got: {expl_n}")

# Unknown type returns None (triggers LLM fallback)
expl_u = detector.generate_template_explanation(narrative_id, "unknown_type", "a", "b")
T("unknown type returns None", expl_u is None)

# No docs assigned -- source breakdown says 'unknown sources'
detector_empty = MutationDetector(
    settings=None, repository=repo, llm_client=_fake_llm,
    narrative_assigned_docs={}, pipeline_run_id="r2",
)
expl_no_docs = detector_empty.generate_template_explanation(narrative_id, "score_spike", "0.300", "0.600")
T("score_spike without docs still works", expl_no_docs is not None)
T("no doc count sentence when no docs",
  "new documents" not in expl_no_docs, f"got: {expl_no_docs}")

# ===========================================================================
# D8-U3: Contributing documents JSON
# ===========================================================================
S("D8-U3: Contributing docs JSON")

contrib = detector._get_contributing_docs_json(narrative_id)
T("contrib JSON is not None", contrib is not None)

parsed = json.loads(contrib)
T("contrib is a list", isinstance(parsed, list))
T("contrib has 3 entries", len(parsed) == 3, f"got {len(parsed)}")
T("first entry has doc_id", "doc_id" in parsed[0])
T("first entry has source_domain", "source_domain" in parsed[0])
T("first entry has excerpt_title", "excerpt_title" in parsed[0])
T("first entry has published_at", "published_at" in parsed[0])

# Excerpt truncation
long_excerpt = "A" * 200
long_doc_id = str(uuid.uuid4())
repo.insert_document_evidence({
    "doc_id": long_doc_id,
    "narrative_id": "n-trunc",
    "source_url": "https://test.com/long",
    "source_domain": "test.com",
    "published_at": "2026-03-21T10:00:00Z",
    "author": "Test",
    "excerpt": long_excerpt,
})
det_trunc = MutationDetector(
    settings=None, repository=repo, llm_client=_fake_llm,
    narrative_assigned_docs={"n-trunc": [long_doc_id]}, pipeline_run_id="r3",
)
contrib_trunc = det_trunc._get_contributing_docs_json("n-trunc")
parsed_trunc = json.loads(contrib_trunc)
T("excerpt_title truncated to 80 chars",
  len(parsed_trunc[0]["excerpt_title"]) == 80,
  f"got {len(parsed_trunc[0]['excerpt_title'])}")

# Empty docs returns None
contrib_none = detector._get_contributing_docs_json("nonexistent-narrative")
T("no docs returns None", contrib_none is None)

# ===========================================================================
# D8-U4: GET /api/narratives/{narrative_id}/changelog endpoint
# ===========================================================================
S("D8-U4: Changelog endpoint")

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app  # noqa: E402

with TestClient(app) as client:

    # First, find a narrative that exists (use /api/narratives to get one)
    resp_narr = client.get("/api/narratives")
    narr_list = resp_narr.json() if resp_narr.status_code == 200 else []

    if narr_list and isinstance(narr_list, list) and len(narr_list) > 0:
        test_nid = narr_list[0].get("narrative_id") or narr_list[0].get("id", "")

        resp = client.get(f"/api/narratives/{test_nid}/changelog?days=30")
        T("changelog returns 200", resp.status_code == 200, f"got {resp.status_code}")

        data = resp.json()
        T("response has narrative_id", "narrative_id" in data)
        T("response has narrative_name", "narrative_name" in data)
        T("response has days", "days" in data)
        T("response has total_changes", "total_changes" in data)
        T("response has changelog", "changelog" in data)
        T("changelog is a list", isinstance(data["changelog"], list))
        T("days param reflected", data["days"] == 30)

        # If changelog has entries, check their shape
        if data["changelog"]:
            entry = data["changelog"][0]
            T("entry has id", "id" in entry)
            T("entry has detected_at", "detected_at" in entry)
            T("entry has mutation_type", "mutation_type" in entry)
            T("entry has previous_value", "previous_value" in entry)
            T("entry has new_value", "new_value" in entry)
            T("entry has magnitude", "magnitude" in entry)
            T("entry has explanation", "explanation" in entry)
            T("entry has contributing_documents", "contributing_documents" in entry)
            T("entry has pipeline_run_id", "pipeline_run_id" in entry)
        else:
            # Empty changelog is fine -- just verify shape
            T("empty changelog is valid list", data["total_changes"] == 0)

        # Test days param
        resp_7 = client.get(f"/api/narratives/{test_nid}/changelog?days=7")
        T("changelog with days=7 returns 200", resp_7.status_code == 200)
        T("days=7 reflected", resp_7.json()["days"] == 7)

    else:
        # No narratives in DB -- test with a fake ID for 404
        T("no narratives available (skip shape tests)", True)

    # 404 for nonexistent narrative
    resp_404 = client.get("/api/narratives/nonexistent-id-12345/changelog")
    T("404 for nonexistent narrative", resp_404.status_code == 404,
      f"got {resp_404.status_code}")

    # Health endpoint regression
    resp_health = client.get("/api/health")
    T("health endpoint still 200", resp_health.status_code == 200)


# ===========================================================================
# D8-U5: Enrichment integration -- mutations get contributing_documents + run_id
# ===========================================================================
S("D8-U5: Enrichment integration")

# Set up a fresh temp DB with narratives and snapshots
_tmp_dir_2 = tempfile.mkdtemp()
_test_db_2 = str(Path(_tmp_dir_2) / "test_d8_enrich.db")
repo2 = SqliteRepository(_test_db_2)
repo2.migrate()

nid_enrich = "n-enrich-1"
today = datetime.now(timezone.utc).date().isoformat()
yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

# Insert narrative
_conn2 = sqlite3.connect(_test_db_2)
_conn2.execute(
    "INSERT INTO narratives (narrative_id, name, velocity, document_count, stage, ns_score, entropy, cohesion) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    (nid_enrich, "Enrichment Test", 0.2, 15, "Growing", 0.7, 1.5, 0.8),
)
_conn2.commit()

# Insert yesterday's snapshot (lower score to trigger score_spike)
snap_yesterday_id = str(uuid.uuid4())
_conn2.execute(
    "INSERT INTO narrative_snapshots (id, narrative_id, snapshot_date, ns_score, velocity, "
    "doc_count, lifecycle_stage, entropy, cohesion, polarization, created_at) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    (snap_yesterday_id, nid_enrich, yesterday, 0.4, 0.1, 10, "Emerging", 1.5, 0.8, 0.3,
     datetime.now(timezone.utc).isoformat()),
)
_conn2.commit()

# Insert today's snapshot (higher score)
snap_today_id = str(uuid.uuid4())
_conn2.execute(
    "INSERT INTO narrative_snapshots (id, narrative_id, snapshot_date, ns_score, velocity, "
    "doc_count, lifecycle_stage, entropy, cohesion, polarization, created_at) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    (snap_today_id, nid_enrich, today, 0.7, 0.2, 15, "Growing", 1.5, 0.8, 0.3,
     datetime.now(timezone.utc).isoformat()),
)
_conn2.commit()
_conn2.close()

# Insert document evidence
enrich_doc_id = str(uuid.uuid4())
repo2.insert_document_evidence({
    "doc_id": enrich_doc_id,
    "narrative_id": nid_enrich,
    "source_url": "https://reuters.com/test",
    "source_domain": "reuters.com",
    "published_at": "2026-03-21T10:00:00Z",
    "author": "Test",
    "excerpt": "Enrichment test article",
})

# Create detector with assigned docs and pipeline_run_id
run_id = "cycle-enrich-123"
det_enrich = MutationDetector(
    settings=None, repository=repo2, llm_client=_fake_llm,
    narrative_assigned_docs={nid_enrich: [enrich_doc_id]},
    pipeline_run_id=run_id,
)

# detect_mutations should find score_spike (0.4 -> 0.7 = 0.3 > 0.15 threshold)
# and stage_change (Emerging -> Growing)
muts = det_enrich.detect_mutations(nid_enrich)
T("mutations detected", len(muts) >= 1, f"got {len(muts)}")

# Find score_spike mutation
score_muts = [m for m in muts if m["mutation_type"] == "score_spike"]
T("score_spike detected", len(score_muts) == 1, f"types: {[m['mutation_type'] for m in muts]}")

if score_muts:
    sm = score_muts[0]
    T("mutation has pipeline_run_id", sm.get("pipeline_run_id") == run_id,
      f"got: {sm.get('pipeline_run_id')}")
    T("mutation has contributing_documents", sm.get("contributing_documents") is not None,
      f"got: {sm.get('contributing_documents')}")

    # Verify contributing_documents is valid JSON
    cd = json.loads(sm["contributing_documents"])
    T("contributing_documents is list", isinstance(cd, list))
    T("contributing_documents has 1 entry", len(cd) == 1)
    T("entry has source_domain reuters.com",
      cd[0].get("source_domain") == "reuters.com")

    # Verify explanation is template-based (not LLM fallback)
    T("explanation is template (not LLM)",
      "increased" in sm.get("haiku_explanation", ""),
      f"got: {sm.get('haiku_explanation')}")

# Find stage_change mutation
stage_muts = [m for m in muts if m["mutation_type"] == "stage_change"]
T("stage_change detected", len(stage_muts) == 1, f"types: {[m['mutation_type'] for m in muts]}")

if stage_muts:
    stm = stage_muts[0]
    T("stage mutation has pipeline_run_id", stm.get("pipeline_run_id") == run_id)
    T("stage explanation is template",
      "transitioned" in stm.get("haiku_explanation", ""),
      f"got: {stm.get('haiku_explanation')}")

# ===========================================================================
# D8-U6: get_changelog_for_narrative repository method
# ===========================================================================
S("D8-U6: get_changelog_for_narrative")

# Insert mutations with various dates into repo2
now_utc = datetime.now(timezone.utc)

# Recent mutation (2 days ago)
repo2.save_mutation({
    "id": str(uuid.uuid4()),
    "narrative_id": "n-cl-1",
    "detected_at": (now_utc - timedelta(days=2)).isoformat(),
    "mutation_type": "score_spike",
    "previous_value": "0.3",
    "new_value": "0.5",
    "magnitude": 0.2,
    "haiku_explanation": "Recent change",
    "contributing_documents": None,
    "pipeline_run_id": "run-1",
})

# Old mutation (40 days ago)
repo2.save_mutation({
    "id": str(uuid.uuid4()),
    "narrative_id": "n-cl-1",
    "detected_at": (now_utc - timedelta(days=40)).isoformat(),
    "mutation_type": "stage_change",
    "previous_value": "Emerging",
    "new_value": "Growing",
    "magnitude": 1.0,
    "haiku_explanation": "Old change",
    "contributing_documents": None,
    "pipeline_run_id": "run-0",
})

# Another recent mutation (1 day ago)
repo2.save_mutation({
    "id": str(uuid.uuid4()),
    "narrative_id": "n-cl-1",
    "detected_at": (now_utc - timedelta(days=1)).isoformat(),
    "mutation_type": "doc_surge",
    "previous_value": "5",
    "new_value": "12",
    "magnitude": 2.4,
    "haiku_explanation": "Doc surge",
    "contributing_documents": None,
    "pipeline_run_id": "run-2",
})

# Test days=30 (should get 2 recent, not the 40-day-old one)
cl_30 = repo2.get_changelog_for_narrative("n-cl-1", days=30)
T("changelog days=30 returns 2 entries", len(cl_30) == 2,
  f"got {len(cl_30)}")

# Test ordering (most recent first)
if len(cl_30) >= 2:
    T("ordered DESC by detected_at",
      cl_30[0]["detected_at"] > cl_30[1]["detected_at"],
      f"{cl_30[0]['detected_at']} vs {cl_30[1]['detected_at']}")

# Test days=7 (should still get the same 2)
cl_7 = repo2.get_changelog_for_narrative("n-cl-1", days=7)
T("changelog days=7 returns 2 entries", len(cl_7) == 2,
  f"got {len(cl_7)}")

# Test days=60 (should get all 3)
cl_60 = repo2.get_changelog_for_narrative("n-cl-1", days=60)
T("changelog days=60 returns 3 entries", len(cl_60) == 3,
  f"got {len(cl_60)}")

# Test empty result for nonexistent narrative
cl_empty = repo2.get_changelog_for_narrative("nonexistent", days=30)
T("empty result for nonexistent narrative", len(cl_empty) == 0)

# Test get_document_evidence_by_ids
evidence = repo2.get_document_evidence_by_ids([enrich_doc_id])
T("get_document_evidence_by_ids returns 1", len(evidence) == 1)
T("evidence has correct doc_id", evidence[0]["doc_id"] == enrich_doc_id)

evidence_empty = repo2.get_document_evidence_by_ids([])
T("empty input returns empty list", len(evidence_empty) == 0)

evidence_miss = repo2.get_document_evidence_by_ids(["nonexistent"])
T("nonexistent doc_id returns empty", len(evidence_miss) == 0)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
_print_summary()
sys.exit(0 if _fail == 0 else 1)
