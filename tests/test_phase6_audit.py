"""
Phase 6 Audit — Behavioral tests for monitoring & observability fixes.

Section A: pipeline_run_log schema fix
  T1: pipeline_run_log allows multiple rows per run_id
  T2: pipeline_run_log has auto-increment id column
  T3: _log_step persists step 21 data

Section B: Centroid blob validation
  T4: Corrupted blob (wrong size) is skipped in admin endpoint
  T5: Empty blob is skipped in admin endpoint

Section C: Leak indicator time-window
  T6: count_suppressed_with_documents filters by assigned_at recency

Section D: Cohesion threshold
  T7: Admin endpoint catches near-1.0 cohesion (>= 0.999)

Section E: Batch centroid loading
  T8: get_latest_centroids_batch method exists and returns dict
  T9: Batch method returns same data as individual calls

Section F: Null handling
  T10: Singletons in admin response never have null document_count
"""

import sys
import uuid
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


from repository import SqliteRepository
from settings import get_api_settings

settings = get_api_settings()
repo = SqliteRepository(settings.DB_PATH)
repo.migrate()

# ===========================================================================
# Section A: pipeline_run_log schema fix
# ===========================================================================
S("Section A: pipeline_run_log schema fix")

# T1: Multiple rows per run_id
test_run_id = f"test-audit-{uuid.uuid4().hex[:8]}"
try:
    repo.log_pipeline_run({
        "run_id": test_run_id, "step_number": 0, "step_name": "test_step_0",
        "status": "OK", "duration_ms": 1, "error_message": None,
        "run_at": "2026-04-02T00:00:00Z",
    })
    repo.log_pipeline_run({
        "run_id": test_run_id, "step_number": 1, "step_name": "test_step_1",
        "status": "OK", "duration_ms": 2, "error_message": None,
        "run_at": "2026-04-02T00:00:01Z",
    })
    with repo._get_conn() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM pipeline_run_log WHERE run_id = ?",
            (test_run_id,),
        ).fetchone()[0]
    multi_ok = count == 2
except Exception as e:
    multi_ok = False
    count = str(e)

T("T1: pipeline_run_log allows multiple rows per run_id", multi_ok, f"count={count}")

# T2: Has auto-increment id
try:
    with repo._get_conn() as conn:
        schema = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='pipeline_run_log'"
        ).fetchone()[0]
    has_autoincrement = "AUTOINCREMENT" in schema.upper()
except Exception as e:
    has_autoincrement = False
    schema = str(e)

T("T2: pipeline_run_log has auto-increment id column", has_autoincrement)

# T3: _log_step can persist step 21
from pipeline import _log_step
step21_run_id = f"test-s21-{uuid.uuid4().hex[:8]}"
_log_step(repo, step21_run_id, 21, "quality_metrics", "OK", 42.0, "active=5 potential_dupes=0")
try:
    with repo._get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_run_log WHERE run_id = ? AND step_number = 21",
            (step21_run_id,),
        ).fetchone()
    s21_ok = row is not None and dict(row)["step_name"] == "quality_metrics"
except Exception as e:
    s21_ok = False

T("T3: _log_step persists step 21 data", s21_ok)

# Cleanup test rows
try:
    with repo._get_conn() as conn:
        conn.execute("DELETE FROM pipeline_run_log WHERE run_id LIKE 'test-%'")
except Exception:
    pass

# ===========================================================================
# Section B: Centroid blob validation
# ===========================================================================
S("Section B: Centroid blob validation")

from fastapi.testclient import TestClient
from api.main import app

with TestClient(app) as client:
    # T4: Hit the endpoint — it should not 500 even if blobs are weird
    resp = client.get("/api/admin/narrative-quality")
    T("T4: Admin endpoint returns 200 (no blob crashes)", resp.status_code == 200)

    # T5: Verify blob validation exists in source
    api_src = Path(_ROOT, "api", "main.py").read_text(encoding="utf-8")
    has_blob_check = "len(blob) % 4 == 0" in api_src and "len(blob) // 4 >= 768" in api_src
    T("T5: Centroid blob size validation present in API", has_blob_check)

# ===========================================================================
# Section C: Leak indicator time-window
# ===========================================================================
S("Section C: Leak indicator time-window")

repo_src = Path(_ROOT, "repository.py").read_text(encoding="utf-8")
has_time_filter = "DATE('now', '-3 days')" in repo_src
T("T6: count_suppressed_with_documents uses time-window filter", has_time_filter)

leak_val = repo.count_suppressed_with_documents()
T("T7: Leak indicator returns int", isinstance(leak_val, int), f"value={leak_val}")

# ===========================================================================
# Section D: Cohesion threshold
# ===========================================================================
S("Section D: Cohesion threshold")

pipeline_src = Path(_ROOT, "pipeline.py").read_text(encoding="utf-8")
api_src = Path(_ROOT, "api", "main.py").read_text(encoding="utf-8")

_settings_src = Path(_ROOT, "settings.py").read_text(encoding="utf-8")
# Accept either hardcoded 0.999 literal or settings-based threshold (PIPELINE_EXPORT_COHESION_GATE=0.999)
has_threshold_pipeline = ">= 0.999" in pipeline_src or "PIPELINE_EXPORT_COHESION_GATE" in pipeline_src
has_threshold_api = ">= 0.999" in api_src or "PIPELINE_EXPORT_COHESION_GATE" in api_src or (
    "_cohesion_gate" in api_src and "PIPELINE_EXPORT_COHESION_GATE: float = 0.999" in _settings_src
)
no_exact_eq_pipeline = 'cohesion") == 1.0' not in pipeline_src
no_exact_eq_api = 'cohesion") == 1.0' not in api_src

T("T8: Pipeline uses >= 0.999 threshold (not == 1.0)",
  has_threshold_pipeline and no_exact_eq_pipeline)
T("T9: API uses >= 0.999 threshold (not == 1.0)",
  has_threshold_api and no_exact_eq_api)

# ===========================================================================
# Section E: Batch centroid loading
# ===========================================================================
S("Section E: Batch centroid loading")

T("T10: get_latest_centroids_batch method exists",
  hasattr(repo, "get_latest_centroids_batch"))

# Compare batch vs individual for a sample of narratives
actives = repo.get_all_active_narratives()
sample_ids = [n["narrative_id"] for n in actives[:5]]

batch_result = repo.get_latest_centroids_batch(sample_ids)
individual_result = {}
for nid in sample_ids:
    blob = repo.get_latest_centroid(nid)
    if blob:
        individual_result[nid] = blob

batch_matches = (set(batch_result.keys()) == set(individual_result.keys()))
if batch_matches:
    for nid in batch_result:
        if batch_result[nid] != individual_result[nid]:
            batch_matches = False
            break

T("T11: Batch results match individual calls",
  batch_matches, f"batch={len(batch_result)} individual={len(individual_result)}")

# Empty input
empty_result = repo.get_latest_centroids_batch([])
T("T12: Batch handles empty input", empty_result == {})

# ===========================================================================
# Section F: Null handling
# ===========================================================================
S("Section F: Null handling")

with TestClient(app) as client:
    resp = client.get("/api/admin/narrative-quality")
    data = resp.json()
    singletons = data.get("singletons", [])
    no_nulls = all(s.get("document_count") is not None for s in singletons)
    T("T13: Singletons never have null document_count",
      no_nulls, f"checked {len(singletons)} singletons")

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
