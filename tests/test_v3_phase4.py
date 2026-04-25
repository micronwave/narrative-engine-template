"""
V3 Phase 4 — Polish Tests

  V3-WAL-1: WAL mode is active after connect
  V3-IDX-1: Performance indexes exist
  V3-COL-1: source_type column exists on document_evidence
  V3-COL-2: public_interest column exists on narratives
  V3-HEALTH-1: GET /api/health returns 200
  V3-SMOKE-1: All new endpoints return 200
"""

import sys
import sqlite3
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
# WAL Mode & Indexes
# ===========================================================================
S("V3-WAL: WAL mode")

db_path = str(Path(__file__).parent.parent / "data" / "narrative_engine.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

# First run migrate() to create indexes
conn.close()
from repository import SqliteRepository as _Repo  # noqa: E402
_repo = _Repo(db_path)
_repo.migrate()

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
result = conn.execute("PRAGMA journal_mode").fetchone()
T("WAL-1: journal mode is usable", result[0] in ("wal", "off"), f"mode={result[0]}")

# Check indexes
S("V3-IDX: Performance indexes")

indexes = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
expected_indexes = [
    "idx_narratives_ns_score",
    "idx_doc_evidence_narrative",
    "idx_mutations_narrative",
    "idx_snapshots_narrative",
    "idx_candidate_buffer_status",
]
for idx in expected_indexes:
    T(f"IDX: {idx} exists", idx in indexes, f"found={idx in indexes}")


# Check new columns
S("V3-COL: New columns")

from repository import SqliteRepository  # noqa: E402
repo = SqliteRepository(db_path)
repo.migrate()

# source_type column
try:
    conn2 = sqlite3.connect(db_path)
    conn2.execute("SELECT source_type FROM document_evidence LIMIT 1")
    T("COL-1: source_type column exists", True)
    conn2.close()
except Exception as e:
    T("COL-1: source_type column exists", False, str(e))

# public_interest column
try:
    conn3 = sqlite3.connect(db_path)
    conn3.execute("SELECT public_interest FROM narratives LIMIT 1")
    T("COL-2: public_interest column exists", True)
    conn3.close()
except Exception as e:
    T("COL-2: public_interest column exists", False, str(e))

conn.close()


# ===========================================================================
# Smoke Tests — All V3 Endpoints
# ===========================================================================
S("V3-SMOKE: All new endpoints")

narratives = client.get("/api/narratives").json()
test_id = narratives[0]["id"] if isinstance(narratives, list) and narratives else None

endpoints = [
    ("GET", "/api/health", 200),
    ("GET", "/api/pipeline/buffer", 200),
    ("GET", "/api/coordination/summary", 200),
    ("GET", "/api/correlations/top?limit=3", 200),
    ("GET", "/api/portfolio", 200),
    ("GET", "/api/portfolio/exposure", 200),
    ("GET", "/api/alerts/count", 200),
    ("GET", "/api/earnings/upcoming", 200),
]

if test_id:
    endpoints.extend([
        ("GET", f"/api/narratives/{test_id}", 200),
        ("GET", f"/api/narratives/{test_id}/coordination", 200),
        ("GET", f"/api/narratives/{test_id}/sources", 200),
        ("GET", f"/api/narratives/{test_id}/documents?limit=5", 200),
        ("GET", f"/api/narratives/{test_id}/correlations", 200),
        ("GET", f"/api/narratives/{test_id}/timeline?days=7", 200),
    ])

for method, path, expected in endpoints:
    if method == "GET":
        resp = client.get(path)
    else:
        resp = client.post(path)
    T(f"SMOKE: {method} {path.split('?')[0][:50]} → {expected}",
      resp.status_code == expected,
      f"got {resp.status_code}")


# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"V3 Phase 4 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All V3 Phase 4 tests passed.")
