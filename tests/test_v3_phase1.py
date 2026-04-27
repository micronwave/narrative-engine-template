"""
V3 Phase 1 — Surface Hidden Value Tests

Unit:
  V3-AUTH-1: get_current_user returns user_id="local" for no token (single-user mode)
  V3-AUTH-2: get_current_user returns user_id="local" for valid stub token
  V3-AUTH-3: get_current_user raises 403 for invalid token
  V3-DET-1: GET /api/narratives/{id} includes sonnet_analysis field
  V3-DET-2: GET /api/narratives/{id} includes sentiment field
  V3-DET-3: GET /api/narratives/{id} includes coordination field
  V3-DET-4: GET /api/narratives/{id} includes ns_score, document_count, topic_tags
  V3-COORD-1: GET /api/narratives/{id}/coordination returns 200
  V3-COORD-2: GET /api/coordination/summary returns 200 with total_events
  V3-SRC-1: GET /api/narratives/{id}/sources returns list of domains
  V3-BUF-1: GET /api/pipeline/buffer returns pending/clustered/total
  V3-DOC-1: GET /api/narratives/{id}/documents returns paginated items
  V3-COR-1: GET /api/correlations/top returns pairs list
  V3-COR-2: GET /api/narratives/{id}/correlations returns list
  V3-SENT-1: compute_sentiment_scores returns correct shape
  V3-SENT-2: compute_sentiment_scores handles empty input
"""

import sys
from pathlib import Path

# Add project root + api/ directory to sys.path
_PROJECT_ROOT = str(Path(__file__).parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
_API_DIR = str(Path(__file__).parent.parent / "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

from fastapi.testclient import TestClient  # noqa: E402
from api.main import app  # noqa: E402

# ---------------------------------------------------------------------------
# Test runner helpers
# ---------------------------------------------------------------------------
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
# Auth Scaffolding Tests
# ===========================================================================
S("V3-AUTH: Auth dependency")

# AUTH-2: Valid stub token → works
resp = client.get("/api/auth/me", headers={"x-auth-token": "stub-auth-token"})
T("AUTH-2: Valid stub token → auth/me returns 200", resp.status_code == 200, f"status={resp.status_code}")

# AUTH-3: Invalid token → 403
resp = client.get("/api/auth/me", headers={"x-auth-token": "bad-token"})
T("AUTH-3: Invalid token → 403", resp.status_code == 403, f"status={resp.status_code}")

# ===========================================================================
# Narrative Detail Enrichment Tests
# ===========================================================================
S("V3-DET: Enriched narrative detail")

# Get a real narrative ID
narratives_resp = client.get("/api/narratives")
narratives = narratives_resp.json()
has_narratives = isinstance(narratives, list) and len(narratives) > 0
T("Setup: narratives list available", has_narratives, f"count={len(narratives) if isinstance(narratives, list) else 'N/A'}")

if has_narratives:
    test_id = narratives[0]["id"]
    detail_resp = client.get(f"/api/narratives/{test_id}")
    T("DET-0: Detail returns 200", detail_resp.status_code == 200)
    detail = detail_resp.json()

    T("DET-1: sonnet_analysis field exists", "sonnet_analysis" in detail,
      f"keys={list(detail.keys())[:10]}")
    T("DET-2: sentiment field exists", "sentiment" in detail)
    T("DET-3: coordination field exists", "coordination" in detail)
    if "coordination" in detail:
        coord = detail["coordination"]
        T("DET-3a: coordination has flags", "flags" in coord, f"coord_keys={list(coord.keys())}")
        T("DET-3b: coordination has events list", isinstance(coord.get("events"), list))

    T("DET-4a: ns_score field exists", "ns_score" in detail)
    T("DET-4b: document_count field exists", "document_count" in detail)
    T("DET-4c: topic_tags field exists", "topic_tags" in detail)
    T("DET-4d: stage field exists", "stage" in detail)
    T("DET-4e: burst_velocity field exists", "burst_velocity" in detail)
    T("DET-4f: entity_tags field exists", "entity_tags" in detail)
    T("DET-4g: source_stats field exists", "source_stats" in detail)


# ===========================================================================
# Coordination Endpoints
# ===========================================================================
S("V3-COORD: Coordination endpoints")

if has_narratives:
    resp = client.get(f"/api/narratives/{test_id}/coordination")
    T("COORD-1: narrative coordination → 200", resp.status_code == 200)
    data = resp.json()
    T("COORD-1a: has events list", isinstance(data.get("events"), list))

resp = client.get("/api/coordination/summary")
T("COORD-2: coordination summary → 200", resp.status_code == 200)
data = resp.json()
T("COORD-2a: has total_events", "total_events" in data, f"keys={list(data.keys())}")


# ===========================================================================
# Source Breakdown Endpoint
# ===========================================================================
S("V3-SRC: Source breakdown")

if has_narratives:
    resp = client.get(f"/api/narratives/{test_id}/sources")
    T("SRC-1: sources → 200", resp.status_code == 200)
    sources = resp.json()
    T("SRC-1a: returns list", isinstance(sources, list))
    if sources:
        T("SRC-1b: has domain field", "domain" in sources[0])
        T("SRC-1c: has count field", "count" in sources[0])
        T("SRC-1d: has percentage field", "percentage" in sources[0])


# ===========================================================================
# Buffer Status Endpoint
# ===========================================================================
S("V3-BUF: Buffer status")

resp = client.get("/api/pipeline/buffer")
T("BUF-1: buffer status → 200", resp.status_code == 200)
buf = resp.json()
T("BUF-1a: has pending", "pending" in buf)
T("BUF-1b: has clustered", "clustered" in buf)
T("BUF-1c: has total", "total" in buf)
T("BUF-1d: total = pending + clustered",
  buf.get("total") == buf.get("pending", 0) + buf.get("clustered", 0),
  f"total={buf.get('total')}, pending={buf.get('pending')}, clustered={buf.get('clustered')}")


# ===========================================================================
# Paginated Documents Endpoint
# ===========================================================================
S("V3-DOC: Paginated documents")

if has_narratives:
    resp = client.get(f"/api/narratives/{test_id}/documents?limit=5&offset=0")
    T("DOC-1: documents → 200", resp.status_code == 200)
    doc_data = resp.json()
    T("DOC-1a: has items list", isinstance(doc_data.get("items"), list))
    T("DOC-1b: has total", "total" in doc_data)
    T("DOC-1c: items ≤ limit", len(doc_data.get("items", [])) <= 5,
      f"count={len(doc_data.get('items', []))}")


# ===========================================================================
# Correlation Batch Endpoints
# ===========================================================================
S("V3-COR: Correlation endpoints")

resp = client.get("/api/correlations/top?limit=5")
T("COR-1: top correlations → 200", resp.status_code == 200)
cor_data = resp.json()
T("COR-1a: has pairs list", isinstance(cor_data.get("pairs"), list))

if has_narratives:
    resp = client.get(f"/api/narratives/{test_id}/correlations")
    T("COR-2: narrative correlations → 200", resp.status_code == 200)
    T("COR-2a: returns list", isinstance(resp.json(), list))


# ===========================================================================
# Sentiment Unit Tests
# ===========================================================================
S("V3-SENT: Sentiment computation")

from signals import compute_sentiment_scores  # noqa: E402

result = compute_sentiment_scores(["This is great and amazing news", "Terrible crash disaster"])
T("SENT-1a: returns dict", isinstance(result, dict))
T("SENT-1b: has mean", "mean" in result)
T("SENT-1c: has std", "std" in result)
T("SENT-1d: has count", result.get("count") == 2, f"count={result.get('count')}")
T("SENT-1e: mean between -1 and 1", -1 <= result.get("mean", 99) <= 1, f"mean={result.get('mean')}")
T("SENT-1f: has polarization_label", "polarization_label" in result)

result_empty = compute_sentiment_scores([])
T("SENT-2a: empty input returns count=0", result_empty.get("count") == 0)
T("SENT-2b: empty input mean=0", result_empty.get("mean") == 0.0)


# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"V3 Phase 1 Results: {passed}/{total} passed")
if passed < total:
    print("\nFailed tests:")
    for name, ok in _results:
        if not ok:
            print(f"  ✗ {name}")
    sys.exit(1)
else:
    print("All V3 Phase 1 tests passed.")
