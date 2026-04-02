"""Phase 3 integration tests: clustering, signals, centrality, adversarial."""

import sys
import traceback
import uuid

import numpy as np

sys.path.insert(0, ".")

results = []


def passed(name, detail=""):
    tag = f" ({detail})" if detail else ""
    results.append((name, "PASS", ""))
    print(f"  PASS: {name}{tag}")


def failed(name, reason):
    results.append((name, "FAIL", reason))
    print(f"  FAIL: {name}")
    for line in reason.strip().splitlines():
        print(f"        {line}")


# ---------------------------------------------------------------------------
# Shared mock helpers
# ---------------------------------------------------------------------------

def make_blob(vec):
    return vec.astype(np.float32).tobytes()


class MockEmbedder:
    def dimension(self):
        return 768


class MockVectorStore:
    def __init__(self):
        self._vecs = {}

    def count(self):
        return len(self._vecs)

    def is_empty(self):
        return not self._vecs

    def get_all_ids(self):
        return list(self._vecs)

    def get_vector(self, nid):
        return self._vecs.get(nid)

    def initialize(self, dim):
        pass

    def add(self, vecs, ids):
        for i, nid in enumerate(ids):
            self._vecs[nid] = vecs[i]


class MockRepository:
    def __init__(self, candidates=None):
        self._cands = candidates or []
        self.narratives = []
        self.centroid_history = []
        self.assignments = []

    def get_candidate_buffer(self, status="pending"):
        return [c for c in self._cands if c["status"] == status]

    def insert_narrative(self, n):
        self.narratives.append(n)

    def insert_centroid_history(self, narrative_id, date, blob):
        self.centroid_history.append((narrative_id, date))

    def record_narrative_assignment(self, narrative_id, date):
        self.assignments.append((narrative_id, date))

    def update_candidate_status(self, doc_id, status, narrative_id=None):
        for c in self._cands:
            if c["doc_id"] == doc_id:
                c["status"] = status
                c["narrative_id_assigned"] = narrative_id

    def log_adversarial_event(self, event):
        pass

    def get_narrative(self, narrative_id):
        return {"coordination_flag_count": 0}

    def get_coordination_flags_rolling_window(self, narrative_id, days):
        return 0

    def update_narrative(self, narrative_id, updates):
        pass

    def insert_document_evidence(self, evidence):
        pass


class MockSettings:
    LSH_THRESHOLD = 0.85
    SYNC_BURST_MIN_SOURCES = 5
    SYNC_BURST_WINDOW_SECONDS = 300


# ---------------------------------------------------------------------------
# 1. clustering.py
# ---------------------------------------------------------------------------
print("--- clustering.py ---")

try:
    from clustering import run_clustering

    # 1a: fewer than 5 docs -> skip gracefully, return []
    try:
        np.random.seed(0)
        few = [
            {
                "doc_id": str(uuid.uuid4()),
                "embedding_blob": make_blob(np.random.rand(768).astype(np.float32)),
                "status": "pending",
                "narrative_id_assigned": None,
            }
            for _ in range(3)
        ]
        result = run_clustering(
            MockRepository(few), MockVectorStore(), MockEmbedder(), MockSettings()
        )
        assert result == [], f"expected [] got {result}"
        passed("clustering: <5 docs returns [] gracefully")
    except Exception as exc:
        failed("clustering: <5 docs graceful skip", traceback.format_exc(limit=4))

    # 1b: 10 docs in 2 tight synthetic clusters
    try:
        np.random.seed(1)
        cands = []
        for centre in [[1.0] + [0.0] * 767, [0.0, 1.0] + [0.0] * 766]:
            for _ in range(5):
                v = np.array(centre, dtype=np.float32) + (
                    np.random.randn(768).astype(np.float32) * 0.05
                )
                v /= np.linalg.norm(v)
                cands.append(
                    {
                        "doc_id": str(uuid.uuid4()),
                        "embedding_blob": make_blob(v),
                        "status": "pending",
                        "narrative_id_assigned": None,
                    }
                )
        vs = MockVectorStore()
        repo = MockRepository(cands)
        result = run_clustering(repo, vs, MockEmbedder(), MockSettings())
        assert isinstance(result, list)
        assert len(result) >= 1, f"expected >=1 cluster, got {len(result)}"
        assert all(n["stage"] == "Emerging" for n in repo.narratives)
        assert len(repo.centroid_history) == len(result)
        for nid in result:
            assert vs.get_vector(nid) is not None, f"centroid missing for {nid}"
        passed(
            "clustering: 10 docs HDBSCAN",
            f"{len(result)} narrative(s), {len(repo.assignments)} assignments",
        )
    except Exception as exc:
        failed("clustering: 10 docs HDBSCAN", traceback.format_exc(limit=4))

except ImportError as exc:
    failed("clustering: import", str(exc))

# ---------------------------------------------------------------------------
# 2. signals.py
# ---------------------------------------------------------------------------
print()
print("--- signals.py ---")

try:
    from signals import (
        compute_cohesion,
        compute_entropy,
        compute_intent_weight,
        compute_ns_score,
        compute_polarization,
        compute_velocity,
    )

    # compute_velocity
    try:
        np.random.seed(2)
        v = compute_velocity(np.random.rand(768), np.random.rand(768))
        assert isinstance(v, float) and v >= 0.0
        passed("signals: compute_velocity", f"value={v:.4f}")
    except Exception as exc:
        failed("signals: compute_velocity", str(exc))

    # compute_entropy
    try:
        e = compute_entropy(["AAPL mentioned MSFT earnings"], 2)
        # 2 tickers (AAPL, MSFT) + 'earnings' entity -> unique count >= 2
        assert e is None or (isinstance(e, float) and e >= 0.0)
        passed("signals: compute_entropy", f"value={e}")
    except Exception as exc:
        failed("signals: compute_entropy", str(exc))

    # compute_intent_weight
    try:
        iw = compute_intent_weight(["company is acquiring assets"])
        assert 0.0 < iw <= 1.0, f"acquiring must produce > 0, got {iw}"
        passed("signals: compute_intent_weight", f"value={iw:.4f}")
    except Exception as exc:
        failed("signals: compute_intent_weight", str(exc))

    # compute_cohesion
    try:
        np.random.seed(3)
        a = np.random.rand(768).astype(np.float32)
        b = np.random.rand(768).astype(np.float32)
        a /= np.linalg.norm(a)
        b /= np.linalg.norm(b)
        coh = compute_cohesion([a, b])
        assert -1.0 <= coh <= 1.0
        passed("signals: compute_cohesion", f"value={coh:.4f}")
    except Exception as exc:
        failed("signals: compute_cohesion", str(exc))

    # compute_polarization
    try:
        pol = compute_polarization(
            ["great earnings rally surge profit growth", "terrible losses crash decline miss"]
        )
        assert pol >= 0.0
        passed("signals: compute_polarization", f"value={pol:.4f}")
    except Exception as exc:
        failed("signals: compute_polarization", str(exc))

    # compute_ns_score
    try:
        ns = compute_ns_score(0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5)
        assert isinstance(ns, float)
        assert 0.0 <= ns <= 1.5, f"ns out of expected range: {ns}"
        passed("signals: compute_ns_score", f"value={ns:.4f}")
    except Exception as exc:
        failed("signals: compute_ns_score", str(exc))

except ImportError as exc:
    failed("signals: import", str(exc))

# ---------------------------------------------------------------------------
# 3. centrality.py
# ---------------------------------------------------------------------------
print()
print("--- centrality.py ---")

try:
    from centrality import build_narrative_graph, compute_centrality, flag_catalysts

    class MockVS3:
        def __init__(self, vecs):
            self._v = vecs

        def get_vector(self, nid):
            return self._v.get(nid)

    v1 = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    v2 = np.array([0.9, 0.43, 0.0], dtype=np.float32)
    v2 /= np.linalg.norm(v2)
    v3 = np.array([0.0, 0.0, 1.0], dtype=np.float32)
    narrs = [
        {"narrative_id": "n1"},
        {"narrative_id": "n2"},
        {"narrative_id": "n3"},
    ]
    vs3 = MockVS3({"n1": v1, "n2": v2, "n3": v3})

    # build graph
    try:
        g = build_narrative_graph(narrs, vs3, similarity_threshold=0.40)
        assert g.number_of_nodes() == 3
        passed(
            "centrality: build_narrative_graph",
            f"{g.number_of_nodes()} nodes, {g.number_of_edges()} edges",
        )
    except Exception as exc:
        failed("centrality: build_narrative_graph", traceback.format_exc(limit=4))

    # compute centrality
    try:
        scores = compute_centrality(g)
        assert isinstance(scores, dict)
        assert len(scores) == 3
        assert all(0.0 <= s <= 1.0 for s in scores.values())
        passed(
            "centrality: compute_centrality",
            str({k: round(v, 3) for k, v in scores.items()}),
        )
    except Exception as exc:
        failed("centrality: compute_centrality", traceback.format_exc(limit=4))

    # flag catalysts
    try:
        cats = flag_catalysts(scores)
        assert isinstance(cats, list)
        assert len(cats) >= 1  # 3 narratives -> top 1
        passed("centrality: flag_catalysts", f"flagged={cats}")
    except Exception as exc:
        failed("centrality: flag_catalysts", traceback.format_exc(limit=4))

except ImportError as exc:
    failed("centrality: import", str(exc))

# ---------------------------------------------------------------------------
# 4. adversarial.py
# ---------------------------------------------------------------------------
print()
print("--- adversarial.py ---")

try:
    from adversarial import check_coordination
    from deduplicator import _extract_shingles
    from datasketch import MinHash
    from ingester import RawDocument

    def make_sig(text, num_perm=128):
        m = MinHash(num_perm=num_perm)
        for sh in _extract_shingles(text):
            m.update(sh.encode("utf-8"))
        return m

    class MockDedup:
        def __init__(self, sigs):
            self._s = sigs

        def get_batch_signatures(self):
            return self._s

    # 4a: 2 docs, different domains, different content -> no false positive
    try:
        docs = [
            RawDocument(
                "d1", "AAPL earnings beat expectations strong growth",
                "http://reuters.com/1", "reuters.com",
                "2026-03-14T10:00:00+00:00", "2026-03-14T10:00:00+00:00",
            ),
            RawDocument(
                "d2", "Federal Reserve holds rates steady amid inflation",
                "http://bloomberg.com/1", "bloomberg.com",
                "2026-03-14T10:01:00+00:00", "2026-03-14T10:01:00+00:00",
            ),
        ]
        sigs = {d.doc_id: make_sig(d.raw_text) for d in docs}
        events = check_coordination(
            docs, MockDedup(sigs), [], MockSettings(), MockRepository()
        )
        assert events == [], f"false positive: {events}"
        passed("adversarial: 2 different-content docs, different domains -> 0 events")
    except Exception as exc:
        failed("adversarial: no false positive on different content", traceback.format_exc(limit=4))

    # 4b: identical content, only 2 domains (< SYNC_BURST_MIN_SOURCES=5) -> no event
    try:
        text = "company acquiring capex committed executing deployed divesting"
        docs2 = [
            RawDocument(
                "e1", text, "http://a.com/", "a.com",
                "2026-03-14T10:00:00+00:00", "2026-03-14T10:00:00+00:00",
            ),
            RawDocument(
                "e2", text, "http://b.com/", "b.com",
                "2026-03-14T10:00:00+00:00", "2026-03-14T10:00:00+00:00",
            ),
        ]
        sigs2 = {d.doc_id: make_sig(d.raw_text) for d in docs2}
        events2 = check_coordination(
            docs2, MockDedup(sigs2), [], MockSettings(), MockRepository()
        )
        assert events2 == [], f"2 domains < SYNC_BURST_MIN_SOURCES=5 should not trigger: {events2}"
        passed("adversarial: identical content, 2 domains < threshold -> 0 events")
    except Exception as exc:
        failed("adversarial: domain count threshold gate", traceback.format_exc(limit=4))

except ImportError as exc:
    failed("adversarial: import", str(exc))

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print()
print("=" * 55)
total = len(results)
passes = sum(1 for _, r, _ in results if r == "PASS")
fails = sum(1 for _, r, _ in results if r == "FAIL")
print(f"RESULT: {passes}/{total} PASS" + (f"  |  {fails} FAIL" if fails else ""))
if fails:
    print()
    for name, res, reason in results:
        if res == "FAIL":
            print(f"  FAIL: {name}")
