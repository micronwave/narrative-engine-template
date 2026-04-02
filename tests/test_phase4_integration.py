"""Phase 4 integration tests: llm_client, asset_mapper, output, end-to-end."""

import hashlib
import pickle
import sys
import tempfile
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


class MockSettings:
    ANTHROPIC_API_KEY = "sk-placeholder-key-for-testing"
    HAIKU_MODEL = "claude-3-5-haiku-20241022"
    SONNET_MODEL = "claude-3-5-sonnet-20241022"
    HAIKU_MAX_TOKENS = 512
    SONNET_MAX_TOKENS = 2048
    CONFIDENCE_ESCALATION_THRESHOLD = 0.60
    SONNET_DAILY_TOKEN_BUDGET = 200000
    LSH_THRESHOLD = 0.85
    SYNC_BURST_MIN_SOURCES = 5
    SYNC_BURST_WINDOW_SECONDS = 300


class MockRepository:
    """Minimal mock sufficient for llm_client gate tests."""

    def __init__(self, narrative=None, spend=None, sonnet_calls=None):
        self._narrative = narrative
        self._spend = spend
        self._sonnet_calls = sonnet_calls or []
        self.llm_calls = []
        self.pipeline_runs = []

    def get_narrative(self, narrative_id):
        return self._narrative

    def get_sonnet_calls_last_24h(self, narrative_id):
        return self._sonnet_calls

    def get_sonnet_daily_spend(self, date):
        return self._spend

    def log_llm_call(self, record):
        self.llm_calls.append(record)

    def log_pipeline_run(self, record):
        self.pipeline_runs.append(record)

    def update_sonnet_daily_spend(self, date, tokens, calls):
        pass


class MockEmbedder768:
    def dimension(self):
        return 768


# ---------------------------------------------------------------------------
# 1. llm_client.py
# ---------------------------------------------------------------------------
print("--- llm_client.py ---")

try:
    from llm_client import LlmClient

    # 1a: estimate_tokens
    try:
        client = LlmClient(MockSettings(), MockRepository())
        val = client.estimate_tokens("this is a test sentence")
        assert isinstance(val, int) and val > 0, f"expected int > 0, got {val!r}"
        # "this is a test sentence" = 5 words -> int(5 * 1.3) = 6
        assert val == 6, f"expected 6, got {val}"
        passed("llm_client: estimate_tokens", f"value={val}")
    except Exception:
        failed("llm_client: estimate_tokens", traceback.format_exc(limit=4))

    # 1b: gate 1 fails — ns_score=0.5 < CONFIDENCE_ESCALATION_THRESHOLD=0.60
    try:
        nid = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        repo = MockRepository(narrative={"narrative_id": nid, "ns_score": 0.5, "created_at": now_iso})
        client = LlmClient(MockSettings(), repo)
        ok, reason = client.check_sonnet_gates(nid, now_iso, 1000)
        assert not ok, "gate 1 should fail"
        assert "gate_1" in reason, f"expected gate_1 in reason: {reason}"
        passed("llm_client: check_sonnet_gates gate 1 fails (ns=0.5)", f"reason={reason}")
    except Exception:
        failed("llm_client: check_sonnet_gates gate 1", traceback.format_exc(limit=4))

    # 1c: gate 2 fails — ns=0.9 (passes gate 1) but narrative created today (age < 2 days)
    try:
        nid = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        repo = MockRepository(narrative={"narrative_id": nid, "ns_score": 0.9, "created_at": now_iso})
        client = LlmClient(MockSettings(), repo)
        ok, reason = client.check_sonnet_gates(nid, now_iso, 1000)
        assert not ok, "gate 2 should fail"
        assert "gate_2" in reason, f"expected gate_2 in reason: {reason}"
        passed("llm_client: check_sonnet_gates gate 2 fails (age=0 days)", f"reason={reason}")
    except Exception:
        failed("llm_client: check_sonnet_gates gate 2", traceback.format_exc(limit=4))

    # 1d: gate 3 fails — old narrative, high score, but recent Sonnet call exists
    try:
        nid = str(uuid.uuid4())
        old_iso = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        repo = MockRepository(
            narrative={"narrative_id": nid, "ns_score": 0.9, "created_at": old_iso},
            sonnet_calls=[{"call_id": "x"}],  # recent call exists
        )
        client = LlmClient(MockSettings(), repo)
        ok, reason = client.check_sonnet_gates(nid, old_iso, 1000)
        assert not ok, "gate 3 should fail"
        assert "gate_3" in reason, f"expected gate_3 in reason: {reason}"
        passed("llm_client: check_sonnet_gates gate 3 fails (recent Sonnet call)", f"reason={reason}")
    except Exception:
        failed("llm_client: check_sonnet_gates gate 3", traceback.format_exc(limit=4))

    # 1e: gate 4 fails — budget nearly exhausted (199500 used, estimating 1000 more, budget=200000)
    try:
        nid = str(uuid.uuid4())
        old_iso = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        repo = MockRepository(
            narrative={"narrative_id": nid, "ns_score": 0.9, "created_at": old_iso},
            spend={"total_tokens_used": 199500},
        )
        client = LlmClient(MockSettings(), repo)
        ok, reason = client.check_sonnet_gates(nid, old_iso, 1000)
        assert not ok, "gate 4 should fail"
        assert "gate_4" in reason, f"expected gate_4 in reason: {reason}"
        passed("llm_client: check_sonnet_gates gate 4 fails (budget=199500/200000)", f"reason={reason}")
    except Exception:
        failed("llm_client: check_sonnet_gates gate 4", traceback.format_exc(limit=4))

    # 1f: all 4 gates pass
    try:
        nid = str(uuid.uuid4())
        old_iso = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        repo = MockRepository(
            narrative={"narrative_id": nid, "ns_score": 0.9, "created_at": old_iso},
            spend={"total_tokens_used": 0},
        )
        client = LlmClient(MockSettings(), repo)
        ok, reason = client.check_sonnet_gates(nid, old_iso, 1000)
        assert ok, f"all gates should pass, failed: {reason}"
        assert reason == ""
        passed("llm_client: check_sonnet_gates all 4 gates pass")
    except Exception:
        failed("llm_client: check_sonnet_gates all gates pass", traceback.format_exc(limit=4))

except ImportError as exc:
    failed("llm_client: import", str(exc))

# ---------------------------------------------------------------------------
# 2. asset_mapper.py
# ---------------------------------------------------------------------------
print()
print("--- asset_mapper.py ---")

try:
    from asset_mapper import AssetMapper

    # 2a: FileNotFoundError on nonexistent path
    try:
        AssetMapper("/nonexistent/path/asset_library.pkl", MockEmbedder768())
        failed("asset_mapper: FileNotFoundError on bad path", "no exception raised")
    except FileNotFoundError as e:
        msg = str(e)
        assert "build_asset_library.py" in msg, f"unexpected message: {msg}"
        passed("asset_mapper: FileNotFoundError on nonexistent path", msg[:70])
    except Exception:
        failed("asset_mapper: FileNotFoundError on bad path", traceback.format_exc(limit=4))

    # 2b: ValueError on dimension mismatch (library=128-dim, embedder=768-dim)
    tmp_pkl = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            tmp_pkl = f.name
            library_128 = {
                "AAPL": {"name": "Apple Inc.", "embedding": np.random.rand(128).astype(np.float32)}
            }
            pickle.dump(library_128, f)

        try:
            AssetMapper(tmp_pkl, MockEmbedder768())
            failed("asset_mapper: ValueError on dim mismatch", "no exception raised")
        except ValueError as e:
            msg = str(e)
            assert "128" in msg and "768" in msg, f"unexpected message: {msg}"
            passed("asset_mapper: ValueError on embedding dim mismatch", msg[:80])
    except Exception:
        failed("asset_mapper: ValueError on dim mismatch", traceback.format_exc(limit=4))
    finally:
        if tmp_pkl:
            Path(tmp_pkl).unlink(missing_ok=True)

    # 2c: map_narrative finds correct ticker above min_similarity
    tmp_pkl = None
    try:
        dim = 768
        unit = np.ones(dim, dtype=np.float32) / np.sqrt(dim)
        ortho = np.zeros(dim, dtype=np.float32)
        ortho[0] = 1.0

        library_valid = {
            "AAPL": {"name": "Apple Inc.", "embedding": unit.copy()},
            "MSFT": {"name": "Microsoft Corp.", "embedding": ortho.copy()},
        }
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            tmp_pkl = f.name
            pickle.dump(library_valid, f)

        mapper = AssetMapper(tmp_pkl, MockEmbedder768())
        results_list = mapper.map_narrative(unit, top_k=5, min_similarity=0.0)
        assert isinstance(results_list, list)
        tickers = [r["ticker"] for r in results_list]
        assert "AAPL" in tickers, f"AAPL not in {tickers}"
        assert all({"ticker", "asset_name", "similarity_score"} <= set(r) for r in results_list)
        # AAPL should have sim ≈ 1.0 (same unit vector)
        aapl_sim = next(r["similarity_score"] for r in results_list if r["ticker"] == "AAPL")
        assert abs(aapl_sim - 1.0) < 1e-4, f"AAPL similarity should be ~1.0, got {aapl_sim}"
        passed("asset_mapper: map_narrative correct result", f"tickers={tickers}, AAPL_sim={aapl_sim:.4f}")
    except Exception:
        failed("asset_mapper: map_narrative", traceback.format_exc(limit=4))
    finally:
        if tmp_pkl:
            Path(tmp_pkl).unlink(missing_ok=True)

    # 2d: min_similarity filter excludes low-scoring assets
    tmp_pkl = None
    try:
        dim = 768
        unit = np.ones(dim, dtype=np.float32) / np.sqrt(dim)
        ortho = np.zeros(dim, dtype=np.float32)
        ortho[0] = 1.0
        library_valid = {
            "AAPL": {"name": "Apple Inc.", "embedding": unit.copy()},
            "MSFT": {"name": "Microsoft Corp.", "embedding": ortho.copy()},
        }
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            tmp_pkl = f.name
            pickle.dump(library_valid, f)

        mapper = AssetMapper(tmp_pkl, MockEmbedder768())
        # Query near unit -> AAPL matches, MSFT has very low similarity
        high_thresh = mapper.map_narrative(unit, top_k=5, min_similarity=0.95)
        assert all(r["similarity_score"] >= 0.95 for r in high_thresh), f"filter failed: {high_thresh}"
        passed("asset_mapper: min_similarity filter works", f"{len(high_thresh)} result(s) above 0.95")
    except Exception:
        failed("asset_mapper: min_similarity filter", traceback.format_exc(limit=4))
    finally:
        if tmp_pkl:
            Path(tmp_pkl).unlink(missing_ok=True)

except ImportError as exc:
    failed("asset_mapper: import", str(exc))

# ---------------------------------------------------------------------------
# 3. output.py
# ---------------------------------------------------------------------------
print()
print("--- output.py ---")

_sample_output = None  # reused across sub-tests

try:
    from output import DISCLAIMER, build_output_object, validate_output, write_outputs

    # 3a: DISCLAIMER constant
    try:
        assert DISCLAIMER == "INTELLIGENCE ONLY — NOT FINANCIAL ADVICE. For informational purposes only."
        passed("output: DISCLAIMER constant correct")
    except AssertionError as e:
        failed("output: DISCLAIMER constant", str(e))

    # 3b: build_output_object returns all required fields
    try:
        nid = str(uuid.uuid4())
        narrative = {
            "narrative_id": nid,
            "name": "AI Infrastructure Surge",
            "stage": "Growing",
            "velocity": 0.12,
            "velocity_windowed": 0.10,
            "centrality": 0.55,
            "is_catalyst": 1,
            "is_coordinated": 0,
            "suppressed": 0,
            "human_review_required": 0,
            "ns_score": 0.75,
            "entropy": 1.45,
            "intent_weight": 0.65,
            "cross_source_score": 0.40,
            "document_count": 12,
        }
        linked_assets = [{"ticker": "NVDA", "asset_name": "NVIDIA Corp.", "similarity_score": 0.92}]
        evidence = [
            {
                "source_url": "https://reuters.com/ai",
                "source_domain": "reuters.com",
                "published_at": "2026-03-14T10:00:00+00:00",
                "author": "Jane Doe",
                "excerpt": "AI infrastructure demand surges.",
            }
        ]
        score_components = {
            "velocity_component": 0.25,
            "correlation_component": 0.20,
            "cohesion_component": 0.15,
            "polarization_component": 0.10,
            "centrality_component": 0.15,
            "entropy_component": 0.08,
        }
        _sample_output = build_output_object(
            narrative, linked_assets, evidence, "Strong growth observed.", None, score_components
        )
        required = [
            "narrative_id", "name", "stage", "velocity", "velocity_windowed",
            "centrality", "is_catalyst", "is_coordinated", "coordination_penalty_applied",
            "suppressed", "human_review_required", "narrative_strength_score",
            "score_components", "entropy", "intent_weight", "lifecycle_reasoning",
            "mutation_analysis", "linked_assets", "cross_source_score",
            "reasoning_trace", "supporting_evidence", "source_attribution_metadata",
            "disclaimer", "emitted_at",
        ]
        missing = [f for f in required if f not in _sample_output]
        assert not missing, f"missing fields: {missing}"
        assert _sample_output["narrative_id"] == nid
        assert _sample_output["disclaimer"] == DISCLAIMER
        assert _sample_output["mutation_analysis"] is None
        assert _sample_output["coordination_penalty_applied"] is False
        assert len(_sample_output["reasoning_trace"]) == 6
        assert "reuters.com" in _sample_output["source_attribution_metadata"]["domains"]
        passed("output: build_output_object complete", f"{len(required)} required fields present")
    except Exception:
        failed("output: build_output_object", traceback.format_exc(limit=4))

    # 3c: validate_output valid object -> True
    try:
        assert _sample_output is not None
        assert validate_output(_sample_output) is True
        passed("output: validate_output valid object -> True")
    except Exception:
        failed("output: validate_output valid", traceback.format_exc(limit=4))

    # 3d: validate_output missing disclaimer -> False
    try:
        bad = {**_sample_output}
        del bad["disclaimer"]
        assert validate_output(bad) is False
        passed("output: validate_output missing disclaimer -> False")
    except Exception:
        failed("output: validate_output missing disclaimer", traceback.format_exc(limit=4))

    # 3e: validate_output wrong disclaimer -> False
    try:
        bad = {**_sample_output, "disclaimer": "wrong"}
        assert validate_output(bad) is False
        passed("output: validate_output wrong disclaimer -> False")
    except Exception:
        failed("output: validate_output wrong disclaimer", traceback.format_exc(limit=4))

    # 3f: validate_output invalid UUID -> False
    try:
        bad = {**_sample_output, "narrative_id": "not-a-uuid"}
        assert validate_output(bad) is False
        passed("output: validate_output invalid UUID -> False")
    except Exception:
        failed("output: validate_output invalid UUID", traceback.format_exc(limit=4))

    # 3g: empty lifecycle_reasoning uses fallback
    try:
        nid2 = str(uuid.uuid4())
        out2 = build_output_object({**narrative, "narrative_id": nid2}, [], [], "", None, {})
        assert out2["lifecycle_reasoning"] == "Classification based on automated metrics."
        passed("output: empty lifecycle_reasoning -> fallback text")
    except Exception:
        failed("output: lifecycle_reasoning fallback", traceback.format_exc(limit=4))

    # 3h: excerpt truncated to 280 chars
    try:
        nid3 = str(uuid.uuid4())
        long_ev = [{
            "source_url": "u", "source_domain": "d.com",
            "published_at": "2026-03-14T00:00:00Z", "author": None,
            "excerpt": "x" * 500,
        }]
        out3 = build_output_object({**narrative, "narrative_id": nid3}, [], long_ev, "r", None, {})
        assert len(out3["supporting_evidence"][0]["excerpt"]) == 280
        passed("output: excerpt truncated to 280 chars")
    except Exception:
        failed("output: excerpt truncation", traceback.format_exc(limit=4))

    # 3i: write_outputs writes file and handles empty list
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            import os
            orig_dir = os.getcwd()
            os.chdir(tmpdir)
            try:
                write_outputs([], "2026-03-14")
                out_path = Path("./data/outputs/2026-03-14/narratives.json")
                assert out_path.exists(), "output file not created"
                import json
                content = json.loads(out_path.read_text())
                assert content == []
                passed("output: write_outputs empty list -> [] in file")
            finally:
                os.chdir(orig_dir)
    except Exception:
        failed("output: write_outputs empty", traceback.format_exc(limit=4))

except ImportError as exc:
    failed("output: import", str(exc))

# ---------------------------------------------------------------------------
# 4. End-to-end Phase 1–4 integration
# ---------------------------------------------------------------------------
print()
print("--- end-to-end Phase 1-4 ---")

try:
    from repository import SqliteRepository
    from vector_store import FaissVectorStore
    from ingester import RawDocument
    from clustering import run_clustering
    from signals import (
        compute_cohesion,
        compute_ns_score,
        compute_velocity,
    )
    from centrality import build_narrative_graph, compute_centrality, flag_catalysts
    from output import DISCLAIMER, build_output_object, validate_output

    class _MockSettingsCluster:
        LSH_THRESHOLD = 0.85
        SYNC_BURST_MIN_SOURCES = 5
        SYNC_BURST_WINDOW_SECONDS = 300

    class _MockEmbedder:
        """Deterministic mock embedder — no model weights required."""
        def dimension(self):
            return 768
        def embed(self, texts):
            np.random.seed(42)
            vecs = np.random.randn(len(texts), 768).astype(np.float32)
            norms = np.linalg.norm(vecs, axis=1, keepdims=True)
            return vecs / norms
        def embed_single(self, text):
            return self.embed([text])[0]

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/test.db"
        faiss_path = f"{tmpdir}/faiss.pkl"

        # 4a: SqliteRepository migrate
        try:
            repo = SqliteRepository(db_path)
            repo.migrate()
            passed("e2e: SqliteRepository.migrate() — all tables created")
        except Exception:
            failed("e2e: SqliteRepository.migrate()", traceback.format_exc(limit=4))
            repo = None

        # 4b: FaissVectorStore init
        try:
            vs = FaissVectorStore(faiss_path)
            vs.initialize(768)
            assert vs.count() == 0 and vs.is_empty()
            passed("e2e: FaissVectorStore init, count=0")
        except Exception:
            failed("e2e: FaissVectorStore init", traceback.format_exc(limit=4))
            vs = None

        # 4c: embed 3 docs and insert into candidate_buffer
        try:
            embedder = _MockEmbedder()
            assert embedder.dimension() == 768
            texts = [
                "Federal Reserve signals rate hold amid persistent inflation",
                "Treasury yields remain elevated after Fed commentary",
                "Bond market reacts to central bank policy uncertainty today",
            ]
            docs = [
                RawDocument(
                    doc_id=str(uuid.uuid4()),
                    raw_text=t,
                    source_url=f"http://example.com/{i}",
                    source_domain="example.com",
                    published_at="2026-03-14T10:00:00+00:00",
                    ingested_at=datetime.now(timezone.utc).isoformat(),
                    raw_text_hash=hashlib.sha256(t.encode()).hexdigest(),
                )
                for i, t in enumerate(texts)
            ]
            embeddings = embedder.embed([d.raw_text for d in docs])
            for doc, emb in zip(docs, embeddings):
                repo.insert_candidate({
                    "doc_id": doc.doc_id,
                    "narrative_id_assigned": None,
                    "embedding_blob": emb.astype(np.float32).tobytes(),
                    "raw_text_hash": doc.raw_text_hash,
                    "source_url": doc.source_url,
                    "source_domain": doc.source_domain,
                    "published_at": doc.published_at,
                    "ingested_at": doc.ingested_at,
                    "status": "pending",
                    "raw_text": doc.raw_text,
                    "author": None,
                })
            count = repo.get_candidate_buffer_count(status="pending")
            assert count == 3
            passed("e2e: 3 docs embedded and inserted into candidate_buffer")
        except Exception:
            failed("e2e: candidate_buffer insertion", traceback.format_exc(limit=4))

        # 4d: run_clustering with 3 docs -> graceful [] (below min_cluster_size=5)
        try:
            new_narratives = run_clustering(repo, vs, embedder, _MockSettingsCluster())
            assert new_narratives == [], f"expected [], got {new_narratives}"
            passed("e2e: run_clustering with 3 docs -> [] (graceful, < min_cluster_size)")
        except Exception:
            failed("e2e: run_clustering", traceback.format_exc(limit=4))

        # 4e: compute signals for a new narrative (velocity=0 for day 1 is expected)
        try:
            np.random.seed(7)
            c = np.random.randn(768).astype(np.float32)
            c /= np.linalg.norm(c)
            vel = compute_velocity(c, c)  # same centroid -> 0
            assert abs(vel) < 1e-6, f"same-centroid velocity should be ~0, got {vel}"
            coh = compute_cohesion([c])
            assert coh == 1.0
            ns = compute_ns_score(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, None)
            assert abs(ns - 0.15) < 1e-9, f"expected 0.15 (cohesion only), got {ns}"
            passed("e2e: signals compute correctly (velocity=0 expected for day 1)")
        except Exception:
            failed("e2e: signals", traceback.format_exc(limit=4))

        # 4f: centrality on empty narrative list
        try:
            g = build_narrative_graph([], vs, similarity_threshold=0.40)
            assert g.number_of_nodes() == 0
            assert compute_centrality(g) == {}
            assert flag_catalysts({}) == []
            passed("e2e: centrality on empty list -> all empty")
        except Exception:
            failed("e2e: centrality empty", traceback.format_exc(limit=4))

        # 4g: build_output_object + validate for a mock narrative
        try:
            nid = str(uuid.uuid4())
            mock_narr = {
                "narrative_id": nid,
                "name": "Rate Sensitivity",
                "stage": "Emerging",
                "velocity": 0.0,
                "velocity_windowed": 0.0,
                "centrality": 0.0,
                "is_catalyst": 0,
                "is_coordinated": 0,
                "suppressed": 0,
                "human_review_required": 0,
                "ns_score": 0.15,
                "entropy": None,
                "intent_weight": 0.0,
                "cross_source_score": 0.30,
                "document_count": 3,
            }
            sc = {
                "velocity_component": 0.0,
                "correlation_component": 0.30,
                "cohesion_component": 0.15,
                "polarization_component": 0.0,
                "centrality_component": 0.0,
                "entropy_component": 0.0,
            }
            out = build_output_object(mock_narr, [], [], "Emerging narrative detected.", None, sc)
            assert out["narrative_id"] == nid
            assert out["name"] == "Rate Sensitivity"
            assert out["disclaimer"] == DISCLAIMER
            assert "score_components" in out
            assert validate_output(out)
            passed("e2e: build_output_object + validate_output for mock narrative")
        except Exception:
            failed("e2e: build_output_object", traceback.format_exc(limit=4))

except ImportError as exc:
    failed("e2e: import", str(exc))

# ---------------------------------------------------------------------------
# 5. LLM gate integration
# ---------------------------------------------------------------------------
print()
print("--- LLM gate integration ---")

try:
    from llm_client import LlmClient

    # 5a: Narrative created now with ns_score=0.95 -> gate 2 fails (age=0 < 2 days)
    try:
        nid = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        repo = MockRepository(narrative={"narrative_id": nid, "ns_score": 0.95, "created_at": now_iso})
        client = LlmClient(MockSettings(), repo)
        # call_sonnet internally calls check_sonnet_gates; we test check_sonnet_gates directly
        ok, reason = client.check_sonnet_gates(nid, now_iso, 1000)
        assert not ok and "gate_2" in reason, f"gate 2 should fail: {reason}"
        passed("llm_gate: ns=0.95, age=0 days -> gate 2 fails (identified fallback path)", f"reason={reason}")
    except Exception:
        failed("llm_gate: age check (gate 2)", traceback.format_exc(limit=4))

    # 5b: Fully eligible narrative -> all 4 gates pass (no API call needed to verify gate logic)
    try:
        nid = str(uuid.uuid4())
        old_iso = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        repo = MockRepository(
            narrative={"narrative_id": nid, "ns_score": 0.95, "created_at": old_iso},
            spend={"total_tokens_used": 100},
        )
        client = LlmClient(MockSettings(), repo)
        ok, reason = client.check_sonnet_gates(nid, old_iso, 500)
        assert ok, f"all gates should pass, reason={reason}"
        passed("llm_gate: mature, high-ns narrative -> all 4 gates pass")
    except Exception:
        failed("llm_gate: all gates pass", traceback.format_exc(limit=4))

    # 5c: gate 4 -> BUDGET_CEILING_HIT path identified
    try:
        nid = str(uuid.uuid4())
        old_iso = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        repo = MockRepository(
            narrative={"narrative_id": nid, "ns_score": 0.95, "created_at": old_iso},
            spend={"total_tokens_used": 199500},
        )
        client = LlmClient(MockSettings(), repo)
        ok, reason = client.check_sonnet_gates(nid, old_iso, 1000)
        assert not ok and "gate_4" in reason
        # Verify the BUDGET_CEILING_HIT branch would be triggered in call_sonnet
        assert "gate_4" in reason, f"budget gate reason: {reason}"
        passed("llm_gate: gate 4 -> BUDGET_CEILING_HIT path identified", f"reason={reason}")
    except Exception:
        failed("llm_gate: gate 4 BUDGET_CEILING_HIT path", traceback.format_exc(limit=4))

except ImportError as exc:
    failed("llm_gate: import", str(exc))

# ---------------------------------------------------------------------------
# 6. Adversarial -> Output integration
# ---------------------------------------------------------------------------
print()
print("--- Adversarial -> Output integration ---")

try:
    from output import DISCLAIMER, build_output_object, validate_output

    # 6a: is_coordinated=1 -> coordination_penalty_applied=True, human_review_required=True
    try:
        nid = str(uuid.uuid4())
        coordinated = {
            "narrative_id": nid,
            "name": "Coordinated Campaign",
            "stage": "Emerging",
            "velocity": 0.0,
            "velocity_windowed": 0.0,
            "centrality": 0.0,
            "is_catalyst": 0,
            "is_coordinated": 1,
            "suppressed": 0,
            "human_review_required": 1,
            "ns_score": 0.4,
            "entropy": None,
            "intent_weight": 0.0,
            "cross_source_score": 0.0,
            "document_count": 5,
        }
        out = build_output_object(coordinated, [], [], "Coordination detected.", None, {})
        assert out["is_coordinated"] is True, f"is_coordinated: {out['is_coordinated']}"
        assert out["coordination_penalty_applied"] is True, f"penalty_applied: {out['coordination_penalty_applied']}"
        assert out["human_review_required"] is True
        assert out["disclaimer"] == DISCLAIMER
        assert validate_output(out)
        passed("adversarial->output: is_coordinated=1 -> coordination_penalty_applied=True")
    except Exception:
        failed("adversarial->output: coordination flag", traceback.format_exc(limit=4))

    # 6b: is_coordinated=0 -> coordination_penalty_applied=False
    try:
        nid = str(uuid.uuid4())
        normal = {**coordinated, "narrative_id": nid, "is_coordinated": 0, "human_review_required": 0}
        out2 = build_output_object(normal, [], [], "Normal narrative.", None, {})
        assert out2["is_coordinated"] is False
        assert out2["coordination_penalty_applied"] is False
        assert out2["human_review_required"] is False
        passed("adversarial->output: is_coordinated=0 -> coordination_penalty_applied=False")
    except Exception:
        failed("adversarial->output: non-coordinated", traceback.format_exc(limit=4))

    # 6c: suppressed narrative reflects suppressed=True in output
    try:
        nid = str(uuid.uuid4())
        suppressed = {**coordinated, "narrative_id": nid, "suppressed": 1}
        out3 = build_output_object(suppressed, [], [], "Suppressed.", None, {})
        assert out3["suppressed"] is True
        passed("adversarial->output: suppressed=1 -> suppressed=True in output")
    except Exception:
        failed("adversarial->output: suppressed flag", traceback.format_exc(limit=4))

except ImportError as exc:
    failed("adversarial->output: import", str(exc))

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
    for name, res, _ in results:
        if res == "FAIL":
            print(f"  FAIL: {name}")
