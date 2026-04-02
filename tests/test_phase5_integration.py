"""Phase 5 integration tests — 6 criteria."""
import pickle
import sys
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np

results = []


def check(n, desc, fn):
    try:
        fn()
        results.append((n, desc, "PASS", ""))
    except Exception as e:
        results.append((n, desc, "FAIL", str(e)))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(tmp, asset_lib):
    ms = MagicMock()
    ms.ASSET_LIBRARY_PATH = str(asset_lib)
    ms.DB_PATH = str(tmp / "test.db")
    ms.FAISS_INDEX_PATH = str(tmp / "faiss.pkl")
    ms.LSH_INDEX_PATH = str(tmp / "lsh.pkl")
    ms.LSH_THRESHOLD = 0.85
    ms.LSH_NUM_PERM = 128
    ms.CENTROID_ALPHA = 0.15
    ms.NOISE_BUFFER_THRESHOLD = 200
    ms.ASSIGNMENT_SIMILARITY_FLOOR = 0.45
    ms.CONFIDENCE_ESCALATION_THRESHOLD = 0.60
    ms.VELOCITY_WINDOW_DAYS = 7
    ms.ENTROPY_VOCAB_WINDOW = 10
    ms.SONNET_DAILY_TOKEN_BUDGET = 200000
    ms.TRUSTED_DOMAINS = []
    ms.EMBEDDING_MODE = "dense"
    return ms


def _make_asset_lib(tmp):
    asset_lib = tmp / "asset_lib.pkl"
    emb = np.random.randn(768).astype(np.float32)
    emb /= np.linalg.norm(emb)
    with open(asset_lib, "wb") as f:
        pickle.dump({"AAPL": {"name": "Apple Inc.", "embedding": emb}}, f)
    return asset_lib


def _minimal_repo():
    repo = MagicMock()
    repo.migrate.return_value = None
    repo.get_narrative_count.return_value = 0
    repo.get_sonnet_daily_spend.return_value = None
    repo.get_retryable_failed_jobs.return_value = []
    repo.get_narratives_needing_decay.return_value = []
    repo.get_candidate_buffer_count.return_value = 0
    repo.get_all_active_narratives.return_value = []
    repo.get_candidate_buffer.return_value = []
    repo.delete_old_candidate_buffer.return_value = 0
    return repo


def _minimal_vs():
    vs = MagicMock()
    vs.load.return_value = False
    vs.count.return_value = 0
    vs.is_empty.return_value = True
    vs.get_all_ids.return_value = []
    return vs


def _minimal_dedup():
    dedup = MagicMock()
    dedup.load.return_value = False
    dedup.is_duplicate.return_value = (False, MagicMock())
    dedup.get_batch_signatures.return_value = {}
    return dedup


def _minimal_embedder():
    embedder = MagicMock()
    embedder.dimension.return_value = 768
    return embedder


def _run_pipeline_patched(tmp, asset_lib, repo, vs, dedup, embedder, ingester_docs=None):
    ms = _make_settings(tmp, asset_lib)
    if ingester_docs is None:
        ingester_docs = []

    with patch("pipeline.settings", ms), \
         patch("pipeline.SqliteRepository", return_value=repo), \
         patch("pipeline.MiniLMEmbedder", return_value=embedder), \
         patch("pipeline.FaissVectorStore", return_value=vs), \
         patch("pipeline.Deduplicator", return_value=dedup), \
         patch("pipeline.LlmClient"), \
         patch("pipeline.RssIngester") as MockIngester, \
         patch("pipeline.check_coordination", return_value=[]), \
         patch("pipeline.write_outputs"), \
         patch("pipeline.run_clustering", return_value=[]):

        ingester_inst = MagicMock()
        ingester_inst.ingest.return_value = ingester_docs
        MockIngester.return_value = ingester_inst

        import pipeline
        pipeline.run()


# ---------------------------------------------------------------------------
# Test 1: FATAL halt on missing asset library
# ---------------------------------------------------------------------------
def test_fatal_missing_asset_lib():
    tmp = Path(tempfile.mkdtemp())
    ms = _make_settings(tmp, tmp / "nonexistent_asset_lib.pkl")
    repo = _minimal_repo()
    fatal_calls = []

    with patch("pipeline.settings", ms), \
         patch("pipeline.SqliteRepository", return_value=repo), \
         patch("pipeline.MiniLMEmbedder"), \
         patch("pipeline.FaissVectorStore"), \
         patch("pipeline.Deduplicator"), \
         patch("pipeline.AssetMapper"), \
         patch("pipeline.LlmClient"), \
         patch("pipeline.logger") as mock_logger:

        mock_logger.critical.side_effect = lambda *a, **kw: fatal_calls.append(str(a))

        import pipeline
        pipeline.run()

    assert fatal_calls, "Expected logger.critical to be called for missing asset library"
    assert any("Asset library not found" in c for c in fatal_calls), \
        f"Expected 'Asset library not found' in fatal calls: {fatal_calls}"
    assert not repo.get_sonnet_daily_spend.called, \
        "Pipeline should have halted before Step 1"


check(1, "FATAL halt on missing asset library", test_fatal_missing_asset_lib)


# ---------------------------------------------------------------------------
# Test 2: Empty state first run
# ---------------------------------------------------------------------------
def test_empty_state_first_run():
    tmp = Path(tempfile.mkdtemp())
    asset_lib = _make_asset_lib(tmp)
    repo = _minimal_repo()
    vs = _minimal_vs()
    dedup = _minimal_dedup()
    embedder = _minimal_embedder()

    # Must complete without raising
    _run_pipeline_patched(tmp, asset_lib, repo, vs, dedup, embedder, ingester_docs=[])

    # Step 0 must have been logged — c.args[0] is the dict passed to log_pipeline_run
    logged_steps = [c.args[0].get("step_number") for c in repo.log_pipeline_run.call_args_list]
    assert 0 in logged_steps, f"Step 0 not logged; logged: {logged_steps}"


check(2, "Empty state first run completes without error", test_empty_state_first_run)


# ---------------------------------------------------------------------------
# Test 3: Zero docs surviving deduplication
# ---------------------------------------------------------------------------
def test_zero_survivors():
    from ingester import RawDocument

    tmp = Path(tempfile.mkdtemp())
    asset_lib = _make_asset_lib(tmp)
    repo = _minimal_repo()
    vs = _minimal_vs()
    dedup = _minimal_dedup()
    dedup.is_duplicate.return_value = (True, MagicMock())  # all duplicates
    embedder = _minimal_embedder()

    doc = RawDocument(
        doc_id=str(uuid.uuid4()),
        raw_text="test document",
        source_url="http://example.com",
        source_domain="example.com",
        published_at="2026-03-14T00:00:00+00:00",
        ingested_at="2026-03-14T00:00:00+00:00",
    )

    _run_pipeline_patched(tmp, asset_lib, repo, vs, dedup, embedder, ingester_docs=[doc])

    # embed() must NOT have been called
    assert not embedder.embed.called, \
        "embed() should not be called when zero documents survive deduplication"


check(3, "Zero docs surviving dedup: embed() not called, pipeline continues", test_zero_survivors)


# ---------------------------------------------------------------------------
# Test 4: Candidate buffer below threshold — run_clustering not called
# ---------------------------------------------------------------------------
def test_buffer_below_threshold():
    tmp = Path(tempfile.mkdtemp())
    asset_lib = _make_asset_lib(tmp)
    repo = _minimal_repo()
    repo.get_candidate_buffer_count.return_value = 50  # below 200 threshold
    vs = _minimal_vs()
    dedup = _minimal_dedup()
    embedder = _minimal_embedder()

    cluster_called = []

    with patch("pipeline.settings", _make_settings(tmp, asset_lib)), \
         patch("pipeline.SqliteRepository", return_value=repo), \
         patch("pipeline.MiniLMEmbedder", return_value=embedder), \
         patch("pipeline.FaissVectorStore", return_value=vs), \
         patch("pipeline.Deduplicator", return_value=dedup), \
         patch("pipeline.LlmClient"), \
         patch("pipeline.RssIngester") as MockIngester, \
         patch("pipeline.check_coordination", return_value=[]), \
         patch("pipeline.write_outputs"), \
         patch("pipeline.run_clustering") as MockCluster:

        ingester_inst = MagicMock()
        ingester_inst.ingest.return_value = []
        MockIngester.return_value = ingester_inst

        MockCluster.side_effect = lambda *a, **kw: cluster_called.append(True) or []

        import pipeline
        pipeline.run()

    assert not cluster_called, "run_clustering should NOT be called when buffer < threshold"


check(4, "Candidate buffer below threshold: run_clustering not called", test_buffer_below_threshold)


# ---------------------------------------------------------------------------
# Test 5: All key steps log to pipeline_run_log with step_number and duration_ms
# ---------------------------------------------------------------------------
def test_all_steps_log_to_pipeline_run_log():
    tmp = Path(tempfile.mkdtemp())
    asset_lib = _make_asset_lib(tmp)
    repo = _minimal_repo()
    vs = _minimal_vs()
    dedup = _minimal_dedup()
    embedder = _minimal_embedder()

    logged_records = []
    repo.log_pipeline_run.side_effect = lambda rec: logged_records.append(rec)

    _run_pipeline_patched(tmp, asset_lib, repo, vs, dedup, embedder, ingester_docs=[])

    logged_steps = {r.get("step_number") for r in logged_records}
    # Steps expected to log in a minimal (no-doc) run
    expected = {0, 1, 2, 3, 5, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
    missing = expected - logged_steps
    assert not missing, f"Steps not logged to pipeline_run_log: {sorted(missing)}"

    # Every logged record must have step_number and duration_ms
    for rec in logged_records:
        assert "step_number" in rec, f"Missing step_number in: {rec}"
        assert "duration_ms" in rec, f"Missing duration_ms in: {rec}"
        assert "status" in rec, f"Missing status in: {rec}"


check(5, "All steps log to pipeline_run_log with step_number/duration_ms/status",
      test_all_steps_log_to_pipeline_run_log)


# ---------------------------------------------------------------------------
# Test 6: build_asset_library creates valid pickle with correct embedding dims
# ---------------------------------------------------------------------------
def test_build_asset_library_valid_pickle():
    import build_asset_library as bal

    tmp = Path(tempfile.mkdtemp())
    asset_lib = tmp / "asset_lib.pkl"

    emb768 = np.random.randn(768).astype(np.float32)
    emb768 /= np.linalg.norm(emb768)

    fake_text = "ITEM 1. BUSINESS " + " ".join(["financials revenue earnings"] * 800)

    embedder_mock = MagicMock()
    embedder_mock.dimension.return_value = 768
    embedder_mock.embed_single.return_value = emb768

    settings_mock = MagicMock()
    settings_mock.ASSET_LIBRARY_PATH = str(asset_lib)
    settings_mock.EMBEDDING_MODE = "dense"
    settings_mock.EMBEDDING_MODEL_NAME = "all-mpnet-base-v2"
    settings_mock.FAISS_INDEX_PATH = str(tmp / "faiss.pkl")

    mock_dl = MagicMock()
    mock_dl.get.return_value = None
    mock_dl_class = MagicMock(return_value=mock_dl)

    # sec_edgar_downloader is imported inside build(), so inject via sys.modules
    fake_sed_module = MagicMock()
    fake_sed_module.Downloader = mock_dl_class

    orig_tickers = bal.TICKERS.copy()
    try:
        bal.TICKERS = {"AAPL": "Apple Inc.", "MSFT": "Microsoft Corporation"}
        with patch.object(bal, "settings", settings_mock), \
             patch("build_asset_library.MiniLMEmbedder", return_value=embedder_mock), \
             patch("build_asset_library._find_filing_text", return_value=fake_text), \
             patch.dict(sys.modules, {"sec_edgar_downloader": fake_sed_module}):
            bal.build(download_dir=str(tmp / "filings"))
    finally:
        bal.TICKERS = orig_tickers

    assert asset_lib.exists(), "Asset library pickle was not created"

    from safe_pickle import safe_load
    lib = safe_load(str(asset_lib), allowed={
        "builtins": {"dict", "list", "tuple", "str", "int", "float", "bool"},
        "numpy": {"ndarray", "dtype", "float64", "float32", "int64"},
        "numpy.core.multiarray": {"scalar", "_reconstruct"},
        "numpy._core.multiarray": {"scalar", "_reconstruct"},
        "numpy._core.numeric": {"_frombuffer"},
        "numpy.core.numeric": {"_frombuffer"},
    })

    assert "AAPL" in lib, "AAPL missing from library"
    assert "MSFT" in lib, "MSFT missing from library"

    for ticker, data in lib.items():
        assert "name" in data, f"'name' missing for {ticker}"
        assert "embedding" in data, f"'embedding' missing for {ticker}"
        assert data["embedding"].shape == (768,), \
            f"Wrong dim for {ticker}: {data['embedding'].shape}"
        norm = float(np.linalg.norm(data["embedding"]))
        assert abs(norm - 1.0) < 0.01, f"Not L2-normalized for {ticker}: norm={norm:.4f}"


check(6, "build_asset_library creates valid pickle with correct embedding dims",
      test_build_asset_library_valid_pickle)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
print()
for n, desc, result, err in results:
    status = "PASS" if result == "PASS" else "FAIL"
    print(f"  [{status}] {n}. {desc}")
    if err:
        print(f"        Error: {err}")

passed = sum(1 for _, _, r, _ in results if r == "PASS")
print(f"\n{passed}/{len(results)} PASS")
sys.exit(0 if passed == len(results) else 1)
