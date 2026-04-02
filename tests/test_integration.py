"""
Phase 1 & 2 Integration Tests
Run: python test_integration.py
"""

import os
import sys
import sqlite3
import tempfile
import traceback

import numpy as np

# ── helpers ─────────────────────────────────────────────────────────────────

def passed(name: str) -> None:
    print(f"  PASS  {name}")

def failed(name: str, reason: str) -> None:
    print(f"  FAIL  {name}")
    # strip non-ascii to avoid cp1252 terminal errors on Windows
    safe_reason = reason.encode("ascii", errors="replace").decode("ascii")
    print(f"        {safe_reason}")
    sys.exit(1)

def run(name: str, fn):
    try:
        fn()
        passed(name)
    except AssertionError as exc:
        failed(name, str(exc))
    except Exception:
        failed(name, traceback.format_exc().strip().splitlines()[-1])


# ── 1. Settings ──────────────────────────────────────────────────────────────

def test_settings():
    from settings import Settings
    s = Settings()
    assert s.ANTHROPIC_API_KEY, "ANTHROPIC_API_KEY is empty"
    assert 0 < s.CENTROID_ALPHA < 1, f"CENTROID_ALPHA out of range: {s.CENTROID_ALPHA}"
    assert 0 < s.ASSIGNMENT_SIMILARITY_FLOOR < 1
    assert 0 < s.CONFIDENCE_ESCALATION_THRESHOLD < 1
    assert 0 < s.LSH_THRESHOLD < 1
    assert s.NOISE_BUFFER_THRESHOLD > 0
    assert s.SONNET_DAILY_TOKEN_BUDGET > 0
    assert s.LSH_NUM_PERM >= 64
    assert s.EMBEDDING_MODE in ("dense", "hybrid")
    assert isinstance(s.TRUSTED_DOMAINS, list) and len(s.TRUSTED_DOMAINS) > 0
    assert s.DB_PATH
    assert s.FAISS_INDEX_PATH
    assert s.LSH_INDEX_PATH


# ── 2. Repository ────────────────────────────────────────────────────────────

EXPECTED_TABLES = {
    "narratives", "centroid_history", "candidate_buffer", "llm_audit_log",
    "sonnet_daily_spend", "adversarial_log", "robots_cache",
    "failed_ingestion_jobs", "pipeline_run_log", "narrative_assignments",
    "document_evidence",
}

def test_repository():
    from repository import SqliteRepository
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        repo = SqliteRepository(db_path)
        repo.migrate()
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        found = {r[0] for r in rows}
        missing = EXPECTED_TABLES - found
        assert not missing, f"Missing tables: {missing}"
        assert len(found) >= 11, f"Expected 11 tables, found {len(found)}"
    finally:
        os.unlink(db_path)


# ── 3. VectorStore ───────────────────────────────────────────────────────────

def test_vector_store():
    from vector_store import FaissVectorStore
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        idx_path = f.name

    try:
        store = FaissVectorStore(idx_path)
        store.initialize(768)
        assert store.is_empty(), "Should be empty after init"
        assert store.count() == 0

        rng = np.random.default_rng(42)
        vecs = rng.random((3, 768)).astype(np.float32)
        # L2-normalise
        vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
        store.add(vecs, ["a", "b", "c"])
        assert store.count() == 3
        assert not store.is_empty()

        # search
        query = vecs[0]
        dists, ids = store.search(query, k=2)
        assert len(ids) == 2, f"Expected 2 results, got {len(ids)}"
        assert ids[0] == "a", f"Top result should be 'a' (self), got '{ids[0]}'"

        # get_vector round-trip
        v = store.get_vector("b")
        assert v is not None
        assert v.shape == (768,)

        # update
        new_vec = rng.random(768).astype(np.float32)
        new_vec /= np.linalg.norm(new_vec)
        store.update("b", new_vec)
        assert store.count() == 3

        # delete
        store.delete("c")
        assert store.count() == 2
        assert store.get_all_ids() == ["a", "b"]

        # save / load
        store.save()
        store2 = FaissVectorStore(idx_path)
        ok = store2.load()
        assert ok, "load() returned False — file should exist"
        assert store2.count() == 2
        assert set(store2.get_all_ids()) == {"a", "b"}

        # empty-index search
        store3 = FaissVectorStore(idx_path)
        store3.initialize(768)
        dists_empty, ids_empty = store3.search(query, k=3)
        assert list(ids_empty) == [], f"Empty store should return [], got {ids_empty}"
    finally:
        os.unlink(idx_path)


# ── 4. EmbeddingModel ────────────────────────────────────────────────────────

def test_embedding_model():
    from settings import Settings
    from embedding_model import MiniLMEmbedder

    s = Settings()
    embedder = MiniLMEmbedder(s)

    out = embedder.embed(["test sentence"])
    assert out.shape == (1, 768), f"Expected (1, 768), got {out.shape}"
    assert out.dtype == np.float32, f"Expected float32, got {out.dtype}"

    norm = float(np.linalg.norm(out[0]))
    assert abs(norm - 1.0) < 1e-5, f"Vector not unit-normalised: norm={norm:.6f}"

    single = embedder.embed_single("test sentence")
    assert single.shape == (768,), f"embed_single shape wrong: {single.shape}"
    assert embedder.dimension() == 768


# ── 5. Deduplicator ──────────────────────────────────────────────────────────

def test_deduplicator():
    from deduplicator import Deduplicator
    from ingester import RawDocument
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()

    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        lsh_path = f.name
    os.unlink(lsh_path)   # deduplicator expects absent file for fresh init

    try:
        dup = Deduplicator(threshold=0.85, num_perm=128, lsh_path=lsh_path)
        loaded = dup.load()
        assert not loaded, "load() should return False for fresh index"

        doc_a = RawDocument(
            doc_id="doc-a",
            raw_text="The Federal Reserve raised interest rates by 25 basis points today.",
            source_url="https://reuters.com/a",
            source_domain="reuters.com",
            published_at=now,
            ingested_at=now,
        )
        doc_b = RawDocument(
            doc_id="doc-b",
            raw_text="Completely unrelated content about gardening and tomatoes in summer.",
            source_url="https://example.com/b",
            source_domain="example.com",
            published_at=now,
            ingested_at=now,
        )

        # before add — not a duplicate
        is_dup, sig = dup.is_duplicate(doc_a)
        assert not is_dup, "doc_a should not be dup before add"

        dup.add_with_signature(doc_a, sig)

        # same doc now flagged as duplicate
        is_dup, _ = dup.is_duplicate(doc_a)
        assert is_dup, "doc_a should be dup after add"

        # different doc — not a duplicate
        is_dup, _ = dup.is_duplicate(doc_b)
        assert not is_dup, "doc_b should not be dup"

        # batch signatures
        sigs = dup.get_batch_signatures()
        assert "doc-a" in sigs, "doc-a missing from batch signatures"

        # save / load round-trip
        dup.save()
        dup2 = Deduplicator(threshold=0.85, num_perm=128, lsh_path=lsh_path)
        ok = dup2.load()
        assert ok, "load() should return True after save"
        is_dup, _ = dup2.is_duplicate(doc_a)
        assert is_dup, "doc_a should still be dup after reload"

        # clear_batch
        dup.clear_batch()
        assert dup.get_batch_signatures() == {}
    finally:
        if os.path.exists(lsh_path):
            os.unlink(lsh_path)


# ── 6. Robots ────────────────────────────────────────────────────────────────

def test_robots():
    from repository import SqliteRepository
    from robots import can_fetch

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        repo = SqliteRepository(db_path)
        repo.migrate()

        result = can_fetch("https://www.reuters.com/business/", repo)
        assert isinstance(result, bool), f"can_fetch returned non-bool: {type(result)}"
        # Reuters may allow or disallow — just verify we get a bool without crashing
        print(f"        reuters.com -> {'ALLOW' if result else 'DISALLOW'} (robots.txt respected)")

        # Verify result is cached
        cached = repo.get_robots_cache("www.reuters.com")
        assert cached is not None, "robots_cache entry missing after can_fetch"
        assert "rules_text" in cached
        assert "fetched_at" in cached
    finally:
        os.unlink(db_path)


# ── 7. Ingester ──────────────────────────────────────────────────────────────

_LOCAL_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Finance Feed</title>
    <link>https://test.example.com</link>
    <description>Integration test feed</description>
    <item>
      <title>Fed raises rates 25bps amid inflation concerns</title>
      <link>https://test.example.com/article/fed-rates</link>
      <description>The Federal Reserve raised interest rates by 25 basis points on Wednesday.</description>
      <pubDate>Wed, 14 Mar 2026 12:00:00 +0000</pubDate>
      <author>Test Reporter</author>
    </item>
    <item>
      <title>S&amp;P 500 hits record high on strong earnings</title>
      <link>https://test.example.com/article/sp500</link>
      <description>Markets rallied sharply after strong quarterly results from major tech firms.</description>
      <pubDate>Wed, 14 Mar 2026 13:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"""


def test_ingester():
    from repository import SqliteRepository
    from ingester import RssIngester

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Write local RSS fallback to a temp file
    rss_tmp = tempfile.NamedTemporaryFile(
        suffix=".xml", mode="w", encoding="utf-8", delete=False
    )
    rss_tmp.write(_LOCAL_RSS)
    rss_tmp.close()
    rss_file_url = "file:///" + rss_tmp.name.replace("\\", "/")

    try:
        repo = SqliteRepository(db_path)
        repo.migrate()

        # First try the live feed; fall back to local file if network is down
        ingester = RssIngester(
            repository=repo,
            feed_urls=["https://feeds.reuters.com/reuters/businessNews"],
        )
        docs = ingester.ingest()

        source_label = "Reuters RSS (live)"
        if len(docs) == 0:
            print("        Live RSS unreachable — falling back to local RSS file")
            source_label = "local RSS file"
            ingester2 = RssIngester(repository=repo, feed_urls=[rss_file_url])
            docs = ingester2.ingest()

        assert isinstance(docs, list), "ingest() must return a list"
        assert len(docs) >= 1, f"Expected >=1 RawDocument, got {len(docs)}"

        for doc in docs[:3]:
            assert doc.doc_id, "doc_id is empty"
            assert doc.source_url, "source_url is empty"
            assert doc.source_domain, "source_domain is empty"
            assert doc.published_at, "published_at is empty"
            assert doc.ingested_at, "ingested_at is empty"
            assert doc.raw_text, "raw_text is empty"
            assert doc.raw_text_hash, "raw_text_hash is empty"
            assert len(doc.raw_text_hash) == 64, \
                f"raw_text_hash should be 64 chars (SHA256), got {len(doc.raw_text_hash)}"

        print(f"        returned {len(docs)} RawDocument(s) from {source_label}")
    finally:
        os.unlink(db_path)
        os.unlink(rss_tmp.name)


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nPhase 1 & 2 Integration Tests\n" + "=" * 40)

    print("\n[Phase 1]")
    run("1. Settings    — field parsing and validation", test_settings)
    run("2. Repository  — migrate() creates all 11 tables", test_repository)
    run("3. VectorStore — add / search / update / delete / save / load", test_vector_store)
    run("4. EmbeddingModel — shape (1,768), float32, L2-normalised", test_embedding_model)

    print("\n[Phase 2]")
    run("5. Deduplicator — is_duplicate True/False, batch sigs, save/load", test_deduplicator)
    run("6. Robots       — can_fetch reuters.com, result cached in DB", test_robots)
    run("7. Ingester     — RssIngester returns >=1 doc with all required fields", test_ingester)

    print("\n" + "=" * 40)
    print("All tests passed.\n")
