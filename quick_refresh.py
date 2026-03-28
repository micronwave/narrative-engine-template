"""
Quick Refresh — lightweight ingestion mode.
Assigns new documents to EXISTING narratives without full HDBSCAN clustering.
Runtime target: < 60 seconds.

Key differences from full pipeline:
- No HDBSCAN, no Sonnet calls, no lifecycle reclassification
- Similarity threshold: ASSIGNMENT_THRESHOLD (0.6) vs full pipeline floor (0.45)
- Updates only doc_count on affected narratives
"""

import logging
import time
from datetime import datetime, timezone

import numpy as np

from deduplicator import Deduplicator
from embedding_model import MiniLMEmbedder
from ingester import RssIngester
from repository import SqliteRepository
from settings import Settings
from vector_store import FaissVectorStore

logger = logging.getLogger(__name__)

ASSIGNMENT_THRESHOLD = 0.6  # Higher than full pipeline floor (0.45) — require strong match


class QuickRefresh:
    """
    Light ingestion mode:
    1. Pulls new documents from all RSS sources
    2. Embeds and deduplicates
    3. Assigns to EXISTING narratives only (no new clustering)
    4. Updates doc_count for affected narratives
    5. Skips: full HDBSCAN, Sonnet calls, lifecycle changes
    """

    def __init__(
        self,
        settings: Settings,
        repository: SqliteRepository,
        vector_store: FaissVectorStore,
        embedder: MiniLMEmbedder,
        deduplicator: Deduplicator,
    ):
        self.settings = settings
        self.repository = repository
        self.vector_store = vector_store
        self.embedder = embedder
        self.deduplicator = deduplicator

    def run(self) -> dict:
        start = time.time()

        # Step 1: Ingest from all RSS sources
        ingester = RssIngester(self.repository)
        new_docs = ingester.ingest()

        # Step 2: Deduplicate
        unique_docs = []
        for doc in new_docs:
            is_dup, sig = self.deduplicator.is_duplicate(doc)
            if not is_dup:
                unique_docs.append(doc)
                self.deduplicator.add_with_signature(doc, sig)
        logger.info(
            "quick_refresh dedup: %d/%d documents survived",
            len(unique_docs), len(new_docs),
        )

        if not unique_docs:
            return {
                "docs_ingested": len(new_docs),
                "docs_unique": 0,
                "docs_assigned": 0,
                "docs_buffered": 0,
                "narratives_updated": 0,
                "runtime_seconds": round(time.time() - start, 2),
            }

        # Step 3: Embed
        texts = [doc.raw_text for doc in unique_docs]
        embeddings = self.embedder.embed(texts)

        # Step 4: Get all active narrative centroids
        active_narratives = self.repository.get_all_active_narratives()
        if not active_narratives:
            # No narratives exist yet — buffer all docs for future clustering
            for doc, emb in zip(unique_docs, embeddings):
                self._buffer_doc(doc, emb)
            return {
                "docs_ingested": len(new_docs),
                "docs_unique": len(unique_docs),
                "docs_assigned": 0,
                "docs_buffered": len(unique_docs),
                "narratives_updated": 0,
                "runtime_seconds": round(time.time() - start, 2),
            }

        # Step 5: Assign each doc to nearest narrative or buffer
        # narratives table PK is narrative_id (not id)
        assignments = []
        buffered = []
        narrative_centroids = {
            n["narrative_id"]: self.vector_store.get_vector(n["narrative_id"])
            for n in active_narratives
        }

        today = datetime.now(timezone.utc).date().isoformat()

        for doc, emb in zip(unique_docs, embeddings):
            best_match = None
            best_similarity = 0.0

            for nid, centroid in narrative_centroids.items():
                if centroid is None:
                    continue
                similarity = self._cosine_similarity(emb, centroid)
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = nid

            if best_match and best_similarity >= ASSIGNMENT_THRESHOLD:
                self.repository.assign_doc_to_narrative(doc.doc_id, best_match)
                # Store document evidence consistent with full pipeline
                self.repository.insert_document_evidence({
                    "narrative_id": best_match,
                    "doc_id": doc.doc_id,
                    "source_url": doc.source_url,
                    "source_domain": doc.source_domain,
                    "published_at": doc.published_at,
                    "author": doc.author,
                    "excerpt": doc.raw_text[:500],
                })
                assignments.append({
                    "doc_id": doc.doc_id,
                    "narrative_id": best_match,
                    "similarity": best_similarity,
                })
            else:
                self._buffer_doc(doc, emb)
                buffered.append(doc.doc_id)

        # Step 6: Recalculate lightweight scores for affected narratives
        affected_narrative_ids = set(a["narrative_id"] for a in assignments)
        for nid in affected_narrative_ids:
            self._recalculate_lightweight_scores(nid)

        elapsed = round(time.time() - start, 2)

        # Logging is handled by the pipeline's _log_step wrapper at the integration layer

        return {
            "docs_ingested": len(new_docs),
            "docs_unique": len(unique_docs),
            "docs_assigned": len(assignments),
            "docs_buffered": len(buffered),
            "narratives_updated": len(affected_narrative_ids),
            "runtime_seconds": elapsed,
        }

    def _buffer_doc(self, doc, emb: np.ndarray) -> None:
        """Insert doc into candidate_buffer. Skips silently if doc_id already exists."""
        try:
            self.repository.insert_candidate({
                "doc_id": doc.doc_id,
                "raw_text": doc.raw_text,
                "source_url": doc.source_url,
                "source_domain": doc.source_domain,
                "published_at": doc.published_at,
                "ingested_at": doc.ingested_at,
                "author": doc.author,
                "raw_text_hash": doc.raw_text_hash,
                "status": "pending",
                "embedding_blob": emb.tobytes(),
                "narrative_id_assigned": None,
            })
        except Exception as exc:
            if "UNIQUE constraint" in str(exc) or "PRIMARY KEY" in str(exc):
                pass  # doc_id already in buffer — safe to skip
            else:
                logger.warning("Failed to buffer doc %s: %s", doc.doc_id, exc)

    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return float(np.dot(vec1, vec2) / (norm1 * norm2))

    def _recalculate_lightweight_scores(self, narrative_id: str) -> None:
        """Recalculates only doc_count. Skips expensive cohesion/entropy/velocity."""
        doc_count = self.repository.get_narrative_doc_count(narrative_id)
        self.repository.update_narrative_doc_count(narrative_id, doc_count)
