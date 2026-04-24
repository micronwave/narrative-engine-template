"""
This pipeline operates under the assumption that all data sources have been
reviewed for Terms of Service compliance by the operator. The system logs
all source URLs for audit purposes.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from adversarial import check_coordination
from asset_mapper import AssetMapper
from centrality import build_narrative_graph, compute_centrality, flag_catalysts
from clustering import deduplicate_new_narratives, periodic_narrative_dedup, run_clustering
from deduplicator import Deduplicator
from embedding_model import MiniLMEmbedder
from ingester import RawDocument, RssIngester
from llm_client import LlmClient, parse_signal_json
from output import build_output_object, validate_output, write_outputs
from repository import SqliteRepository
from settings import settings
from signals import (
    compute_cohesion,
    compute_cross_source_score,
    compute_entropy,
    compute_inflow_velocity,
    compute_intent_weight,
    compute_burst_velocity,
    compute_lifecycle_stage,
    compute_ns_score,
    compute_polarization,
    compute_velocity,
    compute_velocity_windowed,
    get_narrative_age_days,
    validate_signal_fields,
)
from api.sector_map import SECTOR_MAP
from convergence import compute_all_convergences
from source_tiers import compute_source_escalation, compute_weighted_source_score
from vector_store import FaissVectorStore
from prompt_utils import sanitize_for_prompt, strip_control_chars

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Topic keyword classifier (Finding 4: replaces Haiku classify_topic call)
# ---------------------------------------------------------------------------

_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "crypto": ["crypto", "bitcoin", "ethereum", "blockchain", "defi", "token", "mining"],
    "geopolitical": ["military", "war", "sanction", "nato", "invasion", "conflict", "geopolit"],
    "macro": ["fed", "inflation", "rate", "gdp", "unemployment", "recession", "monetary", "fiscal"],
    "regulatory": ["regulat", "sec", "ftc", "antitrust", "compliance", "ban", "legislat"],
    "esg": ["climate", "carbon", "emission", "sustainability", "renewable", "green", "esg"],
    "m&a": ["merger", "acquisition", "takeover", "buyout", "deal"],
    "earnings": ["earnings", "revenue", "profit", "quarter", "guidance", "eps", "forecast"],
}

_VALID_TOPIC_TAGS: frozenset[str] = frozenset(_TOPIC_KEYWORDS)


def _classify_topic_keywords(narrative_name: str, excerpts: list[str]) -> list[str]:
    """Deterministic topic classification via keyword matching.

    Returns matched topic tags (1+). Empty list means ambiguous — caller
    should fall back to Haiku.
    """
    corpus = (narrative_name + " " + " ".join(excerpts)).lower()
    return [topic for topic, kws in _TOPIC_KEYWORDS.items() if any(kw in corpus for kw in kws)]


# ---------------------------------------------------------------------------
# Post-labeling relevance gate (Section 12)
# ---------------------------------------------------------------------------

_FINANCIAL_KEYWORDS: frozenset[str] = frozenset({
    "market", "stock", "price", "trade", "invest", "fund",
    "rate", "inflation", "earnings", "revenue", "gdp",
    "tariff", "ipo", "m&a", "acquisition", "crypto",
    "bitcoin", "oil", "commodity", "yield", "bond",
    "equity", "sector", "portfolio",
})


def check_financial_relevance(
    name: str, description: str, topic_tags_json: str | None,
) -> bool:
    """Return True if the narrative appears financially relevant.

    Checks name+description for financial keywords and topic_tags for any
    non-empty tag list. Returns True only when both checks succeed.
    """
    combined = (name + " " + description).lower()
    has_financial = any(kw in combined for kw in _FINANCIAL_KEYWORDS)

    has_investable_tag = False
    # All tags from classify_topic are investable by construction
    if topic_tags_json and topic_tags_json not in ("null", "[]"):
        try:
            parsed = json.loads(topic_tags_json) if isinstance(topic_tags_json, str) else topic_tags_json
            has_investable_tag = bool(parsed)
        except (json.JSONDecodeError, TypeError):
            pass

    return has_financial and has_investable_tag


def _flag_post_label_review(
    repository: "SqliteRepository",
    narrative_id: str,
    narrative: dict,
    name: str,
    description: str,
    topic_tags_json: str | None,
) -> None:
    """Flag a labeled narrative for review when it is non-financial or single-source."""
    review_required = bool(narrative.get("human_review_required"))

    if not review_required and not check_financial_relevance(name, description or "", topic_tags_json):
        repository.update_narrative(narrative_id, {"human_review_required": 1})
        review_required = True
        logger.info(
            "Narrative %s flagged for human review (name=%r lacks financial relevance)",
            narrative_id,
            name,
        )

    source_count = int(narrative.get("source_count") or 0)
    if source_count == 1 and not review_required:
        repository.update_narrative(narrative_id, {"human_review_required": 1})
        logger.info(
            "Narrative %s flagged for human review (single source domain, source_count=%d)",
            narrative_id,
            source_count,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _backfill_centroids(
    repository: "SqliteRepository",
    vector_store: "FaissVectorStore",
    emb_dim: int,
    prefix: str = "",
) -> int:
    """Load missing centroid vectors from centroid_history into VectorStore.

    Returns the number of vectors backfilled.
    """
    existing_ids = set(vector_store.get_all_ids())
    active_narratives = repository.get_all_active_narratives()
    missing_ids = [
        n["narrative_id"] for n in active_narratives
        if n["narrative_id"] not in existing_ids
    ]
    if not missing_ids:
        return 0
    blob_map = repository.get_latest_centroids_batch(missing_ids)
    backfilled = 0
    for nid, blob in blob_map.items():
        try:
            vec = np.frombuffer(blob, dtype=np.float32).copy()
        except ValueError:
            logger.warning(
                "%sCentroid blob for %s is not valid float32 — skipped",
                prefix, nid,
            )
            continue
        if vec.shape[0] == emb_dim:
            vector_store.add(vec.reshape(1, -1), [nid])
            backfilled += 1
        else:
            logger.warning(
                "%sCentroid dim mismatch for %s: got %d, expected %d — skipped",
                prefix, nid, vec.shape[0], emb_dim,
            )
    if backfilled:
        logger.info(
            "%sBackfilled %d/%d missing centroid vectors from centroid_history",
            prefix, backfilled, len(missing_ids),
        )
    return backfilled


def _log_step(
    repository: SqliteRepository,
    run_id: str,
    step_number: int,
    step_name: str,
    status: str,
    duration_ms: float,
    error_message: str | None = None,
) -> None:
    """Write a step result to pipeline_run_log. Never raises."""
    try:
        repository.log_pipeline_run({
            "run_id": run_id,
            "step_number": step_number,
            "step_name": step_name,
            "status": status,
            "duration_ms": int(duration_ms),
            "error_message": error_message,
            "run_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        logger.warning(
            "Could not write pipeline_run_log for step %d (%s): %s",
            step_number, step_name, exc,
        )



def _load_centroid_history_vecs(
    repository: SqliteRepository,
    narrative_id: str,
    days: int,
    emb_dim: int,
) -> list[np.ndarray]:
    """Deserialize centroid history blobs into numpy arrays (most-recent first)."""
    records = repository.get_centroid_history(narrative_id, days=days)
    vecs: list[np.ndarray] = []
    for rec in records:
        blob = rec.get("centroid_blob")
        if blob:
            try:
                v = np.frombuffer(blob, dtype=np.float32).copy()
                if v.size == emb_dim:
                    vecs.append(v)
            except Exception:
                pass
    return vecs


def _handle_failed_labeling_attempt(
    repository: SqliteRepository,
    vector_store: FaissVectorStore,
    narrative: dict,
    needs_label: bool,
    label_persisted: bool,
    now_iso: str,
) -> bool:
    """Track a failed labeling attempt and retire after three misses."""
    if not needs_label or label_persisted:
        return False

    narrative_id = narrative["narrative_id"]
    attempts = int(narrative.get("labeling_attempts") or 0) + 1

    if attempts >= 3:
        repository.update_narrative(narrative_id, {
            "stage": "Dormant",
            "labeling_attempts": attempts,
            "description": f"Auto-retired: labeling failed after {attempts} attempts",
            "last_updated_at": now_iso,
        })
        try:
            vector_store.delete(narrative_id)
        except Exception:
            pass
        logger.warning(
            "Narrative %s retired after %d failed labeling attempts",
            narrative_id, attempts,
        )
        return True

    repository.update_narrative(narrative_id, {"labeling_attempts": attempts})
    logger.info(
        "Narrative %s: labeling attempt %d failed, will retry (%d remaining)",
        narrative_id, attempts, 3 - attempts,
    )
    return False


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run() -> None:
    """Execute one full pipeline cycle."""
    cycle_start = time.monotonic()
    today = datetime.now(timezone.utc).date().isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()

    # Shared state populated by steps and consumed by later steps
    surviving_docs: list[RawDocument] = []
    doc_embeddings: dict[str, np.ndarray] = {}          # doc_id -> L2-normalized embedding
    narrative_assigned_docs: dict[str, list[str]] = {}  # narrative_id -> [doc_id, ...]
    new_narrative_ids: list[str] = []
    mutation_analyses: dict[str, str] = {}              # narrative_id -> Sonnet output
    emb_dim: int = 0
    cycle_id: str = str(uuid.uuid4())                   # unique ID for this pipeline invocation

    # ------------------------------------------------------------------ #
    # Step 0: First-Run Initialization and Consistency Check              #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    repository: SqliteRepository | None = None
    try:
        repository = SqliteRepository(settings.DB_PATH)
        repository.migrate()

        # Asset library must exist before any pipeline run
        if not Path(settings.ASSET_LIBRARY_PATH).exists():
            msg = (
                "Asset library not found. Run build_asset_library.py before "
                "first pipeline execution."
            )
            logger.critical(msg)
            _log_step(repository, cycle_id, 0, "initialization", "FATAL",
                      (time.monotonic() - step_start) * 1000, msg)
            return

        # Load embedding model
        embedder = MiniLMEmbedder(settings)
        emb_dim = embedder.dimension()

        # Load or initialize VectorStore
        vector_store = FaissVectorStore(settings.FAISS_INDEX_PATH)
        loaded = vector_store.load()
        if not loaded:
            vector_store.initialize(emb_dim)
            logger.info("Initialized fresh FAISS index with dimension %d", emb_dim)
        else:
            # Validate dimension consistency when index is non-empty
            if vector_store.count() > 0:
                sample_id = vector_store.get_all_ids()[0]
                sample_vec = vector_store.get_vector(sample_id)
                if sample_vec is not None and sample_vec.shape[0] != emb_dim:
                    idx_dim = sample_vec.shape[0]
                    msg = (
                        f"FAISS index dimension mismatch. Index has {idx_dim}, "
                        f"embedder expects {emb_dim}. Delete FAISS_INDEX_PATH to rebuild."
                    )
                    logger.critical(msg)
                    _log_step(repository, cycle_id, 0, "initialization", "FATAL",
                              (time.monotonic() - step_start) * 1000, msg)
                    return

        # Backfill centroid vectors from centroid_history for narratives
        # missing from the VectorStore.  This covers the common case where the
        # FAISS pickle cannot be deserialised (safe_pickle whitelist) and the
        # index is re-initialised empty every cycle.
        _backfill_centroids(repository, vector_store, emb_dim)

        # Load or initialize LSH / Deduplicator
        deduplicator = Deduplicator(
            threshold=settings.LSH_THRESHOLD,
            num_perm=settings.LSH_NUM_PERM,
            lsh_path=settings.LSH_INDEX_PATH,
        )
        deduplicator.load()  # returns False if not found — fresh init is fine

        # SQLite-FAISS consistency check (warn only, no auto-repair)
        db_count = repository.get_narrative_count()
        faiss_count = vector_store.count()
        if db_count > 0 or faiss_count > 0:
            max_count = max(db_count, faiss_count)
            diff_pct = abs(db_count - faiss_count) / (max_count + 1e-9)
            if diff_pct > 0.10:
                logger.warning(
                    "Narrative count mismatch: DB has %d, FAISS has %d. "
                    "Consider manual reconciliation.",
                    db_count, faiss_count,
                )

        # Load asset mapper
        asset_mapper = AssetMapper(settings.ASSET_LIBRARY_PATH, embedder)

        # Initialize LLM client
        llm_client = LlmClient(settings, repository)

        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 0, "initialization", "OK", step_duration)
        logger.info("Step 0 complete in %.0fms", step_duration)

    except Exception as exc:
        logger.critical("Step 0 (initialization) failed: %s", exc, exc_info=True)
        if repository is not None:
            _log_step(repository, cycle_id, 0, "initialization", "ERROR",
                      (time.monotonic() - step_start) * 1000, str(exc))
        return  # Fatal: all subsequent steps depend on objects created here

    # ------------------------------------------------------------------ #
    # Step 1: Log Budget Status                                           #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        spend_record = repository.get_sonnet_daily_spend(today)
        if spend_record is None:
            repository.update_sonnet_daily_spend(today, 0, 0)
            tokens_used = 0
        else:
            tokens_used = int(spend_record.get("total_tokens_used") or 0)

        remaining = settings.SONNET_DAILY_TOKEN_BUDGET - tokens_used
        logger.info(
            "Budget: %d/%d tokens used today, %d remaining",
            tokens_used, settings.SONNET_DAILY_TOKEN_BUDGET, remaining,
        )
        if remaining < settings.SONNET_DAILY_TOKEN_BUDGET * 0.20:
            logger.warning(
                "Sonnet budget WARNING: only %d tokens remain (< 20%% of %d daily budget)",
                remaining, settings.SONNET_DAILY_TOKEN_BUDGET,
            )

        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 1, "budget_status", "OK", step_duration)

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 1 (budget status) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 1, "budget_status", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 2: Retry Failed Ingestion Jobs                                 #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        retryable_jobs = repository.get_retryable_failed_jobs(now_iso)
        logger.info("Step 2: %d retryable failed ingestion jobs", len(retryable_jobs))

        # Permanently broken feeds — skip and delete from queue
        _SKIP_DOMAINS = {"nasdaq.com", "feeds.reuters.com"}
        for job in retryable_jobs:
            source_url = job.get("source_url") or ""
            if any(d in source_url for d in _SKIP_DOMAINS):
                repository.delete_failed_job(job.get("job_id", ""))
                continue
            retry_count = int(job.get("retry_count") or 0)
            if retry_count >= 3:
                continue
            job_id = job.get("job_id") or ""
            source_type = job.get("source_type") or "rss"
            try:
                if source_type == "rss" and source_url:
                    ingester = RssIngester(repository, feed_urls=[source_url])
                    docs = ingester.ingest()
                    if docs:
                        repository.delete_failed_job(job_id)
                        logger.info("Retry succeeded for job %s (%s)", job_id, source_url)
                    else:
                        next_retry_secs = min(300, 60 * (2 ** (retry_count + 1)))
                        next_retry = (
                            datetime.now(timezone.utc) + timedelta(seconds=next_retry_secs)
                        ).isoformat()
                        repository.update_failed_job_retry(job_id, retry_count + 1, next_retry)
                else:
                    logger.debug(
                        "Skipping retry for unsupported source_type=%s job=%s",
                        source_type, job_id,
                    )
            except Exception as job_exc:
                logger.warning("Retry failed for job %s: %s", job_id, job_exc)

        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 2, "retry_failed_jobs", "OK", step_duration)

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 2 (retry failed jobs) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 2, "retry_failed_jobs", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 3: Ingest Raw Documents                                        #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    raw_documents: list[RawDocument] = []
    try:
        ingesters = [RssIngester(repository)]
        try:
            from api_ingesters import ApiIngestionManager
            ingesters.append(ApiIngestionManager(settings, repository))
        except Exception as api_exc:
            logger.warning("API ingesters unavailable: %s", api_exc)
        for ingester in ingesters:
            try:
                docs = ingester.ingest()
                raw_documents.extend(docs)
                logger.info(
                    "Ingested %d documents from %s",
                    len(docs), type(ingester).__name__,
                )
            except Exception as ing_exc:
                logger.error("Ingester %s failed: %s", type(ingester).__name__, ing_exc)

        # Reddit is handled by ApiIngestionManager above — no standalone call needed.

        logger.info("Step 3: %d total raw documents ingested", len(raw_documents))
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 3, "ingest", "OK", step_duration,
                  f"ingested={len(raw_documents)}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 3 (ingest) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 3, "ingest", "ERROR", step_duration, str(exc))
        # Non-fatal: raw_documents stays empty, downstream steps skip new-doc processing

    # Step 4 is implicit — robots.txt enforcement is handled inside each Ingester.

    # ------------------------------------------------------------------ #
    # Step 5: LSH Deduplication                                           #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        for doc in raw_documents:
            is_dup, sig = deduplicator.is_duplicate(doc)
            if is_dup:
                logger.debug("Duplicate suppressed: %s", doc.source_url)
            else:
                deduplicator.add_with_signature(doc, sig)
                surviving_docs.append(doc)

        deduplicator.save()
        logger.info(
            "Step 5: %d/%d documents survived deduplication",
            len(surviving_docs), len(raw_documents),
        )

        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 5, "deduplication", "OK", step_duration,
                  f"surviving={len(surviving_docs)} of {len(raw_documents)}")

        if not surviving_docs:
            logger.info("No new unique documents this cycle")
            # Steps 6-9 deal with new documents only — skip them.
            # Steps 10-20 still run for existing narratives.

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 5 (deduplication) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 5, "deduplication", "ERROR", step_duration, str(exc))
        surviving_docs = []  # Ensure downstream skips new-doc processing

    has_new_docs = bool(surviving_docs)

    # ------------------------------------------------------------------ #
    # Step 6: Embed Surviving Documents                                   #
    # ------------------------------------------------------------------ #
    if has_new_docs:
        step_start = time.monotonic()
        try:
            texts = [doc.raw_text for doc in surviving_docs]
            raw_embeddings = embedder.embed(texts)  # already L2-normalized

            for i, doc in enumerate(surviving_docs):
                emb = raw_embeddings[i].astype(np.float32)
                norm = np.linalg.norm(emb)
                if norm > 0:
                    emb = emb / norm
                doc_embeddings[doc.doc_id] = emb

            logger.info("Step 6: embedded %d documents (dim=%d)", len(doc_embeddings), emb_dim)
            step_duration = (time.monotonic() - step_start) * 1000
            _log_step(repository, cycle_id, 6, "embed", "OK", step_duration)

        except Exception as exc:
            step_duration = (time.monotonic() - step_start) * 1000
            logger.error("Step 6 (embed) failed: %s", exc, exc_info=True)
            _log_step(repository, cycle_id, 6, "embed", "ERROR", step_duration, str(exc))
            has_new_docs = False  # Skip Step 7, proceed to existing narrative steps

    # ------------------------------------------------------------------ #
    # Step 7: Assign Documents to Narratives                              #
    # ------------------------------------------------------------------ #
    if has_new_docs:
        step_start = time.monotonic()
        assigned_count = 0
        buffered_count = 0
        try:
            alpha = settings.CENTROID_ALPHA
            floor = settings.ASSIGNMENT_SIMILARITY_FLOOR

            if vector_store.is_empty():
                logger.info(
                    "Step 7: VectorStore empty — buffering all %d documents",
                    len(surviving_docs),
                )
                for doc in surviving_docs:
                    emb = doc_embeddings[doc.doc_id]
                    repository.insert_candidate({
                        "doc_id": doc.doc_id,
                        "raw_text": doc.raw_text,
                        "source_url": doc.source_url,
                        "source_domain": doc.source_domain,
                        "published_at": doc.published_at,
                        "ingested_at": doc.ingested_at,
                        "author": doc.author,
                        "raw_text_hash": doc.raw_text_hash,
                        "embedding_blob": emb.tobytes(),
                        "status": "pending",
                        "narrative_id_assigned": None,
                    })
                    buffered_count += 1
            else:
                # Load IDs of narratives that should never receive documents
                with repository._get_conn() as conn:
                    _excluded_ids = {
                        row[0] for row in conn.execute(
                            "SELECT narrative_id FROM narratives WHERE suppressed = 1 OR stage = 'Dormant'"
                        ).fetchall()
                    }

                for doc in surviving_docs:
                    emb = doc_embeddings[doc.doc_id]
                    distances, ids = vector_store.search(emb, k=1)

                    if ids and len(distances) > 0 and float(distances[0]) >= floor:
                        narrative_id = ids[0]

                        if narrative_id in _excluded_ids:
                            logger.warning("Step 7: skipping excluded narrative %s (stale centroid)", narrative_id)
                            repository.insert_candidate({
                                "doc_id": doc.doc_id,
                                "raw_text": doc.raw_text,
                                "source_url": doc.source_url,
                                "source_domain": doc.source_domain,
                                "published_at": doc.published_at,
                                "ingested_at": doc.ingested_at,
                                "author": doc.author,
                                "raw_text_hash": doc.raw_text_hash,
                                "embedding_blob": emb.tobytes(),
                                "status": "pending",
                                "narrative_id_assigned": None,
                            })
                            buffered_count += 1
                            continue

                        # Momentum centroid update
                        old_vec = vector_store.get_vector(narrative_id)
                        if old_vec is not None:
                            new_vec = ((1 - alpha) * old_vec + alpha * emb).astype(np.float32)
                            norm = np.linalg.norm(new_vec)
                            if norm > 0:
                                new_vec = new_vec / norm
                            vector_store.update(narrative_id, new_vec)
                            repository.insert_centroid_history(
                                narrative_id, today, new_vec.tobytes()
                            )

                        repository.record_narrative_assignment(narrative_id, today)
                        repository.update_narrative(narrative_id, {
                            "last_assignment_date": today,
                            "last_updated_at": now_iso,
                        })

                        # Store evidence for output and signal computation
                        repository.insert_document_evidence({
                            "narrative_id": narrative_id,
                            "doc_id": doc.doc_id,
                            "source_url": doc.source_url,
                            "source_domain": doc.source_domain,
                            "published_at": doc.published_at,
                            "author": doc.author,
                            "excerpt": doc.raw_text[:500],
                        })

                        narrative_assigned_docs.setdefault(narrative_id, []).append(doc.doc_id)
                        assigned_count += 1
                    else:
                        # Below similarity floor — buffer as candidate
                        repository.insert_candidate({
                            "doc_id": doc.doc_id,
                            "raw_text": doc.raw_text,
                            "source_url": doc.source_url,
                            "source_domain": doc.source_domain,
                            "published_at": doc.published_at,
                            "ingested_at": doc.ingested_at,
                            "author": doc.author,
                            "raw_text_hash": doc.raw_text_hash,
                            "embedding_blob": emb.tobytes(),
                            "status": "pending",
                            "narrative_id_assigned": None,
                        })
                        buffered_count += 1

            logger.info("Step 7: assigned=%d buffered=%d", assigned_count, buffered_count)
            step_duration = (time.monotonic() - step_start) * 1000
            _log_step(repository, cycle_id, 7, "assign_documents", "OK", step_duration,
                      f"assigned={assigned_count} buffered={buffered_count}")

        except Exception as exc:
            step_duration = (time.monotonic() - step_start) * 1000
            logger.error("Step 7 (assign documents) failed: %s", exc, exc_info=True)
            _log_step(repository, cycle_id, 7, "assign_documents", "ERROR", step_duration, str(exc))
            # Non-fatal: proceed with existing narrative processing

    # ------------------------------------------------------------------ #
    # Step 8: Centroid Decay                                              #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        decay_ids = repository.get_narratives_needing_decay(today)
        alpha = settings.CENTROID_ALPHA

        for narrative_id in decay_ids:
            old_vec = vector_store.get_vector(narrative_id)
            if old_vec is None:
                continue
            # Decay toward origin — reduce magnitude to weaken match affinity.
            # Do NOT renormalize: scaling a unit vector then renormalizing is a no-op.
            decayed = (old_vec * (1 - alpha)).astype(np.float32)
            vector_store.update(narrative_id, decayed)

        logger.info("Step 8: decayed %d narratives", len(decay_ids))
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 8, "centroid_decay", "OK", step_duration,
                  f"decayed={len(decay_ids)}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 8 (centroid decay) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 8, "centroid_decay", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 9: Run Clustering                                              #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        buffer_count = repository.get_candidate_buffer_count(status="pending")

        if buffer_count >= settings.NOISE_BUFFER_THRESHOLD:
            new_narrative_ids = run_clustering(repository, vector_store, embedder, settings, llm_client)
            logger.info("Step 9: clustering produced %d new narratives", len(new_narrative_ids))
        elif buffer_count > 0:
            logger.info(
                "Candidate buffer has %d documents, below threshold %d. "
                "Waiting for more documents.",
                buffer_count, settings.NOISE_BUFFER_THRESHOLD,
            )

        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 9, "clustering", "OK", step_duration,
                  f"buffer={buffer_count} new_narratives={len(new_narrative_ids)}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 9 (clustering) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 9, "clustering", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 9.5: Post-Clustering Deduplication                             #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    merged_count = 0
    try:
        if new_narrative_ids:
            surviving_ids = deduplicate_new_narratives(
                new_narrative_ids, repository, vector_store
            )
            merged_count = len(new_narrative_ids) - len(surviving_ids)
            new_narrative_ids = surviving_ids
            logger.info(
                "Step 9.5: post-cluster dedup merged %d, %d survivors",
                merged_count, len(new_narrative_ids),
            )
            if merged_count > 0:
                try:
                    vector_store.save()
                except Exception:
                    logger.warning("Step 9.5: failed to persist vector store after dedup", exc_info=True)

        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 95, "post_cluster_dedup", "OK", step_duration,
                  f"merged={merged_count}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 9.5 (post-cluster dedup) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 95, "post_cluster_dedup", "ERROR", step_duration, str(exc))
        # Non-fatal: continue with original new_narrative_ids

    # ------------------------------------------------------------------ #
    # Step 9.6: Periodic Full-Sweep Deduplication                        #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    sweep_merged = 0
    try:
        with repository._get_conn() as conn:
            run_count = conn.execute(
                "SELECT COUNT(*) FROM pipeline_run_log WHERE step_number = 0"
            ).fetchone()[0]

        if run_count > 0 and run_count % 6 == 0:
            sweep_merged = periodic_narrative_dedup(repository, vector_store)
            if sweep_merged > 0:
                try:
                    vector_store.save()
                except Exception:
                    logger.warning(
                        "Step 9.6: failed to persist vector store after periodic dedup",
                        exc_info=True,
                    )
                logger.info(
                    "Step 9.6: periodic dedup sweep merged %d narratives",
                    sweep_merged,
                )
            else:
                logger.info("Step 9.6: periodic dedup sweep found no merges")
        else:
            logger.info("Step 9.6: periodic dedup skipped (run_count=%d)", run_count)

        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(
            repository,
            cycle_id,
            96,
            "periodic_dedup",
            "OK",
            step_duration,
            f"run_count={run_count} merged={sweep_merged}",
        )

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 9.6 (periodic dedup) failed: %s", exc, exc_info=True)
        _log_step(
            repository,
            cycle_id,
            96,
            "periodic_dedup",
            "ERROR",
            step_duration,
            str(exc),
        )
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 10: Compute Signals                                            #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        active_narratives = repository.get_all_active_narratives()

        # Total unique domains across corpus for cross-source score denominator
        corpus_domain_count = repository.get_corpus_domain_count()

        for narrative in active_narratives:
            narrative_id = narrative["narrative_id"]
            is_new = narrative_id in new_narrative_ids

            history_vecs = _load_centroid_history_vecs(
                repository, narrative_id,
                days=settings.VELOCITY_WINDOW_DAYS + 1,
                emb_dim=emb_dim,
            )

            # First-run guard: new narrative has no meaningful history yet
            if is_new or len(history_vecs) < 2:
                velocity = 0.0
                velocity_windowed = 0.0
            else:
                velocity = compute_velocity(history_vecs[0], history_vecs[1])
                velocity_windowed = compute_velocity_windowed(
                    history_vecs, settings.VELOCITY_WINDOW_DAYS
                )

            evidence = repository.get_document_evidence(narrative_id)
            doc_texts = [e.get("excerpt") or "" for e in evidence]
            doc_domains = [e.get("source_domain") or "" for e in evidence
                           if e.get("source_domain")]

            entropy = compute_entropy(doc_texts, settings.ENTROPY_VOCAB_WINDOW) \
                if doc_texts else None
            intent_weight = compute_intent_weight(doc_texts) if doc_texts else 0.0
            cross_source_score = compute_cross_source_score(doc_domains, corpus_domain_count)

            # Phase 2: Source tier escalation + weighted source score
            escalation = compute_source_escalation(evidence)
            weighted_src_score = compute_weighted_source_score(evidence, corpus_domain_count)

            # Cohesion/polarization from new embeddings this cycle (if any)
            new_doc_ids = narrative_assigned_docs.get(narrative_id, [])
            new_embeddings = [doc_embeddings[did] for did in new_doc_ids
                              if did in doc_embeddings]

            ema_alpha = settings.COHESION_EMA_ALPHA

            if new_embeddings:
                old_cohesion = min(1.0, max(0.0, float(narrative.get("cohesion") or 0.0)))

                if len(new_embeddings) < 2:
                    # compute_cohesion returns 0.0 for <2 embeddings —
                    # use doc-to-centroid similarity as a proxy instead.
                    centroid = vector_store.get_vector(narrative_id)
                    if centroid is not None:
                        cycle_cohesion = min(1.0, max(0.0, float(
                            np.dot(new_embeddings[0], centroid)
                        )))
                        cohesion = ema_alpha * cycle_cohesion + (1 - ema_alpha) * old_cohesion
                    else:
                        cohesion = old_cohesion
                else:
                    cycle_cohesion = min(1.0, max(0.0, compute_cohesion(new_embeddings)))
                    if old_cohesion == 0.0 and int(narrative.get("document_count") or 0) == 0:
                        # Brand new narrative — use raw value
                        cohesion = cycle_cohesion
                    else:
                        # EMA: blend new measurement with historical value
                        cohesion = ema_alpha * cycle_cohesion + (1 - ema_alpha) * old_cohesion

                new_doc_texts = [e.get("excerpt") or "" for e in evidence
                                 if e.get("doc_id") in set(new_doc_ids)]
                if len(new_doc_texts) >= 2:
                    polarization = compute_polarization(new_doc_texts)
                else:
                    # <2 docs: insufficient for polarization measurement
                    polarization = float(narrative.get("polarization") or 0.0)
            else:
                # No new docs this cycle — retain existing values
                cohesion = min(1.0, max(0.0, float(narrative.get("cohesion") or 0.0)))
                polarization = float(narrative.get("polarization") or 0.0)

            # document_count: existing count + new assignments this cycle
            new_assignment_count = len(new_doc_ids)
            doc_count = int(narrative.get("document_count") or 0) + new_assignment_count

            # Baseline doc rate (shared by inflow velocity + burst velocity)
            freq = max(settings.PIPELINE_FREQUENCY_HOURS, 1)
            cycles_per_day = 24.0 / freq
            try:
                baseline_daily = repository.get_baseline_doc_rate(narrative_id, lookback_days=7)
            except Exception:
                baseline_daily = 0.0

            # Phase 5: Inflow velocity — document arrival rate relative to 7-day average
            avg_docs_per_cycle_7d = (baseline_daily / cycles_per_day) if baseline_daily > 0 else float(narrative.get("avg_docs_per_cycle_7d") or 0.0)
            inflow_vel = compute_inflow_velocity(new_assignment_count, avg_docs_per_cycle_7d)

            # Sentiment mean/variance and source count for signal validation
            from signals import compute_sentiment_scores
            if doc_texts:
                sent_result = compute_sentiment_scores(doc_texts)
                sentiment_mean = sent_result["mean"]
                sentiment_variance = round(sent_result["std"] ** 2, 6)
            else:
                sentiment_mean = None
                sentiment_variance = None
            source_count = len(set(doc_domains)) if doc_domains else 0

            repository.update_narrative(narrative_id, {
                "velocity": velocity,
                "velocity_windowed": velocity_windowed,
                "entropy": entropy,
                "intent_weight": intent_weight,
                "cross_source_score": cross_source_score,
                "cohesion": cohesion,
                "polarization": polarization,
                "document_count": doc_count,
                "inflow_velocity": inflow_vel,
                "avg_docs_per_cycle_7d": avg_docs_per_cycle_7d,
                "sentiment_mean": sentiment_mean,
                "sentiment_variance": sentiment_variance,
                "source_count": source_count,
                "last_updated_at": now_iso,
                "source_highest_tier": escalation["highest_tier"],
                "source_tier_breadth": escalation["tier_breadth"],
                "source_escalation_velocity": escalation["escalation_velocity"],
                "source_institutional_pickup": 1 if escalation["is_institutional_pickup"] else 0,
                "weighted_source_score": weighted_src_score,
            })

            # F1: Lifecycle stage progression (with hysteresis)
            age_days = get_narrative_age_days(narrative.get("created_at") or now_iso)
            cycles_in_stage = int(narrative.get("cycles_in_current_stage") or 0)
            new_stage = compute_lifecycle_stage(
                current_stage=narrative.get("stage") or "Emerging",
                document_count=doc_count,
                velocity_windowed=velocity_windowed,
                entropy=entropy,
                consecutive_declining_cycles=int(narrative.get("consecutive_declining_cycles") or 0),
                days_since_creation=age_days,
                cycles_in_current_stage=cycles_in_stage,
            )
            if new_stage != (narrative.get("stage") or "Emerging"):
                repository.update_narrative(narrative_id, {"stage": new_stage, "cycles_in_current_stage": 0})
                logger.info("Narrative %s stage: %s → %s", narrative_id, narrative.get("stage"), new_stage)
            else:
                repository.update_narrative(narrative_id, {"cycles_in_current_stage": cycles_in_stage + 1})

            # F2: Burst velocity — doc ingestion rate acceleration
            # (reuses baseline_daily + freq + cycles_per_day from inflow velocity above)
            try:
                baseline_per_cycle = baseline_daily / cycles_per_day if baseline_daily > 0 else 0.0
                # Fallback: established narrative with docs but no snapshot history
                if baseline_per_cycle <= 0 and doc_count > 0:
                    baseline_per_cycle = max(doc_count / (7.0 * cycles_per_day), 1.0)
                burst = compute_burst_velocity(
                    recent_doc_count=new_assignment_count,
                    window_hours=settings.PIPELINE_FREQUENCY_HOURS,
                    baseline_docs_per_window=baseline_per_cycle,
                    alert_ratio=settings.BURST_VELOCITY_ALERT_RATIO,
                )
                repository.update_narrative(narrative_id, {"burst_ratio": burst["ratio"]})
                if burst["is_burst"]:
                    logger.warning("[BURST] Narrative '%s' — %.1fx normal rate",
                                   narrative.get("name", narrative_id), burst["ratio"])
            except Exception as exc:
                logger.debug("Burst velocity skipped for %s: %s", narrative_id, exc)

            # V3: Public interest indicator
            try:
                from signals import compute_public_interest
                reddit_doc_count = sum(1 for e in evidence if (e.get("source_type") or "").lower() == "reddit")
                public_interest = compute_public_interest(
                    cross_source_score=cross_source_score,
                    cross_source_prev=float(narrative.get("cross_source_score") or 0.0),
                    doc_count=doc_count,
                    doc_count_prev=int(narrative.get("document_count") or 0),
                    reddit_doc_count=reddit_doc_count,
                )
                repository.update_narrative(narrative_id, {"public_interest": public_interest})
            except Exception as exc:
                logger.debug("Public interest skipped for %s: %s", narrative_id, exc)

        logger.info("Step 10: signals computed for %d active narratives", len(active_narratives))
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 10, "compute_signals", "OK", step_duration)

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 10 (compute signals) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 10, "compute_signals", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 11: Network Centrality                                         #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        active_narratives = repository.get_all_active_narratives()

        if len(active_narratives) < 2:
            logger.info("Step 11: fewer than 2 active narratives — skipping centrality")
            for n in active_narratives:
                repository.update_narrative(n["narrative_id"], {"centrality": 0.0})
        else:
            graph = build_narrative_graph(active_narratives, vector_store)
            centrality_scores = compute_centrality(graph)
            catalyst_ids = set(flag_catalysts(centrality_scores))

            for n in active_narratives:
                nid = n["narrative_id"]
                repository.update_narrative(nid, {
                    "centrality": centrality_scores.get(nid, 0.0),
                    "is_catalyst": 1 if nid in catalyst_ids else 0,
                })

            logger.info(
                "Step 11: centrality for %d narratives, %d catalysts",
                len(active_narratives), len(catalyst_ids),
            )

        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 11, "centrality", "OK", step_duration)

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 11 (centrality) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 11, "centrality", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 11.5: Narrative Convergence Detection                          #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        active_narratives = repository.get_all_active_narratives()

        # Clear stale convergence data before full recompute
        repository.clear_ticker_convergences()

        convergences = compute_all_convergences(
            active_narratives, repository, vector_store,
        )

        for ticker, conv_data in convergences.items():
            repository.upsert_ticker_convergence({
                "ticker": ticker,
                "computed_at": datetime.now(timezone.utc).isoformat(),
                **conv_data,
            })

        # Propagate convergence_exposure to each narrative:
        # max(pressure_score) across all tickers the narrative is linked to.
        for n in active_narratives:
            raw = n.get("linked_assets")
            max_exposure = 0.0
            if raw:
                try:
                    assets = json.loads(raw) if isinstance(raw, str) else raw
                    if isinstance(assets, list):
                        for asset in assets:
                            t = None
                            if isinstance(asset, dict):
                                t = asset.get("ticker", "").upper()
                            elif isinstance(asset, str):
                                t = asset.upper()
                            if t and t in convergences:
                                ps = convergences[t].get("pressure_score", 0.0)
                                max_exposure = max(max_exposure, ps)
                except (json.JSONDecodeError, TypeError):
                    pass
            repository.update_narrative(n["narrative_id"], {
                "convergence_exposure": max_exposure if max_exposure > 0 else None,
            })

        logger.info(
            "Step 11.5: convergence for %d tickers, top pressure=%.3f",
            len(convergences),
            max((c.get("pressure_score", 0) for c in convergences.values()), default=0),
        )
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 115, "convergence_detection", "OK",
                  step_duration, f"tickers={len(convergences)}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 11.5 (convergence) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 115, "convergence_detection", "ERROR",
                  step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 12: Compute Ns Score (with learned weights)                    #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        from signal_trainer import load_or_train_model, compute_learned_ns_score, _safe_float
        from signals import direction_to_float, certainty_to_float, magnitude_to_float
        import math as _math

        signal_model = load_or_train_model(
            repository,
            settings.SIGNAL_MODEL_PATH,
            retrain_days=settings.SIGNAL_MODEL_RETRAIN_DAYS,
            min_samples=settings.SIGNAL_MIN_TRAINING_SAMPLES,
        )
        model_method = signal_model.get("method", "default")
        logger.info("Step 12: using signal model method=%s (n=%d)",
                     model_method, signal_model.get("n_samples", 0))

        active_narratives = repository.get_all_active_narratives()

        for narrative in active_narratives:
            narrative_id = narrative["narrative_id"]

            velocity = float(narrative.get("velocity") or 0.0)
            intent_weight = float(narrative.get("intent_weight") or 0.0)
            cross_source_score = float(narrative.get("cross_source_score") or 0.0)
            cohesion = float(narrative.get("cohesion") or 0.0)
            polarization = float(narrative.get("polarization") or 0.0)
            centrality = float(narrative.get("centrality") or 0.0)
            entropy = narrative.get("entropy")  # None is valid

            # Build 15-feature dict for learned model
            signal = repository.get_narrative_signal(narrative_id)
            entropy_normalized = 0.0
            if entropy is not None:
                try:
                    log_window = _math.log(settings.ENTROPY_VOCAB_WINDOW) if settings.ENTROPY_VOCAB_WINDOW > 1 else 1.0
                    entropy_normalized = min(float(entropy) / log_window, 1.0)
                except (TypeError, ValueError):
                    pass

            features = {
                "velocity_windowed": float(narrative.get("velocity_windowed") or 0.0),
                "inflow_velocity": float(narrative.get("inflow_velocity") or 0.0),
                "cross_source_score": cross_source_score,
                "cohesion": cohesion,
                "intent_weight": intent_weight,
                "centrality": centrality,
                "entropy_normalized": entropy_normalized,
                "direction_float": direction_to_float(signal.get("direction", "neutral")) if signal else 0.0,
                "confidence": _safe_float(signal.get("confidence")) if signal else 0.0,
                "certainty_float": certainty_to_float(signal.get("certainty", "speculative")) if signal else 0.2,
                "magnitude_float": magnitude_to_float(signal.get("magnitude", "incremental")) if signal else 0.3,
                "source_escalation_velocity": float(narrative.get("source_escalation_velocity") or 0.0),
                "convergence_exposure": float(narrative.get("convergence_exposure") or 0.0),
                "catalyst_proximity_score": float(narrative.get("catalyst_proximity_score") or 0.0),
                "macro_alignment": float(narrative.get("macro_alignment") or 0.0),
                # Extra fields needed by default fallback path
                "polarization": polarization,
                "entropy": entropy,
                "entropy_vocab_window": settings.ENTROPY_VOCAB_WINDOW,
            }

            ns_score = compute_learned_ns_score(features, signal_model)
            repository.update_narrative(narrative_id, {"ns_score": ns_score})

        logger.info("Step 12: Ns scores for %d narratives (method=%s)",
                     len(active_narratives), model_method)
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 12, "ns_score", "OK", step_duration,
                  f"method={model_method}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 12 (Ns score) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 12, "ns_score", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 13: Adversarial Integrity Filter                               #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        adversarial_events = check_coordination(
            batch_documents=surviving_docs,
            deduplicator=deduplicator,
            trusted_domains=settings.TRUSTED_DOMAINS,
            settings=settings,
            repository=repository,
        )

        # Apply −0.25 Ns penalty to affected narratives
        for event in adversarial_events:
            for narrative_id in event.affected_narrative_ids:
                narrative = repository.get_narrative(narrative_id)
                if narrative is None:
                    continue
                current_ns = float(narrative.get("ns_score") or 0.0)
                repository.update_narrative(narrative_id, {
                    "ns_score": max(0.0, current_ns - 0.25),
                })

        logger.info("Step 13: adversarial check — %d events", len(adversarial_events))
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 13, "adversarial", "OK", step_duration,
                  f"events={len(adversarial_events)}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 13 (adversarial) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 13, "adversarial", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 14: Dispatch Haiku — Labeling and Lifecycle Classification     #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        active_narratives = repository.get_all_active_narratives()
        haiku_count = 0
        vs_dirty = False

        for narrative in active_narratives:
            narrative_id = narrative["narrative_id"]
            is_new = narrative_id in new_narrative_ids
            ns_score = float(narrative.get("ns_score") or 0.0)

            if bool(narrative.get("suppressed", 0)):
                continue

            # Label: always for new narratives, else if unnamed/undescribed and below escalation threshold
            needs_label = is_new or (
                ns_score < settings.CONFIDENCE_ESCALATION_THRESHOLD
                and (
                    not (narrative.get("name") or "").strip()
                    or not (narrative.get("description") or "").strip()
                )
            )
            if needs_label:
                # FIX(health_fix2): Per-narrative try/except — one failure must not
                # abort labeling for all remaining narratives in the loop.  Prior to
                # this fix, any exception (DB hiccup, unexpected LLM response, etc.)
                # in a single narrative's labeling propagated to the outer step-14
                # try/except, skipping every subsequent narrative.  This was the root
                # cause of 64 zombie narratives (name IS NULL) accumulating over time.
                label_persisted = False
                try:
                    evidence = repository.get_document_evidence(narrative_id)
                    # Sanitize excerpts: strip control chars + format markers
                    sanitized_excerpts = []
                    for e in evidence[:5]:
                        excerpt = (e.get("excerpt") or "")[:200]
                        for marker in ("NAME:", "DESCRIPTION:", "TAG:", "TAGS:", "SIGNAL_JSON:", "Signal_Json:", "signal_json:"):
                            excerpt = excerpt.replace(marker, "")
                        excerpt = strip_control_chars(excerpt)
                        sanitized_excerpts.append(excerpt)
                    excerpt_text = " ".join(sanitized_excerpts)
                    label_prompt = (
                        "Analyze this set of financial news excerpts and identify the single "
                        "underlying narrative theme.\n\n"
                        f"Excerpts:\n{excerpt_text}\n\n"
                        "Respond in exactly this format (no extra text):\n"
                        "NAME: <3-7 word theme label>\n"
                        "DESCRIPTION: <2 sentences explaining what this narrative claims "
                        "and why it matters financially>\n"
                        'SIGNAL_JSON: {"direction":"bullish or bearish or neutral",'
                        '"confidence":0.0,"timeframe":"immediate or near_term or long_term",'
                        '"magnitude":"incremental or significant or transformative",'
                        '"certainty":"speculative or rumored or expected or confirmed",'
                        '"key_actors":["entity1"],"affected_sectors":["sector1"],'
                        '"catalyst_type":"earnings or regulatory or geopolitical or macro or corporate"}'
                    )
                    raw_label = llm_client.call_haiku("label_narrative", narrative_id, label_prompt)
                    # Parse structured response (case-insensitive prefix matching)
                    name = ""
                    description = ""
                    for line in raw_label.splitlines():
                        stripped = line.strip()
                        stripped_upper = stripped.upper()
                        if stripped_upper.startswith("NAME:"):
                            name = stripped[5:].strip()[:100]
                        elif stripped_upper.startswith("DESCRIPTION:"):
                            description = stripped[12:].strip()[:500]
                    if not name:
                        # Fallback: use first non-JSON, non-marker, non-preamble line as name.
                        # Skip lines ending with ":" (LLM preamble like "Here is my analysis:")
                        # and lines >60 chars (too long for a 3-7 word theme label).
                        for fallback_line in raw_label.splitlines():
                            fl = fallback_line.strip()
                            if (fl and not fl.startswith("{")
                                    and not fl.upper().startswith("SIGNAL_JSON:")
                                    and not fl.upper().startswith("DESCRIPTION:")
                                    and not fl.endswith(":")
                                    and len(fl) <= 60):
                                name = fl[:100]
                                break

                    # Guard: never persist empty/None labels — leave name as NULL so
                    # needs_label stays True and the narrative is retried next cycle.
                    if name and name.strip():
                        label_updates: dict = {"name": name.strip()}
                        if description and description.strip():
                            label_updates["description"] = description.strip()
                        repository.update_narrative(narrative_id, label_updates)
                        label_persisted = True
                        haiku_count += 1

                        # Extract signal from combined response (non-fatal)
                        try:
                            signal_data = parse_signal_json(raw_label)
                            validated_signal = validate_signal_fields(signal_data)
                            repository.upsert_narrative_signal({
                                "narrative_id": narrative_id,
                                **validated_signal,
                                "extracted_at": now_iso,
                                "raw_response": raw_label,
                            })
                        except Exception as sig_exc:
                            logger.debug("Signal upsert failed for %s: %s", narrative_id, sig_exc)
                    else:
                        logger.warning(
                            "Narrative %s: Haiku returned but label extraction failed. Response: %.200s",
                            narrative_id, raw_label,
                        )
                except Exception as label_exc:
                    logger.warning(
                        "Narrative %s: labeling failed, will retry next cycle: %s",
                        narrative_id, label_exc,
                    )

                if _handle_failed_labeling_attempt(
                    repository,
                    vector_store,
                    narrative,
                    needs_label,
                    label_persisted,
                    now_iso,
                ):
                    vs_dirty = True
                    continue

            # Standalone signal extraction for already-labeled narratives with stale/missing signals
            if not needs_label:
                try:
                    existing_signal = repository.get_narrative_signal(narrative_id)
                    signal_is_stale = True
                    if existing_signal and existing_signal.get("extracted_at"):
                        try:
                            extracted_dt = datetime.fromisoformat(
                                existing_signal["extracted_at"]
                            )
                            if extracted_dt.tzinfo is None:
                                extracted_dt = extracted_dt.replace(tzinfo=timezone.utc)
                            staleness_cutoff = datetime.now(timezone.utc) - timedelta(
                                hours=settings.SIGNAL_EXTRACTION_STALENESS_HOURS
                            )
                            signal_is_stale = extracted_dt < staleness_cutoff
                        except (ValueError, TypeError):
                            signal_is_stale = True

                    if signal_is_stale:
                        sa_evidence = repository.get_document_evidence(narrative_id)
                        sa_excerpts = []
                        for e in sa_evidence[:5]:
                            excerpt = (e.get("excerpt") or "")[:200]
                            for marker in ("NAME:", "DESCRIPTION:", "TAG:", "TAGS:", "SIGNAL_JSON:", "Signal_Json:", "signal_json:"):
                                excerpt = excerpt.replace(marker, "")
                            excerpt = strip_control_chars(excerpt)
                            sa_excerpts.append(excerpt)
                        sa_text = " ".join(sa_excerpts)

                        if sa_text.strip():
                            nar_name = sanitize_for_prompt(narrative.get("name") or "unknown narrative")
                            signal_prompt = (
                                f'Analyze these excerpts about the narrative "{nar_name}".\n\n'
                                f"Excerpts:\n{sa_text}\n\n"
                                "Return ONLY a single-line JSON object:\n"
                                'SIGNAL_JSON: {"direction":"bullish or bearish or neutral",'
                                '"confidence":0.0,"timeframe":"immediate or near_term or long_term",'
                                '"magnitude":"incremental or significant or transformative",'
                                '"certainty":"speculative or rumored or expected or confirmed",'
                                '"key_actors":["entity1"],"affected_sectors":["sector1"],'
                                '"catalyst_type":"earnings or regulatory or geopolitical or macro or corporate"}'
                            )
                            raw_signal = llm_client.call_haiku(
                                "extract_signal", narrative_id, signal_prompt
                            )
                            sig_data = parse_signal_json(raw_signal)
                            validated = validate_signal_fields(sig_data)
                            repository.upsert_narrative_signal({
                                "narrative_id": narrative_id,
                                **validated,
                                "extracted_at": now_iso,
                                "raw_response": raw_signal,
                            })
                            haiku_count += 1
                except Exception as sig_exc:
                    logger.debug(
                        "Standalone signal extraction failed for %s: %s",
                        narrative_id, sig_exc,
                    )

            # F4: Topic classification (OUTSIDE needs_label — runs for all untagged narratives)
            # Keyword classifier first; Haiku only for ambiguous cases (0 keyword matches).
            existing_tags = narrative.get("topic_tags")
            if existing_tags is None or existing_tags == "null" or existing_tags == "[]":
                nar_name = narrative.get("name") or ""
                top_docs = repository.get_document_evidence(narrative_id, limit=3)
                excerpts = [(d.get("excerpt") or "") for d in top_docs]
                tags = _classify_topic_keywords(nar_name, excerpts)
                if not tags:
                    # Ambiguous — fall back to Haiku
                    nar_name_safe = sanitize_for_prompt(nar_name or "unknown")
                    topic_prompt = (
                        f'Classify this narrative about "{nar_name_safe}" into 1-3 topic tags from this list:\n'
                        "regulatory, earnings, geopolitical, macro, esg, m&a, crypto\n"
                        "Return ONLY the tag names, comma-separated. Example: regulatory, macro"
                    )
                    try:
                        raw_tags = llm_client.call_haiku("classify_topic", narrative_id, topic_prompt)
                        tags = [t.strip().lower() for t in raw_tags.split(",")
                                if t.strip().lower() in _VALID_TOPIC_TAGS]
                        haiku_count += 1
                    except Exception as exc:
                        logger.debug("Topic classification fallback failed for %s: %s", narrative_id, exc)
                if tags:
                    repository.update_narrative_tags(narrative_id, tags)
                    logger.info("Narrative %s topics: %s", narrative_id, tags)

            # Re-fetch to get stage and any topic_tags persisted this cycle.
            # Also run the relevance gate here so it sees freshly-classified tags.
            fresh = repository.get_narrative(narrative_id) or narrative
            if label_persisted:
                _flag_post_label_review(
                    repository,
                    narrative_id,
                    fresh,
                    fresh.get("name") or "",
                    fresh.get("description") or "",
                    fresh.get("topic_tags"),
                )
            stage = fresh.get("stage") or "Emerging"

            if stage == "Declining":
                consecutive_declining = int(
                    fresh.get("consecutive_declining_cycles") or 0
                ) + 1
            else:
                consecutive_declining = 0

            updates: dict = {
                "consecutive_declining_cycles": consecutive_declining,
                "last_updated_at": now_iso,
            }

            # Noise eviction: Declining > 84 consecutive cycles (~14 days at 4h) AND Ns < 0.20
            ns_current = float(fresh.get("ns_score") or 0.0)
            if stage == "Declining" and consecutive_declining > 84 and ns_current < 0.20:
                logger.info(
                    "Evicting narrative %s: %d consecutive declining cycles, ns=%.3f",
                    narrative_id, consecutive_declining, ns_current,
                )
                updates["suppressed"] = 1
                repository.update_narrative(narrative_id, updates)
                try:
                    vector_store.delete(narrative_id)
                except Exception as del_exc:
                    logger.warning(
                        "Could not delete FAISS vector for %s: %s", narrative_id, del_exc
                    )
                continue

            repository.update_narrative(narrative_id, updates)

        if vs_dirty:
            try:
                vector_store.save()
            except Exception:
                logger.warning(
                    "Step 14: failed to persist vector store after failed-label retirements",
                    exc_info=True,
                )

        logger.info("Step 14: Haiku labeling — %d calls", haiku_count)
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 14, "haiku_labeling", "OK", step_duration,
                  f"haiku_calls={haiku_count}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 14 (Haiku labeling) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 14, "haiku_labeling", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 15: Check Sonnet Escalation                                    #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        active_narratives = repository.get_all_active_narratives()
        sonnet_count = 0

        # Secondary Sonnet trigger: narratives with doc_surge or score_spike
        today_mutations = repository.get_mutations_today()
        mutation_qualified_ids = {
            m["narrative_id"] for m in today_mutations
            if m.get("mutation_type") in ("doc_surge", "score_spike")
        }

        for narrative in active_narratives:
            narrative_id = narrative["narrative_id"]
            ns_score = float(narrative.get("ns_score") or 0.0)

            if bool(narrative.get("suppressed", 0)):
                continue

            if ns_score >= settings.CONFIDENCE_ESCALATION_THRESHOLD or narrative_id in mutation_qualified_ids:
                evidence = repository.get_document_evidence(narrative_id)
                sa_excerpts_mut = []
                for e in evidence[:10]:
                    excerpt = (e.get("excerpt") or "")[:300]
                    for marker in ("NAME:", "DESCRIPTION:", "TAG:", "TAGS:", "SIGNAL_JSON:", "Signal_Json:", "signal_json:"):
                        excerpt = excerpt.replace(marker, "")
                    excerpt = strip_control_chars(excerpt)
                    sa_excerpts_mut.append(excerpt)
                excerpt_text = " ".join(sa_excerpts_mut)
                mutation_prompt = (
                    "Analyze how this financial narrative has evolved. "
                    "Identify key mutations, theme shifts, and emerging sub-themes. "
                    f"Be concise (max 200 words):\n\n{excerpt_text}"
                )
                result = llm_client.call_sonnet(narrative_id, mutation_prompt)
                if result is not None:
                    mutation_analyses[narrative_id] = result
                    sonnet_count += 1

        logger.info("Step 15: Sonnet dispatched for %d narratives", sonnet_count)
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 15, "sonnet_escalation", "OK", step_duration,
                  f"sonnet_calls={sonnet_count}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 15 (Sonnet escalation) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 15, "sonnet_escalation", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # Step 16: Sonnet daily spend is updated atomically inside LlmClient.call_sonnet().
    _log_step(repository, cycle_id, 16, "sonnet_daily_spend", "OK", 0.0,
              "Managed by LlmClient.call_sonnet()")

    # ------------------------------------------------------------------ #
    # Step 17: Persist Indices                                            #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        vector_store.save()
        deduplicator.save()
        logger.info("Step 17: FAISS and LSH indices persisted")
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 17, "persist_indices", "OK", step_duration)

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 17 (persist indices) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 17, "persist_indices", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 18: Write Narrative State                                      #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        active_narratives = repository.get_all_active_narratives()
        for narrative in active_narratives:
            repository.update_narrative(narrative["narrative_id"], {
                "last_updated_at": now_iso,
            })
        logger.info("Step 18: state finalized for %d narratives", len(active_narratives))
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 18, "write_narrative_state", "OK", step_duration)

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 18 (write narrative state) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 18, "write_narrative_state", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 19: Emit Output                                                #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        active_narratives = repository.get_all_active_narratives()
        non_suppressed = [n for n in active_narratives if not bool(n.get("suppressed", 0))]
        output_objects: list[dict] = []

        for narrative in non_suppressed:
            narrative_id = narrative["narrative_id"]

            evidence = repository.get_document_evidence(narrative_id)
            supporting_evidence = [
                {
                    "source_url": e.get("source_url") or "",
                    "source_domain": e.get("source_domain") or "",
                    "published_at": e.get("published_at") or "",
                    "author": e.get("author"),
                    "excerpt": (e.get("excerpt") or "")[:280],
                }
                for e in evidence[:20]
            ]

            centroid_vec = vector_store.get_vector(narrative_id)
            topic_tags_raw = narrative.get("topic_tags")
            narrative_topic_tags = json.loads(topic_tags_raw) if topic_tags_raw else []
            linked_assets = asset_mapper.map_narrative(
                centroid_vec,
                min_similarity=settings.ASSET_MAPPING_MIN_SIMILARITY,
                topic_tags=narrative_topic_tags,
                sector_map=SECTOR_MAP,
            ) if centroid_vec is not None else []

            # Phase 6: Enrich linked_assets with directional impact scores
            if linked_assets:
                try:
                    from impact_scorer import enrich_linked_assets
                    enriched = enrich_linked_assets(narrative_id, linked_assets, repository)
                    if enriched:
                        linked_assets = enriched
                        # Persist each impact score to impact_scores table
                        from datetime import datetime as _dt, timezone as _tz
                        computed_at = _dt.now(_tz.utc).isoformat()
                        for asset_impact in enriched:
                            try:
                                repository.upsert_impact_score({
                                    "narrative_id": narrative_id,
                                    "ticker": asset_impact.get("ticker", ""),
                                    "direction": asset_impact.get("direction", "neutral"),
                                    "impact_score": asset_impact.get("impact_score", 0.0),
                                    "confidence": asset_impact.get("confidence", 0.0),
                                    "time_horizon": asset_impact.get("time_horizon", ""),
                                    "signal_components": asset_impact.get("signal_components", {}),
                                    "computed_at": computed_at,
                                })
                            except Exception as ie:
                                logger.debug("Impact score persist failed for %s/%s: %s",
                                             narrative_id, asset_impact.get("ticker"), ie)
                except Exception as enrich_exc:
                    logger.debug("Impact enrichment failed for %s: %s", narrative_id, enrich_exc)

            # Persist linked_assets to DB for API access
            if linked_assets:
                repository.update_narrative(narrative_id, {
                    "linked_assets": json.dumps(linked_assets),
                })

            score_components = {
                "velocity": float(narrative.get("velocity") or 0.0),
                "intent_weight": float(narrative.get("intent_weight") or 0.0),
                "cross_source_score": float(narrative.get("cross_source_score") or 0.0),
                "cohesion": float(narrative.get("cohesion") or 0.0),
                "polarization": float(narrative.get("polarization") or 0.0),
                "centrality": float(narrative.get("centrality") or 0.0),
            }

            stage = narrative.get("stage", "Emerging")
            velocity_windowed = float(narrative.get("velocity_windowed") or 0.0)
            doc_count = int(narrative.get("document_count") or 0)
            lifecycle_reasoning = (
                f"Stage: {stage}. Windowed velocity: {velocity_windowed:.4f}. "
                f"Document count: {doc_count}."
            )

            obj = build_output_object(
                narrative=narrative,
                linked_assets=linked_assets,
                supporting_evidence=supporting_evidence,
                lifecycle_reasoning=lifecycle_reasoning,
                mutation_analysis=mutation_analyses.get(narrative_id),
                score_components=score_components,
            )

            if validate_output(obj):
                output_objects.append(obj)
            else:
                logger.error(
                    "Output validation failed for narrative %s — excluded", narrative_id
                )

        write_outputs(output_objects, today)
        logger.info("Step 19: emitted %d narrative output objects", len(output_objects))
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 19, "emit_output", "OK", step_duration,
                  f"emitted={len(output_objects)}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 19 (emit output) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 19, "emit_output", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 19.1: Catalyst Anchoring (Phase 4)                            #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        from catalyst_service import compute_catalyst_proximity

        active_for_catalyst = repository.get_all_active_narratives()
        catalyst_count = 0

        for narrative in active_for_catalyst:
            narrative_id = narrative["narrative_id"]
            linked_raw = narrative.get("linked_assets")
            if not linked_raw:
                continue

            try:
                assets = json.loads(linked_raw) if isinstance(linked_raw, str) else linked_raw
            except (json.JSONDecodeError, TypeError):
                continue
            if not assets:
                continue

            # Get direction and sectors from narrative_signals (Phase 1)
            signal = repository.get_narrative_signal(narrative_id)
            direction = signal.get("direction", "neutral") if signal else "neutral"
            try:
                sectors = json.loads(signal.get("affected_sectors", "[]")) if signal else []
            except (json.JSONDecodeError, TypeError):
                sectors = []

            best_proximity = 0.0
            best_result = None

            for asset in assets:
                ticker = asset.get("ticker", "")
                if not ticker or ticker.startswith("TOPIC:"):
                    continue
                try:
                    result = compute_catalyst_proximity(ticker, direction, sectors)
                    if result["proximity_score"] > best_proximity:
                        best_proximity = result["proximity_score"]
                        best_result = result
                except Exception as exc:
                    logger.debug("Catalyst proximity failed for %s/%s: %s", narrative_id, ticker, exc)
                    continue

            if best_result:
                repository.update_narrative(narrative_id, {
                    "catalyst_proximity_score": best_result["proximity_score"],
                    "days_to_catalyst": best_result["days_to_earnings"] if best_result["catalyst_type"] == "earnings" else best_result["days_to_fomc"],
                    "catalyst_type": best_result["catalyst_type"],
                    "macro_alignment": best_result["macro_alignment"],
                })
                catalyst_count += 1

        logger.info("Step 19.1: catalyst anchoring computed for %d narratives", catalyst_count)
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 191, "catalyst_anchoring", "OK", step_duration,
                  f"anchored={catalyst_count}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 19.1 (catalyst anchoring) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 191, "catalyst_anchoring", "ERROR", step_duration, str(exc))
        # Non-fatal: continue

    # ------------------------------------------------------------------ #
    # Step 19.5: Snapshot and Detect Mutations                           #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        from mutations import MutationDetector
        detector = MutationDetector(
            settings, repository, llm_client,
            narrative_assigned_docs=narrative_assigned_docs,
            pipeline_run_id=cycle_id,
            mutation_analyses=mutation_analyses,
        )
        active_for_mutations = repository.get_all_active_narratives()

        mutations_detected = 0
        for narrative in active_for_mutations:
            narrative_id = narrative["narrative_id"]
            detector.take_daily_snapshot(narrative_id)
            mutations = detector.detect_mutations(narrative_id)
            mutations_detected += len(mutations)

        logger.info("Step 19.5: snapshotted %d narratives, detected %d mutations",
                    len(active_for_mutations), mutations_detected)
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 195, "snapshot_and_mutations", "OK", step_duration,
                  f"narratives={len(active_for_mutations)} mutations={mutations_detected}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 19.5 (snapshot and mutations) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 195, "snapshot_and_mutations", "ERROR", step_duration, str(exc))
        # Non-fatal: continue to cleanup

    # ------------------------------------------------------------------ #
    # Step 19.7: Check Notification Rules                                #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        from notifications import NotificationManager
        notif_manager = NotificationManager(repository)
        triggered = notif_manager.check_rules()
        step_duration = (time.monotonic() - step_start) * 1000
        logger.info("Step 19.7: checked notification rules, triggered=%d", len(triggered))
        _log_step(repository, cycle_id, 197, "check_notifications", "OK", step_duration,
                  f"triggered={len(triggered)}")
    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.warning("Step 19.7 (check_notifications) failed: %s", exc)
        _log_step(repository, cycle_id, 197, "check_notifications", "ERROR", step_duration, str(exc))
        # Non-fatal: continue to cleanup

    # Hook: add your own post-pipeline dispatch here (e.g., Twitter bot, Slack, etc.)

    # ------------------------------------------------------------------ #
    # Step 20: Cleanup                                                    #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        deleted_count = repository.delete_old_candidate_buffer(days=7)
        cycle_duration_ms = int((time.monotonic() - cycle_start) * 1000)
        logger.info(
            "Step 20: deleted %d old buffer entries. Pipeline cycle complete in %dms.",
            deleted_count, cycle_duration_ms,
        )
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 20, "cleanup", "OK", step_duration,
                  f"deleted_candidates={deleted_count} total_cycle_ms={cycle_duration_ms}")

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 20 (cleanup) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 20, "cleanup", "ERROR", step_duration, str(exc))

    # ------------------------------------------------------------------ #
    # Step 21: Quality Metrics                                            #
    # ------------------------------------------------------------------ #
    step_start = time.monotonic()
    try:
        active_narratives = repository.get_all_active_narratives()
        active_count = len(active_narratives)

        # Potential duplicates: centroid cosine similarity > 0.80
        all_ids = [n["narrative_id"] for n in active_narratives]
        raw_centroids = repository.get_latest_centroids_batch(all_ids)
        centroids = {}
        for nid, blob in raw_centroids.items():
            if len(blob) % 4 == 0 and len(blob) // 4 >= 768:
                centroids[nid] = np.frombuffer(blob, dtype=np.float32)

        ids = list(centroids.keys())
        dup_count = 0
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                sim = float(np.dot(centroids[ids[i]], centroids[ids[j]]))
                if sim > 0.80:
                    dup_count += 1

        # Singletons: suspiciously high cohesion and low doc count
        singleton_count = sum(
            1 for n in active_narratives
            if (n.get("cohesion") or 0) >= 0.999 and (n.get("document_count") or 0) <= 5
        )

        # Leak indicator: suppressed narratives still receiving documents
        leak_count = repository.count_suppressed_with_documents()

        metrics_msg = (
            f"active={active_count} potential_dupes={dup_count} "
            f"singletons={singleton_count} suppressed_leak={leak_count}"
        )
        logger.info("Step 21: quality metrics — %s", metrics_msg)
        step_duration = (time.monotonic() - step_start) * 1000
        _log_step(repository, cycle_id, 21, "quality_metrics", "OK", step_duration, metrics_msg)

    except Exception as exc:
        step_duration = (time.monotonic() - step_start) * 1000
        logger.error("Step 21 (quality metrics) failed: %s", exc, exc_info=True)
        _log_step(repository, cycle_id, 21, "quality_metrics", "ERROR", step_duration, str(exc))
        # Non-fatal: continue


def run_quick() -> dict:
    """
    Execute one quick-refresh cycle: ingest → deduplicate → embed → assign to
    existing narratives.  No HDBSCAN, no Sonnet, no lifecycle changes.
    Returns the result dict from QuickRefresh.run().
    """
    from quick_refresh import QuickRefresh

    cycle_id = str(uuid.uuid4())
    step_start = time.monotonic()

    repository = SqliteRepository(settings.DB_PATH)
    repository.migrate()

    embedder = MiniLMEmbedder(settings)
    emb_dim = embedder.dimension()

    vector_store = FaissVectorStore(settings.FAISS_INDEX_PATH)
    if not vector_store.load():
        vector_store.initialize(emb_dim)

    # Backfill centroids from DB (same logic as run())
    _backfill_centroids(repository, vector_store, emb_dim, prefix="run_quick: ")

    deduplicator = Deduplicator(
        threshold=settings.LSH_THRESHOLD,
        num_perm=settings.LSH_NUM_PERM,
        lsh_path=settings.LSH_INDEX_PATH,
    )
    deduplicator.load()

    refresher = QuickRefresh(settings, repository, vector_store, embedder, deduplicator)
    result = refresher.run()

    # Persist updated deduplicator and vector store state
    deduplicator.save()
    vector_store.save()

    step_duration = (time.monotonic() - step_start) * 1000
    _log_step(
        repository, cycle_id, 0, "quick_refresh", "OK", step_duration,
        f"assigned={result['docs_assigned']} buffered={result['docs_buffered']} "
        f"runtime={result['runtime_seconds']}s",
    )

    return result


def run_light() -> dict:
    """
    V3 Phase 3.4 — Light pipeline cycle: ingest + dedup + buffer only.
    Skips: embedding, clustering, centrality, adversarial, LLM, asset map.
    Runs every 1 hour to keep the buffer fresh.
    """
    cycle_id = str(uuid.uuid4())
    step_start = time.monotonic()
    repository = SqliteRepository(settings.DB_PATH)
    repository.migrate()

    deduplicator = Deduplicator(
        threshold=settings.LSH_THRESHOLD,
        num_perm=settings.LSH_NUM_PERM,
        lsh_path=settings.LSH_INDEX_PATH,
    )
    deduplicator.load()

    # Step 1: Ingest
    ingester = RssIngester(repository)
    raw_docs = ingester.ingest()

    # Reddit is handled by ApiIngestionManager in the main run() path.
    try:
        from api_ingesters import ApiIngestionManager
        api_mgr = ApiIngestionManager(settings, repository)
        raw_docs.extend(api_mgr.ingest())
    except Exception:
        pass

    # Step 2: Deduplicate
    unique = []
    for doc in raw_docs:
        is_dup, sig = deduplicator.is_duplicate(doc)
        if not is_dup:
            deduplicator.add_with_signature(doc, sig)
            unique.append(doc)
    logger.info(
        "run_light dedup: %d/%d documents survived",
        len(unique), len(raw_docs),
    )

    # Step 3: Buffer — insert into candidate_buffer
    buffered = 0
    for doc in unique:
        try:
            candidate = {
                "doc_id": doc.doc_id if hasattr(doc, 'doc_id') else doc.get('doc_id', ''),
                "raw_text": doc.raw_text if hasattr(doc, 'raw_text') else doc.get('raw_text', ''),
                "source_url": doc.source_url if hasattr(doc, 'source_url') else doc.get('source_url', ''),
                "source_domain": doc.source_domain if hasattr(doc, 'source_domain') else doc.get('source_domain', ''),
                "published_at": doc.published_at if hasattr(doc, 'published_at') else doc.get('published_at', ''),
                "ingested_at": doc.ingested_at if hasattr(doc, 'ingested_at') else doc.get('ingested_at', ''),
                "status": "pending",
            }
            repository.insert_candidate(candidate)
            buffered += 1
        except Exception:
            continue

    deduplicator.save()

    duration = (time.monotonic() - step_start) * 1000
    _log_step(repository, cycle_id, 0, "light_cycle", "OK", duration,
              f"ingested={len(raw_docs)} unique={len(unique)} buffered={buffered}")

    return {"ingested": len(raw_docs), "unique": len(unique), "buffered": buffered,
            "runtime_ms": round(duration)}


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if "--quick" in sys.argv:
        result = run_quick()
        print(result)
    elif "--light" in sys.argv:
        result = run_light()
        print(result)
    else:
        run()
