import logging
import random
import uuid
from datetime import datetime, timezone

import hdbscan
import numpy as np

from embedding_model import EmbeddingModel
from repository import Repository
from settings import Settings
from signals import format_cycle_slot, get_narrative_age_days
from vector_store import VectorStore

logger = logging.getLogger(__name__)

_MIN_CLUSTER_SIZE = 5
_MIN_SAMPLES = 3


def run_clustering(
    repository: Repository,
    vector_store: VectorStore,
    embedder: EmbeddingModel,
    settings: Settings,
    llm_client=None,
) -> list[str]:
    """
    Run HDBSCAN on pending candidate_buffer documents (plus up to 500 existing
    narrative centroids for density context).  Returns a list of newly created
    narrative_ids.  Noise points remain pending; clustered docs become 'clustered'.

    llm_client (optional): if provided, each new cluster is validated for thematic
    coherence via a Haiku call.  Incoherent clusters (score < 0.5) are suppressed
    immediately rather than polluting active narratives.
    """
    min_cluster_size = getattr(settings, 'HDBSCAN_MIN_CLUSTER_SIZE', _MIN_CLUSTER_SIZE)
    min_samples = getattr(settings, 'HDBSCAN_MIN_SAMPLES', _MIN_SAMPLES)
    cycle_slot = format_cycle_slot(datetime.now(timezone.utc), settings.PIPELINE_FREQUENCY_HOURS)

    candidates = repository.get_candidate_buffer(status="pending")

    if len(candidates) < min_cluster_size:
        logger.info(
            "Candidate buffer has %d documents — below min_cluster_size %d, skipping HDBSCAN",
            len(candidates),
            min_cluster_size,
        )
        return []

    # ------------------------------------------------------------------
    # Deserialize pending embeddings
    # Blobs are stored as raw float32 bytes (numpy.ndarray.tobytes()).
    # ------------------------------------------------------------------
    pending_embeddings: list[np.ndarray] = []
    valid_candidates: list[dict] = []

    dim = embedder.dimension()

    for c in candidates:
        blob = c.get("embedding_blob")
        if blob is None:
            continue
        try:
            arr = np.frombuffer(blob, dtype=np.float32).copy()
            if arr.size != dim:
                logger.warning(
                    "Embedding size mismatch for doc_id=%s: expected %d, got %d — skipping",
                    c["doc_id"],
                    dim,
                    arr.size,
                )
                continue
            pending_embeddings.append(arr)
            valid_candidates.append(c)
        except Exception as exc:
            logger.warning(
                "Could not deserialize embedding for doc_id=%s: %s", c["doc_id"], exc
            )

    if len(valid_candidates) < min_cluster_size:
        logger.info(
            "Fewer than %d valid embeddings in candidate buffer — skipping HDBSCAN",
            min_cluster_size,
        )
        return []

    pending_array = np.array(pending_embeddings, dtype=np.float32)  # (N_pending, D)
    n_pending = len(pending_array)

    # ------------------------------------------------------------------
    # Sample existing narrative centroids for density context (up to 500)
    # ------------------------------------------------------------------
    existing_ids = vector_store.get_all_ids()
    existing_vecs: list[np.ndarray] = []

    if existing_ids:
        sample_ids = random.sample(existing_ids, min(500, len(existing_ids)))
        for nid in sample_ids:
            v = vector_store.get_vector(nid)
            if v is not None:
                existing_vecs.append(v.astype(np.float32))

    if existing_vecs:
        all_embeddings = np.vstack(
            [pending_array, np.array(existing_vecs, dtype=np.float32)]
        )
    else:
        all_embeddings = pending_array

    # ------------------------------------------------------------------
    # Run HDBSCAN
    # metric='euclidean' on L2-normalized vectors ≡ cosine distance.
    # ------------------------------------------------------------------
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="euclidean",
    )
    labels = clusterer.fit_predict(all_embeddings)

    # Only examine labels for pending documents (first n_pending rows).
    pending_labels = labels[:n_pending]
    unique_cluster_labels = set(pending_labels) - {-1}

    if not unique_cluster_labels:
        logger.info("HDBSCAN found no clusters in current buffer")
        return []

    # ------------------------------------------------------------------
    # Initialize VectorStore if this is the first run
    # ------------------------------------------------------------------
    if vector_store.count() == 0:
        vector_store.initialize(dim)

    now = datetime.now(timezone.utc).isoformat()
    today = now[:10]

    new_narrative_ids: list[str] = []

    for label in sorted(unique_cluster_labels):
        member_indices = [i for i, lbl in enumerate(pending_labels) if lbl == label]

        # Guard: HDBSCAN should already enforce min_cluster_size, but be defensive.
        if len(member_indices) < min_cluster_size:
            continue

        member_docs = [valid_candidates[i] for i in member_indices]
        member_embeddings = pending_array[member_indices]  # (K, D)

        # Compute L2-normalized centroid.
        centroid = member_embeddings.mean(axis=0).astype(np.float32)
        norm = np.linalg.norm(centroid)
        if norm > 0.0:
            centroid = centroid / norm
        else:
            logger.warning(
                "Zero-norm centroid for HDBSCAN label=%d (%d docs) — skipping cluster",
                label, len(member_indices),
            )
            for doc in member_docs:
                repository.update_candidate_status(doc["doc_id"], "clustered", None)
            continue

        narrative_id = str(uuid.uuid4())

        # Add centroid to VectorStore.
        vector_store.add(centroid.reshape(1, -1), [narrative_id])

        # Insert narrative record with all required fields.
        narrative: dict = {
            "narrative_id": narrative_id,
            "name": None,
            "stage": "Emerging",
            "created_at": now,
            "last_updated_at": now,
            "is_coordinated": 0,
            "coordination_flag_count": 0,
            "suppressed": 0,
            "linked_assets": None,
            "disclaimer": None,
            "human_review_required": 0,
            "is_catalyst": 0,
            "document_count": len(member_indices),
            "velocity": 0.0,
            "velocity_windowed": 0.0,
            "centrality": 0.0,
            "entropy": None,
            "intent_weight": 0.0,
            "ns_score": 0.0,
            "cohesion": 0.0,
            "polarization": 0.0,
            "cross_source_score": 0.0,
            "last_assignment_date": today,
            "consecutive_declining_cycles": 0,
        }
        repository.insert_narrative(narrative)

        # ------------------------------------------------------------------
        # Optional: validate cluster coherence via Haiku.
        # Builds a sample from the first 6 member excerpts and asks Haiku
        # whether they form a coherent, ongoing, investable narrative.
        # Response format: "SCORE: 0.0-1.0 | REASON: ..."
        # Clusters scoring < 0.5 are suppressed immediately.
        # Falls back to accepting the cluster on any error.
        # ------------------------------------------------------------------
        if llm_client is not None:
            try:
                sample_excerpts = "\n---\n".join(
                    (doc.get("raw_text") or "")[:200]
                    for doc in member_docs[:6]
                    if doc.get("raw_text")
                )
                validation_prompt = (
                    "Score this cluster on coherence (0.0-1.0). A high-quality narrative must:\n"
                    "1. Form a single coherent theme (not a grab-bag of unrelated articles)\n"
                    "2. Describe an ongoing market-relevant trend (not a single isolated event)\n"
                    "3. Have a plausible financial or investment implication\n"
                    "Score 0.0 if it fails any criterion.\n\n"
                    "Excerpts:\n"
                    f"{sample_excerpts}\n\n"
                    "Respond in exactly this format:\n"
                    "SCORE: <number> | REASON: <one sentence>"
                )
                validation_result = llm_client.call_haiku(
                    "validate_cluster", narrative_id, validation_prompt
                )
                # Parse score from response.
                # Default accept: new clusters get benefit of doubt on parse
                # failure (contrast cleanup script which defaults to 0.0).
                coherence_score = 1.0
                if "SCORE:" in validation_result:
                    score_part = validation_result.split("SCORE:")[1].split("|")[0].strip()
                    try:
                        coherence_score = float(score_part)
                    except ValueError:
                        pass

                if coherence_score < 0.5:
                    logger.warning(
                        "Cluster %s failed coherence validation (score=%.2f, %d docs) — suppressing",
                        narrative_id,
                        coherence_score,
                        len(member_indices),
                    )
                    repository.update_narrative(narrative_id, {"suppressed": 1})
                    # Remove centroid from VectorStore so it doesn't pollute similarity search
                    try:
                        vector_store.delete(narrative_id)
                    except Exception:
                        pass
                    # Still mark docs as clustered so they don't re-enter the buffer
                    for doc in member_docs:
                        repository.update_candidate_status(doc["doc_id"], "clustered", narrative_id)
                    continue  # skip adding to new_narrative_ids
                else:
                    logger.info(
                        "Cluster %s passed coherence validation (score=%.2f)",
                        narrative_id,
                        coherence_score,
                    )
            except Exception as exc:
                logger.warning(
                    "Cluster validation failed for %s (accepting cluster): %s",
                    narrative_id,
                    exc,
                )

        # Store centroid snapshot (blob = raw float32 bytes, same convention as
        # embedding_blob in candidate_buffer).
        repository.insert_centroid_history(narrative_id, cycle_slot, centroid.tobytes())

        # Record per-doc assignments, evidence, and update candidate statuses.
        for doc in member_docs:
            repository.record_narrative_assignment(narrative_id, today)
            repository.update_candidate_status(doc["doc_id"], "clustered", narrative_id)
            repository.insert_document_evidence({
                "narrative_id": narrative_id,
                "doc_id": doc["doc_id"],
                "source_url": doc.get("source_url") or "",
                "source_domain": doc.get("source_domain") or "",
                "published_at": doc.get("published_at") or "",
                "author": doc.get("author") or "",
                "excerpt": (doc.get("raw_text") or "")[:500],
            })

        new_narrative_ids.append(narrative_id)
        logger.info(
            "Created narrative %s with %d documents (HDBSCAN label=%d)",
            narrative_id,
            len(member_indices),
            label,
        )

    return new_narrative_ids


def deduplicate_new_narratives(
    new_ids: list[str],
    repository: Repository,
    vector_store: VectorStore,
    threshold: float = 0.80,
) -> list[str]:
    """Compare new narrative centroids against all existing centroids and merge
    near-duplicates (cosine similarity >= threshold).

    Returns the subset of *new_ids* that survived (were not absorbed).
    Non-fatal: catches all exceptions and returns *new_ids* unchanged on error.
    """
    if not new_ids:
        return new_ids

    try:
        new_id_set = set(new_ids)
        merged_away: set[str] = set()

        # Preload Dormant + suppressed IDs — one query replaces per-match lookups
        _excluded_ids: set[str] = set()
        try:
            with repository._get_conn() as conn:
                _excluded_ids = {
                    row[0] for row in conn.execute(
                        "SELECT narrative_id FROM narratives "
                        "WHERE suppressed = 1 OR stage = 'Dormant'"
                    ).fetchall()
                }
        except Exception:
            logger.warning("deduplicate_new_narratives: could not preload exclusions")

        for nid in new_ids:
            if nid in merged_away:
                continue

            centroid = vector_store.get_vector(nid)
            if centroid is None:
                logger.warning("deduplicate_new_narratives: no centroid for %s — skipping", nid)
                continue

            distances, match_ids = vector_store.search(centroid, k=10)

            for sim, match_id in zip(distances, match_ids):
                if match_id == nid:
                    continue  # self-match
                if match_id in merged_away:
                    continue
                if match_id not in new_id_set and match_id in _excluded_ids:
                    continue
                if float(sim) < threshold:
                    break  # results are sorted descending by similarity

                # Determine survivor vs absorbed
                if match_id not in new_id_set:
                    # Existing narrative survives — new one is absorbed
                    survivor_id, absorbed_id = match_id, nid
                else:
                    # Intra-batch: higher document_count survives; ties → first in iteration order
                    nid_rec = repository.get_narrative(nid)
                    match_rec = repository.get_narrative(match_id)
                    nid_count = (nid_rec or {}).get("document_count", 0) or 0
                    match_count = (match_rec or {}).get("document_count", 0) or 0

                    if match_count > nid_count:
                        survivor_id, absorbed_id = match_id, nid
                    else:
                        survivor_id, absorbed_id = nid, match_id

                logger.info(
                    "deduplicate_new_narratives: merging %s into %s (sim=%.3f)",
                    absorbed_id, survivor_id, float(sim),
                )
                try:
                    repository.merge_narrative(survivor_id, absorbed_id, vector_store)
                    merged_away.add(absorbed_id)
                except Exception as merge_exc:
                    logger.warning(
                        "deduplicate_new_narratives: merge %s->%s failed: %s",
                        absorbed_id, survivor_id, merge_exc,
                    )
                break  # each narrative merges at most once

        survivors = [nid for nid in new_ids if nid not in merged_away]
        if merged_away:
            logger.info(
                "deduplicate_new_narratives: %d merged away, %d survivors",
                len(merged_away), len(survivors),
            )
        return survivors

    except Exception as exc:
        logger.error("deduplicate_new_narratives failed: %s", exc, exc_info=True)
        return list(new_ids)


def periodic_narrative_dedup(
    repository: Repository,
    vector_store: VectorStore,
    threshold: float = 0.85,
    min_age_days_skip: int = 7,
    min_docs_skip: int = 100,
) -> int:
    """Full pairwise dedup across all active narratives.

    Skips pairs where BOTH narratives are older than min_age_days_skip
    AND both have more than min_docs_skip documents — these are established
    narratives with potentially drifted centroids, not true duplicates.

    Returns number of narratives merged.
    """
    try:
        active = repository.get_all_active_narratives()
        active = [
            n for n in active
            if (n.get("name") or "").strip() not in ("", "None")
        ]

        if len(active) < 2:
            return 0

        centroids: dict[str, np.ndarray] = {}
        records: dict[str, dict] = {}

        for narrative in active:
            narrative_id = narrative["narrative_id"]
            records[narrative_id] = narrative
            vec = vector_store.get_vector(narrative_id)
            if vec is None:
                logger.warning(
                    "periodic_narrative_dedup: no centroid for %s — skipping",
                    narrative_id,
                )
                continue

            arr = np.asarray(vec, dtype=np.float32).copy()
            norm = float(np.linalg.norm(arr))
            if norm <= 0.0:
                logger.warning(
                    "periodic_narrative_dedup: zero-norm centroid for %s — skipping",
                    narrative_id,
                )
                continue
            if abs(norm - 1.0) > 1e-6:
                arr = arr / norm
            centroids[narrative_id] = arr

        if len(centroids) < 2:
            return 0

        merged_ids: set[str] = set()
        merge_count = 0

        for i, narrative_1 in enumerate(active):
            narrative_id_1 = narrative_1["narrative_id"]
            if narrative_id_1 in merged_ids or narrative_id_1 not in centroids:
                continue

            rec_1 = records.get(narrative_id_1) or repository.get_narrative(narrative_id_1)
            if not rec_1:
                continue

            docs_1 = int(rec_1.get("document_count") or 0)
            age_1 = get_narrative_age_days(rec_1.get("created_at", ""))

            for narrative_2 in active[i + 1:]:
                narrative_id_2 = narrative_2["narrative_id"]
                if narrative_id_2 in merged_ids or narrative_id_2 not in centroids:
                    continue

                rec_2 = records.get(narrative_id_2) or repository.get_narrative(narrative_id_2)
                if not rec_2:
                    continue

                docs_2 = int(rec_2.get("document_count") or 0)
                age_2 = get_narrative_age_days(rec_2.get("created_at", ""))

                if (
                    age_1 >= min_age_days_skip and age_2 >= min_age_days_skip
                    and docs_1 >= min_docs_skip and docs_2 >= min_docs_skip
                ):
                    continue

                sim = float(np.dot(centroids[narrative_id_1], centroids[narrative_id_2]))
                if sim < threshold:
                    continue

                if docs_1 >= docs_2:
                    survivor_id, absorbed_id = narrative_id_1, narrative_id_2
                    survivor_name, absorbed_name = rec_1.get("name"), rec_2.get("name")
                else:
                    survivor_id, absorbed_id = narrative_id_2, narrative_id_1
                    survivor_name, absorbed_name = rec_2.get("name"), rec_1.get("name")

                try:
                    repository.merge_narrative(survivor_id, absorbed_id, vector_store)
                    merged_ids.add(absorbed_id)
                    merge_count += 1
                    logger.info(
                        "periodic_narrative_dedup: merged %s into %s (sim=%.3f)",
                        absorbed_name, survivor_name, sim,
                    )

                    refreshed_survivor = repository.get_narrative(survivor_id)
                    if refreshed_survivor is not None:
                        records[survivor_id] = refreshed_survivor
                        if survivor_id == narrative_id_1:
                            rec_1 = refreshed_survivor
                            docs_1 = int(refreshed_survivor.get("document_count") or 0)
                    if absorbed_id == narrative_id_1:
                        break
                except Exception as exc:
                    logger.error(
                        "periodic_narrative_dedup: failed to merge %s into %s: %s",
                        absorbed_id, survivor_id, exc,
                    )

        return merge_count

    except Exception as exc:
        logger.error("periodic_narrative_dedup failed: %s", exc, exc_info=True)
        return 0
