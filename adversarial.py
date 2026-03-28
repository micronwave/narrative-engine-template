import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from deduplicator import Deduplicator
from ingester import RawDocument
from repository import Repository
from settings import Settings

logger = logging.getLogger(__name__)


@dataclass
class AdversarialEvent:
    event_id: str
    affected_narrative_ids: list[str]
    source_domains: list[str]
    similarity_score: float
    detected_at: str  # ISO8601


def check_coordination(
    batch_documents: list[RawDocument],
    deduplicator: Deduplicator,
    trusted_domains: list[str],
    settings: Settings,
    repository: Repository,
) -> list[AdversarialEvent]:
    """
    Detect coordinated posting patterns in the current ingestion batch.

    A coordination event is flagged when a cluster of near-duplicate documents
    (Jaccard similarity >= LSH_THRESHOLD) originates from >= SYNC_BURST_MIN_SOURCES
    distinct non-trusted domains within SYNC_BURST_WINDOW_SECONDS of each other.

    Trusted domains are excluded entirely to prevent wire-service syndication
    from generating false positives.

    Returns a list of detected AdversarialEvents and applies narrative penalties
    via the repository.
    """
    trusted_set = set(trusted_domains)

    # Filter out trusted-domain documents before any analysis.
    untrusted_docs = [
        doc for doc in batch_documents if doc.source_domain not in trusted_set
    ]
    if len(untrusted_docs) < 2:
        return []

    # Get MinHash signatures computed during this ingestion batch.
    batch_signatures = deduplicator.get_batch_signatures()

    # Keep only documents that have a MinHash signature in the current batch.
    docs_with_sigs: list[RawDocument] = [
        doc for doc in untrusted_docs if doc.doc_id in batch_signatures
    ]
    if len(docs_with_sigs) < 2:
        return []

    # ------------------------------------------------------------------
    # Compute pairwise Jaccard similarity for cross-domain document pairs.
    # ------------------------------------------------------------------
    n = len(docs_with_sigs)
    similarity_threshold = settings.LSH_THRESHOLD

    similar_pairs: list[tuple[int, int]] = []
    for i in range(n):
        doc_i = docs_with_sigs[i]
        sig_i = batch_signatures[doc_i.doc_id]
        for j in range(i + 1, n):
            doc_j = docs_with_sigs[j]
            if doc_i.source_domain == doc_j.source_domain:
                continue  # Only flag cross-domain similarity.
            sig_j = batch_signatures[doc_j.doc_id]
            if sig_i.jaccard(sig_j) >= similarity_threshold:
                similar_pairs.append((i, j))

    if not similar_pairs:
        return []

    # ------------------------------------------------------------------
    # Single-linkage clustering via union-find.
    # ------------------------------------------------------------------
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # path compression
            x = parent[x]
        return x

    def union(x: int, y: int) -> None:
        parent[find(x)] = find(y)

    for i, j in similar_pairs:
        union(i, j)

    clusters: dict[int, list[int]] = {}
    for idx in range(n):
        root = find(idx)
        clusters.setdefault(root, []).append(idx)

    # ------------------------------------------------------------------
    # Build doc_id → narrative_id mapping from candidate_buffer.
    # Docs may be 'pending' (unassigned) or 'clustered' (assigned).
    # ------------------------------------------------------------------
    doc_id_to_narrative: dict[str, str] = {}
    try:
        for candidate in repository.get_candidate_buffer(status="clustered"):
            nid = candidate.get("narrative_id_assigned")
            if nid:
                doc_id_to_narrative[candidate["doc_id"]] = nid
        for candidate in repository.get_candidate_buffer(status="pending"):
            nid = candidate.get("narrative_id_assigned")
            if nid:
                doc_id_to_narrative[candidate["doc_id"]] = nid
    except Exception as exc:
        logger.warning("Could not load candidate buffer for narrative lookup: %s", exc)

    # ------------------------------------------------------------------
    # Evaluate each similarity cluster against coordination criteria.
    # ------------------------------------------------------------------
    events: list[AdversarialEvent] = []
    now = datetime.now(timezone.utc).isoformat()

    for root, indices in clusters.items():
        if len(indices) < 2:
            continue

        cluster_docs = [docs_with_sigs[i] for i in indices]

        # Criterion 1: >= SYNC_BURST_MIN_SOURCES distinct non-trusted domains.
        cluster_domains = [doc.source_domain for doc in cluster_docs]
        unique_untrusted_domains = [
            d for d in set(cluster_domains) if d not in trusted_set
        ]
        if len(unique_untrusted_domains) < settings.SYNC_BURST_MIN_SOURCES:
            continue

        # Criterion 2: all published_at timestamps within SYNC_BURST_WINDOW_SECONDS.
        try:
            timestamps = [
                datetime.fromisoformat(doc.published_at).timestamp()
                for doc in cluster_docs
            ]
        except Exception as exc:
            logger.warning(
                "Could not parse published_at for coordination cluster: %s", exc
            )
            continue

        time_span = max(timestamps) - min(timestamps)
        if time_span > settings.SYNC_BURST_WINDOW_SECONDS:
            continue

        # ------------------------------------------------------------------
        # Coordination confirmed.
        # ------------------------------------------------------------------

        # Compute average pairwise Jaccard for this cluster as the event score.
        pair_sims: list[float] = []
        for i in range(len(cluster_docs)):
            for j in range(i + 1, len(cluster_docs)):
                sig_i = batch_signatures[cluster_docs[i].doc_id]
                sig_j = batch_signatures[cluster_docs[j].doc_id]
                pair_sims.append(sig_i.jaccard(sig_j))
        avg_similarity = (
            sum(pair_sims) / len(pair_sims) if pair_sims else similarity_threshold
        )

        # Resolve affected narrative_ids.
        affected_narrative_ids: list[str] = list(
            {
                doc_id_to_narrative[doc.doc_id]
                for doc in cluster_docs
                if doc.doc_id in doc_id_to_narrative
            }
        )

        event_id = str(uuid.uuid4())
        event = AdversarialEvent(
            event_id=event_id,
            affected_narrative_ids=affected_narrative_ids,
            source_domains=unique_untrusted_domains,
            similarity_score=avg_similarity,
            detected_at=now,
        )
        events.append(event)

        # Log to adversarial_log.
        repository.log_adversarial_event(
            {
                "event_id": event_id,
                "narrative_id": (
                    affected_narrative_ids[0] if affected_narrative_ids else None
                ),
                "detected_at": now,
                "source_domains": json.dumps(unique_untrusted_domains),
                "similarity_score": avg_similarity,
                "action_taken": "coordination_flag",
            }
        )

        # Apply penalty and flags to each affected narrative.
        for narrative_id in affected_narrative_ids:
            narrative = repository.get_narrative(narrative_id)
            if narrative is None:
                continue

            flag_count = narrative.get("coordination_flag_count", 0) + 1
            updates: dict = {
                "is_coordinated": 1,
                "coordination_flag_count": flag_count,
            }

            # Check rolling 7-day window; escalate to human review if > 2 flags.
            try:
                rolling_flags = repository.get_coordination_flags_rolling_window(
                    narrative_id, days=7
                )
                if rolling_flags > 2:
                    updates["human_review_required"] = 1
                    logger.warning(
                        "Narrative %s has %d coordination flags in 7 days "
                        "— flagging for human review",
                        narrative_id,
                        rolling_flags,
                    )
            except Exception as exc:
                logger.warning(
                    "Could not query coordination rolling window for %s: %s",
                    narrative_id,
                    exc,
                )

            repository.update_narrative(narrative_id, updates)

        logger.warning(
            "Coordination detected: event_id=%s, %d docs, %d domains, "
            "similarity=%.3f, affected_narratives=%s",
            event_id,
            len(cluster_docs),
            len(unique_untrusted_domains),
            avg_similarity,
            affected_narrative_ids,
        )

    return events
