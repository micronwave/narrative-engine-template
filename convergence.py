"""
Narrative Convergence Detection (Signal Redesign Phase 3)

Detects when multiple independent narratives converge on the same ticker,
creating compound pressure that individual narrative scores miss.

Independence is measured via centroid cosine similarity: narratives with
similarity below the threshold are considered independent voices.
Connected components on a dependency graph determine independent clusters.
"""

import json
import logging

import networkx as nx
import numpy as np

from signals import direction_to_float

logger = logging.getLogger(__name__)

def _safe_defaults() -> dict:
    """Fresh safe-default dict. Returns a new list each call to avoid aliasing."""
    return {
        "convergence_count": 0,
        "direction_agreement": 0.0,
        "direction_consensus": 0.0,
        "weighted_confidence": 0.0,
        "source_diversity": 0,
        "pressure_score": 0.0,
        "contributing_narrative_ids": [],
    }


def compute_ticker_convergence(
    ticker: str,
    narratives_for_ticker: list[dict],
    signals_for_narratives: dict[str, dict],
    vector_store,
    independence_threshold: float = 0.30,
) -> dict:
    """
    Compute convergence metrics for a single ticker.

    Args:
        ticker: The ticker symbol.
        narratives_for_ticker: List of narrative dicts whose linked_assets include this ticker.
        signals_for_narratives: {narrative_id: signal_dict} from narrative_signals table.
        vector_store: VectorStore instance for centroid retrieval.
        independence_threshold: Cosine similarity below which two narratives are independent.

    Returns:
        Dict with convergence_count, direction_agreement, direction_consensus,
        weighted_confidence, source_diversity, pressure_score, contributing_narrative_ids.
    """
    n_count = len(narratives_for_ticker)
    nids = [n["narrative_id"] for n in narratives_for_ticker]

    if n_count == 0:
        return _safe_defaults()

    if n_count == 1:
        result = _safe_defaults()
        result["convergence_count"] = 1
        result["contributing_narrative_ids"] = list(nids)
        return result

    # --- Build dependency graph (edges = similar/dependent narratives) ---
    graph = nx.Graph()
    graph.add_nodes_from(nids)

    vectors: dict[str, np.ndarray] = {}
    for nid in nids:
        vec = vector_store.get_vector(nid)
        if vec is not None:
            vectors[nid] = vec.astype(np.float32)

    ids_with_vecs = list(vectors.keys())
    for i in range(len(ids_with_vecs)):
        for j in range(i + 1, len(ids_with_vecs)):
            nid_a = ids_with_vecs[i]
            nid_b = ids_with_vecs[j]
            sim = float(np.dot(vectors[nid_a], vectors[nid_b]))
            if sim >= independence_threshold:
                graph.add_edge(nid_a, nid_b, weight=sim)

    # Each connected component = one independent signal cluster
    components = list(nx.connected_components(graph))
    convergence_count = len(components)

    # --- Aggregate direction & confidence from Phase 1 signals ---
    total_weighted_direction = 0.0
    total_confidence = 0.0

    for nid in nids:
        sig = signals_for_narratives.get(nid, {})
        direction = sig.get("direction", "neutral")
        confidence = sig.get("confidence", 0.0)
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))

        dir_val = direction_to_float(direction)
        total_weighted_direction += dir_val * confidence
        total_confidence += confidence

    if total_confidence > 0.0:
        direction_agreement = total_weighted_direction / total_confidence
        direction_agreement = max(-1.0, min(1.0, direction_agreement))
    else:
        direction_agreement = 0.0

    direction_consensus = abs(direction_agreement)

    weighted_confidence = total_confidence / n_count
    weighted_confidence = max(0.0, min(1.0, weighted_confidence))

    # --- Source diversity: distinct source_highest_tier values ---
    tiers = set()
    for n in narratives_for_ticker:
        tier = n.get("source_highest_tier")
        if tier is not None:
            tiers.add(tier)
    source_diversity = len(tiers)

    # --- Pressure score: compound signal strength ---
    pressure_score = convergence_count * direction_consensus * weighted_confidence

    return {
        "convergence_count": convergence_count,
        "direction_agreement": round(direction_agreement, 6),
        "direction_consensus": round(direction_consensus, 6),
        "weighted_confidence": round(weighted_confidence, 6),
        "source_diversity": source_diversity,
        "pressure_score": round(pressure_score, 6),
        "contributing_narrative_ids": list(nids),
    }


def compute_all_convergences(
    active_narratives: list[dict],
    repository,
    vector_store,
) -> dict[str, dict]:
    """
    Compute convergence for all tickers that have 2+ narrative mappings.

    Args:
        active_narratives: List of narrative dicts from get_all_active_narratives().
        repository: Repository instance (used to fetch narrative signals).
        vector_store: VectorStore instance for centroid retrieval.

    Returns:
        {ticker: convergence_dict} for tickers with 2+ contributing narratives.
    """
    # Build inverted index: ticker -> [narrative dicts], deduplicated by narrative_id
    ticker_narratives: dict[str, dict[str, dict]] = {}  # ticker -> {nid: narrative}

    for n in active_narratives:
        raw = n.get("linked_assets")
        if not raw:
            continue
        try:
            assets = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(assets, list):
            continue

        nid = n["narrative_id"]
        for asset in assets:
            ticker = None
            if isinstance(asset, dict):
                ticker = asset.get("ticker")
            elif isinstance(asset, str):
                ticker = asset
            if not ticker:
                continue
            ticker = ticker.upper()
            if ticker not in ticker_narratives:
                ticker_narratives[ticker] = {}
            if nid not in ticker_narratives[ticker]:
                ticker_narratives[ticker][nid] = n

    # Prefetch all signals in one pass.
    # If prefetch fails, continue in explicit degraded mode (empty signals map)
    # so per-ticker convergence still computes from topology/direction data.
    try:
        all_signals = repository.get_all_narrative_signals()
        signals_map = {s["narrative_id"]: s for s in all_signals}
    except Exception as exc:
        logger.warning(
            "Convergence signal prefetch failed; using degraded empty-signal mode: %s",
            exc,
        )
        signals_map = {}

    # Compute convergence for tickers with 2+ narratives
    results: dict[str, dict] = {}
    for ticker, nid_map in ticker_narratives.items():
        narratives_list = list(nid_map.values())
        if len(narratives_list) < 2:
            continue
        try:
            conv = compute_ticker_convergence(
                ticker, narratives_list, signals_map, vector_store,
            )
            results[ticker] = conv
        except Exception as exc:
            logger.warning("Convergence computation failed for %s: %s", ticker, exc)

    return results
