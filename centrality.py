import logging

import networkx as nx
import numpy as np

from vector_store import VectorStore

logger = logging.getLogger(__name__)


def build_narrative_graph(
    narratives: list[dict],
    vector_store: VectorStore,
    similarity_threshold: float = 0.40,
) -> nx.Graph:
    """
    Build an undirected graph where each node is an active narrative.
    An edge is added between two narratives when their centroid cosine
    similarity exceeds similarity_threshold; the edge weight = similarity.

    For L2-normalized centroid vectors, cosine similarity = dot product.
    """
    graph = nx.Graph()

    for n in narratives:
        graph.add_node(n["narrative_id"])

    if len(narratives) < 2:
        return graph

    # Retrieve centroid vectors for all narratives in one pass.
    narrative_ids = [n["narrative_id"] for n in narratives]
    vectors: dict[str, np.ndarray] = {}
    for nid in narrative_ids:
        v = vector_store.get_vector(nid)
        if v is not None:
            vectors[nid] = v.astype(np.float32)
        else:
            logger.warning("No centroid vector for narrative %s — excluded from graph edges", nid)

    ids_with_vecs = list(vectors.keys())

    for i in range(len(ids_with_vecs)):
        for j in range(i + 1, len(ids_with_vecs)):
            nid_a = ids_with_vecs[i]
            nid_b = ids_with_vecs[j]
            # Cosine similarity for L2-normalized vectors = dot product.
            sim = float(np.dot(vectors[nid_a], vectors[nid_b]))
            if sim > similarity_threshold:
                graph.add_edge(nid_a, nid_b, weight=sim)

    return graph


def compute_centrality(graph: nx.Graph) -> dict[str, float]:
    """
    Compute betweenness centrality for all narrative nodes, normalized to [0, 1].

    Returns empty dict if fewer than 2 nodes exist.
    If no edges exist, betweenness centrality is 0.0 for all nodes — correct.

    # TODO SCALE: approximate harmonic centrality + sampling when n > 500
    """
    if graph.number_of_nodes() < 2:
        return {}

    raw_scores: dict[str, float] = nx.betweenness_centrality(graph, normalized=True)

    max_score = max(raw_scores.values()) if raw_scores else 0.0
    if max_score > 0.0:
        return {nid: score / max_score for nid, score in raw_scores.items()}

    return {nid: 0.0 for nid in raw_scores}


def flag_catalysts(centrality_scores: dict[str, float]) -> list[str]:
    """
    Return narrative_ids in the top decile by centrality score.
    If fewer than 10 narratives, return the top 1.
    Returns empty list if centrality_scores is empty.
    """
    if not centrality_scores:
        return []

    n = len(centrality_scores)
    top_count = max(1, n // 10)

    sorted_ids = sorted(
        centrality_scores, key=centrality_scores.__getitem__, reverse=True
    )
    return sorted_ids[:top_count]
