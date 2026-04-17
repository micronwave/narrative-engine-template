import json
import logging
from pathlib import Path

from safe_pickle import safe_load

import faiss
import numpy as np

from embedding_model import EmbeddingModel

logger = logging.getLogger(__name__)

# Topic tag → set of relevant GICS sectors. Empty set means all sectors are valid.
TOPIC_SECTOR_RELEVANCE: dict[str, set[str]] = {
    "crypto": {"Crypto", "Technology", "Financials"},
    "earnings": set(),
    "regulatory": set(),
    "geopolitical": {"Energy", "Industrials"},
    "macro": set(),
    "esg": {"Energy", "Utilities", "Industrials", "Materials"},
    "m&a": set(),
}


class AssetMapper:
    """
    Maps narratives to financial assets via embedding similarity.
    Zero LLM calls.
    """

    def __init__(self, asset_library_path: str, embedder: EmbeddingModel) -> None:
        """
        Load and validate the asset library.

        Raises FileNotFoundError if the asset library pickle is not found.
        Raises ValueError if embedding dimension mismatches the pipeline embedder.

        The library is a pickled dict: {ticker: {'name': str, 'embedding': np.ndarray}}
        A FAISS IndexFlatIP is built in memory from the asset embeddings on init.
        """
        path = Path(asset_library_path)
        if not path.exists():
            raise FileNotFoundError(
                f"Asset library not found at {asset_library_path}. "
                "Run build_asset_library.py first."
            )

        library: dict = safe_load(str(path), allowed={
            "builtins": {"dict", "list", "tuple", "str", "int", "float", "bool"},
            "numpy": {"ndarray", "dtype", "float32", "float64"},
            "numpy.core.multiarray": {"scalar", "_reconstruct"},
            "numpy._core.multiarray": {"scalar", "_reconstruct"},
            "numpy._core.numeric": {"_frombuffer"},
            "numpy.core.numeric": {"_frombuffer"},
        })

        pipeline_dim: int = embedder.dimension()

        self._tickers: list[str] = []
        self._names: list[str] = []
        embeddings: list[np.ndarray] = []

        for ticker, data in library.items():
            emb = np.asarray(data["embedding"], dtype=np.float32)
            asset_dim = emb.shape[0]
            if asset_dim != pipeline_dim:
                raise ValueError(
                    f"Asset library embedding dimension {asset_dim} does not match "
                    f"pipeline embedding dimension {pipeline_dim}. Rebuild asset library."
                )
            self._tickers.append(str(ticker))
            self._names.append(str(data.get("name", ticker)))
            embeddings.append(emb)

        self._pipeline_dim: int = pipeline_dim

        if embeddings:
            matrix = np.stack(embeddings).astype(np.float32)
            self._index: faiss.IndexFlatIP | None = faiss.IndexFlatIP(pipeline_dim)
            self._index.add(matrix)
            logger.info("AssetMapper: loaded %d assets (dim=%d)", len(self._tickers), pipeline_dim)
        else:
            self._index = None
            logger.warning("AssetMapper: asset library at %s is empty", asset_library_path)

    def map_narrative(
        self,
        centroid: np.ndarray,
        top_k: int = 5,
        min_similarity: float = 0.60,
        topic_tags: list[str] | None = None,
        sector_map: dict[str, str] | None = None,
    ) -> list[dict]:
        """
        Find matching assets for a narrative centroid using cosine similarity
        (dot product on L2-normalized vectors).

        If topic_tags specify a narrow domain and sector_map is provided,
        tickers whose sector is irrelevant to the topic are suppressed.

        Returns: [{'ticker': str, 'asset_name': str, 'similarity_score': float}]
        Results are ordered by descending similarity and filtered to >= min_similarity.
        """
        if self._index is None or self._index.ntotal == 0:
            return []

        query = centroid.astype(np.float32).reshape(1, -1)
        norm = np.linalg.norm(query)
        if norm == 0:
            return []
        query = query / norm
        k = min(top_k, self._index.ntotal)
        distances, indices = self._index.search(query, k)

        # Determine allowed sectors from topic_tags
        allowed_sectors: set[str] | None = None
        if topic_tags and sector_map:
            for tag in topic_tags:
                tag_lower = tag.lower()
                if tag_lower in TOPIC_SECTOR_RELEVANCE:
                    relevant = TOPIC_SECTOR_RELEVANCE[tag_lower]
                    if relevant:  # non-empty = narrow domain
                        if allowed_sectors is None:
                            allowed_sectors = set(relevant)
                        else:
                            allowed_sectors |= relevant

        results: list[dict] = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx < 0:
                continue
            sim = float(dist)
            if sim < min_similarity:
                continue
            ticker = self._tickers[idx]
            if ticker.startswith("TOPIC:"):
                continue  # Skip macro event embeddings — not securities
            # Sector validation: suppress irrelevant sectors for narrow topics
            if allowed_sectors is not None and sector_map:
                ticker_sector = sector_map.get(ticker)
                if ticker_sector and ticker_sector not in allowed_sectors:
                    continue
            results.append(
                {
                    "ticker": ticker,
                    "asset_name": self._names[idx],
                    "similarity_score": sim,
                }
            )
        return results

    def get_all_tickers(self) -> list[str]:
        """Returns all ticker symbols in the asset library (excludes TOPIC: entries)."""
        return [t for t in self._tickers if not t.startswith("TOPIC:")]
