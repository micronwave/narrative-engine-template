import json
import logging
from pathlib import Path

import faiss
import numpy as np

from embedding_model import EmbeddingModel

logger = logging.getLogger(__name__)


class AssetMapper:
    """Maps narratives to financial assets via embedding similarity."""

    def __init__(self, asset_library_path: str, embedder: EmbeddingModel) -> None:
        path = Path(asset_library_path)
        if not path.exists():
            raise FileNotFoundError(
                f"Asset library not found at {asset_library_path}. "
                "Run build_asset_library.py first."
            )

        with open(path, "r", encoding="utf-8") as f:
            library: dict = json.load(f)

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
        min_similarity: float = 0.50,
    ) -> list[dict]:
        """
        Find matching assets for a narrative centroid using cosine similarity
        (dot product on L2-normalized vectors).

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

        results: list[dict] = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx < 0:
                continue
            sim = float(dist)
            if sim < min_similarity:
                continue
            results.append(
                {
                    "ticker": self._tickers[idx],
                    "asset_name": self._names[idx],
                    "similarity_score": sim,
                }
            )
        return results

    def get_all_tickers(self) -> list[str]:
        """Returns all ticker symbols in the asset library (excludes TOPIC: entries)."""
        return [t for t in self._tickers if not t.startswith("TOPIC:")]
