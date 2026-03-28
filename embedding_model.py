import logging
import pickle
from abc import ABC, abstractmethod
from pathlib import Path

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer

logger = logging.getLogger(__name__)


class EmbeddingModel(ABC):
    """
    Abstract interface for text embedding.
    MVP implementation: MiniLMEmbedder.
    """

    @abstractmethod
    def embed(self, texts: list[str]) -> np.ndarray:
        """
        Embed a list of texts.
        Returns: np.ndarray of shape (len(texts), dimension)
        All vectors must be L2-normalized.
        """
        ...

    @abstractmethod
    def embed_single(self, text: str) -> np.ndarray:
        """
        Embed a single text.
        Returns: np.ndarray of shape (dimension,)
        Convenience method: calls embed([text])[0]
        """
        ...

    @abstractmethod
    def dimension(self) -> int:
        """
        Return the embedding dimension.
        Dense mode: 768
        Hybrid mode: 832
        """
        ...


class MiniLMEmbedder(EmbeddingModel):

    def __init__(self, settings: "Settings") -> None:
        self._model_name: str = settings.EMBEDDING_MODEL_NAME
        self._mode: str = settings.EMBEDDING_MODE
        self._model = SentenceTransformer(self._model_name)

        self._tfidf: TfidfVectorizer | None = None
        self._svd: TruncatedSVD | None = None
        self._tfidf_path: Path | None = None
        self._svd_path: Path | None = None

        if self._mode == "hybrid":
            data_dir = Path(settings.FAISS_INDEX_PATH).parent
            self._tfidf_path = data_dir / "tfidf_vectorizer.pkl"
            self._svd_path = data_dir / "tfidf_svd.pkl"
            self._load_hybrid_components()

    def _load_hybrid_components(self) -> None:
        if self._tfidf_path.exists() and self._svd_path.exists():
            try:
                with open(self._tfidf_path, "rb") as f:
                    tfidf = pickle.load(f)
                with open(self._svd_path, "rb") as f:
                    svd = pickle.load(f)
                if not isinstance(tfidf, TfidfVectorizer):
                    raise TypeError(f"Expected TfidfVectorizer, got {type(tfidf).__name__}")
                if not isinstance(svd, TruncatedSVD):
                    raise TypeError(f"Expected TruncatedSVD, got {type(svd).__name__}")
                self._tfidf = tfidf
                self._svd = svd
                logger.info("Loaded hybrid TF-IDF components from disk.")
            except Exception as exc:
                logger.warning(
                    "Hybrid TF-IDF pickle unusable (%s) — will refit on next batch.",
                    exc,
                )
                self._tfidf = None
                self._svd = None
        else:
            self._tfidf = None
            self._svd = None
            logger.info(
                "Hybrid TF-IDF components not found on disk; will fit on first batch."
            )

    def _fit_hybrid_components(self, texts: list[str]) -> np.ndarray:
        """Fit TF-IDF + SVD on the given texts and persist them. Returns the (N, 64) sparse component."""
        logger.warning(
            "Fitting TF-IDF vectorizer and TruncatedSVD on current batch (%d texts). "
            "This should only happen once. Subsequent runs will load from disk.",
            len(texts),
        )
        self._tfidf = TfidfVectorizer()
        tfidf_matrix = self._tfidf.fit_transform(texts)
        n_features = tfidf_matrix.shape[1]
        if n_features == 0:
            logger.warning("TF-IDF produced 0 features (all texts empty?) — sparse component will be zeros.")
            self._svd = TruncatedSVD(n_components=1)
            return np.zeros((len(texts), 1), dtype=np.float32)
        n_components = max(1, min(64, n_features - 1))
        self._svd = TruncatedSVD(n_components=n_components)
        sparse_reduced = self._svd.fit_transform(tfidf_matrix).astype(np.float32)
        # Atomic writes — temp file then rename
        tmp_tfidf = str(self._tfidf_path) + ".tmp"
        tmp_svd = str(self._svd_path) + ".tmp"
        with open(tmp_tfidf, "wb") as f:
            pickle.dump(self._tfidf, f)
        with open(tmp_svd, "wb") as f:
            pickle.dump(self._svd, f)
        Path(tmp_tfidf).replace(self._tfidf_path)
        Path(tmp_svd).replace(self._svd_path)
        return sparse_reduced

    def embed(self, texts: list[str]) -> np.ndarray:
        """
        Embed a list of texts. Returns float32 L2-normalized array of shape (N, dimension).
        """
        if not texts:
            return np.empty((0, self.dimension()), dtype=np.float32)

        dense: np.ndarray = self._model.encode(
            texts,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        ).astype(np.float32)  # shape: (N, 768)

        if self._mode == "dense":
            return dense

        # Hybrid mode: concatenate dense (768) + TF-IDF SVD (64) = 832
        if self._tfidf is None:
            sparse_reduced = self._fit_hybrid_components(texts)
        else:
            tfidf_matrix = self._tfidf.transform(texts)
            sparse_reduced = self._svd.transform(tfidf_matrix).astype(np.float32)

        # Pad to exactly 64 columns if vocabulary was too small at fit time
        sparse_64 = np.zeros((len(texts), 64), dtype=np.float32)
        actual_cols = sparse_reduced.shape[1]
        sparse_64[:, :actual_cols] = sparse_reduced

        combined = np.concatenate([dense, sparse_64], axis=1)  # (N, 832)

        # L2-normalize the combined vector (concatenation breaks unit norm)
        norms = np.linalg.norm(combined, axis=1, keepdims=True)
        norms = np.where(norms == 0.0, 1.0, norms)
        combined = (combined / norms).astype(np.float32)
        return combined

    def embed_single(self, text: str) -> np.ndarray:
        """Embed a single text. Returns shape (dimension,)."""
        return self.embed([text])[0]

    def dimension(self) -> int:
        """Return the embedding dimension: 768 (dense) or 832 (hybrid)."""
        return 768 if self._mode == "dense" else 832
