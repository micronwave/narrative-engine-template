import logging
import pickle
from abc import ABC, abstractmethod
from pathlib import Path

from safe_pickle import safe_load

import faiss
import numpy as np

logger = logging.getLogger(__name__)


class VectorStore(ABC):
    """
    Abstract interface for vector storage operations.
    MVP implementation: FaissVectorStore using IndexFlatIP.

    # TODO SCALE: swap FaissVectorStore for PgVectorStore or PineconeVectorStore when moving to AWS
    """

    @abstractmethod
    def add(self, vectors: np.ndarray, ids: list[str]) -> None:
        """Add vectors with associated IDs to the index."""
        ...

    @abstractmethod
    def search(
        self, query_vector: np.ndarray, k: int
    ) -> tuple[np.ndarray, list[str]]:
        """
        Search for k nearest neighbors.
        Returns: (distances, ids)
        Must handle empty index: return (np.array([]), [])
        """
        ...

    @abstractmethod
    def update(self, doc_id: str, new_vector: np.ndarray) -> None:
        """Update the vector for an existing ID."""
        ...

    @abstractmethod
    def delete(self, doc_id: str) -> None:
        """Remove a vector by ID."""
        ...

    @abstractmethod
    def save(self) -> None:
        """Persist the index to FAISS_INDEX_PATH."""
        ...

    @abstractmethod
    def load(self) -> bool:
        """
        Load the index from FAISS_INDEX_PATH.
        Returns False if file does not exist (do not raise).
        """
        ...

    @abstractmethod
    def count(self) -> int:
        """Return the number of vectors in the index."""
        ...

    @abstractmethod
    def get_vector(self, doc_id: str) -> np.ndarray | None:
        """Retrieve a vector by ID, or None if not found."""
        ...

    @abstractmethod
    def get_all_ids(self) -> list[str]:
        """Return all IDs in the index."""
        ...

    @abstractmethod
    def initialize(self, dimension: int) -> None:
        """Initialize a fresh empty index with the given dimension."""
        ...

    @abstractmethod
    def is_empty(self) -> bool:
        """Return True if the index contains zero vectors."""
        ...


class FaissVectorStore(VectorStore):
    # TODO SCALE: swap FaissVectorStore for PgVectorStore or PineconeVectorStore when moving to AWS

    def __init__(self, index_path: str) -> None:
        self._index_path = index_path
        self.index: faiss.IndexFlatIP | None = None
        self.id_to_index: dict[str, int] = {}
        self.index_to_id: list[str] = []

    def initialize(self, dimension: int) -> None:
        """Initialize a fresh empty IndexFlatIP with the given dimension."""
        self.index = faiss.IndexFlatIP(dimension)
        assert isinstance(self.index, faiss.IndexFlatIP)
        self.id_to_index = {}
        self.index_to_id = []
        logger.info("Initialized fresh FAISS IndexFlatIP with dimension=%d", dimension)

    def load(self) -> bool:
        """Load index from disk. Returns False if file does not exist.
        Handles corrupt files by cleaning up and returning False."""
        path = Path(self._index_path)
        if not path.exists():
            return False
        try:
            loaded = safe_load(str(path), allowed={
                "builtins": {"dict", "list", "tuple", "str", "int", "float", "bool"},
                "numpy": {"ndarray", "dtype", "float64", "float32", "int64"},
                "numpy.core.multiarray": {"scalar", "_reconstruct"},
                "numpy._core.multiarray": {"scalar", "_reconstruct"},
                # FAISS index types — discover via logging technique in safe_pickle.py
            })
            if not isinstance(loaded, tuple) or len(loaded) != 3:
                raise TypeError(
                    f"Expected 3-tuple, got {type(loaded).__name__}"
                )
            idx, id_map, id_list = loaded
            if not isinstance(id_map, dict) or not isinstance(id_list, list):
                raise TypeError(
                    f"Bad inner types: id_map={type(id_map).__name__}, "
                    f"id_list={type(id_list).__name__}"
                )
            self.index, self.id_to_index, self.index_to_id = idx, id_map, id_list
            logger.info(
                "Loaded FAISS index from %s (%d vectors)", self._index_path, self.count()
            )
            return True
        except Exception as exc:
            logger.warning(
                "FAISS pickle unusable at %s (%s) — reinitializing",
                self._index_path, exc,
            )
            path.unlink(missing_ok=True)
            self.index = None
            self.id_to_index = {}
            self.index_to_id = []
            return False

    def save(self) -> None:
        """Persist the index and ID mappings to disk via atomic temp-file rename."""
        tmp = self._index_path + ".tmp"
        with open(tmp, "wb") as f:
            pickle.dump((self.index, self.id_to_index, self.index_to_id), f)
        Path(tmp).replace(self._index_path)
        logger.info(
            "Saved FAISS index to %s (%d vectors)", self._index_path, self.count()
        )

    def add(self, vectors: np.ndarray, ids: list[str]) -> None:
        """Add vectors with associated IDs to the index.
        Validates vector/ID count match and skips duplicate IDs."""
        vectors = np.array(vectors, dtype=np.float32)
        if vectors.ndim == 1:
            vectors = vectors.reshape(1, -1)
        if vectors.shape[0] != len(ids):
            raise ValueError(
                f"Vector count ({vectors.shape[0]}) does not match "
                f"ID count ({len(ids)})"
            )

        # Build mask to filter out duplicate IDs while keeping vectors aligned
        keep = []
        for doc_id in ids:
            if doc_id in self.id_to_index:
                logger.debug("Skipping duplicate ID %s in add()", doc_id)
                keep.append(False)
            else:
                keep.append(True)

        if not any(keep):
            return

        filtered_ids = [d for d, k in zip(ids, keep) if k]
        filtered_vectors = vectors[keep]

        # Assign sequential positions BEFORE adding to the FAISS index
        for doc_id in filtered_ids:
            self.id_to_index[doc_id] = len(self.index_to_id)
            self.index_to_id.append(doc_id)
        self.index.add(filtered_vectors)

    def search(
        self, query_vector: np.ndarray, k: int
    ) -> tuple[np.ndarray, list[str]]:
        """Search for k nearest neighbors. Returns (distances, ids)."""
        if self.is_empty() or k <= 0:
            return (np.array([]), [])
        query = np.array(query_vector, dtype=np.float32).reshape(1, -1)
        k_actual = min(k, self.count())
        distances, indices = self.index.search(query, k_actual)
        valid_ids: list[str] = []
        for idx in indices[0]:
            if idx == -1 or idx >= len(self.index_to_id):
                continue
            valid_ids.append(self.index_to_id[idx])
        return (distances[0][: len(valid_ids)], valid_ids)

    def update(self, doc_id: str, new_vector: np.ndarray) -> None:
        """Update the vector for an existing ID by rebuilding the index."""
        if doc_id not in self.id_to_index:
            logger.warning("update() called for unknown id=%s — skipped", doc_id)
            return
        new_vector = np.array(new_vector, dtype=np.float32)
        dim = self.index.d
        all_ids = list(self.index_to_id)
        all_vectors = []
        for i, vid in enumerate(all_ids):
            if vid == doc_id:
                all_vectors.append(new_vector)
            else:
                all_vectors.append(self.index.reconstruct(i))
        self.index = faiss.IndexFlatIP(dim)
        self.id_to_index = {}
        self.index_to_id = []
        if all_vectors:
            self.add(np.array(all_vectors, dtype=np.float32), all_ids)

    def delete(self, doc_id: str) -> None:
        """Remove a vector by ID by rebuilding the index without it."""
        if doc_id not in self.id_to_index:
            logger.warning("delete() called for unknown id=%s — skipped", doc_id)
            return
        dim = self.index.d
        surviving_ids = [vid for vid in self.index_to_id if vid != doc_id]
        surviving_vectors = [
            self.index.reconstruct(i)
            for i, vid in enumerate(self.index_to_id)
            if vid != doc_id
        ]
        self.index = faiss.IndexFlatIP(dim)
        self.id_to_index = {}
        self.index_to_id = []
        if surviving_vectors:
            self.add(np.array(surviving_vectors, dtype=np.float32), surviving_ids)

    def count(self) -> int:
        """Return the number of vectors in the index."""
        return self.index.ntotal if self.index is not None else 0

    def get_vector(self, doc_id: str) -> np.ndarray | None:
        """Retrieve a vector by ID, or None if not found."""
        if doc_id not in self.id_to_index:
            return None
        pos = self.id_to_index[doc_id]
        return self.index.reconstruct(pos)

    def get_all_ids(self) -> list[str]:
        """Return all IDs currently in the index."""
        return list(self.index_to_id)

    def is_empty(self) -> bool:
        """Return True if the index contains zero vectors."""
        return self.count() == 0
