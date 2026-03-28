import json
import logging
import re
from pathlib import Path

import numpy as np
from datasketch import MinHash, MinHashLSH

from ingester import RawDocument

logger = logging.getLogger(__name__)

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)


class Deduplicator:
    """
    LSH-based deduplication using MinHashLSH.

    # TODO SCALE: replace JSON persistence with Redis backend for multi-worker deployment
    """

    def __init__(self, threshold: float, num_perm: int, lsh_path: str) -> None:
        self._threshold = threshold
        self._num_perm = num_perm
        self._lsh_path = lsh_path
        self._lsh: MinHashLSH = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self._all_signatures: dict[str, list[int]] = {}
        self._batch_signatures: dict[str, MinHash] = {}

    # ------------------------------------------------------------------
    # Persistence (JSON — no pickle/arbitrary code execution)
    # ------------------------------------------------------------------

    def load(self) -> bool:
        """Load from disk. Returns False if file does not exist."""
        path = Path(self._lsh_path)
        if not path.exists():
            logger.info("LSH index not found at %s — starting fresh", self._lsh_path)
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("num_perm") != self._num_perm:
                raise ValueError(
                    f"LSH num_perm mismatch: file has {data.get('num_perm')}, "
                    f"config expects {self._num_perm}"
                )
            self._lsh = MinHashLSH(threshold=self._threshold, num_perm=self._num_perm)
            self._all_signatures = {}
            for doc_id, hashvals in data.get("signatures", {}).items():
                mh = MinHash(num_perm=self._num_perm)
                mh.hashvalues = np.array(hashvals, dtype=np.uint64)
                try:
                    self._lsh.insert(doc_id, mh)
                except ValueError:
                    pass
                self._all_signatures[doc_id] = hashvals
            logger.info("Loaded %d signatures from %s", len(self._all_signatures), self._lsh_path)
            return True
        except Exception as exc:
            logger.warning("LSH index unusable at %s (%s) — reinitializing", self._lsh_path, exc)
            path.unlink(missing_ok=True)
            self._lsh = MinHashLSH(threshold=self._threshold, num_perm=self._num_perm)
            self._all_signatures = {}
            return False

    def save(self) -> None:
        """Persist to disk via atomic temp-file rename."""
        data = {
            "threshold": self._threshold,
            "num_perm": self._num_perm,
            "signatures": {
                doc_id: [int(v) for v in hashvals]
                for doc_id, hashvals in self._all_signatures.items()
            },
        }
        tmp = self._lsh_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        Path(tmp).replace(self._lsh_path)
        logger.debug("Saved MinHashLSH (%d sigs) to %s", len(self._all_signatures), self._lsh_path)

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def get_signature(self, doc: RawDocument) -> MinHash:
        """Compute MinHash signature for a document using 3-word shingles."""
        m = MinHash(num_perm=self._num_perm)
        shingles = _extract_shingles(doc.raw_text)
        if not shingles:
            # Deterministic sentinel so all empty/whitespace-only docs
            # hash identically and are detected as duplicates of each other.
            m.update(b"__EMPTY_DOCUMENT__")
            return m
        for shingle in shingles:
            m.update(shingle.encode("utf-8"))
        return m

    def is_duplicate(self, doc: RawDocument) -> tuple[bool, MinHash]:
        """Check if document is a near-duplicate of anything in the index.
        Returns (is_dup, signature) so callers can pass the signature to
        add_with_signature() without recomputing it."""
        sig = self.get_signature(doc)
        try:
            result = self._lsh.query(sig)
        except ValueError:
            # LSH index is empty — datasketch may raise on query before any insert
            return False, sig
        return len(result) > 0, sig

    def add_with_signature(self, doc: RawDocument, sig: MinHash) -> None:
        """Add document to the index using a pre-computed signature."""
        try:
            self._lsh.insert(doc.doc_id, sig)
        except ValueError as exc:
            logger.debug("Skipping duplicate insert for doc_id=%s: %s", doc.doc_id, exc)
            return
        self._all_signatures[doc.doc_id] = [int(v) for v in sig.hashvalues]
        self._batch_signatures[doc.doc_id] = sig

    def add(self, doc: RawDocument) -> None:
        """Add document signature to the persistent index and the current batch."""
        self.add_with_signature(doc, self.get_signature(doc))

    def get_batch_signatures(self) -> dict[str, MinHash]:
        """Return signatures for documents ingested in the current batch only."""
        return dict(self._batch_signatures)

    def clear_batch(self) -> None:
        """Clear the current batch signature mapping."""
        self._batch_signatures.clear()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_shingles(text: str) -> set[str]:
    """Extract 3-word shingles from text after lowercasing and stripping punctuation.
    Returns empty set for blank/whitespace-only text so callers can skip hashing."""
    cleaned = _PUNCT_RE.sub("", text.lower())
    words = cleaned.split()
    if not words:
        return set()
    if len(words) < 3:
        return {" ".join(words)}
    return {
        f"{words[i]} {words[i + 1]} {words[i + 2]}"
        for i in range(len(words) - 2)
    }
