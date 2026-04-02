import logging
import pickle
import re
from pathlib import Path

from safe_pickle import safe_load

from datasketch import MinHash, MinHashLSH

from ingester import RawDocument

logger = logging.getLogger(__name__)

# Strip everything that isn't alphanumeric or whitespace
_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)


class Deduplicator:
    """
    LSH-based deduplication using MinHashLSH.

    # TODO SCALE: replace pickle persistence with Redis backend for MinHashLSH when moving to multi-worker deployment
    """

    def __init__(self, threshold: float, num_perm: int, lsh_path: str) -> None:
        """Initialize with LSH parameters and persistence path."""
        self._threshold = threshold
        self._num_perm = num_perm
        self._lsh_path = lsh_path
        self._lsh: MinHashLSH = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self._doc_id_to_minhash: dict[str, MinHash] = {}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load(self) -> bool:
        """Load from disk. Returns False if file does not exist (fresh init)."""
        path = Path(self._lsh_path)
        if not path.exists():
            logger.info("LSH index not found at %s — starting fresh", self._lsh_path)
            return False
        try:
            loaded = safe_load(str(path), allowed={
                "builtins": {"dict", "list", "tuple", "set", "str", "int", "float", "bool", "frozenset"},
                "datasketch.lsh": {"MinHashLSH"},
                "datasketch.minhash": {"MinHash"},
                "datasketch.hashfunc": {"sha1_hash32"},
                # May need additional datasketch internal classes
            })
            if not isinstance(loaded, MinHashLSH):
                raise TypeError(
                    f"Expected MinHashLSH, got {type(loaded).__name__}"
                )
            if loaded.h != self._num_perm:
                raise ValueError(
                    f"LSH num_perm mismatch: file has {loaded.h}, "
                    f"config expects {self._num_perm}"
                )
            self._lsh = loaded
            logger.info("Loaded MinHashLSH from %s", self._lsh_path)
            return True
        except Exception as exc:
            logger.warning(
                "LSH index unusable at %s (%s) — reinitializing", self._lsh_path, exc
            )
            path.unlink(missing_ok=True)
            self._lsh = MinHashLSH(threshold=self._threshold, num_perm=self._num_perm)
            return False

    def save(self) -> None:
        """Persist to disk via atomic temp-file rename."""
        tmp = self._lsh_path + ".tmp"
        with open(tmp, "wb") as f:
            pickle.dump(self._lsh, f)
        Path(tmp).replace(self._lsh_path)
        logger.debug("Saved MinHashLSH to %s", self._lsh_path)

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
            # doc_id already exists in index (idempotent re-add)
            logger.debug("Skipping duplicate insert for doc_id=%s: %s", doc.doc_id, exc)
            return
        self._doc_id_to_minhash[doc.doc_id] = sig

    def add(self, doc: RawDocument) -> None:
        """Add document signature to the persistent index and the current batch."""
        self.add_with_signature(doc, self.get_signature(doc))

    # ------------------------------------------------------------------
    # Batch management (used by adversarial detection in Phase 3)
    # ------------------------------------------------------------------

    def get_batch_signatures(self) -> dict[str, MinHash]:
        """Return signatures for documents ingested in the current batch."""
        return dict(self._doc_id_to_minhash)

    def clear_batch(self) -> None:
        """Clear the current batch signature mapping."""
        self._doc_id_to_minhash.clear()


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
