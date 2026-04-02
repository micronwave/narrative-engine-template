"""Safe pickle deserialization with configurable type allowlists."""
import pickle


class RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that only allows explicitly permitted classes."""

    def __init__(self, file, allowed: dict[str, set[str]]):
        super().__init__(file)
        self._allowed = allowed

    def find_class(self, module: str, name: str):
        allowed_names = self._allowed.get(module)
        if allowed_names and name in allowed_names:
            return super().find_class(module, name)
        raise pickle.UnpicklingError(
            f"Forbidden: {module}.{name}. Allowed: {self._allowed}"
        )


def safe_load(path: str, allowed: dict[str, set[str]]):
    """Load a pickle file with restricted class allowlist."""
    with open(path, "rb") as f:
        return RestrictedUnpickler(f, allowed).load()
