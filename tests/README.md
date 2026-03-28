# Tests

## Test runner

Backend tests use a custom minimal runner (no pytest dependency). Two helper functions:

```python
def S(section_name):
    """Print a section header."""

def T(test_name, condition, details=""):
    """Assert condition is truthy; print PASS/FAIL with details."""
```

## Running tests

Always use `-X utf8` on Windows to avoid encoding errors:

```bash
python -X utf8 tests/test_c1_api.py
python -X utf8 tests/test_c2_api.py
```

## Naming conventions

- `test_c{N}_api.py` — Customer-facing API tests
- `test_d{N}_api.py` — Data pipeline tests
- `test_f{N}_api.py` — Feature tests
- `test_signal_p{N}.py` — Signal pipeline validation
- `test_v3_phase{N}.py` — V3 signal redesign tests
- `test_*_audit.py` — Audit/quality pass tests
- `test_*_integration.py` — End-to-end integration tests
