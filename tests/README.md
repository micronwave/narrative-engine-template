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

## Test suite

### Customer API tests (`test_c{N}_api.py`)

| File | Tests |
|------|-------|
| `test_c1_api.py` | Narratives structure, ticker structure, health endpoint |
| `test_c2_api.py` | Velocity timeseries, narrative detail, constellation, credits |
| `test_c3_api.py` | Credits/use, SSE stream endpoint |
| `test_c4_api.py` | Subscription, export, signals, ticker IDs |

### Data pipeline tests (`test_d{N}_api.py`)

| File | Tests |
|------|-------|
| `test_d1_api.py` | Asset classes, securities, narrative assets |
| `test_d2_api.py` | Finnhub integration (quote, cache, rate limiter) |
| `test_d3_api.py` | Narrative impact scores, stocks API sorting/filtering |
| `test_d4_api.py` | Manipulation/coordination detection |
| `test_d5_api.py` | Analytics endpoints (histories, momentum, overlap, convergence) |
| `test_d6_api.py` | Data normalization, circuit breaker |
| `test_d7_api.py` | WebSocket relay |
| `test_d8_api.py` | Additional data provider adapters |
| `test_d9_api.py` | Rate limiting and API usage tracking |

### Feature tests (`test_f{N}_api.py`)

| File | Tests |
|------|-------|
| `test_f1_api.py` | Lifecycle stage progression |
| `test_f2_api.py` | Pipeline frequency, burst velocity |
| `test_f3_api.py` | Pre-earnings intelligence brief |
| `test_f4_api.py` | Topic tagging |
| `test_f5_api.py` | Historical snapshots, price data |
| `test_f6_api.py` | Velocity-price correlation |
| `test_f7_api.py` | Audit fix verification (ns_score clamping, HDBSCAN settings) |
| `test_f8_dormant_filter.py` | Dormant narrative exclusion from active listings |

### Signal redesign tests (`test_v3_phase{N}.py`)

| File | Tests |
|------|-------|
| `test_v3_phase1.py` | Auth scaffolding, narrative detail enrichment, coordination, sources |
| `test_v3_phase2.py` | Portfolio, timeline, compare endpoints |
| `test_v3_phase3.py` | Reddit ingester, public interest, earnings, sentiment |
| `test_v3_phase4.py` | WAL mode, performance indexes, schema columns, smoke tests |

### Signal pipeline validation (`test_signal_p{N}.py`)

| File | Tests |
|------|-------|
| `test_signal_p1.py` | LLM structured signal extraction (converters, validators, parser, repo) |
| `test_signal_p2.py` | Source tier tracking (domain tiers, escalation, weighted scores) |
| `test_signal_p3.py` | Convergence detection |
| `test_signal_p4.py` | Catalyst anchoring |
| `test_signal_p5.py` | Learned signal weights |
| `test_signal_p6.py` | End-to-end signal pipeline integration |

### Security tests (`test_sec_s{N}.py`)

| File | Tests |
|------|-------|
| `test_sec_s1a.py` | Rate limiting, singleton thread pool |
| `test_sec_s1b.py` | Input validation, SQL injection prevention |
| `test_sec_s1c.py` | Auth token handling, CORS configuration |
| `test_sec_s2a.py` | Safe pickle deserialization |
| `test_sec_s2b.py` | File upload validation, path traversal |
| `test_sec_s2c.py` | LLM output sanitization |
| `test_sec_s3.py` | API key masking, error message sanitization |
| `test_sec_s4.py` | Session management, CSRF protection |
| `test_sec_s4b.py` | Additional security hardening |
| `test_sec_s5.py` | Security batch 5 tests |
| `test_sec_s5c.py` | Security batch 5 continuation |

### Integration tests

| File | Tests |
|------|-------|
| `test_integration.py` | Phase 1 & 2 integration |
| `test_phase3_integration.py` | Clustering, signals, centrality, adversarial |
| `test_phase4_integration.py` | LLM labeling, asset mapping, output |
| `test_phase5_integration.py` | Pipeline orchestration |
| `test_full_integration.py` | End-to-end all phases |

### Other tests

| File | Tests |
|------|-------|
| `test_pipeline_audit.py` | Pipeline edge cases, settings validation |
| `test_charting_api.py` | OHLCV price history, interval/period params |
| `test_sentiment_api.py` | Social sentiment system, spike detection |
| `test_portfolio_alerts.py` | Portfolio analytics, alert system |
| `test_cat4_api.py` | Category 4 API integration (Edgar, data normalization) |
| `test_foundation_fixes.py` | Foundation fix verification (stage hysteresis, filters) |

## Naming conventions

- `test_c{N}_api.py` — Customer-facing API tests
- `test_d{N}_api.py` — Data pipeline tests
- `test_f{N}_api.py` — Feature tests
- `test_signal_p{N}.py` — Signal pipeline validation
- `test_v3_phase{N}.py` — V3 signal redesign tests
- `test_sec_s{N}.py` — Security hardening tests
- `test_*_integration.py` — End-to-end integration tests
