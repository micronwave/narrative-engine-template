# Test Matrix — Narrative Intelligence Platform

| Phase | Unit | Integration | Auth/E2E | Manual | Status |
|-------|------|-------------|----------|--------|--------|
| C1 | 3 | 2 | 0 | 2 | PASS |
| C2 | 5 | 2 | 2 | 2 | PASS |
| C3 | 6 | 4 | 0 | 2 | PASS |
| C4 | 8 | 4 | 0 | 3 | PASS |

## Automated Test Counts (actual)

| Suite | Runner | Tests | Status |
|-------|--------|-------|--------|
| `test_c2_api.py` | Python custom S/T | 91 | ✅ PASS |
| `test_c3_api.py` | Python custom S/T | 15 | ✅ PASS |
| `test_c4_api.py` | Python custom S/T | 40 | ✅ PASS |
| `c2.test.tsx` | Jest / React Testing Library | 5 | ✅ PASS |
| `c3.test.tsx` | Jest / React Testing Library | 17 | ✅ PASS |
| `c4.test.tsx` | Jest / React Testing Library | 13 | ✅ PASS |

**Total automated tests: 181 (146 backend + 35 frontend)**

## Manual Check Coverage

| Phase | Check | Description |
|-------|-------|-------------|
| C1 | C1-M1 | Ticker bar displays entries, updates on reload |
| C1 | C1-M2 | Blurred card CTA modal — appears/dismisses |
| C2 | C2-M1 | `/narrative/{id}` renders detail fields |
| C2 | C2-M2 | `/billing` credit top-up increments balance |
| C3 | C3-M1 | Tab → Investigate → drawer opens → Escape closes |
| C3 | C3-M2 | Ticker updates within 15s (polling) |
| C4 | C4-M1 | Coordination flag tooltip on `/signals` |
| C4 | C4-M2 | Credit top-up modal resembles Stripe checkout |
| C4 | C4-M3 | Font consistency (Inter body, Roboto Mono data) |
