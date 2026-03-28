# Contributing

Thanks for your interest in contributing to the Narrative Intelligence Engine template.

## Getting started

1. Fork the repo
2. Create a feature branch: `git checkout -b my-feature`
3. Make your changes
4. Run the tests: `python -X utf8 tests/test_c1_api.py`
5. Commit and push
6. Open a pull request

## Code style

- Backend is pure Python, no linter enforced. Keep it readable.
- Frontend is Next.js + TypeScript + Tailwind. Run `npx tsc --noEmit` before submitting.

## Tests

Backend tests use a custom minimal runner (no pytest). See `tests/README.md` for details.

Frontend tests use Jest + @testing-library/react. Run with `cd frontend && npx jest --watchAll=false`.

## What to contribute

- New ingester adapters (RSS, API, or scraper)
- Signal computation modules
- Frontend components and pages
- Documentation improvements
- Bug fixes
