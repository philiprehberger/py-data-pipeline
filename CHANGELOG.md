# Changelog

## 0.6.0 (2026-06-15)

- Add `peek(n=5)` terminal that materializes the first `n` items for quick inspection during debugging
- Add `count_by(key)` terminal returning a per-key occurrence count
- Add package-card image to README

## 0.5.0 (2026-04-06)

- Add `enumerate(start)` to pair each item with its index
- Add `zip_with(other)` to merge two iterables item-by-item
- Add `take_while(predicate)` to take items while condition holds
- Add `skip_while(predicate)` to skip items while condition holds

## 0.4.0 (2026-04-01)

- Add `branch()` to split pipeline into parallel branches that merge results
- Add `tap()` step for side effects (logging, metrics) without altering data, skipped in dry-run mode
- Add `retry()` wrapper for individual steps with configurable retries, delay, and error callback
- Add pipeline composition via `+` operator to combine two pipelines into one
- Add `dry_run()` mode that logs each step's input/output without executing side effects

## 0.3.1 (2026-03-31)

- Standardize README to 3-badge format with emoji Support section
- Update CI checkout action to v5 for Node.js 24 compatibility

## 0.3.0 (2026-03-27)

- Add `window(size, step)` for sliding window grouping
- Add `deduplicate()` to remove duplicate items preserving order
- Add pytest and mypy configuration to pyproject.toml
- Add issue templates, PR template, and dependabot config
- Update README with full badges, Support section, and new feature docs

## 0.2.3 (2026-03-25)

- Add Development section to README

## 0.2.1 (2026-03-24)

- Remove undefined `AggregateResult` from `__all__`

## 0.2.0 (2026-03-23)

- Add input validation for `chunk()`, `take()`, and `skip()` with clear error messages
- Expand test suite with edge case and validation tests
- Add `min()`, `max()`, `reduce()` to README operations table

## 0.1.1 (2026-03-12)

- Add project URLs to pyproject.toml

## 0.1.0 (2026-03-10)

- Initial release
