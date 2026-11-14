# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.2.0] - 2026-11-14

### Added

- Added automatic idle-key cleanup, enabled by default. Opt out with `with_auto_prune(false)`.
- Added `prune_partial()` and `prune_partial_at()` for manual incremental cleanup.
- Keyed checks, peeks, remaining requests, and reset now accept `&str` for `String` keys.
- Added Header-based rate limiting with `by_header()`, `HeaderLimiterLayer` and `IpLimiterLayer` aliases

## [v0.1.0] - 2026-11-13

- Initial release
