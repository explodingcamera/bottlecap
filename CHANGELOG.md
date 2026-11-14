# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Borrowed queries for keyed checks, peeks, remaining requests, and reset.
  `KeyedLimiter<String>` accepts `&str` without creating a `String` unless inserting
  a new key.
- `IpLimiterLayer` aliases in the Tower and Actix Web integrations for per-IP layers.
- Automatic incremental cleanup of fully replenished keys during missing-key checks,
  enabled by default. Existing-key checks do not trigger cleanup.
- `KeyedLimiter::with_auto_prune(false)` to disable proactive cleanup while retaining
  manual pruning and cleanup when the key storage limit is full.
- `KeyedLimiter::prune_partial()` and `prune_partial_at()` to run one rotating
  cleanup batch explicitly.

## [v0.1.0] - 2026-11-13

- Initial release
