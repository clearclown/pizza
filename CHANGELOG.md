# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
自動更新は [release-please](https://github.com/googleapis/release-please) が担います。

## [Unreleased]

### Added
- Phase 0 Foundation scaffolding: documentation, proto contracts, polyglot monorepo layout.
- gRPC service definitions for Seed (M1), Kitchen (M2), Delivery (M3), BI (M4).
- Multi-LLM provider registry (Anthropic / OpenAI / Gemini) for Delivery module.
- TDD Red baseline tests for `internal/grid`, `internal/oven`, `internal/scoring`,
  and `services/delivery/tests/`.
- CI workflows: ci, buf, codeql, release-please, upstream-sync.
- OSS community files: LICENSE (MIT), CODE_OF_CONDUCT, CONTRIBUTING, SECURITY.

[Unreleased]: https://github.com/clearclown/pizza/compare/main...HEAD
