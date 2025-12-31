# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2024-12-30

### Added
- Initial project setup with TypeScript and Temporal SDK
- 2-stack concurrency control system for PBI workflows
- PBI workflow orchestration with retry logic
- Temporal client for workflow submission
- Temporal worker for workflow execution
- Unit test suite with Jest
- System/E2E test infrastructure
- GitHub Actions CI/CD pipeline
- Auto-merge workflow for approved PRs
- Branch protection workflow
- Docker Compose setup for Temporal server
- Runner scripts for E2E testing
- Comprehensive documentation (README, ARCHITECTURE, DEVELOPMENT)
- Code quality tools (ESLint, Prettier)
- Apache-2.0 license

### Security
- Branch protection preventing direct pushes to main
- npm audit integration in CI
- TypeScript strict mode enabled

[Unreleased]: https://github.com/munyamunya/pbi-orchestrator/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/munyamunya/pbi-orchestrator/releases/tag/v0.1.0
