# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PBI Orchestrator is a TypeScript-based workflow orchestration system built on Temporal OSS for safe PBI (Product Backlog Item) execution with 2-stack concurrency control (max 2 concurrent workflows).

## Essential Commands

```bash
# Build
npm run build          # TypeScript compilation
npm run clean          # Remove dist/

# Test
npm test               # All tests
npm run test:unit      # Unit tests only (no Temporal needed)
npm run test:system    # E2E tests (requires Temporal server)

# Development
npm run dev            # Start worker (ts-node)
./scripts/start-temporal.sh   # Start Temporal server (Docker)
./scripts/stop-temporal.sh    # Stop Temporal server

# Code Quality
npm run lint           # ESLint
npm run lint:fix       # Auto-fix lint issues
npm run format         # Prettier format
npm run format:check   # Check formatting
```

## Architecture

### Core Components (src/)

- **workflows.ts**: Main `pbiWorkflow` - acquires concurrency slot, executes PBI, releases slot
- **activities.ts**: Activity implementations (`acquireLock`, `releaseLock`, `processPBI`) with in-memory semaphore
- **concurrency-manager.ts**: Wrapper that proxies lock activities to Temporal runtime
- **client.ts**: `PBIOrchestrator` class for workflow submission/result retrieval
- **worker.ts**: Temporal worker registration
- **config.ts**: Configuration types and `DEFAULT_PBI_CONFIG`

### Data Flow

```
Client.submitPBIWorkflow() → Temporal Server → Worker
  → ConcurrencyManager.acquireSlot() [blocks if 2 already running]
  → activities.processPBI()
  → ConcurrencyManager.releaseSlot()
  → return PBIWorkflowResult
```

### Extended Types (src/types/)

`pbi.types.ts` contains rich types for the full PBI lifecycle: `ParsedPBI`, `GeneratedCode`, `FileChange`, `CreatePRRequest`, etc. These support GitHub integration and LLM-based code generation.

## Temporal Patterns

- **Workflow determinism**: Workflows use `proxyActivities` to call activities - keep workflow code deterministic
- **Activity retries**: Configured with exponential backoff (1s initial, 2x coefficient, 3 max attempts)
- **Slot acquisition**: Workflows block via `ConcurrencyManager.acquireSlot()` until under the 2-workflow limit

## Testing Strategy

- **Unit tests** (`tests/unit/`): Test isolated logic, no Temporal dependency
- **System/E2E tests** (`tests/system/`): Full workflow execution with Temporal server
- **Coverage threshold**: 80% (branches, functions, lines, statements)

## Environment Variables

```bash
TEMPORAL_ADDRESS=localhost:7233    # Temporal gRPC endpoint
ANTHROPIC_API_KEY=...              # Claude API (for code generation features)
GITHUB_TOKEN=...                   # GitHub PAT for PR creation
```

## Git Workflow

- **No direct pushes to main** - all changes via PRs
- CI runs: lint → build → unit tests → system tests (with Temporal)
- Auto-merge enabled after all checks pass
