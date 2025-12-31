# PBI Orchestrator

[![CI](https://github.com/munyamunya/pbi-orchestrator/workflows/CI/badge.svg)](https://github.com/munyamunya/pbi-orchestrator/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

安全なPBIワークフローオーケストレーションシステム / A safe PBI workflow orchestration system built on Temporal OSS.

## Overview

PBI Orchestrator は、Temporal OSS 上で PBI (Product Backlog Item) 単位のワークフローを安全にオーケストレーションするためのシステムです。2スタック制（同時実行数2）による中央制御を実装し、unit + system (E2E) テストが通過した後に GitHub の Auto-merge 機能で main ブランチに統合されます。

**Key Features:**
- 🔒 **2-Stack Concurrency Control**: Maximum 2 concurrent workflow executions
- ⚡ **Temporal OSS**: Built on proven workflow orchestration technology
- 🧪 **Comprehensive Testing**: Unit and system/E2E test coverage
- 🤖 **CI/CD Integration**: Automated testing and auto-merge to main
- 🔐 **Branch Protection**: No direct pushes to main; all changes via PRs
- 📝 **TypeScript**: Type-safe implementation with Temporal SDK

## Architecture

The orchestrator consists of several key components:

1. **Workflows**: Define the PBI execution logic with concurrency control
2. **Activities**: Discrete units of work (lock management, PBI processing)
3. **Concurrency Manager**: 2-stack semaphore-based concurrency control
4. **Client**: API for submitting and managing PBI workflows
5. **Worker**: Executes workflows and activities

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quick Start

### Prerequisites

- Node.js >= 18.0.0
- Docker (for running Temporal server)
- npm or yarn

### Installation

```bash
# Clone the repository
git clone https://github.com/munyamunya/pbi-orchestrator.git
cd pbi-orchestrator

# Install dependencies
npm install

# Build the project
npm run build
```

### Running Temporal Server

```bash
# Start Temporal server with Docker
./scripts/start-temporal.sh

# Temporal Web UI will be available at http://localhost:8080
```

### Running the Worker

```bash
# Start a worker to execute workflows
npm run dev
```

### Submitting a Workflow

```typescript
import { PBIOrchestrator } from 'pbi-orchestrator';

async function main() {
  const orchestrator = new PBIOrchestrator();
  await orchestrator.connect();

  const workflowId = await orchestrator.submitPBIWorkflow({
    pbiId: 'PBI-001',
    pbiName: 'Example PBI',
    parameters: {
      environment: 'production',
    },
  });

  console.log(`Workflow started: ${workflowId}`);

  const result = await orchestrator.getPBIWorkflowResult(workflowId);
  console.log('Result:', result);

  await orchestrator.close();
}

main().catch(console.error);
```

## Testing

```bash
# Run all tests
npm test

# Run unit tests only
npm run test:unit

# Run system/E2E tests (requires Temporal server)
npm run test:system

# Run E2E tests with runner script
./scripts/run-e2e-tests.sh
```

## Development

```bash
# Lint code
npm run lint

# Format code
npm run format

# Clean build artifacts
npm run clean
```

For detailed development guide, see [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## CI/CD Workflow

The project uses GitHub Actions for continuous integration:

1. **Lint**: Code quality checks with ESLint and Prettier
2. **Build**: TypeScript compilation
3. **Unit Tests**: Fast, isolated tests
4. **System/E2E Tests**: Full workflow tests with Temporal server
5. **Auto-merge**: Automatic merge to main if all checks pass

### Branch Protection

- ❌ Direct pushes to `main` are **disabled**
- ✅ All changes must go through pull requests
- ✅ CI must pass before merge
- ✅ Auto-merge enabled for approved PRs

### AI Agent Permissions

- ✅ AI can commit, push, and create PRs
- ❌ AI cannot push directly to main
- ✅ AI execution proposals only
- ✅ Docker/E2E tests handled by Runner

## Project Structure

```
pbi-orchestrator/
├── src/                      # Source code
│   ├── index.ts             # Main entry point
│   ├── config.ts            # Configuration
│   ├── workflows.ts         # Workflow definitions
│   ├── activities.ts        # Activity implementations
│   ├── concurrency-manager.ts  # Concurrency control
│   ├── client.ts            # Temporal client
│   └── worker.ts            # Temporal worker
├── tests/                   # Test files
│   ├── unit/               # Unit tests
│   └── system/             # System/E2E tests
├── docs/                    # Documentation
│   ├── ARCHITECTURE.md     # Architecture overview
│   └── DEVELOPMENT.md      # Development guide
├── scripts/                # Utility scripts
│   ├── start-temporal.sh   # Start Temporal server
│   ├── stop-temporal.sh    # Stop Temporal server
│   ├── run-e2e-tests.sh    # E2E test runner
│   └── docker-compose.temporal.yml  # Temporal Docker setup
└── .github/workflows/      # GitHub Actions CI/CD
    ├── ci.yml              # Main CI pipeline
    ├── auto-merge.yml      # Auto-merge workflow
    └── branch-protection.yml  # Branch protection
```

## Configuration

Configuration is centralized in `src/config.ts`:

- `maxConcurrentWorkflows`: 2 (2-stack system)
- `taskQueue`: 'pbi-orchestrator-queue'
- `workflowTimeout`: 3600000 (1 hour)
- `retryPolicy`: Automatic retry with exponential backoff

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

All contributions must:
- Pass all CI checks (lint, build, unit tests, E2E tests)
- Maintain code coverage above 80%
- Include appropriate tests
- Follow the existing code style

## Support

For questions, issues, or feature requests, please open an issue on GitHub.

## Acknowledgments

Built with:
- [Temporal](https://temporal.io/) - Workflow orchestration engine
- [TypeScript](https://www.typescriptlang.org/) - Type-safe JavaScript
- [Jest](https://jestjs.io/) - Testing framework
- [ESLint](https://eslint.org/) - Code linting
- [Prettier](https://prettier.io/) - Code formatting