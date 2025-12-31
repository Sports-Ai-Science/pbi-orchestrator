# Development Guide

## Prerequisites

- Node.js >= 18.0.0
- npm or yarn
- Docker (for running Temporal server locally)
- Git

## Setup

1. Clone the repository:
```bash
git clone https://github.com/munyamunya/pbi-orchestrator.git
cd pbi-orchestrator
```

2. Install dependencies:
```bash
npm install
```

3. Start Temporal server (using Docker):
```bash
./scripts/start-temporal.sh
```

## Building

Build the TypeScript code:
```bash
npm run build
```

Clean build artifacts:
```bash
npm run clean
```

## Testing

Run all tests:
```bash
npm test
```

Run unit tests only:
```bash
npm run test:unit
```

Run system/E2E tests (requires Temporal server):
```bash
npm run test:system
```

## Code Quality

Lint code:
```bash
npm run lint
```

Auto-fix linting issues:
```bash
npm run lint:fix
```

Format code:
```bash
npm run format
```

Check formatting:
```bash
npm run format:check
```

## Running the Orchestrator

### Start a Worker

```bash
npm run dev
```

Or using the compiled code:
```bash
node dist/worker.js
```

### Submit a Workflow (Example)

```typescript
import { PBIOrchestrator } from './src/client';

async function main() {
  const orchestrator = new PBIOrchestrator();
  await orchestrator.connect();

  const workflowId = await orchestrator.submitPBIWorkflow({
    pbiId: 'PBI-001',
    pbiName: 'Example PBI',
    parameters: {
      environment: 'development',
      priority: 'high',
    },
  });

  console.log(`Workflow submitted: ${workflowId}`);

  const result = await orchestrator.getPBIWorkflowResult(workflowId);
  console.log('Workflow result:', result);

  await orchestrator.close();
}

main().catch(console.error);
```

## Project Structure

```
pbi-orchestrator/
├── src/                    # Source code
│   ├── index.ts           # Main entry point
│   ├── config.ts          # Configuration
│   ├── workflows.ts       # Workflow definitions
│   ├── activities.ts      # Activity implementations
│   ├── concurrency-manager.ts  # Concurrency control
│   ├── client.ts          # Temporal client
│   └── worker.ts          # Temporal worker
├── tests/                 # Test files
│   ├── unit/             # Unit tests
│   └── system/           # System/E2E tests
├── docs/                  # Documentation
├── scripts/              # Utility scripts
└── dist/                 # Compiled JavaScript (generated)
```

## Git Workflow

1. Create a feature branch:
```bash
git checkout -b feature/my-feature
```

2. Make changes and commit:
```bash
git add .
git commit -m "Description of changes"
```

3. Push and create PR:
```bash
git push origin feature/my-feature
```

4. CI will run tests automatically
5. If tests pass, auto-merge to main is triggered

**Important**: Direct pushes to `main` are disabled. All changes must go through PRs.

## Debugging

Enable debug logging:
```bash
export TEMPORAL_LOG_LEVEL=debug
npm run dev
```

## Environment Variables

- `TEMPORAL_ADDRESS`: Temporal server address (default: `localhost:7233`)
- `TEMPORAL_LOG_LEVEL`: Logging level (default: `info`)

## Contributing

1. Ensure all tests pass
2. Maintain code coverage above 80%
3. Follow TypeScript best practices
4. Update documentation as needed
5. Add tests for new features

## Troubleshooting

### Cannot connect to Temporal server

Ensure Temporal server is running:
```bash
docker ps | grep temporal
```

### Tests failing

Clear build artifacts and reinstall:
```bash
npm run clean
rm -rf node_modules
npm install
npm test
```

### Type errors

Ensure TypeScript is compiled:
```bash
npm run build
```
