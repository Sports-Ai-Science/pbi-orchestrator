# Examples

This directory contains examples demonstrating how to use the PBI Orchestrator.

## Prerequisites

1. Install dependencies:
```bash
npm install
```

2. Build the project:
```bash
npm run build
```

3. Start Temporal server:
```bash
./scripts/start-temporal.sh
```

## Running Examples

### 1. Start a Worker

First, start a worker to execute workflows:

```bash
ts-node examples/worker.ts
```

Keep this running in one terminal.

### 2. Submit a Single Workflow

In another terminal, submit a workflow:

```bash
ts-node examples/submit-workflow.ts
```

This will:
- Connect to Temporal
- Submit a single PBI workflow
- Wait for completion
- Display the result

### 3. Test Concurrent Workflows

Submit multiple workflows to test concurrency control:

```bash
ts-node examples/concurrent-workflows.ts
```

This will:
- Submit 5 workflows simultaneously
- Demonstrate that only 2 execute concurrently
- Show execution times for each workflow
- Display a summary

## Example Outputs

### Single Workflow
```
=== PBI Orchestrator Example: Submit Workflow ===

✓ Connected to Temporal server

✓ Workflow submitted: pbi-PBI-001-1703952123456

Waiting for workflow to complete...

✓ Workflow completed!

Result: {
  "pbiId": "PBI-001",
  "status": "completed",
  "executionTime": 1234,
  "result": {
    "pbiId": "PBI-001",
    "pbiName": "Example PBI Workflow",
    "processedAt": "2024-12-30T12:00:00.000Z"
  }
}

✓ Connection closed
```

### Concurrent Workflows
```
=== PBI Orchestrator Example: Concurrent Workflows ===

✓ Connected to Temporal server

Submitting workflow 1...
Submitting workflow 2...
Submitting workflow 3...
Submitting workflow 4...
Submitting workflow 5...
✓ Workflow 1 submitted: pbi-PBI-001-1703952123456
✓ Workflow 2 submitted: pbi-PBI-002-1703952123457
✓ Workflow 3 submitted: pbi-PBI-003-1703952123458
✓ Workflow 4 submitted: pbi-PBI-004-1703952123459
✓ Workflow 5 submitted: pbi-PBI-005-1703952123460

✓ All 5 workflows submitted

Note: Only 2 workflows execute concurrently due to 2-stack control

Waiting for all workflows to complete...

✓ Workflow 1 completed (1234ms)
✓ Workflow 2 completed (1235ms)
✓ Workflow 3 completed (2456ms)
✓ Workflow 4 completed (2457ms)
✓ Workflow 5 completed (3678ms)

=== Summary ===
Total workflows: 5
Successful: 5
Failed: 0
Average execution time: 2212ms

✓ Connection closed
```

## Customization

You can modify the examples to:
- Change PBI parameters
- Adjust concurrency limits
- Add error handling scenarios
- Test different workflow patterns

## Troubleshooting

If examples fail:

1. Ensure Temporal server is running:
```bash
docker ps | grep temporal
```

2. Check worker is running:
- Worker should be started before submitting workflows

3. Check connection:
- Default: `localhost:7233`
- Set `TEMPORAL_ADDRESS` environment variable if different
