# PBI Orchestrator Architecture

## Overview

The PBI Orchestrator is a TypeScript-based workflow orchestration system built on Temporal OSS. It provides safe, reliable orchestration of PBI (Product Backlog Item) workflows with built-in concurrency control.

## Design Goals

1. **Concurrent Execution Control**: Implement a 2-stack system that allows only 2 PBI workflows to execute concurrently
2. **Reliability**: Leverage Temporal's built-in reliability features for fault-tolerant workflow execution
3. **Testability**: Clear separation between unit tests and system/E2E tests
4. **CI/CD Integration**: Automated testing and auto-merge to main upon successful test completion
5. **Security**: No direct pushes to main branch; all changes go through PR and automated validation

## Architecture Components

### 1. Workflows (`src/workflows.ts`)

The main workflow orchestration logic:

- **pbiWorkflow**: Main workflow that executes PBI processing with concurrency control
- Acquires concurrency slot before execution
- Releases slot after completion (even on error)
- Returns detailed execution results

### 2. Activities (`src/activities.ts`)

Discrete units of work executed by workers:

- **acquireLock**: Acquires a concurrency slot
- **releaseLock**: Releases a concurrency slot
- **processPBI**: Executes the actual PBI processing logic

### 3. Concurrency Manager (`src/concurrency-manager.ts`)

Manages the 2-stack concurrency control:

- Implements a semaphore-based locking mechanism
- Ensures only 2 workflows execute concurrently
- Blocks additional workflows until a slot becomes available

### 4. Client (`src/client.ts`)

API for submitting and managing workflows:

- **PBIOrchestrator**: Main client class
- `submitPBIWorkflow()`: Submit a new workflow for execution
- `getPBIWorkflowResult()`: Query workflow execution results

### 5. Worker (`src/worker.ts`)

Temporal worker that executes workflows and activities:

- Connects to Temporal server
- Registers workflows and activities
- Processes tasks from the task queue

## Data Flow

```
Client → Submit Workflow → Temporal Server → Task Queue
                                               ↓
                                            Worker
                                               ↓
                                   Acquire Concurrency Slot
                                               ↓
                                        Execute PBI
                                               ↓
                                   Release Concurrency Slot
                                               ↓
                                         Return Result
```

## Concurrency Control

The 2-stack concurrency system is implemented using an in-memory semaphore:

1. When a workflow starts, it attempts to acquire a slot
2. If fewer than 2 workflows are running, the slot is acquired immediately
3. If 2 workflows are already running, the new workflow blocks until a slot is released
4. When a workflow completes (or fails), it releases its slot
5. Waiting workflows are unblocked in FIFO order

**Note**: In production, the in-memory semaphore should be replaced with a distributed locking mechanism (e.g., Redis, etcd) to support multi-worker deployments.

## Testing Strategy

### Unit Tests (`tests/unit/`)

- Test individual components in isolation
- No external dependencies (Temporal server not required)
- Fast execution, suitable for CI/CD

### System/E2E Tests (`tests/system/`)

- Test full workflow execution with Temporal server
- Verify concurrency control behavior
- Test error handling and retry logic
- **Executed by Runner** (not AI)

## Configuration

Configuration is centralized in `src/config.ts`:

- `maxConcurrentWorkflows`: Set to 2 for 2-stack system
- `taskQueue`: Task queue name for worker registration
- `workflowTimeout`: Maximum workflow execution time
- `retryPolicy`: Automatic retry configuration

## Deployment Considerations

1. **Temporal Server**: Requires a running Temporal server instance
2. **Workers**: Deploy worker processes to execute workflows
3. **Clients**: Deploy client applications to submit workflows
4. **Monitoring**: Use Temporal Web UI for workflow monitoring
5. **Scaling**: Multiple workers can be deployed (with distributed locking for concurrency control)

## Future Enhancements

1. Distributed locking mechanism for production environments
2. Metrics and monitoring integration
3. Advanced workflow patterns (child workflows, signals, queries)
4. Dynamic concurrency limits based on system load
5. Priority-based workflow scheduling
