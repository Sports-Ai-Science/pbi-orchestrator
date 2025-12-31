#!/bin/bash
# Run E2E tests with Temporal server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== PBI Orchestrator E2E Test Runner ==="
echo ""

# Start Temporal server
echo "1. Starting Temporal server..."
"$SCRIPT_DIR/start-temporal.sh"

# Wait for server to be fully ready
echo ""
echo "2. Waiting for Temporal server to be fully ready..."
sleep 15

# Build the project
echo ""
echo "3. Building project..."
cd "$PROJECT_ROOT"
npm run build

# Run unit tests first
echo ""
echo "4. Running unit tests..."
npm run test:unit

# Start worker in background
echo ""
echo "5. Starting Temporal worker..."
node dist/worker.js &
WORKER_PID=$!
echo "Worker started with PID: $WORKER_PID"

# Wait for worker to initialize
sleep 5

# Run system tests
echo ""
echo "6. Running system/E2E tests..."
npm run test:system

# Cleanup
echo ""
echo "7. Cleaning up..."
kill $WORKER_PID 2>/dev/null || true
"$SCRIPT_DIR/stop-temporal.sh"

echo ""
echo "=== E2E Tests Completed Successfully ==="
