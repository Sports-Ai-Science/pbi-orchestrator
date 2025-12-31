#!/bin/bash
# Start Temporal server using Docker Compose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Starting Temporal server..."

docker compose -f "$SCRIPT_DIR/docker-compose.temporal.yml" up -d

echo "Waiting for Temporal server to be ready..."
sleep 10

echo "Temporal server is running!"
echo "Web UI: http://localhost:8080"
echo "gRPC: localhost:7233"
