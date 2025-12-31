#!/bin/bash
# Stop Temporal server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Stopping Temporal server..."

docker compose -f "$SCRIPT_DIR/docker-compose.temporal.yml" down

echo "Temporal server stopped."
