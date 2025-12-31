/**
 * Example: Start a Temporal worker
 *
 * This example shows how to start a worker that executes PBI workflows.
 */

import { runWorker } from '../src/worker';

async function main() {
  console.log('=== PBI Orchestrator Example: Worker ===\n');
  console.log('Starting Temporal worker...\n');
  console.log('Worker will process workflows from the task queue');
  console.log('Press Ctrl+C to stop\n');

  await runWorker();
}

main().catch((error) => {
  console.error('Error:', error);
  process.exit(1);
});
