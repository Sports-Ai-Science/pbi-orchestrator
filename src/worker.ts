/**
 * Temporal Worker
 *
 * Worker that executes PBI workflows and activities.
 */

import { Worker, NativeConnection } from '@temporalio/worker';
import * as activities from './activities';
import { DEFAULT_PBI_CONFIG } from './config';

export async function createWorker(): Promise<Worker> {
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS || 'localhost:7233',
  });

  return Worker.create({
    connection,
    namespace: 'default',
    taskQueue: DEFAULT_PBI_CONFIG.taskQueue,
    workflowsPath: require.resolve('./workflows'),
    activities,
    maxConcurrentActivityTaskExecutions: 10,
    maxConcurrentWorkflowTaskExecutions: 10,
  });
}

export async function runWorker(): Promise<void> {
  const worker = await createWorker();
  console.log(`Worker started on task queue: ${DEFAULT_PBI_CONFIG.taskQueue}`);
  await worker.run();
}
