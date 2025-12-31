/**
 * Concurrency Manager
 *
 * Manages the 2-stack concurrency control system for PBI workflow execution.
 * Ensures that only 2 workflows can execute concurrently at any time.
 */

import { proxyActivities } from '@temporalio/workflow';
import type * as activities from './activities';

const { acquireLock, releaseLock } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: {
    initialInterval: '1s',
    backoffCoefficient: 2,
    maximumInterval: '10s',
    maximumAttempts: 5,
  },
});

export class ConcurrencyManager {
  /**
   * Acquire a concurrency slot for workflow execution
   * Blocks until a slot is available
   */
  static async acquireSlot(workflowId: string): Promise<void> {
    await acquireLock(workflowId);
  }

  /**
   * Release a concurrency slot after workflow execution
   */
  static async releaseSlot(workflowId: string): Promise<void> {
    await releaseLock(workflowId);
  }
}
