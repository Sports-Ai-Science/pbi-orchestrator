/**
 * PBI Workflow Definition
 *
 * Main workflow for orchestrating PBI execution with concurrency control.
 */

import { proxyActivities, sleep } from '@temporalio/workflow';
import type * as activities from './activities';
import { ConcurrencyManager } from './concurrency-manager';
import type { PBIWorkflowInput, PBIWorkflowResult } from './config';

const { processPBI } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: {
    initialInterval: '1s',
    backoffCoefficient: 2,
    maximumInterval: '30s',
    maximumAttempts: 3,
  },
});

export async function pbiWorkflow(input: PBIWorkflowInput): Promise<PBIWorkflowResult> {
  const startTime = Date.now();
  const workflowId = `${input.pbiId}-${startTime}`;
  let slotAcquired = false;

  try {
    // Acquire concurrency slot (blocks if 2 workflows already running)
    await ConcurrencyManager.acquireSlot(workflowId);
    slotAcquired = true;

    // Execute PBI processing
    const result = await processPBI(input.pbiId, input.pbiName, input.parameters);

    // Simulate some workflow processing time
    await sleep(100);

    // Release concurrency slot
    await ConcurrencyManager.releaseSlot(workflowId);
    slotAcquired = false;

    const executionTime = Date.now() - startTime;

    return {
      pbiId: input.pbiId,
      status: 'completed',
      executionTime,
      result: result.result,
    };
  } catch (error) {
    // Ensure we release the slot even on error, but only if it was acquired
    if (slotAcquired) {
      await ConcurrencyManager.releaseSlot(workflowId);
    }

    const executionTime = Date.now() - startTime;
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';

    return {
      pbiId: input.pbiId,
      status: 'failed',
      executionTime,
      error: errorMessage,
    };
  }
}
