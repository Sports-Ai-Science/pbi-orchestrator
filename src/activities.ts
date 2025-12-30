/**
 * PBI Workflow Activities
 *
 * Activities for PBI workflow execution, including concurrency control
 * and PBI processing logic.
 */

/**
 * In-memory semaphore for concurrency control
 * In production, this should be backed by a distributed locking mechanism
 */
class ConcurrencySemaphore {
  private currentCount = 0;
  private readonly maxCount: number;
  private readonly waitQueue: Array<{
    resolve: () => void;
    timestamp: number;
    workflowId: string;
  }> = [];
  private readonly acquireTimeout = 300000; // 5 minutes

  constructor(maxCount: number) {
    this.maxCount = maxCount;
  }

  async acquire(workflowId: string): Promise<void> {
    if (this.currentCount < this.maxCount) {
      this.currentCount++;
      return Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
      const timestamp = Date.now();
      this.waitQueue.push({ resolve, timestamp, workflowId });

      // Timeout mechanism to prevent indefinite waiting
      const timeoutId = setTimeout(() => {
        const index = this.waitQueue.findIndex((item) => item.workflowId === workflowId);
        if (index !== -1) {
          this.waitQueue.splice(index, 1);
          reject(
            new Error(
              `Timeout waiting for concurrency slot (${this.acquireTimeout}ms) for workflow ${workflowId}`
            )
          );
        }
      }, this.acquireTimeout);

      // Override resolve to clear timeout
      const originalResolve = resolve;
      const wrappedResolve = () => {
        clearTimeout(timeoutId);
        originalResolve();
      };
      const queueItem = this.waitQueue[this.waitQueue.length - 1];
      if (queueItem) {
        queueItem.resolve = wrappedResolve;
      }
    });
  }

  release(): void {
    if (this.waitQueue.length > 0) {
      const item = this.waitQueue.shift();
      if (item) {
        item.resolve();
      }
    } else {
      this.currentCount = Math.max(0, this.currentCount - 1);
    }
  }

  getCurrentCount(): number {
    return this.currentCount;
  }
}

const semaphore = new ConcurrencySemaphore(2);

export async function acquireLock(workflowId: string): Promise<void> {
  console.log(`[${workflowId}] Acquiring concurrency slot...`);
  await semaphore.acquire(workflowId);
  console.log(`[${workflowId}] Concurrency slot acquired. Active: ${semaphore.getCurrentCount()}`);
}

export async function releaseLock(workflowId: string): Promise<void> {
  console.log(`[${workflowId}] Releasing concurrency slot...`);
  semaphore.release();
  console.log(`[${workflowId}] Concurrency slot released. Active: ${semaphore.getCurrentCount()}`);
  return Promise.resolve();
}

export async function processPBI(
  pbiId: string,
  pbiName: string,
  parameters: Record<string, unknown>
): Promise<{ success: boolean; result: unknown }> {
  console.log(`[${pbiId}] Processing PBI: ${pbiName}`);
  console.log(`[${pbiId}] Parameters:`, parameters);

  // Simulate PBI processing
  await new Promise((resolve) => setTimeout(resolve, 1000));

  console.log(`[${pbiId}] PBI processing completed`);
  return {
    success: true,
    result: {
      pbiId,
      pbiName,
      processedAt: new Date().toISOString(),
    },
  };
}
