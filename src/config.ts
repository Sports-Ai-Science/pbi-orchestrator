/**
 * PBI Workflow Configuration
 *
 * Configuration for the PBI orchestrator including concurrency limits
 * and workflow execution parameters.
 */

export interface PBIConfig {
  /**
   * Maximum number of concurrent PBI workflows (2-stack system)
   */
  maxConcurrentWorkflows: number;

  /**
   * Task queue name for Temporal workers
   */
  taskQueue: string;

  /**
   * Workflow execution timeout in milliseconds
   */
  workflowTimeout: number;

  /**
   * Retry policy for failed workflows
   */
  retryPolicy: {
    maximumAttempts: number;
    initialInterval: number;
    maximumInterval: number;
    backoffCoefficient: number;
  };
}

export const DEFAULT_PBI_CONFIG: PBIConfig = {
  maxConcurrentWorkflows: 2,
  taskQueue: 'pbi-orchestrator-queue',
  workflowTimeout: 3600000, // 1 hour
  retryPolicy: {
    maximumAttempts: 3,
    initialInterval: 1000,
    maximumInterval: 10000,
    backoffCoefficient: 2,
  },
};

export interface PBIWorkflowInput {
  pbiId: string;
  pbiName: string;
  parameters: Record<string, unknown>;
}

export interface PBIWorkflowResult {
  pbiId: string;
  status: 'completed' | 'failed';
  executionTime: number;
  result?: unknown;
  error?: string;
}
