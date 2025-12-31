/**
 * Temporal Client
 *
 * Client for submitting PBI workflows to the orchestrator.
 */

import { Connection, Client, WorkflowNotFoundError } from '@temporalio/client';
import { pbiWorkflow } from './workflows';
import type { PBIWorkflowInput, PBIWorkflowResult } from './config';
import { DEFAULT_PBI_CONFIG } from './config';

export class PBIOrchestrator {
  private client?: Client;

  async connect(): Promise<void> {
    const connection = await Connection.connect({
      address: process.env.TEMPORAL_ADDRESS || 'localhost:7233',
    });

    this.client = new Client({
      connection,
      namespace: 'default',
    });
  }

  async submitPBIWorkflow(input: PBIWorkflowInput): Promise<string> {
    if (!this.client) {
      throw new Error('Client not connected. Call connect() first.');
    }

    const workflowId = `pbi-${input.pbiId}-${Date.now()}`;

    const handle = await this.client.workflow.start(pbiWorkflow, {
      taskQueue: DEFAULT_PBI_CONFIG.taskQueue,
      workflowId,
      args: [input],
      workflowExecutionTimeout: DEFAULT_PBI_CONFIG.workflowTimeout,
      retry: DEFAULT_PBI_CONFIG.retryPolicy,
    });

    console.log(`Workflow started: ${workflowId}`);
    return handle.workflowId;
  }

  async getPBIWorkflowResult(workflowId: string): Promise<PBIWorkflowResult> {
    if (!this.client) {
      throw new Error('Client not connected. Call connect() first.');
    }

    try {
      const handle = this.client.workflow.getHandle(workflowId);
      const result = await handle.result();
      return result as PBIWorkflowResult;
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) {
        throw new Error(`Workflow not found: ${workflowId}`);
      }
      throw error;
    }
  }

  async close(): Promise<void> {
    if (this.client) {
      await this.client.connection.close();
    }
  }
}
