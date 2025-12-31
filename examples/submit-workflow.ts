/**
 * Example: Submit a PBI workflow
 *
 * This example demonstrates how to submit a PBI workflow for execution.
 */

import { PBIOrchestrator } from '../src/client';

async function main() {
  console.log('=== PBI Orchestrator Example: Submit Workflow ===\n');

  // Create and connect orchestrator
  const orchestrator = new PBIOrchestrator();
  await orchestrator.connect();
  console.log('✓ Connected to Temporal server\n');

  // Submit a workflow
  const workflowId = await orchestrator.submitPBIWorkflow({
    pbiId: 'PBI-001',
    pbiName: 'Example PBI Workflow',
    parameters: {
      environment: 'development',
      priority: 'high',
      assignee: 'developer@example.com',
    },
  });

  console.log(`✓ Workflow submitted: ${workflowId}\n`);

  // Wait for result
  console.log('Waiting for workflow to complete...\n');
  const result = await orchestrator.getPBIWorkflowResult(workflowId);

  console.log('✓ Workflow completed!\n');
  console.log('Result:', JSON.stringify(result, null, 2));

  // Cleanup
  await orchestrator.close();
  console.log('\n✓ Connection closed');
}

main().catch((error) => {
  console.error('Error:', error);
  process.exit(1);
});
