/**
 * Example: Submit multiple concurrent workflows
 *
 * This example demonstrates the 2-stack concurrency control by submitting
 * multiple workflows concurrently.
 */

import { PBIOrchestrator } from '../src/client';

async function main() {
  console.log('=== PBI Orchestrator Example: Concurrent Workflows ===\n');

  const orchestrator = new PBIOrchestrator();
  await orchestrator.connect();
  console.log('✓ Connected to Temporal server\n');

  // Submit 5 workflows (only 2 will execute concurrently)
  const workflowPromises = [];

  for (let i = 1; i <= 5; i++) {
    console.log(`Submitting workflow ${i}...`);
    const promise = orchestrator
      .submitPBIWorkflow({
        pbiId: `PBI-00${i}`,
        pbiName: `Concurrent Test PBI ${i}`,
        parameters: {
          index: i,
          timestamp: new Date().toISOString(),
        },
      })
      .then((workflowId) => {
        console.log(`✓ Workflow ${i} submitted: ${workflowId}`);
        return { id: i, workflowId };
      });

    workflowPromises.push(promise);
  }

  const submissions = await Promise.all(workflowPromises);
  console.log(`\n✓ All ${submissions.length} workflows submitted\n`);

  console.log('Note: Only 2 workflows execute concurrently due to 2-stack control\n');

  // Wait for all workflows to complete
  console.log('Waiting for all workflows to complete...\n');
  const results = await Promise.all(
    submissions.map(async ({ id, workflowId }) => {
      const result = await orchestrator.getPBIWorkflowResult(workflowId);
      console.log(`✓ Workflow ${id} completed (${result.executionTime}ms)`);
      return result;
    })
  );

  console.log('\n=== Summary ===');
  console.log(`Total workflows: ${results.length}`);
  console.log(`Successful: ${results.filter((r) => r.status === 'completed').length}`);
  console.log(`Failed: ${results.filter((r) => r.status === 'failed').length}`);

  const avgExecutionTime =
    results.reduce((sum, r) => sum + r.executionTime, 0) / results.length;
  console.log(`Average execution time: ${avgExecutionTime.toFixed(0)}ms\n`);

  await orchestrator.close();
  console.log('✓ Connection closed');
}

main().catch((error) => {
  console.error('Error:', error);
  process.exit(1);
});
