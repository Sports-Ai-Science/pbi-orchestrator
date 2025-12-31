/**
 * Unit tests for configuration
 */

import { DEFAULT_PBI_CONFIG } from '../../src/config';

describe('Configuration', () => {
  describe('DEFAULT_PBI_CONFIG', () => {
    it('should have maxConcurrentWorkflows set to 2', () => {
      expect(DEFAULT_PBI_CONFIG.maxConcurrentWorkflows).toBe(2);
    });

    it('should have a valid task queue name', () => {
      expect(DEFAULT_PBI_CONFIG.taskQueue).toBe('pbi-orchestrator-queue');
    });

    it('should have a workflow timeout', () => {
      expect(DEFAULT_PBI_CONFIG.workflowTimeout).toBeGreaterThan(0);
    });

    it('should have retry policy configured', () => {
      expect(DEFAULT_PBI_CONFIG.retryPolicy).toBeDefined();
      expect(DEFAULT_PBI_CONFIG.retryPolicy.maximumAttempts).toBeGreaterThan(0);
    });
  });
});
