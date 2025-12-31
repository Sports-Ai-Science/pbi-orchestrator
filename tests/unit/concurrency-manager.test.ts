/**
 * Unit tests for ConcurrencyManager
 */

import { ConcurrencyManager } from '../../src/concurrency-manager';

describe('ConcurrencyManager', () => {
  describe('acquireSlot', () => {
    it('should have acquireSlot method', () => {
      expect(typeof ConcurrencyManager.acquireSlot).toBe('function');
    });
  });

  describe('releaseSlot', () => {
    it('should have releaseSlot method', () => {
      expect(typeof ConcurrencyManager.releaseSlot).toBe('function');
    });
  });
});
