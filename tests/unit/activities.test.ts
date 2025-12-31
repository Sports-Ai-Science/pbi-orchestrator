/**
 * Unit tests for activities
 */

import { processPBI } from '../../src/activities';

describe('Activities', () => {
  describe('processPBI', () => {
    it('should process PBI successfully', async () => {
      const result = await processPBI('PBI-001', 'Test PBI', { key: 'value' });

      expect(result.success).toBe(true);
      expect(result.result).toBeDefined();
    });

    it('should return result with PBI information', async () => {
      const result = await processPBI('PBI-002', 'Another PBI', {});

      expect(result.result).toHaveProperty('pbiId', 'PBI-002');
      expect(result.result).toHaveProperty('pbiName', 'Another PBI');
      expect(result.result).toHaveProperty('processedAt');
    });
  });
});
