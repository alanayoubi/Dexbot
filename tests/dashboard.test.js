import test from 'node:test';
import assert from 'node:assert/strict';
import { deriveDashboardChatId } from '../src/dashboard.js';

test('deriveDashboardChatId is deterministic and negative', () => {
  const a = deriveDashboardChatId('main');
  const b = deriveDashboardChatId('main');
  const c = deriveDashboardChatId('sales');

  assert.equal(a, b);
  assert.notEqual(a, c);
  assert.ok(a < 0);
  assert.ok(c < 0);
});
