import test from 'node:test';
import assert from 'node:assert/strict';

import { upsertEnvText } from '../src/env-file.js';

test('upsertEnvText updates existing keys and appends missing keys', () => {
  const existing = [
    'TELEGRAM_BOT_TOKEN=old',
    'ALLOWED_TELEGRAM_USER_IDS=1',
    '# comment',
    'CODEX_CWD=/tmp/app',
    ''
  ].join('\n');

  const out = upsertEnvText(existing, {
    TELEGRAM_BOT_TOKEN: 'new-token',
    CODEX_CWD: '/Users/a b/app',
    TELEGRAM_PRIVATE_ONLY: 'true'
  });

  assert.match(out, /^TELEGRAM_BOT_TOKEN=new-token/m);
  assert.match(out, /^CODEX_CWD="\/Users\/a b\/app"$/m);
  assert.match(out, /^TELEGRAM_PRIVATE_ONLY=true$/m);
  assert.match(out, /^# comment$/m);
});

test('upsertEnvText handles empty file', () => {
  const out = upsertEnvText('', {
    A: '1',
    B: 'two'
  });
  assert.equal(out, 'A=1\nB=two\n');
});
