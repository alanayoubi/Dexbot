import test from 'node:test';
import assert from 'node:assert/strict';

import { parseScheduleCreateSpec } from '../src/telegram.js';

test('parseScheduleCreateSpec accepts valid marker payload', () => {
  const raw = JSON.stringify({
    kind: 'report',
    scheduleSpec: 'daily 09:00',
    timezone: 'Europe/Amsterdam',
    prompt: 'Send me a short morning reminder.',
    confirmation: 'Done. I scheduled your morning reminder.'
  });

  const parsed = parseScheduleCreateSpec(raw, 'UTC');
  assert.ok(parsed);
  assert.equal(parsed.kind, 'report');
  assert.equal(parsed.scheduleSpec, 'daily 09:00');
  assert.equal(parsed.timezone, 'Europe/Amsterdam');
  assert.match(parsed.prompt, /morning reminder/i);
  assert.match(parsed.confirmation, /scheduled/i);
});

test('parseScheduleCreateSpec defaults timezone when omitted', () => {
  const raw = JSON.stringify({
    kind: 'heartbeat',
    scheduleSpec: 'cron 0 9 * * 1-5',
    prompt: 'Send a short weekday check-in.'
  });

  const parsed = parseScheduleCreateSpec(raw, 'UTC');
  assert.ok(parsed);
  assert.equal(parsed.kind, 'heartbeat');
  assert.equal(parsed.timezone, 'UTC');
});

test('parseScheduleCreateSpec rejects missing prompt', () => {
  const raw = JSON.stringify({
    kind: 'report',
    scheduleSpec: 'daily 09:00',
    timezone: 'UTC'
  });

  const parsed = parseScheduleCreateSpec(raw, 'UTC');
  assert.equal(parsed, null);
});

