import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { MemoryStore } from '../src/db.js';
import { computeNextRunIso, parseScheduleSpec } from '../src/schedule.js';

function makeStore() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dexbot-sched-'));
  const dbPath = path.join(root, 'memory.db');
  const store = new MemoryStore(dbPath);
  return {
    root,
    store,
    close() {
      store.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  };
}

test('parseScheduleSpec accepts daily alias', () => {
  const parsed = parseScheduleSpec('daily 09:30');
  assert.equal(parsed.mode, 'daily');
  assert.equal(parsed.cronExpr, '30 9 * * *');
});

test('parseScheduleSpec accepts cron prefix and raw cron', () => {
  const a = parseScheduleSpec('cron 0 9 * * 1-5');
  assert.equal(a.mode, 'cron');
  assert.equal(a.cronExpr, '0 9 * * 1-5');

  const b = parseScheduleSpec('15 14 * * *');
  assert.equal(b.mode, 'cron');
  assert.equal(b.cronExpr, '15 14 * * *');
});

test('computeNextRunIso resolves next UTC run correctly', () => {
  const next = computeNextRunIso({
    cronExpr: '30 9 * * *',
    timezone: 'UTC',
    from: new Date('2026-02-22T09:00:00.000Z')
  });
  assert.equal(next, '2026-02-22T09:30:00.000Z');
});

test('scheduled jobs can be persisted, listed, updated and deleted', () => {
  const h = makeStore();
  try {
    const job = h.store.createScheduledJob({
      baseChatId: -1003810215818,
      scopedChatId: 7000000001234,
      topicId: 17,
      title: 'Morning report',
      kind: 'report',
      prompt: 'Send morning report',
      cronExpr: '0 9 * * *',
      timezone: 'UTC',
      active: true,
      nextRunAt: '2026-02-23T09:00:00.000Z'
    });

    assert.ok(job.id > 0);
    assert.equal(job.active, true);

    const listed = h.store.listScheduledJobs(7000000001234);
    assert.equal(listed.length, 1);
    assert.equal(listed[0].title, 'Morning report');

    const paused = h.store.setScheduledJobActive(job.id, 7000000001234, false);
    assert.equal(paused, true);
    const afterPause = h.store.getScheduledJob(job.id, 7000000001234);
    assert.equal(afterPause.active, false);

    const dueRows = h.store.listDueScheduledJobs('2026-02-24T10:00:00.000Z', 10);
    assert.equal(dueRows.length, 0);

    const resumed = h.store.setScheduledJobActive(job.id, 7000000001234, true);
    assert.equal(resumed, true);
    const dueRows2 = h.store.listDueScheduledJobs('2026-02-24T10:00:00.000Z', 10);
    assert.equal(dueRows2.length, 1);

    h.store.markScheduledJobRun(job.id, {
      lastRunAt: '2026-02-24T10:00:00.000Z',
      nextRunAt: '2026-02-25T09:00:00.000Z'
    });
    const afterRun = h.store.getScheduledJob(job.id, 7000000001234);
    assert.equal(afterRun.last_run_at, '2026-02-24T10:00:00.000Z');
    assert.equal(afterRun.next_run_at, '2026-02-25T09:00:00.000Z');

    const removed = h.store.deleteScheduledJob(job.id, 7000000001234);
    assert.equal(removed, true);
    assert.equal(h.store.listScheduledJobs(7000000001234).length, 0);
  } finally {
    h.close();
  }
});

