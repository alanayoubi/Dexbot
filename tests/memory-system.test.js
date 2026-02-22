import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { MemoryStore } from '../src/db.js';
import { MemoryEngine } from '../src/memory.js';

function makeHarness() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dexbot-mem-'));
  const dbPath = path.join(root, 'memory.db');
  const memoryRoot = path.join(root, 'memory');

  const store = new MemoryStore(dbPath);
  const memory = new MemoryEngine({
    store,
    config: {
      root: memoryRoot,
      embeddingDim: 64,
      sessionSummaryEveryTurns: 2,
      maxSessionSummaryLines: 200,
      maxFactsPerRetain: 10,
      maxEpisodesPerRetain: 3,
      maxStableFacts: 8,
      maxEpisodes: 4,
      maxOpenLoops: 6,
      maxInjectionTokens: 360,
      confidenceThreshold: 0.6,
      curatedMinConfidence: 0.75,
      recencyBiasDays: 45,
      multiAgentComplexityThreshold: 0.68,
      maxMemoryLines: 250,
      heartbeatHours: 24,
      heartbeatCompressOlderThanDays: 7,
      heartbeatDecayDays: 45,
      heartbeatDecayStep: 0.04
    }
  });
  memory.init();

  return {
    root,
    store,
    memory,
    close() {
      store.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  };
}

test('Remember timezone', () => {
  const h = makeHarness();
  try {
    const chatId = 11;
    const state = h.store.getChatState(chatId);

    h.memory.retainReflectIndex({
      chatId,
      state,
      userText: 'My timezone is Europe/Amsterdam. Please remember that.',
      assistantText: 'Noted. I will remember your timezone.',
      workingMemory: {
        currentGoal: 'Remember timezone',
        plan: [],
        assumptions: [],
        activeConstraints: [],
        toolResultsSummary: []
      }
    });

    const prepared = h.memory.prepareTurn(chatId, 'What is my timezone?');
    assert.match(prepared.developerInstructions, /Europe\/Amsterdam/i);
    assert.match(prepared.developerInstructions, /\[ALWAYS-ON IDENTITY\]/i);
    assert.match(prepared.developerInstructions, /\[ALWAYS-ON SOUL\]/i);
    assert.match(prepared.developerInstructions, /\[ALWAYS-ON TOOLS\]/i);
    assert.match(prepared.developerInstructions, /\[ALWAYS-ON CAPABILITIES\]/i);
    assert.match(prepared.developerInstructions, /\/skill/i);
  } finally {
    h.close();
  }
});

test('Recall a decision from 2 weeks ago', () => {
  const h = makeHarness();
  try {
    const chatId = 22;
    h.store.getChatState(chatId);

    const twoWeeksAgo = new Date(Date.now() - 14 * 86400000).toISOString();
    h.store.insertEpisode({
      chatId,
      summary: 'Decision: We chose Next.js for the frontend architecture.',
      entities: ['project:atlas', 'Next.js'],
      tags: ['project', 'atlas', 'decision'],
      salience: 0.91,
      timeRangeStart: twoWeeksAgo,
      timeRangeEnd: twoWeeksAgo,
      sourceFiles: ['memory/daily/old.md'],
      embedding: [0.2, 0.5, 0.1, 0.9]
    });

    const prepared = h.memory.prepareTurn(chatId, 'What did we decide two weeks ago for project atlas frontend stack?');
    const episodesText = prepared.retrieval.sections.episodes.join('\n');
    assert.match(episodesText, /Next\.js/i);
  } finally {
    h.close();
  }
});

test("Don't recall irrelevant old memories", () => {
  const h = makeHarness();
  try {
    const chatId = 33;
    h.store.getChatState(chatId);

    h.store.upsertFact({
      chatId,
      subject: 'user',
      predicate: 'favorite_color',
      object: 'red',
      confidence: 0.82,
      tags: ['profile'],
      createdAt: new Date(Date.now() - 90 * 86400000).toISOString(),
      sourceFile: 'memory/daily/old.md',
      sourceExcerpt: 'user favorite color red'
    });

    h.store.upsertFact({
      chatId,
      subject: 'project:apollo',
      predicate: 'uses_stack',
      object: 'TypeScript + React',
      confidence: 0.9,
      tags: ['project', 'apollo', 'stack'],
      createdAt: new Date().toISOString(),
      sourceFile: 'memory/daily/today.md',
      sourceExcerpt: 'apollo uses stack typescript react'
    });

    const prepared = h.memory.prepareTurn(chatId, 'For project apollo, what stack are we using?');
    const factsText = prepared.retrieval.sections.stableFacts.join('\n').toLowerCase();
    assert.match(factsText, /project:apollo uses_stack typescript \+ react/i);
    assert.doesNotMatch(factsText, /favorite_color red/i);
  } finally {
    h.close();
  }
});

test('Contradiction handling works', () => {
  const h = makeHarness();
  try {
    const chatId = 44;
    h.store.getChatState(chatId);

    h.store.upsertFact({
      chatId,
      subject: 'project:zeus',
      predicate: 'deployment_env',
      object: 'staging',
      confidence: 0.86,
      tags: ['project', 'zeus'],
      createdAt: new Date().toISOString(),
      sourceFile: 'memory/daily/today.md',
      sourceExcerpt: 'deployment env staging'
    });

    h.store.upsertFact({
      chatId,
      subject: 'project:zeus',
      predicate: 'deployment_env',
      object: 'production',
      confidence: 0.88,
      tags: ['project', 'zeus'],
      createdAt: new Date().toISOString(),
      sourceFile: 'memory/daily/today.md',
      sourceExcerpt: 'deployment env production'
    });

    const hb = h.memory.runHeartbeat();
    assert.ok(hb.contradictionCount >= 1);

    const contradictions = h.store.listContradictions(chatId);
    assert.ok(contradictions.length >= 1);
    assert.equal(contradictions[0].subject, 'project:zeus');
    assert.equal(contradictions[0].predicate, 'deployment_env');
    assert.ok(contradictions[0].objects.includes('staging'));
    assert.ok(contradictions[0].objects.includes('production'));
  } finally {
    h.close();
  }
});

test('Complexity heuristic prefers single-agent for simple tasks', () => {
  const h = makeHarness();
  try {
    const chatId = 55;
    h.store.getChatState(chatId);
    const prepared = h.memory.prepareTurn(chatId, 'What is my timezone?');
    assert.match(prepared.developerInstructions, /Prefer single-agent execution/i);
    assert.doesNotMatch(prepared.developerInstructions, /Use multi-agent strongly/i);
  } finally {
    h.close();
  }
});

test('Complexity heuristic pushes multi-agent for complex tasks', () => {
  const h = makeHarness();
  try {
    const chatId = 66;
    h.store.getChatState(chatId);
    const complexPrompt = [
      'Implement a full end-to-end migration across multiple files in the repo.',
      'Refactor architecture and update interfaces.',
      'Add validation and safety checks.',
      'Write regression tests and benchmark critical paths.',
      'Verify behavior against edge cases and rollback risks.',
      'Coordinate changes across modules and shared utilities.',
      'Use parallel workstreams for research, implementation, and verification.',
      'Provide final merged output with audit notes.'
    ].join(' ');
    const prepared = h.memory.prepareTurn(chatId, complexPrompt);
    assert.match(prepared.developerInstructions, /Use multi-agent strongly/i);
  } finally {
    h.close();
  }
});
