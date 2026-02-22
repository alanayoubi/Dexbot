import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  buildSkillRunPrompt,
  createSkillManager,
  normalizeSkillName,
  parseNaturalSkillCreateRequest
} from '../src/skills.js';

function makeTmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'dexbot-skills-'));
}

test('normalizeSkillName enforces Codex-safe names', () => {
  assert.equal(normalizeSkillName('Sales Page'), 'sales-page');
  assert.equal(normalizeSkillName('  DATA_tools__v2  '), 'data-tools-v2');
  assert.throws(() => normalizeSkillName('@@@'), /Invalid skill name/i);
});

test('skill manager create/list/read/delete lifecycle', () => {
  const root = makeTmpDir();
  try {
    const manager = createSkillManager({ writableRoot: root });
    const created = manager.createSkill({
      name: 'sales-page',
      description: 'Create high-conversion sales pages.',
      instructions: 'Collect offer details, then draft sections and CTA options.'
    });
    assert.match(created.path, /sales-page\/SKILL\.md$/);

    const listed = manager.listSkills();
    assert.equal(listed.length, 1);
    assert.equal(listed[0].name, 'sales-page');
    assert.equal(listed[0].writable, true);

    const loaded = manager.readSkill('sales-page');
    assert.ok(loaded);
    assert.match(loaded.content, /name:\s*sales-page/);
    assert.match(loaded.content, /description:\s*"Create high-conversion sales pages\."/);
    assert.match(loaded.content, /Collect offer details/);

    const removed = manager.deleteSkill('sales-page');
    assert.equal(removed.name, 'sales-page');
    assert.equal(manager.listSkills().length, 0);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('skill manager dedupes names and prefers writable root', () => {
  const writable = makeTmpDir();
  const shared = makeTmpDir();
  try {
    fs.mkdirSync(path.join(shared, 'sales-page'), { recursive: true });
    fs.writeFileSync(
      path.join(shared, 'sales-page', 'SKILL.md'),
      '---\nname: sales-page\ndescription: "shared"\n---\n',
      'utf8'
    );

    fs.mkdirSync(path.join(writable, 'sales-page'), { recursive: true });
    fs.writeFileSync(
      path.join(writable, 'sales-page', 'SKILL.md'),
      '---\nname: sales-page\ndescription: "local"\n---\n',
      'utf8'
    );

    const manager = createSkillManager({
      writableRoot: writable,
      readRoots: [shared]
    });
    const listed = manager.listSkills();
    assert.equal(listed.length, 1);
    assert.equal(listed[0].writable, true);
    assert.match(listed[0].path, /dexbot-skills-.*sales-page\/SKILL\.md$/);
  } finally {
    fs.rmSync(writable, { recursive: true, force: true });
    fs.rmSync(shared, { recursive: true, force: true });
  }
});

test('buildSkillRunPrompt creates explicit skill trigger', () => {
  assert.equal(buildSkillRunPrompt('Sales Page', ''), '$sales-page');
  assert.equal(
    buildSkillRunPrompt('sales-page', 'Write hero headline'),
    '$sales-page\nWrite hero headline'
  );
});

test('parseNaturalSkillCreateRequest detects conversational create intent', () => {
  const parsed = parseNaturalSkillCreateRequest(
    'Create a skill called sales page for writing fintech landing pages: ask for ICP, offer, and CTA.'
  );
  assert.ok(parsed);
  assert.equal(parsed.nameCandidate, 'sales page');
  assert.match(parsed.description, /writing fintech landing pages/i);
  assert.match(parsed.instructions, /ask for ICP/i);
});

test('parseNaturalSkillCreateRequest supports "<name> skill" phrasing', () => {
  const parsed = parseNaturalSkillCreateRequest(
    'Can you make a cold email skill for outbound SaaS campaigns?'
  );
  assert.ok(parsed);
  assert.equal(parsed.nameCandidate, 'cold email');
});

test('parseNaturalSkillCreateRequest ignores generic questions', () => {
  const parsed = parseNaturalSkillCreateRequest(
    'How do I create a skill in this bot?'
  );
  assert.equal(parsed, null);
});
