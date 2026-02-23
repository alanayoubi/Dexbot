import fs from 'node:fs';
import path from 'node:path';

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function normalizeDashes(text) {
  return String(text || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^-+|-+$/g, '');
}

function titleFromSkillName(name) {
  return String(name || '')
    .split('-')
    .filter(Boolean)
    .map((p) => p[0].toUpperCase() + p.slice(1))
    .join(' ');
}

function yamlQuote(value) {
  return `"${String(value || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

function trimAndLimit(value, maxLen = 400) {
  const txt = String(value || '').replace(/\s+/g, ' ').trim();
  if (txt.length <= maxLen) {
    return txt;
  }
  return `${txt.slice(0, maxLen - 3)}...`;
}

function cleanNameCandidate(raw) {
  return trimAndLimit(
    String(raw || '')
      .replace(/^["'`]+|["'`]+$/g, '')
      .replace(/^(a|an|the|my|our)\s+/i, '')
      .replace(/\s+skill$/i, '')
      .trim(),
    80
  );
}

function isGenericNameCandidate(name) {
  const tokens = String(name || '')
    .toLowerCase()
    .match(/[a-z0-9]+/g) || [];
  if (!tokens.length) {
    return true;
  }

  const pronouns = new Set(['it', 'this', 'that', 'these', 'those', 'something', 'anything', 'everything', 'thing', 'stuff', 'one']);
  const articles = new Set(['a', 'an', 'the', 'my', 'our', 'your']);
  const genericWords = new Set([
    'skill', 'skills', 'info', 'information', 'details', 'content', 'conversation',
    'message', 'text', 'prompt', 'future'
  ]);

  if (tokens.length === 1 && (pronouns.has(tokens[0]) || genericWords.has(tokens[0]))) {
    return true;
  }
  if (tokens.length <= 3 && tokens.every((t) => pronouns.has(t) || articles.has(t) || genericWords.has(t))) {
    return true;
  }
  if (articles.has(tokens[tokens.length - 1])) {
    return true;
  }
  return false;
}

function hasNaturalSkillCreateIntent(raw) {
  const text = String(raw || '').trim();
  if (!text) return false;
  const lower = text.toLowerCase();

  if (lower.startsWith('/skill')) return false;
  if (!/\bskill\b/.test(lower)) return false;
  if (/\bhow\s+do\s+i\b/.test(lower) || /\bhow\s+to\b/.test(lower)) return false;

  if (/\b(create|make|build|add|generate|setup|set up)\b/.test(lower)) {
    return true;
  }
  if (/\bturn\b[\s\S]{0,160}\binto\s+(?:a\s+)?skill\b/i.test(text)) {
    return true;
  }
  if (/\bconvert\b[\s\S]{0,160}\binto\s+(?:a\s+)?skill\b/i.test(text)) {
    return true;
  }
  return false;
}

export function normalizeSkillName(rawName) {
  const normalized = normalizeDashes(rawName).slice(0, 64);
  if (!normalized) {
    throw new Error('Invalid skill name. Use letters, numbers, and dashes.');
  }
  return normalized;
}

export function parseNaturalSkillCreateRequest(text) {
  const raw = String(text || '').trim();
  if (!raw) {
    return null;
  }
  if (!hasNaturalSkillCreateIntent(raw)) return null;

  const lower = raw.toLowerCase();
  const parts = raw.split('|').map((p) => p.trim());
  let nameCandidate = '';
  let description = '';
  let instructions = '';
  let hasExplicitNameSignal = false;

  const calledMatch = raw.match(/\b(?:called|named)\s+["'`]?([a-zA-Z0-9][a-zA-Z0-9 _-]{1,80}?)(?=["'`]?(\s+\bfor\b|\s*:|[|,.!?]|$))/i);
  if (calledMatch) {
    const maybe = cleanNameCandidate(calledMatch[1]);
    if (!isGenericNameCandidate(maybe)) {
      nameCandidate = maybe;
      hasExplicitNameSignal = true;
    }
  }

  if (!nameCandidate) {
    const prefixNameMatch = raw.match(/\b(?:create|make|build|add|generate|setup|set up)\s+(?:a|an|new)?\s*([a-zA-Z0-9][a-zA-Z0-9 _-]{1,80})\s+skill\b/i);
    if (prefixNameMatch) {
      const maybe = cleanNameCandidate(prefixNameMatch[1]);
      if (!isGenericNameCandidate(maybe)) {
        nameCandidate = maybe;
        hasExplicitNameSignal = true;
      }
    }
  }

  if (!hasExplicitNameSignal && /\?\s*$/.test(raw)) {
    return null;
  }

  if (!hasExplicitNameSignal || !nameCandidate) {
    return null;
  }

  if (parts.length >= 2) {
    description = trimAndLimit(parts[1], 180);
  } else {
    const forDesc = raw.match(/\bfor\s+([^|.?!]{6,180})/i);
    if (forDesc) {
      description = trimAndLimit(forDesc[1], 180);
    }
  }

  if (parts.length >= 3) {
    instructions = trimAndLimit(parts.slice(2).join(' | '), 2200);
  } else {
    const colonIdx = raw.indexOf(':');
    if (colonIdx >= 0 && colonIdx < raw.length - 1) {
      instructions = trimAndLimit(raw.slice(colonIdx + 1), 2200);
    }
  }

  return {
    nameCandidate,
    description,
    instructions
  };
}

export function isNaturalSkillCreateMissingName(text) {
  const raw = String(text || '').trim();
  if (!hasNaturalSkillCreateIntent(raw)) {
    return false;
  }
  return parseNaturalSkillCreateRequest(raw) == null;
}

function buildSkillMarkdown({ name, description, instructions = '' }) {
  const displayName = titleFromSkillName(name);
  const cleanDesc = trimAndLimit(
    description || `Specialized workflow skill for ${displayName}.`,
    180
  );
  const cleanInstructions = String(instructions || '').trim();

  const body = cleanInstructions
    ? cleanInstructions
    : [
      `Use this skill when the user asks for ${displayName.toLowerCase()} tasks.`,
      '',
      'Workflow:',
      '1. Clarify the desired outcome and constraints.',
      '2. Produce the result with concrete, verifiable steps.',
      '3. Summarize output and next actions.'
    ].join('\n');

  return [
    '---',
    `name: ${name}`,
    `description: ${yamlQuote(cleanDesc)}`,
    '---',
    '',
    `# ${displayName}`,
    '',
    body.trim(),
    ''
  ].join('\n');
}

function isSkillFolder(dirPath) {
  try {
    const st = fs.statSync(dirPath);
    if (!st.isDirectory()) {
      return false;
    }
    return fs.existsSync(path.join(dirPath, 'SKILL.md'));
  } catch {
    return false;
  }
}

function listSkillsInRoot(rootPath, writable) {
  if (!rootPath || !fs.existsSync(rootPath)) {
    return [];
  }
  const out = [];
  for (const entry of fs.readdirSync(rootPath, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }
    if (entry.name.startsWith('.')) {
      continue;
    }
    const dirPath = path.join(rootPath, entry.name);
    if (!isSkillFolder(dirPath)) {
      continue;
    }
    out.push({
      name: entry.name,
      normalizedName: normalizeDashes(entry.name),
      path: path.join(dirPath, 'SKILL.md'),
      root: rootPath,
      writable
    });
  }
  return out;
}

function dedupeSkills(skills) {
  const byName = new Map();
  for (const skill of skills) {
    if (!skill.normalizedName) {
      continue;
    }
    const existing = byName.get(skill.normalizedName);
    if (!existing) {
      byName.set(skill.normalizedName, skill);
      continue;
    }
    if (!existing.writable && skill.writable) {
      byName.set(skill.normalizedName, skill);
    }
  }
  return Array.from(byName.values()).sort((a, b) => a.name.localeCompare(b.name));
}

export function buildSkillRunPrompt(name, taskText = '') {
  const normalized = normalizeSkillName(name);
  const task = String(taskText || '').trim();
  if (!task) {
    return `$${normalized}`;
  }
  return `$${normalized}\n${task}`;
}

export function createSkillManager({
  writableRoot,
  readRoots = []
}) {
  const writable = path.resolve(writableRoot);
  const reads = Array.from(
    new Set(
      readRoots
        .map((r) => path.resolve(r))
        .filter((r) => r && r !== writable)
    )
  );
  ensureDir(writable);

  function listSkills() {
    const writableSkills = listSkillsInRoot(writable, true);
    const readSkills = reads.flatMap((root) => listSkillsInRoot(root, false));
    return dedupeSkills([...writableSkills, ...readSkills]);
  }

  function getSkill(name) {
    let normalized = '';
    try {
      normalized = normalizeSkillName(name);
    } catch {
      return null;
    }
    return listSkills().find((s) => s.normalizedName === normalized) || null;
  }

  function readSkill(name) {
    const found = getSkill(name);
    if (!found) {
      return null;
    }
    return {
      ...found,
      content: fs.readFileSync(found.path, 'utf8')
    };
  }

  function createSkill({
    name,
    description = '',
    instructions = ''
  }) {
    const normalized = normalizeSkillName(name);
    const dirPath = path.join(writable, normalized);
    if (fs.existsSync(dirPath)) {
      throw new Error(`Skill "${normalized}" already exists.`);
    }
    ensureDir(dirPath);
    const md = buildSkillMarkdown({
      name: normalized,
      description,
      instructions
    });
    const skillPath = path.join(dirPath, 'SKILL.md');
    fs.writeFileSync(skillPath, md, 'utf8');
    return {
      name: normalized,
      path: skillPath
    };
  }

  function deleteSkill(name) {
    const normalized = normalizeSkillName(name);
    const dirPath = path.join(writable, normalized);
    if (!fs.existsSync(dirPath)) {
      throw new Error(`Skill "${normalized}" does not exist in writable root.`);
    }
    if (!isSkillFolder(dirPath)) {
      throw new Error(`"${normalized}" is not a valid skill folder.`);
    }
    fs.rmSync(dirPath, { recursive: true, force: false });
    return {
      name: normalized,
      path: dirPath
    };
  }

  return {
    writableRoot: writable,
    readRoots: reads,
    listSkills,
    getSkill,
    readSkill,
    createSkill,
    deleteSkill
  };
}
