import fs from 'node:fs';
import path from 'node:path';

const STOPWORDS = new Set([
  'the', 'and', 'for', 'with', 'that', 'this', 'from', 'your', 'have', 'will', 'into',
  'about', 'what', 'when', 'where', 'which', 'would', 'could', 'should', 'there', 'their',
  'them', 'then', 'than', 'been', 'were', 'also', 'just', 'like', 'want', 'need', 'does',
  'did', 'you', 'are', 'our', 'but', 'not', 'can', 'its', 'too', 'very', 'get', 'got', 'had',
  'him', 'her', 'his', 'she', 'who', 'how', 'why', 'make', 'made', 'been', 'being'
]);

const SENSITIVE_PATTERNS = [
  /api[_\s-]?key/i,
  /password/i,
  /secret/i,
  /private\s+key/i,
  /token/i,
  /ssn/i,
  /credit\s*card/i
];

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function fileExists(filePath) {
  try {
    fs.accessSync(filePath, fs.constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

function readText(filePath, fallback = '') {
  try {
    return fs.readFileSync(filePath, 'utf8');
  } catch {
    return fallback;
  }
}

function writeText(filePath, text) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, text, 'utf8');
}

function appendText(filePath, text) {
  ensureDir(path.dirname(filePath));
  fs.appendFileSync(filePath, text, 'utf8');
}

function tokenize(text) {
  return (String(text || '').toLowerCase().match(/[a-z0-9_\/-]{3,}/g) || [])
    .filter((t) => !STOPWORDS.has(t));
}

function sanitizeOneLine(text, max = 220) {
  const cleaned = String(text || '').replace(/\s+/g, ' ').trim();
  if (cleaned.length <= max) {
    return cleaned;
  }
  return `${cleaned.slice(0, max - 3)}...`;
}

function redactSensitiveText(text) {
  let out = String(text || '');

  out = out.replace(/\bsk-[A-Za-z0-9_-]{20,}\b/g, 'sk-[REDACTED]');
  out = out.replace(/\bAKIA[0-9A-Z]{16}\b/g, '[REDACTED_AWS_KEY]');
  out = out.replace(/-----BEGIN [^-]+-----[\s\S]*?-----END [^-]+-----/g, '[REDACTED_PRIVATE_KEY_BLOCK]');
  out = out.replace(/\b(api[_\s-]?key\s*[:=]\s*)([^\s"'`]{4,})/ig, '$1[REDACTED]');
  out = out.replace(/\b(token\s*[:=]\s*)([^\s"'`]{4,})/ig, '$1[REDACTED]');
  out = out.replace(/\b(password\s*[:=]\s*)([^\s"'`]{1,})/ig, '$1[REDACTED]');
  out = out.replace(/\b(bearer\s+)([A-Za-z0-9._-]{8,})/ig, '$1[REDACTED]');

  return out;
}

function estimateTokens(text) {
  const words = String(text || '').trim().split(/\s+/).filter(Boolean).length;
  return Math.ceil(words * 1.3);
}

function capLines(lines, maxLines) {
  if (lines.length <= maxLines) {
    return lines;
  }
  return lines.slice(lines.length - maxLines);
}

function toIsoWeek(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return `${d.getUTCFullYear()}-W${String(week).padStart(2, '0')}`;
}

function daysAgoIso(days) {
  const d = new Date(Date.now() - days * 86400000);
  return d.toISOString();
}

function extractEntities(text) {
  const entities = new Set();
  const source = String(text || '');
  for (const m of source.matchAll(/(?:project\s*[:\s]+)([a-z0-9_-]{2,40})/ig)) {
    entities.add(`project:${m[1].toLowerCase()}`);
  }
  for (const m of source.matchAll(/#([a-z0-9_-]{2,40})/ig)) {
    entities.add(`tag:${m[1].toLowerCase()}`);
  }
  for (const m of source.matchAll(/\b[A-Z][a-zA-Z0-9_-]{2,}\b/g)) {
    entities.add(m[0]);
  }
  return Array.from(entities).slice(0, 12);
}

function hashToken(token) {
  let h = 2166136261;
  for (let i = 0; i < token.length; i += 1) {
    h ^= token.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function embedText(text, dim = 96) {
  const vec = new Array(dim).fill(0);
  const tokens = tokenize(text);
  if (!tokens.length) {
    return vec;
  }
  for (const token of tokens) {
    const h = hashToken(token);
    const idx = h % dim;
    const sign = ((h >> 1) & 1) ? 1 : -1;
    vec[idx] += sign * (1 + (token.length % 4) * 0.15);
  }
  let norm = 0;
  for (const v of vec) {
    norm += v * v;
  }
  norm = Math.sqrt(norm) || 1;
  return vec.map((v) => Number((v / norm).toFixed(6)));
}

function overlapScore(queryTokens, text) {
  const tokenSet = new Set(tokenize(text));
  if (!queryTokens.length || !tokenSet.size) {
    return 0;
  }
  let hit = 0;
  for (const t of queryTokens) {
    if (tokenSet.has(t)) {
      hit += 1;
    }
  }
  return hit / queryTokens.length;
}

function parseQuery(query) {
  const text = String(query || '');
  const tokens = tokenize(text);
  const entities = extractEntities(text);
  const tags = entities.filter((e) => e.startsWith('tag:')).map((e) => e.slice(4));
  const projects = entities.filter((e) => e.startsWith('project:')).map((e) => e.slice(8));

  const predicates = [];
  if (/timezone|time\s*zone|utc/i.test(text)) predicates.push('timezone');
  if (/prefer|preference|style|tone|short|concise|detailed/i.test(text)) predicates.push('prefers');
  if (/stack|framework|language|typescript|react|next\.?js|backend|frontend/i.test(text)) predicates.push('uses_stack');
  if (/decision|decide|agreed|plan|last time|previous/i.test(text)) predicates.push('decision');

  return {
    raw: text,
    tokens,
    entities,
    tags,
    projects,
    predicates,
    timeHints: {
      relative: /last\s+time|yesterday|today|this\s+week|last\s+week|two\s+weeks/i.test(text),
      explicitMonth: /(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i.test(text)
    }
  };
}

function estimateTaskComplexity(userPrompt, parsed, threshold) {
  const text = String(userPrompt || '');
  const tokens = parsed?.tokens || tokenize(text);
  const lower = text.toLowerCase();
  let score = 0;
  const reasons = [];

  if (tokens.length >= 30) {
    score += 0.18;
    reasons.push('long_prompt');
  }
  if (tokens.length >= 70) {
    score += 0.2;
    reasons.push('very_long_prompt');
  }

  if (/(implement|refactor|migrate|rewrite|architecture|system design|build|end-to-end|pipeline)/i.test(lower)) {
    score += 0.28;
    reasons.push('engineering_scope');
  }

  if (/(across|multiple files|whole repo|entire project|all modules|orchestrate|delegate|parallel)/i.test(lower)) {
    score += 0.22;
    reasons.push('cross_cutting_scope');
  }

  if (/(test|verify|validate|benchmark|regression|edge case|hardening|self-healing)/i.test(lower)) {
    score += 0.16;
    reasons.push('verification_required');
  }

  if ((text.match(/\n/g) || []).length >= 8) {
    score += 0.12;
    reasons.push('structured_multistep');
  }

  if ((parsed?.projects?.length || 0) >= 1 && /(timeline|strategy|plan|rollout|migration)/i.test(lower)) {
    score += 0.14;
    reasons.push('project_coordination');
  }

  score = Math.min(1, Number(score.toFixed(2)));
  const mode = score >= threshold ? 'multi' : 'single';
  return { score, threshold, reasons, mode };
}

function isSensitive(text) {
  return SENSITIVE_PATTERNS.some((rx) => rx.test(String(text || '')));
}

function uniqueFacts(facts) {
  const seen = new Set();
  const out = [];
  for (const fact of facts) {
    const key = `${fact.subject}|${fact.predicate}|${fact.object}`.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(fact);
  }
  return out;
}

function extractFactCandidates(userText, assistantText) {
  const text = `${userText}\n${assistantText}`;
  if (isSensitive(text)) {
    return [];
  }

  const facts = [];
  const pushFact = (fact) => {
    if (!fact || !fact.subject || !fact.predicate || !fact.object) {
      return;
    }
    if (isSensitive(`${fact.subject} ${fact.predicate} ${fact.object}`)) {
      return;
    }
    facts.push({
      confidence: 0.72,
      tags: [],
      ...fact
    });
  };

  const tzRx = /(?:timezone(?:\s+is|\s*=|\s*:)?|time\s*zone(?:\s+is|\s*=|\s*:)?|i\s*(?:am|'m)\s+in\s+timezone)\s*([A-Za-z_+-]+\/[A-Za-z_+-]+|UTC[+-]?\d{1,2})/i;
  const tz = userText.match(tzRx);
  if (tz) {
    pushFact({
      subject: 'user',
      predicate: 'timezone',
      object: tz[1],
      confidence: 0.9,
      tags: ['profile', 'timezone']
    });
  }

  if (/(i\s+prefer|prefer\s+that|please\s+keep)/i.test(userText) && /(short|concise|brief)/i.test(userText)) {
    pushFact({
      subject: 'user',
      predicate: 'prefers',
      object: 'short answers',
      confidence: 0.86,
      tags: ['preference', 'style']
    });
  }

  if (/(i\s+prefer|prefer\s+that|please\s+keep)/i.test(userText) && /(detailed|long|thorough)/i.test(userText)) {
    pushFact({
      subject: 'user',
      predicate: 'prefers',
      object: 'detailed answers',
      confidence: 0.86,
      tags: ['preference', 'style']
    });
  }

  const projectName = userText.match(/project\s*[:\s]+([a-zA-Z0-9_-]{2,40})/i)?.[1]?.toLowerCase() || null;
  const stackMatch = text.match(/(?:uses?|using|stack(?:\s+is)?|built\s+with)\s+([A-Za-z0-9+.#,\s\/-]{3,120})/i);
  if (stackMatch && projectName) {
    pushFact({
      subject: `project:${projectName}`,
      predicate: 'uses_stack',
      object: sanitizeOneLine(stackMatch[1], 90),
      confidence: 0.78,
      tags: ['project', projectName, 'stack']
    });
  }

  const recurringConstraint = userText.match(/\b(?:always|never|must)\b\s+([^.!?]{8,140})/i);
  if (recurringConstraint) {
    pushFact({
      subject: projectName ? `project:${projectName}` : 'user',
      predicate: 'constraint',
      object: sanitizeOneLine(recurringConstraint[1], 100),
      confidence: 0.75,
      tags: ['constraint']
    });
  }

  const platform = userText.match(/\b(i\s+use|we\s+use|using)\s+([A-Za-z0-9+.#\/-]{2,60})/i);
  if (platform) {
    pushFact({
      subject: 'user',
      predicate: 'uses_tool',
      object: sanitizeOneLine(platform[2], 70),
      confidence: 0.68,
      tags: ['tooling']
    });
  }

  return uniqueFacts(facts);
}

function extractEpisodeCandidates({ userText, assistantText, maxEpisodes = 3 }) {
  const combined = `${userText}\n${assistantText}`;
  const hasDecision = /(decid|decision|agreed|plan|we\s+will|chosen|next\s+step|roadmap)/i.test(combined);
  const hasProject = /project[:\s]+[a-z0-9_-]{2,40}/i.test(combined);

  if (!hasDecision && !hasProject && combined.length < 180) {
    return [];
  }

  const project = userText.match(/project\s*[:\s]+([a-zA-Z0-9_-]{2,40})/i)?.[1]?.toLowerCase();
  const topicTokens = tokenize(userText).slice(0, 6);
  const topic = topicTokens.join(' ');
  const decisionLine = (assistantText.split(/\n+/)
    .map((line) => line.trim())
    .find((line) => /(decid|plan|we\s+will|next\s+step|agreed)/i.test(line))
    || assistantText.split(/[.!?]/).map((x) => x.trim()).find(Boolean)
    || assistantText).trim();

  const summary = sanitizeOneLine(
    `Topic: ${topic || 'session context'}. Decision/context: ${decisionLine}`,
    260
  );

  const tags = ['episode'];
  if (project) {
    tags.push('project', project);
  }
  if (hasDecision) {
    tags.push('decision');
  }

  return [
    {
      summary,
      entities: extractEntities(`${userText} ${assistantText}`),
      tags,
      salience: hasDecision ? 0.85 : 0.68
    }
  ].slice(0, maxEpisodes);
}

function extractOpenLoops(assistantText) {
  const lines = String(assistantText || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  const loops = [];
  for (const line of lines) {
    if (/(todo|to do|follow up|open loop|next step)/i.test(line)) {
      loops.push(sanitizeOneLine(line.replace(/^[-*\d.\s]+/, ''), 180));
    }
  }
  return Array.from(new Set(loops)).slice(0, 6);
}

export class MemoryEngine {
  constructor({ store, config }) {
    this.store = store;
    this.config = config;

    this.paths = {
      root: config.root,
      daily: path.join(config.root, 'daily'),
      weekly: path.join(config.root, 'weekly'),
      sessions: path.join(config.root, 'sessions'),
      heartbeatLog: path.join(config.root, 'heartbeat.log'),
      memory: path.join(config.root, 'MEMORY.md'),
      user: path.join(config.root, 'USER.md'),
      identity: path.join(config.root, 'IDENTITY.md'),
      soul: path.join(config.root, 'SOUL.md'),
      heartbeat: path.join(config.root, 'HEARTBEAT.md'),
      tools: path.join(config.root, 'TOOLS.md')
    };
  }

  init() {
    ensureDir(this.paths.root);
    ensureDir(this.paths.daily);
    ensureDir(this.paths.weekly);
    ensureDir(this.paths.sessions);
    this.ensureCanonicalFiles();
  }

  ensureCanonicalFiles() {
    const files = [
      {
        path: this.paths.identity,
        content: [
          '# IDENTITY',
          '',
          'You are Codex running as a persistent engineering agent for this user.',
          'Boundaries:',
          '- Do not fabricate memory.',
          '- Prefer concrete steps over vague statements.',
          '- Respect user-approved access policy and security constraints.',
          '- Keep memory precise and auditable.'
        ].join('\n')
      },
      {
        path: this.paths.soul,
        content: [
          '# SOUL',
          '',
          'Personality and communication style:',
          '- Direct, clear, calm, and pragmatic.',
          '- Conversational human language, avoid robotic filler.',
          '- Keep momentum: explain what is happening and what is next.',
          '- Preserve continuity with prior user preferences and decisions.',
          '- When uncertain, state assumptions explicitly.'
        ].join('\n')
      },
      {
        path: this.paths.tools,
        content: [
          '# TOOLS',
          '',
          'Tool use rules:',
          '- Use shell/tools only when they materially improve correctness.',
          '- Prefer fast search and deterministic transformations.',
          '- Summarize tool outputs; avoid dumping raw noise.',
          '- Verify critical steps with explicit checks/tests.',
          '',
          'Runtime capabilities:',
          '- Full Codex CLI execution on this isolated machine.',
          '- Telegram commands: /newsession, /memory, /heartbeat, /schedule, /skill, /restart, /autostart, /chatid, /whoami.',
          '- Skills: create/list/show/run via /skill, explicit $skill-name trigger in prompts, and natural-language creation requests.',
          '- For natural skill creation, emit exactly one line in the final reply when ready: SKILL_CREATE: <name> | <description> | <instructions>. If name is missing/ambiguous, ask a follow-up question and do not emit SKILL_CREATE.',
          '- Proactive schedules: recurring reports and heartbeat check-ins via /schedule.'
        ].join('\n')
      },
      {
        path: this.paths.heartbeat,
        content: [
          '# HEARTBEAT',
          '',
          'Run every 24h (configurable):',
          '- Compress older daily logs into weekly summaries.',
          '- Deduplicate facts and decay stale confidence.',
          '- Detect contradictions (same subject/predicate, different object).',
          '- Keep MEMORY.md concise by retaining only stable high-signal items.'
        ].join('\n')
      },
      {
        path: this.paths.memory,
        content: [
          '# MEMORY',
          '',
          'Curated long-term memory. Keep stable and compact.',
          ''
        ].join('\n')
      },
      {
        path: this.paths.user,
        content: [
          '# USER',
          '',
          'Stable user profile and preferences (curated).',
          ''
        ].join('\n')
      }
    ];

    for (const file of files) {
      if (!fileExists(file.path)) {
        writeText(file.path, `${file.content}\n`);
      }
    }

    // Backfill capabilities hints into existing TOOLS.md without clobbering user edits.
    const toolsText = readText(this.paths.tools, '');
    if (!/Telegram commands:/i.test(toolsText) || !/\/skill\b/i.test(toolsText) || !/Proactive schedules:/i.test(toolsText)) {
      appendText(
        this.paths.tools,
        '\nRuntime capabilities:\n- Full Codex CLI execution on this isolated machine.\n- Telegram commands: /newsession, /memory, /heartbeat, /schedule, /skill, /restart, /autostart, /chatid, /whoami.\n- Skills: create/list/show/run via /skill and explicit $skill-name trigger in prompts.\n- Proactive schedules: recurring reports and heartbeat check-ins via /schedule.\n'
      );
    }
  }

  getSessionPaths(chatId, sessionNo) {
    const dir = path.join(this.paths.sessions, String(chatId), String(sessionNo));
    return {
      dir,
      summary: path.join(dir, 'session_summary.md'),
      keyFacts: path.join(dir, 'session_key_facts.json')
    };
  }

  ensureSessionFiles(chatId, sessionNo) {
    const p = this.getSessionPaths(chatId, sessionNo);
    ensureDir(p.dir);
    if (!fileExists(p.summary)) {
      writeText(
        p.summary,
        `# Session Summary\n\nChat: ${chatId}\nSession: ${sessionNo}\n\n## Rolling Notes\n`
      );
    }
    if (!fileExists(p.keyFacts)) {
      writeText(
        p.keyFacts,
        JSON.stringify(
          {
            chat_id: chatId,
            session_no: sessionNo,
            updated_at: new Date().toISOString(),
            facts: []
          },
          null,
          2
        )
      );
    }
    return p;
  }

  getDailyFile(date = new Date()) {
    const key = date.toISOString().slice(0, 10);
    return path.join(this.paths.daily, `${key}.md`);
  }

  buildWorkingMemory(userPrompt) {
    const text = String(userPrompt || '');
    const goal = sanitizeOneLine(text.split(/[.!?\n]/).find((x) => x.trim()) || text, 180);

    const constraints = [];
    for (const m of text.matchAll(/\b(?:must|never|always|only)\b\s+([^.!?]{6,120})/gi)) {
      constraints.push(sanitizeOneLine(m[0], 140));
    }

    const assumptions = [];
    if (/assuming|assume/i.test(text)) {
      assumptions.push(sanitizeOneLine(text, 140));
    }

    const plan = [];
    if (/\b(build|implement|add|create|setup|fix|refactor)\b/i.test(text)) {
      plan.push('Interpret request and implement concrete code changes.');
      plan.push('Validate with checks/tests before responding.');
    }

    return {
      currentGoal: goal || 'Handle user request accurately.',
      plan,
      assumptions,
      activeConstraints: constraints,
      toolResultsSummary: []
    };
  }

  buildAlwaysOnCoreInstructions() {
    const identity = readText(this.paths.identity)
      .split('\n')
      .slice(0, 24)
      .join('\n');
    const soul = readText(this.paths.soul)
      .split('\n')
      .slice(0, 28)
      .join('\n');
    const user = readText(this.paths.user)
      .split('\n')
      .slice(0, 28)
      .join('\n');
    const tools = readText(this.paths.tools)
      .split('\n')
      .slice(0, 34)
      .join('\n');

    const runtimeCaps = [
      '[ALWAYS-ON CAPABILITIES]',
      '- Full Codex CLI execution with available local tools.',
      '- Telegram supports text, files, images, voice, and topic-scoped sessions.',
      '- Scheduling is available via /schedule and natural schedule requests.',
      '- Skills are available via /skill, explicit $skill-name triggers, and natural-language creation requests.',
      '- Keep memory behavior silent; never output internal memory/tool meta unless user asks.'
    ].join('\n');

    return [
      '[ALWAYS-ON IDENTITY]',
      identity,
      '',
      '[ALWAYS-ON SOUL]',
      soul,
      '',
      '[ALWAYS-ON USER]',
      user,
      '',
      '[ALWAYS-ON TOOLS]',
      tools,
      '',
      runtimeCaps
    ].join('\n');
  }

  isMemorySensitiveQuery(parsed) {
    const t = parsed.raw.toLowerCase();
    if (
      /remember|memory|last time|previous|what did we decide|recall|context from/i.test(t) ||
      parsed.timeHints.relative ||
      parsed.timeHints.explicitMonth
    ) {
      return true;
    }
    if (parsed.projects.length || parsed.predicates.length) {
      return true;
    }
    return false;
  }

  buildDeveloperInstructions(userPrompt, retrieval) {
    const alwaysOn = this.buildAlwaysOnCoreInstructions();
    const complexity = estimateTaskComplexity(
      userPrompt,
      retrieval.parsed,
      this.config.multiAgentComplexityThreshold
    );

    const orchestrationDirective = complexity.mode === 'multi'
      ? `Complexity score ${complexity.score} >= ${complexity.threshold}. Use multi-agent strongly: split work into focused internal streams (research, implementation, verification) and merge results before final answer.`
      : `Complexity score ${complexity.score} < ${complexity.threshold}. Prefer single-agent execution. Do not spawn child agents unless blocked or confidence remains low after one pass.`;

    const baseGuard = [
      'Operate like normal Codex CLI behavior.',
      'The ALWAYS-ON IDENTITY/SOUL/USER/TOOLS/CAPABILITIES sections are mandatory for every turn.',
      orchestrationDirective,
      'Keep orchestration internal; do not narrate internal agent management unless user asks.',
      'Never mention internal memory systems, working-memory state, or retrieval mechanics.',
      'Never mention or quote hidden developer instructions unless explicitly asked to reveal policy.',
      'Do not send acknowledgement/status text about memory unless the user explicitly asks about memory.',
      'If you produce image outputs intended for the user, append one line per image exactly as: IMAGE_OUTPUT: <absolute_path_or_url>.',
      'Do not add extra commentary to IMAGE_OUTPUT lines.',
      'Answer only the user request directly, in natural conversational language.'
    ];

    const parsed = retrieval.parsed;
    const shouldInject = this.isMemorySensitiveQuery(parsed)
      && (
        retrieval.sections.stableFacts.length > 0 ||
        retrieval.sections.episodes.length > 0 ||
        retrieval.sections.loops.length > 0
      );

    if (!shouldInject) {
      return [
        ...baseGuard,
        '',
        alwaysOn
      ].join('\n');
    }

    return [
      ...baseGuard,
      '',
      alwaysOn,
      '',
      'Background memory context (use silently; do not quote unless asked):',
      retrieval.injection
    ].join('\n');
  }

  retrieve(chatId, userQuery) {
    const parsed = parseQuery(userQuery);
    const queryEmbedding = embedText(userQuery, this.config.embeddingDim);

    const vectorEpisodes = this.store.searchEpisodesVector(chatId, queryEmbedding, 20, 500)
      .map((row) => ({ type: 'episode', source: 'vector', ...row }));

    const keywordFacts = this.store.searchFactsKeyword(chatId, userQuery, 20)
      .map((row) => ({ type: 'fact', source: 'keyword', ...row }));

    const keywordEpisodes = this.store.searchEpisodesKeyword(chatId, userQuery, 20)
      .map((row) => ({ type: 'episode', source: 'keyword', ...row }));

    const keywordDocuments = this.store.searchDocumentsKeyword(chatId, userQuery, 20)
      .map((row) => ({ type: 'document', source: 'keyword', ...row }));

    const exactFacts = this.store.searchFactsExact(
      chatId,
      parsed.entities,
      parsed.predicates,
      parsed.tags.concat(parsed.projects),
      20
    ).map((row) => ({ type: 'fact', source: 'exact', ...row }));

    const exactEpisodes = this.store.searchEpisodesExact(
      chatId,
      parsed.entities,
      parsed.tags.concat(parsed.projects),
      20
    ).map((row) => ({ type: 'episode', source: 'exact', ...row }));

    const openLoops = this.store.getOpenLoops(chatId, 20, userQuery)
      .map((row) => ({ type: 'loop', source: 'open_loop', ...row }));

    const all = [
      ...vectorEpisodes,
      ...keywordFacts,
      ...keywordEpisodes,
      ...keywordDocuments,
      ...exactFacts,
      ...exactEpisodes,
      ...openLoops
    ];

    const byKey = new Map();

    for (const c of all) {
      const id = c.id != null ? String(c.id) : `${c.path || 'x'}:${c.chunk_index || 0}`;
      const key = `${c.type}:${id}`;
      const text = c.type === 'fact'
        ? `${c.subject} ${c.predicate} ${c.object}`
        : c.type === 'episode'
          ? c.summary
          : c.type === 'document'
            ? c.text
            : c.text;

      const confidence = c.type === 'fact'
        ? Number(c.confidence || 0)
        : c.type === 'episode'
          ? Number(c.salience || 0.6)
          : c.type === 'loop'
            ? Number(c.confidence || 0.7)
            : 0.55;

      const recencyIso = c.last_confirmed_at || c.time_range_end || c.created_at || c.detected_at || null;
      const recencyDays = recencyIso
        ? Math.max(0, (Date.now() - new Date(recencyIso).getTime()) / 86400000)
        : 365;
      const recencyScore = 1 / (1 + recencyDays / this.config.recencyBiasDays);

      const sourceScore = c.source === 'exact'
        ? 0.95
        : c.source === 'vector'
          ? (0.6 + Number(c.vector_score || 0))
          : c.source === 'keyword'
            ? (0.45 + 1 / (1 + Math.max(0, Number(c.fts_rank || 0))))
            : 0.5;

      const overlap = overlapScore(parsed.tokens, text);

      const tags = c.tags || c.entities || [];
      const projectBias = parsed.projects.length
        ? tags.some((t) => parsed.projects.includes(String(t).toLowerCase()) || parsed.projects.includes(String(t).replace('project:', '').toLowerCase()))
          ? 0.22
          : -0.04
        : 0;

      let score = sourceScore + overlap * 0.8 + recencyScore * 0.35 + confidence * 0.55 + projectBias;
      if (confidence < this.config.confidenceThreshold && c.type !== 'document') {
        score -= 0.35;
      }

      const prev = byKey.get(key);
      if (!prev || score > prev.score) {
        byKey.set(key, {
          ...c,
          text,
          tags,
          confidence,
          score,
          recencyDays
        });
      }
    }

    let ranked = Array.from(byKey.values())
      .sort((a, b) => b.score - a.score);

    const hasAboveThreshold = ranked.some((x) => x.confidence >= this.config.confidenceThreshold);
    if (hasAboveThreshold) {
      ranked = ranked.filter((x) => x.type === 'document' || x.confidence >= this.config.confidenceThreshold);
    }

    const stableFacts = ranked
      .filter((x) => x.type === 'fact')
      .slice(0, this.config.maxStableFacts)
      .map((x) => `- ${x.subject} ${x.predicate} ${x.object}`);

    const episodes = ranked
      .filter((x) => x.type === 'episode' || x.type === 'document')
      .slice(0, this.config.maxEpisodes)
      .map((x) => `- ${sanitizeOneLine(x.type === 'episode' ? x.summary : x.text, 190)}`);

    const loops = ranked
      .filter((x) => x.type === 'loop')
      .slice(0, this.config.maxOpenLoops)
      .map((x) => `- ${sanitizeOneLine(x.text, 170)}`);

    const sections = {
      stableFacts,
      episodes,
      loops
    };

    const injection = this.buildInjection(sections);

    return {
      parsed,
      ranked,
      sections,
      injection
    };
  }

  buildInjection(sections) {
    const block = {
      stableFacts: [...sections.stableFacts],
      episodes: [...sections.episodes],
      loops: [...sections.loops]
    };

    const render = () => [
      'Known stable facts:',
      ...(block.stableFacts.length ? block.stableFacts : ['- (none)']),
      '',
      'Relevant past decisions / episodes:',
      ...(block.episodes.length ? block.episodes : ['- (none)']),
      '',
      'Open loops / TODOs:',
      ...(block.loops.length ? block.loops : ['- (none)'])
    ].join('\n');

    while (estimateTokens(render()) > this.config.maxInjectionTokens) {
      if (block.loops.length) {
        block.loops.pop();
        continue;
      }
      if (block.episodes.length) {
        block.episodes.pop();
        continue;
      }
      if (block.stableFacts.length > 1) {
        block.stableFacts.pop();
        continue;
      }
      break;
    }

    return render();
  }

  prepareTurn(chatId, userPrompt) {
    const state = this.store.getChatState(chatId);
    this.ensureSessionFiles(chatId, state.session_no);

    const workingMemory = this.buildWorkingMemory(userPrompt);
    const retrieval = this.retrieve(chatId, userPrompt);

    const developerInstructions = this.buildDeveloperInstructions(userPrompt, retrieval);

    return {
      state,
      workingMemory,
      retrieval,
      developerInstructions
    };
  }

  updateSessionSummary(sessionSummaryPath, turnEntry) {
    const existing = readText(sessionSummaryPath, '# Session Summary\n\n## Rolling Notes\n');
    const lines = existing.split('\n');
    lines.push(turnEntry);
    const capped = capLines(lines, this.config.maxSessionSummaryLines);
    writeText(sessionSummaryPath, `${capped.join('\n')}\n`);
  }

  updateSessionKeyFacts(sessionFactsPath, facts, timestamp) {
    let payload;
    try {
      payload = JSON.parse(readText(sessionFactsPath, '{"facts": []}'));
    } catch {
      payload = { facts: [] };
    }
    const existing = Array.isArray(payload.facts) ? payload.facts : [];

    const byKey = new Map();
    for (const f of existing) {
      byKey.set(`${f.subject}|${f.predicate}|${f.object}`.toLowerCase(), f);
    }

    for (const fact of facts) {
      const key = `${fact.subject}|${fact.predicate}|${fact.object}`.toLowerCase();
      byKey.set(key, {
        subject: fact.subject,
        predicate: fact.predicate,
        object: fact.object,
        confidence: fact.confidence,
        tags: fact.tags,
        updated_at: timestamp
      });
    }

    payload.facts = Array.from(byKey.values());
    payload.updated_at = timestamp;
    writeText(sessionFactsPath, `${JSON.stringify(payload, null, 2)}\n`);
  }

  appendDailyJournal({
    chatId,
    sessionNo,
    timestamp,
    workingMemory,
    userText,
    assistantText,
    facts,
    episodes,
    loops,
    artifacts
  }) {
    const dailyPath = this.getDailyFile(new Date(timestamp));
    if (!fileExists(dailyPath)) {
      writeText(dailyPath, `# Daily Journal ${timestamp.slice(0, 10)}\n\n`);
    }

    const timeOnly = timestamp.slice(11, 16);
    const factLine = facts.length
      ? facts.map((f) => `${f.subject} ${f.predicate} ${f.object}`).join(' | ')
      : '(none)';
    const episodeLine = episodes.length
      ? episodes.map((e) => e.summary).join(' | ')
      : '(none)';

    const block = [
      `## ${timeOnly} UTC | chat ${chatId} | session ${sessionNo}`,
      `- Goal: ${sanitizeOneLine(workingMemory.currentGoal, 180)}`,
      `- User intent: ${sanitizeOneLine(userText, 180)}`,
      `- Assistant outcome: ${sanitizeOneLine(assistantText, 180)}`,
      `- Key facts captured: ${sanitizeOneLine(factLine, 220)}`,
      `- Episode updates: ${sanitizeOneLine(episodeLine, 220)}`,
      `- Open loops: ${loops.length ? loops.map((x) => sanitizeOneLine(x, 80)).join(' | ') : '(none)'}`,
      `- Artifacts: ${artifacts.join(', ')}`,
      ''
    ].join('\n');

    appendText(dailyPath, block);
    return dailyPath;
  }

  chunkDocument(text, size = 900, overlap = 140) {
    const clean = String(text || '').replace(/\r/g, '');
    if (!clean.trim()) {
      return [];
    }
    const chunks = [];
    let i = 0;
    while (i < clean.length) {
      const part = clean.slice(i, i + size).trim();
      if (part) {
        chunks.push(part);
      }
      i += Math.max(1, size - overlap);
    }
    return chunks;
  }

  indexDocument(chatId, docPath, tags = []) {
    const text = readText(docPath, '');
    const chunks = this.chunkDocument(text);
    for (let i = 0; i < chunks.length; i += 1) {
      this.store.upsertDocumentChunk({
        chatId,
        path: docPath,
        chunkIndex: i,
        text: chunks[i],
        embedding: embedText(chunks[i], this.config.embeddingDim),
        tags
      });
    }
  }

  refreshCuratedFiles(chatId) {
    const facts = this.store.getTopFacts(chatId, 200)
      .filter((f) => Number(f.confidence) >= this.config.curatedMinConfidence);

    const memoryLines = [
      '# MEMORY',
      '',
      `Curated on ${new Date().toISOString()}.`,
      'Stable memory only (high confidence):',
      ''
    ];

    const userLines = [
      '# USER',
      '',
      `Updated on ${new Date().toISOString()}.`,
      ''
    ];

    const maxFactBullets = Math.max(1, this.config.maxMemoryLines - 8);
    for (const f of facts.slice(0, maxFactBullets)) {
      memoryLines.push(`- ${f.subject} ${f.predicate} ${f.object}`);
      if (f.subject === 'user' || String(f.subject).startsWith('user:')) {
        userLines.push(`- ${f.predicate}: ${f.object}`);
      }
    }

    const finalMemory = memoryLines;
    const finalUser = userLines.slice(0, this.config.maxMemoryLines);

    writeText(this.paths.memory, `${finalMemory.join('\n')}\n`);
    writeText(this.paths.user, `${finalUser.join('\n')}\n`);

    this.indexDocument(chatId, this.paths.memory, ['canonical', 'memory']);
    this.indexDocument(chatId, this.paths.user, ['canonical', 'user']);
  }

  retainReflectIndex({ chatId, state, userText, assistantText, workingMemory }) {
    const timestamp = new Date().toISOString();
    const sessionNo = state.session_no;
    const safeUserText = redactSensitiveText(userText);
    const safeAssistantText = redactSensitiveText(assistantText);
    const safeWorkingMemory = {
      ...workingMemory,
      currentGoal: redactSensitiveText(workingMemory?.currentGoal || ''),
      assumptions: Array.isArray(workingMemory?.assumptions)
        ? workingMemory.assumptions.map((x) => redactSensitiveText(x))
        : [],
      activeConstraints: Array.isArray(workingMemory?.activeConstraints)
        ? workingMemory.activeConstraints.map((x) => redactSensitiveText(x))
        : [],
      toolResultsSummary: Array.isArray(workingMemory?.toolResultsSummary)
        ? workingMemory.toolResultsSummary.map((x) => redactSensitiveText(x))
        : []
    };

    this.store.recordExchange(chatId, sessionNo, safeUserText, safeAssistantText);
    const turnCount = this.store.getSessionTurnCount(chatId, sessionNo);

    const sessionFiles = this.ensureSessionFiles(chatId, sessionNo);

    const factCandidates = extractFactCandidates(safeUserText, safeAssistantText)
      .slice(0, this.config.maxFactsPerRetain);

    const episodeCandidates = extractEpisodeCandidates({
      userText: safeUserText,
      assistantText: safeAssistantText,
      maxEpisodes: this.config.maxEpisodesPerRetain
    });

    const openLoopCandidates = extractOpenLoops(safeAssistantText).slice(0, this.config.maxOpenLoops);

    const artifacts = [
      path.relative(this.paths.root, sessionFiles.summary),
      path.relative(this.paths.root, sessionFiles.keyFacts)
    ];

    const dailyPath = this.appendDailyJournal({
      chatId,
      sessionNo,
      timestamp,
      workingMemory: safeWorkingMemory,
      userText: safeUserText,
      assistantText: safeAssistantText,
      facts: factCandidates,
      episodes: episodeCandidates,
      loops: openLoopCandidates,
      artifacts
    });

    const turnEntry = `- ${timestamp}: user="${sanitizeOneLine(safeUserText, 100)}" assistant="${sanitizeOneLine(safeAssistantText, 140)}"`;
    this.updateSessionSummary(sessionFiles.summary, turnEntry);
    this.updateSessionKeyFacts(sessionFiles.keyFacts, factCandidates, timestamp);

    for (const fact of factCandidates) {
      this.store.upsertFact({
        chatId,
        subject: fact.subject,
        predicate: fact.predicate,
        object: fact.object,
        confidence: fact.confidence,
        tags: fact.tags,
        createdAt: timestamp,
        sourceFile: dailyPath,
        sourceExcerpt: sanitizeOneLine(`${fact.subject} ${fact.predicate} ${fact.object}`, 150)
      });
    }

    for (const ep of episodeCandidates) {
      this.store.insertEpisode({
        chatId,
        summary: ep.summary,
        entities: ep.entities,
        tags: ep.tags,
        salience: ep.salience,
        timeRangeStart: timestamp,
        timeRangeEnd: timestamp,
        sourceFiles: [dailyPath, sessionFiles.summary],
        embedding: embedText(ep.summary, this.config.embeddingDim)
      });
    }

    for (const loopText of openLoopCandidates) {
      this.store.upsertOpenLoop({
        chatId,
        text: loopText,
        tags: ['todo', ...extractEntities(loopText)],
        confidence: 0.72,
        sourceFile: dailyPath
      });
    }

    if (/\b(done|resolved|completed|closed)\b/i.test(userText)) {
      const open = this.store.getOpenLoops(chatId, 20);
      for (const loop of open) {
        const words = tokenize(loop.text).slice(0, 6);
        if (words.length && words.some((w) => userText.toLowerCase().includes(w))) {
          this.store.resolveOpenLoop(chatId, loop.text);
        }
      }
    }

    this.indexDocument(chatId, dailyPath, ['daily']);
    this.indexDocument(chatId, sessionFiles.summary, ['session']);
    this.indexDocument(chatId, sessionFiles.keyFacts, ['session', 'facts']);
    this.indexDocument(chatId, this.paths.identity, ['canonical', 'identity']);
    this.indexDocument(chatId, this.paths.soul, ['canonical', 'soul']);
    this.indexDocument(chatId, this.paths.tools, ['canonical', 'tools']);
    this.indexDocument(chatId, this.paths.heartbeat, ['canonical', 'heartbeat']);

    if (turnCount % this.config.sessionSummaryEveryTurns === 0) {
      const recent = this.store.getRecentExchanges(chatId, this.config.sessionSummaryEveryTurns, sessionNo);
      const checkpointLines = recent
        .reverse()
        .map((r) => `- ${sanitizeOneLine(r.user_text, 90)} => ${sanitizeOneLine(r.assistant_text, 120)}`);
      appendText(
        sessionFiles.summary,
        `\n## Checkpoint ${timestamp}\n${checkpointLines.join('\n')}\n`
      );
      this.indexDocument(chatId, sessionFiles.summary, ['session']);
    }

    this.refreshCuratedFiles(chatId);

    return {
      factCount: factCandidates.length,
      episodeCount: episodeCandidates.length,
      openLoopCount: openLoopCandidates.length,
      dailyPath
    };
  }

  compressDailyToWeekly() {
    const files = fs.readdirSync(this.paths.daily)
      .filter((name) => /^\d{4}-\d{2}-\d{2}\.md$/.test(name))
      .sort();

    const grouped = new Map();
    const cutoff = new Date(Date.now() - this.config.heartbeatCompressOlderThanDays * 86400000);

    for (const file of files) {
      const iso = file.slice(0, 10);
      const day = new Date(`${iso}T00:00:00Z`);
      if (day > cutoff) {
        continue;
      }
      const week = toIsoWeek(day);
      const list = grouped.get(week) || [];
      list.push(path.join(this.paths.daily, file));
      grouped.set(week, list);
    }

    for (const [week, weekFiles] of grouped.entries()) {
      const outPath = path.join(this.paths.weekly, `${week}.md`);
      const lines = [`# Weekly Summary ${week}`, ''];
      for (const filePath of weekFiles) {
        const date = path.basename(filePath, '.md');
        lines.push(`## ${date}`);
        const body = readText(filePath, '');
        const highlights = body
          .split('\n')
          .map((l) => l.trim())
          .filter((l) => l.startsWith('- '))
          .slice(0, 10);
        if (highlights.length) {
          lines.push(...highlights);
        } else {
          lines.push('- (no highlights)');
        }
        lines.push('');
      }
      writeText(outPath, `${lines.join('\n')}\n`);
    }

    return grouped.size;
  }

  runHeartbeat() {
    const chats = this.store.getChatIds();
    let contradictionCount = 0;

    const weeklyUpdated = this.compressDailyToWeekly();

    this.store.dedupeFacts();

    for (const chatId of chats) {
      this.store.decayFactConfidence(
        chatId,
        this.config.heartbeatDecayDays,
        this.config.heartbeatDecayStep
      );

      const contradictions = this.store.detectContradictions(chatId, this.config.confidenceThreshold);
      contradictionCount += contradictions.length;
      this.refreshCuratedFiles(chatId);

      this.indexDocument(chatId, this.paths.memory, ['canonical', 'memory']);
      this.indexDocument(chatId, this.paths.user, ['canonical', 'user']);
    }

    const line = [
      `[${new Date().toISOString()}] heartbeat`,
      `weekly_summaries=${weeklyUpdated}`,
      `contradictions=${contradictionCount}`,
      `chats=${chats.length}`
    ].join(' ');
    appendText(this.paths.heartbeatLog, `${line}\n`);

    return {
      weeklyUpdated,
      contradictionCount,
      chatCount: chats.length
    };
  }

  getMemoryStatus(chatId) {
    const state = this.store.getChatState(chatId);
    const sessionNo = state.session_no;
    const sessionFiles = this.ensureSessionFiles(chatId, sessionNo);
    const facts = this.store.getTopFacts(chatId, 10);
    const loops = this.store.getOpenLoops(chatId, 6);
    const contradictions = this.store.listContradictions(chatId);
    const turnCount = this.store.getSessionTurnCount(chatId, sessionNo);

    return {
      state,
      sessionNo,
      turnCount,
      sessionFiles,
      facts,
      loops,
      contradictions,
      canonicalFiles: [
        this.paths.soul,
        this.paths.identity,
        this.paths.user,
        this.paths.memory,
        this.paths.heartbeat,
        this.paths.tools
      ]
    };
  }
}
