import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import {
  computeNextRunIso,
  parseScheduleSpec,
  validateTimeZone
} from './schedule.js';
import {
  buildSkillRunPrompt,
  createSkillManager,
  normalizeSkillName
} from './skills.js';
import { transcribeLocalAudioFile } from './transcription.js';
import { upsertEnvText } from './env-file.js';

const IMAGE_EXTS = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tif', '.tiff', '.svg']);
const AUDIO_EXTS = new Set(['.ogg', '.mp3', '.wav', '.m4a', '.aac', '.flac', '.webm']);
const URL_IMAGE_RE = /https?:\/\/[^\s"'`<>]+\.(?:png|jpe?g|gif|webp|bmp|tiff?|svg)(?:\?[^\s"'`<>]*)?/gi;
const ABS_PATH_IMAGE_RE = /\/[^\s"'`<>]+?\.(?:png|jpe?g|gif|webp|bmp|tiff?|svg)/gi;
const ABS_PATH_FILE_RE = /\/[^\s"'`<>]+?\.[a-z0-9]{1,10}/gi;

const DASHBOARD_CHAT_BASE = -8_000_000_000_000;
const DEFAULT_SCOPE = 'main';
const DEFAULT_STREAM_FLUSH_MIN = 110;
const DEFAULT_STREAM_FLUSH_MAX = 320;
const DASHBOARD_MAX_BODY_BYTES = 25 * 1024 * 1024;
const FILE_TOKEN_TTL_MS = 2 * 60 * 60 * 1000;
const MAX_ENV_VALUE_LENGTH = 20_000;

const ENV_FIELD_META = {
  BOT_APP_NAME: { description: 'Display name used by the bot in UI/messages.' },
  TELEGRAM_BOT_TOKEN: { description: 'Telegram Bot API token from @BotFather.', sensitive: true },
  ALLOWED_TELEGRAM_USER_IDS: { description: 'Comma-separated Telegram user IDs allowed to talk to bot.' },
  ALLOWED_TELEGRAM_CHAT_IDS: { description: 'Comma-separated chat IDs allowed when group mode is enabled.' },
  TELEGRAM_PRIVATE_ONLY: { description: 'When true, only private chats are allowed.' },
  TELEGRAM_GROUP_REQUIRE_MENTION: { description: 'When true, group replies require mention/reply.' },
  CODEX_CWD: { description: 'Working directory for Codex operations.' },
  CODEX_APP_SERVER_URL: { description: 'Codex app-server websocket URL.' },
  CODEX_MODEL: { description: 'Optional model name override.' },
  CODEX_MODEL_PROVIDER: { description: 'Optional model provider override.' },
  CODEX_ACCESS_PROFILE: { description: 'Access profile label: full/partial/readonly/custom.' },
  CODEX_SANDBOX: { description: 'Sandbox mode for Codex execution.' },
  CODEX_APPROVAL_POLICY: { description: 'Command approval mode for Codex.' },
  CODEX_ENABLE_MULTI_AGENT: { description: 'Enable Codex multi-agent mode.' },
  CODEX_ENABLE_CHILD_AGENTS_MD: { description: 'Enable child_agents_md feature.' },
  DB_PATH: { description: 'SQLite database path.' },
  MEMORY_ROOT: { description: 'Memory folder root path.' },
  DASHBOARD_ENABLED: { description: 'Enable/disable local dashboard server.' },
  DASHBOARD_HOST: { description: 'Dashboard bind host (use localhost for safety).' },
  DASHBOARD_PORT: { description: 'Dashboard bind port.' },
  DASHBOARD_AUTH_TOKEN: { description: 'Dashboard auth token.', sensitive: true },
  SCHEDULE_POLL_INTERVAL_MS: { description: 'Scheduler polling interval in milliseconds.' },
  SCHEDULE_DEFAULT_TIMEZONE: { description: 'Default timezone for schedule parsing.' },
  SKILLS_ROOT: { description: 'Writable skills root path.' },
  SKILLS_INCLUDE_CODEX_HOME: { description: 'Include shared skills from CODEX_HOME.' },
  SKILLS_CODEX_HOME_ROOT: { description: 'Shared skills root path.' },
  WHISPER_ENABLED: { description: 'Enable local whisper transcription.' },
  WHISPER_COMMAND: { description: 'Whisper CLI command.' },
  WHISPER_MODEL: { description: 'Whisper model to use.' },
  WHISPER_LANGUAGE: { description: 'Optional fixed whisper language hint.' },
  WHISPER_TASK: { description: 'Whisper task: transcribe/translate.' },
  WHISPER_TIMEOUT_MS: { description: 'Whisper timeout in milliseconds.' },
  WHISPER_PERSISTENT: { description: 'Keep whisper worker hot between requests.' },
  WHISPER_PYTHON_COMMAND: { description: 'Explicit python command for whisper worker.' },
  WHISPER_EXTRA_ARGS: { description: 'Extra whisper CLI args.' }
};

const BOOL_KEYS = new Set([
  'TELEGRAM_PRIVATE_ONLY',
  'TELEGRAM_GROUP_REQUIRE_MENTION',
  'CODEX_ENABLE_MULTI_AGENT',
  'CODEX_ENABLE_CHILD_AGENTS_MD',
  'DASHBOARD_ENABLED',
  'SKILLS_INCLUDE_CODEX_HOME',
  'WHISPER_ENABLED',
  'WHISPER_PERSISTENT'
]);

const INT_KEYS = new Set([
  'DASHBOARD_PORT',
  'SCHEDULE_POLL_INTERVAL_MS',
  'WHISPER_TIMEOUT_MS'
]);

const SCHEDULE_ACTION_INSTRUCTIONS = [
  'Automation actions available in this chat runtime:',
  '- To create a schedule from the current conversation, output one line exactly:',
  '  SCHEDULE_CREATE: {"kind":"report|heartbeat","scheduleSpec":"daily HH:MM or cron m h dom mon dow","timezone":"IANA timezone or empty","prompt":"what to send","confirmation":"optional short confirmation"}',
  '- Only emit SCHEDULE_CREATE when the user clearly asked to schedule something.',
  '- If intent is ambiguous, ask a concise clarification question in normal text and do not emit SCHEDULE_CREATE.'
].join('\n');

function nowIso() {
  return new Date().toISOString();
}

function clampText(text, max = 4000) {
  const value = String(text || '').trim();
  if (!value) return '(empty response)';
  if (value.length <= max) return value;
  return `${value.slice(0, max - 20)}\n\n...[truncated]`;
}

function normalizeScopeName(raw) {
  const base = String(raw || '').trim().toLowerCase();
  if (!base) return DEFAULT_SCOPE;
  return base
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64) || DEFAULT_SCOPE;
}

function fnv1a32(text) {
  let h = 0x811c9dc5;
  const input = String(text || '');
  for (let i = 0; i < input.length; i += 1) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export function deriveDashboardChatId(scopeName) {
  const scope = normalizeScopeName(scopeName);
  return DASHBOARD_CHAT_BASE - fnv1a32(scope);
}

function normalizeRef(ref) {
  return String(ref || '').trim().replace(/^['"]|['"]$/g, '');
}

function uniqueRefs(refs) {
  const out = [];
  const seen = new Set();
  for (const raw of refs) {
    const ref = normalizeRef(raw);
    if (!ref) continue;
    const key = ref.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(ref);
  }
  return out;
}

function stripMarkdownDecorations(text) {
  let out = String(text || '');
  if (!out) return out;

  out = out.replace(/```(?:[a-zA-Z0-9_-]+)?\n?/g, '');
  out = out.replace(/\[([^\]\n]+)\]\((https?:\/\/[^\s)]+)\)/g, '$1 ($2)');
  out = out.replace(/<(https?:\/\/[^>\s]+)>/g, '$1');
  out = out.replace(/^#{1,6}\s+/gm, '');
  out = out.replace(/\*\*([^*\n]+)\*\*/g, '$1');
  out = out.replace(/__([^_\n]+)__/g, '$1');
  out = out.replace(/`([^`\n]+)`/g, '$1');
  out = out.replace(/`+/g, '');

  return out;
}

function normalizeSentenceSpacing(text) {
  let out = String(text || '');
  if (!out) return out;
  out = out.replace(/([.!?]["')\]]*)([A-Z])/g, '$1 $2');
  return out;
}

function normalizeOutgoingText(text) {
  const raw = String(text || '').replace(/\r\n?/g, '\n');
  if (!raw.trim()) return '';
  const clean = normalizeSentenceSpacing(stripMarkdownDecorations(raw));

  const lines = clean.split('\n').map((line) => {
    const trimmed = line.trim();
    if (!trimmed) return '';
    const urlOnly = trimmed.match(/^[`'"]?(https?:\/\/[^\s`'"]+)[`'"]?$/i);
    if (urlOnly) return urlOnly[1];
    return line.replace(/\s+$/g, '');
  });

  return lines.join('\n').replace(/\n{3,}/g, '\n\n').trim();
}

function parseOutputMarkers(text) {
  const imageRefs = [];
  const fileRefs = [];
  const skillCreateSpecs = [];
  const scheduleCreateSpecs = [];
  const lines = String(text || '').split('\n');
  const kept = [];

  for (const line of lines) {
    const imageMatch = line.match(/^\s*IMAGE_OUTPUT:\s*(.+)\s*$/i);
    if (imageMatch) {
      imageRefs.push(imageMatch[1]);
      continue;
    }
    const fileMatch = line.match(/^\s*FILE_OUTPUT:\s*(.+)\s*$/i);
    if (fileMatch) {
      fileRefs.push(fileMatch[1]);
      continue;
    }
    const skillCreateMatch = line.match(/^\s*SKILL_CREATE:\s*(.+)\s*$/i);
    if (skillCreateMatch) {
      skillCreateSpecs.push(skillCreateMatch[1]);
      continue;
    }
    const scheduleCreateMatch = line.match(/^\s*SCHEDULE_CREATE:\s*(.+)\s*$/i);
    if (scheduleCreateMatch) {
      scheduleCreateSpecs.push(scheduleCreateMatch[1]);
      continue;
    }
    kept.push(line);
  }

  const cleanText = normalizeOutgoingText(kept.join('\n').replace(/\n{3,}/g, '\n\n').trim());
  return {
    cleanText,
    imageRefs: uniqueRefs(imageRefs),
    fileRefs: uniqueRefs(fileRefs),
    skillCreateSpecs: uniqueRefs(skillCreateSpecs),
    scheduleCreateSpecs: uniqueRefs(scheduleCreateSpecs)
  };
}

function parseSkillCreateSpec(rawSpec) {
  const parts = String(rawSpec || '').split('|').map((p) => p.trim());
  const name = parts[0] || '';
  if (!name) {
    return null;
  }
  const description = parts[1] || '';
  const instructions = parts.slice(2).join(' | ').trim();
  return { name, description, instructions };
}

function extractFirstJsonObject(text) {
  const raw = String(text || '').trim();
  if (!raw) return null;

  const fence = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const body = (fence?.[1] || raw).trim();
  if (!body) return null;

  try {
    return JSON.parse(body);
  } catch {
    // fallback
  }

  const start = body.indexOf('{');
  const end = body.lastIndexOf('}');
  if (start < 0 || end <= start) return null;
  const candidate = body.slice(start, end + 1).trim();
  try {
    return JSON.parse(candidate);
  } catch {
    return null;
  }
}

function parseScheduleCreateSpec(rawSpec, defaultTimezone) {
  const parsed = extractFirstJsonObject(rawSpec);
  if (!parsed || typeof parsed !== 'object') return null;

  const kindRaw = String(parsed.kind || '').trim().toLowerCase();
  const kind = kindRaw === 'heartbeat' ? 'heartbeat' : 'report';

  const scheduleSpec = String(parsed.scheduleSpec || '').trim();
  if (!scheduleSpec) return null;
  parseScheduleSpec(scheduleSpec);

  const tzRaw = String(parsed.timezone || '').trim();
  const timezone = tzRaw || defaultTimezone;
  if (!validateTimeZone(timezone)) {
    throw new Error(`Invalid timezone "${timezone}". Example: tz=America/Los_Angeles`);
  }

  const prompt = String(parsed.prompt || '').trim();
  if (!prompt) return null;

  const confirmation = String(parsed.confirmation || parsed.confirmationMessage || '').trim();
  return {
    kind,
    scheduleSpec,
    timezone,
    prompt,
    confirmation
  };
}

function extractImageRefsFallback(text) {
  const refs = [];
  for (const m of String(text || '').matchAll(URL_IMAGE_RE)) {
    refs.push(m[0]);
  }
  for (const m of String(text || '').matchAll(ABS_PATH_IMAGE_RE)) {
    refs.push(m[0]);
  }
  return uniqueRefs(refs);
}

function extractFileRefsFallback(text) {
  const refs = [];
  for (const m of String(text || '').matchAll(ABS_PATH_FILE_RE)) {
    refs.push(m[0]);
  }
  return uniqueRefs(refs);
}

function isHttpUrl(value) {
  return /^https?:\/\//i.test(String(value || '').trim());
}

function fileLooksLikeImage(filePath) {
  const ext = path.extname(String(filePath || '')).toLowerCase();
  return IMAGE_EXTS.has(ext);
}

function fileLooksLikeAudio(filePath) {
  const ext = path.extname(String(filePath || '')).toLowerCase();
  return AUDIO_EXTS.has(ext);
}

function resolveLocalPath(ref, codexCwd) {
  const normalized = normalizeRef(ref);
  if (!normalized || isHttpUrl(normalized)) {
    return null;
  }
  if (path.isAbsolute(normalized)) {
    return normalized;
  }
  if (normalized.startsWith('~/')) {
    return path.join(process.env.HOME || '', normalized.slice(2));
  }
  return path.resolve(codexCwd, normalized);
}

function shouldFlushStreamingChunk(text, force = false) {
  if (force) return String(text || '').length > 0;
  const raw = String(text || '');
  if (!raw) return false;
  const boundary = findStreamingFlushBoundary(raw);
  if (boundary <= 0) return false;
  return boundary >= DEFAULT_STREAM_FLUSH_MIN || raw.length >= 240;
}

function findStreamingFlushBoundary(text) {
  const raw = String(text || '');
  if (!raw) return 0;

  const boundaries = [];
  const sentenceRe = /[.!?]+["')\]]*(?:\s+|(?=[A-Z])|$)/g;
  let m;
  while ((m = sentenceRe.exec(raw)) !== null) {
    boundaries.push(m.index + m[0].length);
  }
  if (!boundaries.length) return 0;

  for (const b of boundaries) {
    if (b >= DEFAULT_STREAM_FLUSH_MIN && b <= DEFAULT_STREAM_FLUSH_MAX) {
      return b;
    }
  }
  for (const b of boundaries) {
    if (b > DEFAULT_STREAM_FLUSH_MAX) {
      return b;
    }
  }

  return boundaries[boundaries.length - 1];
}

function takeStreamingFlushSlice(text, force = false) {
  const raw = String(text || '');
  if (!raw) return { emit: '', rest: '' };
  if (force) return { emit: raw, rest: '' };
  const boundary = findStreamingFlushBoundary(raw);
  if (boundary <= 0) return { emit: '', rest: raw };
  return {
    emit: raw.slice(0, boundary),
    rest: raw.slice(boundary)
  };
}

function guessFileExt(filename = '', mime = '') {
  const ext = path.extname(String(filename || '')).toLowerCase();
  if (ext) return ext;
  const m = String(mime || '').toLowerCase();
  if (m.includes('png')) return '.png';
  if (m.includes('jpeg') || m.includes('jpg')) return '.jpg';
  if (m.includes('gif')) return '.gif';
  if (m.includes('webp')) return '.webp';
  if (m.includes('pdf')) return '.pdf';
  if (m.includes('json')) return '.json';
  if (m.includes('csv')) return '.csv';
  if (m.includes('markdown')) return '.md';
  if (m.includes('text')) return '.txt';
  if (m.includes('zip')) return '.zip';
  if (m.includes('mp4')) return '.mp4';
  if (m.includes('mpeg') || m.includes('mp3')) return '.mp3';
  if (m.includes('wav')) return '.wav';
  return '.bin';
}

function safeFileName(name) {
  const base = String(name || 'file')
    .replace(/\.[a-zA-Z0-9]{1,10}$/, '')
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .slice(0, 48);
  return base || 'file';
}

function buildFileAwarePrompt(userPrompt, localPath, fileName = '', mimeType = '') {
  const top = String(userPrompt || '').trim() || 'Please analyze the attached file and help me.';
  const details = [
    '',
    '[Attachment]',
    `- path: ${localPath}`,
    `- name: ${fileName || path.basename(localPath)}`,
    `- mime: ${mimeType || 'unknown'}`,
    '- Read the file from disk and use its contents in your response.'
  ].join('\n');
  return `${top}\n${details}`;
}

function buildMultiAttachmentPrompt(userPrompt, attachments) {
  const top = String(userPrompt || '').trim() || 'Please analyze the attached files and help me.';
  const lines = [
    '',
    '[Attachments]'
  ];
  for (const item of attachments) {
    lines.push(`- path: ${item.path}`);
    lines.push(`  name: ${item.name || path.basename(item.path)}`);
    lines.push(`  mime: ${item.mime || 'unknown'}`);
  }
  lines.push('- Read each file from disk and use the contents in your response.');
  return `${top}\n${lines.join('\n')}`;
}

function parsePipeArgs(text) {
  return String(text || '')
    .split('|')
    .map((p) => p.trim());
}

function parseTzHint(text, fallbackTz) {
  const input = String(text || '').trim();
  const m = input.match(/\btz=([A-Za-z_+\-/]+)\b/i);
  if (!m) {
    return { cleaned: input, timezone: fallbackTz };
  }
  const timezone = String(m[1] || '').trim();
  const cleaned = input.replace(m[0], '').replace(/\s{2,}/g, ' ').trim();
  return { cleaned, timezone };
}

function formatRunAt(iso, timezone) {
  if (!iso) return '(none)';
  try {
    const dt = new Date(iso);
    return `${dt.toLocaleString('en-US', {
      timeZone: timezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    })} (${timezone})`;
  } catch {
    return iso;
  }
}

function formatError(err) {
  const msg = err?.message || String(err);
  return `Error: ${msg.slice(0, 3000)}`;
}

function sendJson(res, code, body) {
  const payload = JSON.stringify(body);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(payload),
    'Cache-Control': 'no-store'
  });
  res.end(payload);
}

function sendText(res, code, body, contentType = 'text/plain; charset=utf-8') {
  res.writeHead(code, {
    'Content-Type': contentType,
    'Content-Length': Buffer.byteLength(body),
    'Cache-Control': 'no-store'
  });
  res.end(body);
}

function sendSseEvent(res, event, data) {
  const payload = typeof data === 'string' ? data : JSON.stringify(data);
  res.write(`event: ${event}\n`);
  res.write(`data: ${payload}\n\n`);
}

async function readRequestBody(req, maxBytes = DASHBOARD_MAX_BODY_BYTES) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    req.on('data', (chunk) => {
      total += chunk.length;
      if (total > maxBytes) {
        reject(new Error(`Request body too large (max ${maxBytes} bytes)`));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      resolve(Buffer.concat(chunks));
    });
    req.on('error', reject);
  });
}

async function readJsonBody(req, maxBytes = DASHBOARD_MAX_BODY_BYTES) {
  const body = await readRequestBody(req, maxBytes);
  if (!body.length) return {};
  try {
    return JSON.parse(body.toString('utf8'));
  } catch {
    throw new Error('Invalid JSON body');
  }
}

function isLocalRequest(req) {
  const remote = String(req.socket?.remoteAddress || '');
  if (remote === '127.0.0.1' || remote === '::1' || remote === '::ffff:127.0.0.1') {
    return true;
  }
  return false;
}

function isWithin(parent, target) {
  const rel = path.relative(parent, target);
  return rel && !rel.startsWith('..') && !path.isAbsolute(rel);
}

function parseBoolText(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return null;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  return null;
}

function parseEnvValue(rawValue) {
  const v = String(rawValue ?? '').trim();
  if (!v) return '';
  if (
    (v.startsWith('"') && v.endsWith('"'))
    || (v.startsWith("'") && v.endsWith("'"))
  ) {
    const inner = v.slice(1, -1);
    return inner
      .replace(/\\n/g, '\n')
      .replace(/\\"/g, '"')
      .replace(/\\\\/g, '\\');
  }
  return v;
}

function parseEnvMap(text) {
  const out = new Map();
  const lines = String(text || '').split('\n');
  for (const line of lines) {
    if (!line || /^\s*#/.test(line)) continue;
    const idx = line.indexOf('=');
    if (idx <= 0) continue;
    const key = line.slice(0, idx).trim();
    if (!/^[A-Z0-9_]+$/.test(key)) continue;
    const value = parseEnvValue(line.slice(idx + 1));
    out.set(key, value);
  }
  return out;
}

function isSensitiveEnvKey(key) {
  if (ENV_FIELD_META[key]?.sensitive) return true;
  return /(TOKEN|SECRET|PASSWORD|PRIVATE_KEY|API_KEY|AUTH|CREDENTIAL)/i.test(String(key || ''));
}

function maskSecret(value) {
  const raw = String(value || '');
  if (!raw) return '(not set)';
  if (raw.length <= 8) return `${raw.slice(0, 2)}***`;
  return `${raw.slice(0, 4)}...${raw.slice(-2)}`;
}

function validateCsvIntegers(value, { allowNegative = false } = {}) {
  const items = String(value || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
  for (const item of items) {
    const n = Number(item);
    if (!Number.isInteger(n)) {
      throw new Error(`Invalid integer value in list: "${item}"`);
    }
    if (!allowNegative && n <= 0) {
      throw new Error(`Value must be > 0: "${item}"`);
    }
  }
}

export function createDashboardServer({
  appSettings,
  codexSettings,
  dashboardSettings,
  scheduleSettings,
  skillSettings,
  whisperSettings,
  codex,
  memory,
  store,
  restartService,
  autostart
}) {
  const appName = String(appSettings?.name || 'Dexbot').trim() || 'Dexbot';
  const host = String(dashboardSettings?.host || '127.0.0.1');
  const port = Number(dashboardSettings?.port || 8788);
  let currentAuthToken = String(dashboardSettings?.authToken || '').trim();
  const schedulePollMs = Math.max(5_000, Number(scheduleSettings?.pollIntervalMs || 15_000));
  const scheduleDefaultTimezone = (
    String(scheduleSettings?.defaultTimezone || Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC').trim() || 'UTC'
  );

  const queueByScope = new Map();
  const streamClientsByScope = new Map();
  const fileTokenMap = new Map();
  const runningScheduleJobIds = new Set();

  const skillManager = createSkillManager({
    writableRoot: skillSettings?.root || path.join(codexSettings.cwd, '.agents', 'skills'),
    readRoots: (skillSettings?.includeCodexHome && skillSettings?.codexHomeRoot)
      ? [skillSettings.codexHomeRoot]
      : []
  });

  const scopeRegistryPath = path.join(memory.paths.root, 'system', 'dashboard-scopes.json');
  let scopeRegistry = {
    defaultScope: DEFAULT_SCOPE,
    scopes: {}
  };

  let server = null;
  let scheduleTimer = null;
  let heartbeatTimer = null;

  let webIndex = '';
  let webApp = '';
  let webCss = '';
  const envPath = path.resolve(process.cwd(), '.env');
  const envExamplePath = path.resolve(process.cwd(), '.env.example');

  function scopeFromChatId(chatId) {
    const all = scopeRegistry?.scopes || {};
    for (const [scope, meta] of Object.entries(all)) {
      if (Number(meta?.chatId) === Number(chatId)) {
        return scope;
      }
    }
    return DEFAULT_SCOPE;
  }

  function registerScope(scopeName) {
    const normalized = normalizeScopeName(scopeName);
    const now = nowIso();
    const existing = scopeRegistry.scopes[normalized];
    if (existing) {
      existing.updatedAt = now;
      return {
        name: normalized,
        chatId: Number(existing.chatId)
      };
    }

    const chatId = deriveDashboardChatId(normalized);
    scopeRegistry.scopes[normalized] = {
      chatId,
      createdAt: now,
      updatedAt: now
    };

    return {
      name: normalized,
      chatId
    };
  }

  async function loadScopeRegistry() {
    try {
      const raw = await fsPromises.readFile(scopeRegistryPath, 'utf8');
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object' && parsed.scopes && typeof parsed.scopes === 'object') {
        scopeRegistry = {
          defaultScope: normalizeScopeName(parsed.defaultScope || DEFAULT_SCOPE),
          scopes: parsed.scopes
        };
      }
    } catch {
      scopeRegistry = {
        defaultScope: DEFAULT_SCOPE,
        scopes: {}
      };
    }

    registerScope(scopeRegistry.defaultScope || DEFAULT_SCOPE);
    await saveScopeRegistry();
  }

  async function saveScopeRegistry() {
    await fsPromises.mkdir(path.dirname(scopeRegistryPath), { recursive: true });
    const tmp = `${scopeRegistryPath}.tmp`;
    await fsPromises.writeFile(tmp, JSON.stringify(scopeRegistry, null, 2), 'utf8');
    await fsPromises.rename(tmp, scopeRegistryPath);
  }

  function listScopes() {
    const entries = Object.entries(scopeRegistry.scopes || {})
      .map(([name, meta]) => ({
        name,
        chatId: Number(meta.chatId),
        createdAt: String(meta.createdAt || ''),
        updatedAt: String(meta.updatedAt || '')
      }))
      .sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));

    if (!entries.some((x) => x.name === scopeRegistry.defaultScope)) {
      const main = registerScope(scopeRegistry.defaultScope || DEFAULT_SCOPE);
      entries.unshift({
        name: main.name,
        chatId: main.chatId,
        createdAt: nowIso(),
        updatedAt: nowIso()
      });
    }

    return entries;
  }

  async function readEnvText(filePath) {
    try {
      return await fsPromises.readFile(filePath, 'utf8');
    } catch {
      return '';
    }
  }

  function orderedEnvKeys(currentMap, exampleMap) {
    const keys = new Set([...currentMap.keys(), ...exampleMap.keys()]);
    const ordered = [];

    for (const key of Object.keys(ENV_FIELD_META)) {
      if (keys.has(key)) {
        ordered.push(key);
        keys.delete(key);
      }
    }

    const rest = Array.from(keys).sort((a, b) => a.localeCompare(b));
    return [...ordered, ...rest];
  }

  async function getEnvSettingsSchema() {
    const [currentText, exampleText] = await Promise.all([
      readEnvText(envPath),
      readEnvText(envExamplePath)
    ]);

    const currentMap = parseEnvMap(currentText);
    const exampleMap = parseEnvMap(exampleText);
    const keys = orderedEnvKeys(currentMap, exampleMap);

    const fields = keys.map((key) => {
      const sensitive = isSensitiveEnvKey(key);
      const currentValue = currentMap.get(key) ?? '';
      return {
        key,
        sensitive,
        description: ENV_FIELD_META[key]?.description || 'Custom environment setting.',
        value: sensitive ? '' : currentValue,
        present: currentMap.has(key),
        masked: sensitive ? maskSecret(currentValue) : null
      };
    });

    return {
      envPath,
      fields
    };
  }

  function validateEnvUpdate(key, value) {
    const v = String(value ?? '');
    if (!/^[A-Z0-9_]+$/.test(key)) {
      throw new Error(`Invalid env key: ${key}`);
    }
    if (v.length > MAX_ENV_VALUE_LENGTH) {
      throw new Error(`Value too large for ${key}`);
    }

    if (BOOL_KEYS.has(key)) {
      if (parseBoolText(v) == null && v !== '') {
        throw new Error(`${key} must be true/false`);
      }
    }

    if (INT_KEYS.has(key) && v !== '') {
      const n = Number(v);
      if (!Number.isInteger(n) || n <= 0) {
        throw new Error(`${key} must be a positive integer`);
      }
    }

    if (key === 'ALLOWED_TELEGRAM_USER_IDS' && v !== '') {
      validateCsvIntegers(v, { allowNegative: false });
    }
    if (key === 'ALLOWED_TELEGRAM_CHAT_IDS' && v !== '') {
      validateCsvIntegers(v, { allowNegative: true });
    }
    if (key === 'TELEGRAM_BOT_TOKEN' && v !== '' && !/^\d+:[A-Za-z0-9_-]{20,}$/.test(v)) {
      throw new Error('TELEGRAM_BOT_TOKEN format looks invalid');
    }
    if (key === 'DASHBOARD_HOST' && v !== '' && /\s/.test(v)) {
      throw new Error('DASHBOARD_HOST cannot contain spaces');
    }
    if (key === 'DASHBOARD_PORT' && v !== '') {
      const n = Number(v);
      if (n < 1 || n > 65535) {
        throw new Error('DASHBOARD_PORT must be between 1 and 65535');
      }
    }
  }

  async function applyEnvUpdates(updates) {
    const [currentText, exampleText] = await Promise.all([
      readEnvText(envPath),
      readEnvText(envExamplePath)
    ]);

    const currentMap = parseEnvMap(currentText);
    const exampleMap = parseEnvMap(exampleText);
    const allowedKeys = new Set([...currentMap.keys(), ...exampleMap.keys()]);
    const upserts = {};
    const changedKeys = [];

    for (const item of updates) {
      const key = String(item?.key || '').trim();
      const value = String(item?.value ?? '');
      if (!key) continue;

      if (!allowedKeys.has(key) && !/^[A-Z0-9_]+$/.test(key)) {
        throw new Error(`Key "${key}" is not allowed`);
      }
      validateEnvUpdate(key, value);

      const prev = currentMap.get(key) ?? '';
      if (prev === value) {
        continue;
      }
      upserts[key] = value;
      changedKeys.push(key);
    }

    if (!changedKeys.length) {
      return { changedKeys: [], restartRequired: false };
    }

    const nextText = upsertEnvText(currentText, upserts);
    const tmpPath = `${envPath}.tmp`;
    await fsPromises.writeFile(tmpPath, nextText, 'utf8');
    await fsPromises.rename(tmpPath, envPath);
    await fsPromises.chmod(envPath, 0o600).catch(() => undefined);

    if (Object.prototype.hasOwnProperty.call(upserts, 'DASHBOARD_AUTH_TOKEN')) {
      currentAuthToken = String(upserts.DASHBOARD_AUTH_TOKEN || '').trim();
    }

    return {
      changedKeys,
      restartRequired: true
    };
  }

  function emitToScope(scopeName, event, payload) {
    const scope = normalizeScopeName(scopeName);
    const clients = streamClientsByScope.get(scope);
    if (!clients || !clients.size) return;
    for (const res of clients) {
      try {
        sendSseEvent(res, event, payload);
      } catch {
        // Ignore individual stream failures.
      }
    }
  }

  function pushAssistant(scopeName, text, meta = {}) {
    emitToScope(scopeName, 'assistant', {
      text: normalizeOutgoingText(text),
      ts: Date.now(),
      ...meta
    });
  }

  function pushStatus(scopeName, text) {
    emitToScope(scopeName, 'status', {
      text: String(text || ''),
      ts: Date.now()
    });
  }

  function enqueueScope(scopeName, task) {
    const scope = normalizeScopeName(scopeName);
    const prev = queueByScope.get(scope) || Promise.resolve();
    const next = prev
      .catch(() => undefined)
      .then(task)
      .finally(() => {
        if (queueByScope.get(scope) === next) {
          queueByScope.delete(scope);
        }
      });
    queueByScope.set(scope, next);
    return next;
  }

  function createScheduleForScope(scopeMeta, {
    kind = 'report',
    scheduleSpec,
    timezone,
    prompt
  }) {
    if (!validateTimeZone(timezone)) {
      throw new Error(`Invalid timezone "${timezone}". Example: tz=America/Los_Angeles`);
    }

    const parsed = parseScheduleSpec(scheduleSpec);
    const nextRunAt = computeNextRunIso({
      cronExpr: parsed.cronExpr,
      timezone,
      from: new Date()
    });

    const titleSource = String(prompt || '').split(/\r?\n/)[0].trim();
    const title = (kind === 'heartbeat' ? `Heartbeat ${parsed.normalizedSpec}` : titleSource)
      .slice(0, 80) || 'Scheduled job';

    return store.createScheduledJob({
      baseChatId: scopeMeta.chatId,
      scopedChatId: scopeMeta.chatId,
      topicId: 0,
      title,
      kind,
      prompt,
      cronExpr: parsed.cronExpr,
      timezone,
      active: true,
      nextRunAt
    });
  }

  function buildScheduledPrompt(job) {
    if (job.kind === 'heartbeat') {
      const goal = String(job.prompt || '').trim() || 'Check in proactively with the user.';
      return [
        'Send a short proactive heartbeat message to the user.',
        'Keep it conversational and concise (1-3 short sentences).',
        `Goal for this heartbeat: ${goal}`
      ].join('\n');
    }
    return String(job.prompt || '').trim();
  }

  function resolveOutputRef(ref) {
    const normalized = normalizeRef(ref);
    if (!normalized) return null;

    if (isHttpUrl(normalized)) {
      return {
        type: normalized.match(/\.(png|jpe?g|gif|webp|bmp|tiff?|svg)(\?|$)/i) ? 'image' : 'file',
        label: path.basename(new URL(normalized).pathname) || normalized,
        url: normalized,
        sourcePath: null
      };
    }

    const absPath = resolveLocalPath(normalized, codexSettings.cwd);
    if (!absPath || !fs.existsSync(absPath)) {
      return null;
    }

    const allowedRoots = [
      path.resolve(codexSettings.cwd),
      path.resolve(memory.paths.root)
    ];

    const inside = allowedRoots.some((root) => {
      if (absPath === root) return true;
      return isWithin(root, absPath);
    });

    if (!inside) {
      return null;
    }

    const token = crypto.randomBytes(18).toString('base64url');
    fileTokenMap.set(token, {
      path: absPath,
      expiresAt: Date.now() + FILE_TOKEN_TTL_MS
    });

    return {
      type: fileLooksLikeImage(absPath) ? 'image' : 'file',
      label: path.basename(absPath),
      url: `/api/file/${token}`,
      sourcePath: absPath
    };
  }

  async function saveUploadedFiles(scopeName, files) {
    const scope = normalizeScopeName(scopeName);
    const out = [];
    if (!Array.isArray(files) || !files.length) {
      return out;
    }

    const uploadDir = path.join(memory.paths.root, 'uploads', 'dashboard', scope);
    await fsPromises.mkdir(uploadDir, { recursive: true });

    for (const item of files) {
      const name = String(item?.name || 'upload').trim() || 'upload';
      const mime = String(item?.mime || item?.type || '').trim();
      const data = String(item?.dataBase64 || item?.base64 || '').trim();
      if (!data) continue;

      const bytes = Buffer.from(data, 'base64');
      const ext = guessFileExt(name, mime);
      const base = safeFileName(name);
      const filename = `${base}_${Date.now()}_${crypto.randomUUID().slice(0, 8)}${ext}`;
      const localPath = path.join(uploadDir, filename);
      await fsPromises.writeFile(localPath, bytes);

      out.push({
        name,
        mime,
        path: localPath,
        isImage: fileLooksLikeImage(localPath),
        isAudio: mime.toLowerCase().startsWith('audio/') || fileLooksLikeAudio(localPath)
      });
    }

    return out;
  }

  async function runPromptTurn(scopeMeta, {
    userPrompt,
    inputItems = null,
    introText = 'Thinking...',
    requestId = null
  }) {
    pushStatus(scopeMeta.name, introText);

    const prepared = memory.prepareTurn(scopeMeta.chatId, userPrompt);
    const developerInstructions = [
      String(prepared.developerInstructions || '').trim(),
      SCHEDULE_ACTION_INSTRUCTIONS
    ].filter(Boolean).join('\n\n');

    const threadId = await codex.ensureThread(prepared.state.thread_id || null, {
      developerInstructions
    });
    if (threadId !== prepared.state.thread_id) {
      store.setThreadId(scopeMeta.chatId, threadId);
      prepared.state.thread_id = threadId;
    }

    let liveRaw = '';
    let commandOutput = '';
    let sentLength = 0;
    let pendingChunk = '';

    const emitChunk = (text) => {
      const normalized = normalizeOutgoingText(text);
      if (!normalized) return;
      emitToScope(scopeMeta.name, 'assistant_chunk', {
        text: normalized,
        requestId,
        ts: Date.now()
      });
    };

    const flushPending = async (force = false) => {
      if (!shouldFlushStreamingChunk(pendingChunk, force)) {
        return;
      }
      const sliced = takeStreamingFlushSlice(pendingChunk, force);
      if (!sliced.emit) {
        return;
      }
      emitChunk(sliced.emit);
      sentLength += sliced.emit.length;
      pendingChunk = sliced.rest;
    };

    const result = await codex.runTurn({
      threadId,
      userPrompt,
      inputItems,
      onDelta: (delta) => {
        liveRaw += delta;
        const cleanLive = parseOutputMarkers(liveRaw).cleanText;
        if (!cleanLive) return;
        const consumed = sentLength + pendingChunk.length;
        if (cleanLive.length <= consumed) return;
        pendingChunk += cleanLive.slice(consumed);
        void flushPending(false);
      },
      onCommandDelta: (delta) => {
        commandOutput += delta;
      }
    });

    const rawFinal = (result.finalText || liveRaw || '').trim();
    const parsedFinal = parseOutputMarkers(rawFinal);
    const fallbackRefs = extractImageRefsFallback(`${rawFinal}\n${commandOutput}`);
    const fallbackFileRefs = extractFileRefsFallback(commandOutput);
    const imageRefs = uniqueRefs([...parsedFinal.imageRefs, ...fallbackRefs]).slice(0, 8);
    const fileRefs = uniqueRefs([...parsedFinal.fileRefs, ...fallbackFileRefs]).slice(0, 8);
    const skillCreateSpec = parsedFinal.skillCreateSpecs
      .map(parseSkillCreateSpec)
      .find(Boolean) || null;
    const scheduleCreateSpecs = parsedFinal.scheduleCreateSpecs
      .map((raw) => parseScheduleCreateSpec(raw, scheduleDefaultTimezone))
      .filter(Boolean);

    const hasAutomationOutputs = Boolean(
      imageRefs.length || fileRefs.length || skillCreateSpec || scheduleCreateSpecs.length
    );
    const finalText = parsedFinal.cleanText || (hasAutomationOutputs ? '' : '(empty response)');

    const consumed = sentLength + pendingChunk.length;
    if (finalText.length > consumed) {
      pendingChunk += finalText.slice(consumed);
    }

    await flushPending(true);

    if (!sentLength && finalText) {
      pushAssistant(scopeMeta.name, finalText, { requestId });
    }

    for (const ref of imageRefs) {
      const resolved = resolveOutputRef(ref);
      if (!resolved) continue;
      emitToScope(scopeMeta.name, 'attachment', {
        kind: resolved.type,
        label: resolved.label,
        url: resolved.url,
        requestId,
        sourcePath: resolved.sourcePath
      });
    }

    for (const ref of fileRefs) {
      const resolved = resolveOutputRef(ref);
      if (!resolved) continue;
      emitToScope(scopeMeta.name, 'attachment', {
        kind: resolved.type,
        label: resolved.label,
        url: resolved.url,
        requestId,
        sourcePath: resolved.sourcePath
      });
    }

    if (skillCreateSpec) {
      let skillMsg = '';
      try {
        const normalized = normalizeSkillName(skillCreateSpec.name);
        const created = skillManager.createSkill({
          name: normalized,
          description: skillCreateSpec.description,
          instructions: skillCreateSpec.instructions
        });
        skillMsg = [
          `Skill created: $${created.name}`,
          `path=${created.path}`,
          '',
          `Use it with: $${created.name} <task>`,
          `Or: /skill run ${created.name} | <task>`
        ].join('\n');
      } catch (err) {
        const txt = String(err?.message || err);
        if (/already exists/i.test(txt)) {
          skillMsg = `Skill already exists: $${normalizeSkillName(skillCreateSpec.name)}. Use /skill show ${normalizeSkillName(skillCreateSpec.name)} or run it directly.`;
        } else {
          skillMsg = `Skill creation failed: ${txt}`;
        }
      }
      if (skillMsg) {
        pushAssistant(scopeMeta.name, skillMsg, { requestId, kind: 'system' });
      }
    }

    for (const spec of scheduleCreateSpecs) {
      try {
        const job = createScheduleForScope(scopeMeta, {
          kind: spec.kind,
          scheduleSpec: spec.scheduleSpec,
          timezone: spec.timezone,
          prompt: spec.prompt
        });
        const confirm = spec.confirmation || 'Done. I scheduled it.';
        pushAssistant(scopeMeta.name, `${confirm}\n\nNext run: ${formatRunAt(job.next_run_at, job.timezone)}`, { requestId, kind: 'system' });
      } catch (err) {
        pushAssistant(scopeMeta.name, formatError(err), { requestId, kind: 'system' });
      }
    }

    const assistantForMemory = (imageRefs.length || fileRefs.length)
      ? `${finalText}\n[image_outputs: ${imageRefs.join(', ')}]\n[file_outputs: ${fileRefs.join(', ')}]`
      : finalText;

    memory.retainReflectIndex({
      chatId: scopeMeta.chatId,
      state: prepared.state,
      userText: userPrompt,
      assistantText: assistantForMemory,
      workingMemory: prepared.workingMemory
    });

    emitToScope(scopeMeta.name, 'done', {
      requestId,
      ts: Date.now()
    });
  }

  function buildMemoryStatusText(scopeMeta) {
    const status = memory.getMemoryStatus(scopeMeta.chatId);
    const lines = [
      `Scope: web:${scopeMeta.name}`,
      `Session: #${status.sessionNo}`,
      `Turn count (session): ${status.turnCount}`,
      `Thread ID: ${status.state.thread_id || '(none yet)'}`,
      '',
      `Session summary: ${status.sessionFiles.summary}`,
      `Session key facts: ${status.sessionFiles.keyFacts}`,
      '',
      'Top stable facts:',
      ...(status.facts.length
        ? status.facts.slice(0, 6).map((f) => `- ${f.subject} ${f.predicate} ${f.object} (${Number(f.confidence).toFixed(2)})`)
        : ['- (none)']),
      '',
      'Open loops:',
      ...(status.loops.length
        ? status.loops.slice(0, 6).map((l) => `- ${l.text}`)
        : ['- (none)']),
      '',
      'Open contradictions:',
      ...(status.contradictions.length
        ? status.contradictions.slice(0, 6).map((c) => `- ${c.subject} ${c.predicate}: ${c.objects.join(' | ')}`)
        : ['- (none)']),
      '',
      'Canonical files:',
      ...status.canonicalFiles.map((p) => `- ${p}`)
    ];
    return clampText(lines.join('\n'), 8000);
  }

  async function runScheduledJobNow(job, { manual = false } = {}) {
    const scopeName = scopeFromChatId(job.scoped_chat_id);
    const scopeMeta = registerScope(scopeName);

    const effectivePrompt = buildScheduledPrompt(job);
    const prepared = memory.prepareTurn(scopeMeta.chatId, effectivePrompt);
    const threadId = await codex.ensureThread(prepared.state.thread_id || null, {
      developerInstructions: prepared.developerInstructions
    });
    if (threadId !== prepared.state.thread_id) {
      store.setThreadId(scopeMeta.chatId, threadId);
      prepared.state.thread_id = threadId;
    }

    let liveRaw = '';
    const result = await codex.runTurn({
      threadId,
      userPrompt: effectivePrompt,
      inputItems: [{ type: 'text', text: effectivePrompt }],
      onDelta: (delta) => {
        liveRaw += delta;
      }
    });

    const parsed = parseOutputMarkers((result.finalText || liveRaw || '').trim());
    const finalText = parsed.cleanText || '(empty response)';
    const prefix = manual ? 'Manual run' : 'Scheduled';
    const heading = job.kind === 'heartbeat'
      ? `Heartbeat ${prefix.toLowerCase()}: ${job.title}`
      : `Scheduled ${prefix.toLowerCase()} report: ${job.title}`;

    pushAssistant(scopeMeta.name, `${heading}\n\n${finalText}`, { kind: 'scheduled' });

    memory.retainReflectIndex({
      chatId: scopeMeta.chatId,
      state: prepared.state,
      userText: `[scheduled:${job.kind}] ${job.prompt}`,
      assistantText: finalText,
      workingMemory: prepared.workingMemory
    });
  }

  async function pollScheduledJobs() {
    const now = new Date();
    const nowIsoValue = now.toISOString();
    const due = store.listDueScheduledJobs(nowIsoValue, 16);

    for (const job of due) {
      if (Number(job.base_chat_id) >= 0) {
        continue;
      }
      if (runningScheduleJobIds.has(job.id)) {
        continue;
      }
      runningScheduleJobIds.add(job.id);

      try {
        const timezone = validateTimeZone(job.timezone) ? job.timezone : scheduleDefaultTimezone;
        const nextRunAt = computeNextRunIso({
          cronExpr: job.cron_expr,
          timezone,
          from: now
        });
        store.markScheduledJobRun(job.id, {
          lastRunAt: nowIsoValue,
          nextRunAt
        });

        const scopeName = scopeFromChatId(job.scoped_chat_id);
        enqueueScope(scopeName, async () => {
          try {
            await runScheduledJobNow({
              ...job,
              timezone
            });
          } catch (err) {
            pushAssistant(scopeName, formatError(err), { kind: 'system' });
          } finally {
            runningScheduleJobIds.delete(job.id);
          }
        });
      } catch (err) {
        store.setScheduledJobActive(job.id, job.scoped_chat_id, false);
        runningScheduleJobIds.delete(job.id);
        const scopeName = scopeFromChatId(job.scoped_chat_id);
        pushAssistant(scopeName, `Paused schedule #${job.id} due to config error: ${err?.message || err}`, { kind: 'system' });
      }
    }
  }

  async function handleSkillCommand(scopeMeta, body) {
    const helpText = [
      'Skill command usage:',
      '/skill list',
      '/skill paths',
      '/skill show <name>',
      '/skill create <name> | <description> | <instructions>',
      '/skill run <name> | <task>',
      '/skill delete <name>',
      '',
      'Notes:',
      '- Skills are stored in Codex format: <root>/<skill-name>/SKILL.md',
      '- You can also trigger a skill directly in chat by starting with $skill-name',
      '- You can create skills with /skill create ... or natural language.'
    ].join('\n');

    if (!body) {
      pushAssistant(scopeMeta.name, helpText, { kind: 'system' });
      return;
    }

    const [actionRaw] = body.split(/\s+/);
    const action = String(actionRaw || '').toLowerCase();
    const rest = body.slice(String(actionRaw || '').length).trim();

    if (action === 'paths') {
      const lines = [
        'Skill roots:',
        `writable=${skillManager.writableRoot}`,
        ...skillManager.readRoots.map((r) => `read=${r}`)
      ];
      pushAssistant(scopeMeta.name, lines.join('\n'), { kind: 'system' });
      return;
    }

    if (action === 'list') {
      const skills = skillManager.listSkills();
      if (!skills.length) {
        pushAssistant(scopeMeta.name, 'No skills found yet. Create one with /skill create ...', { kind: 'system' });
        return;
      }
      const lines = [
        `Skills (${skills.length}):`,
        ...skills.slice(0, 120).map((s) => `- $${s.name} [${s.writable ? 'local' : 'shared'}]`)
      ];
      pushAssistant(scopeMeta.name, lines.join('\n'), { kind: 'system' });
      return;
    }

    if (action === 'show') {
      const name = rest.trim();
      if (!name) {
        pushAssistant(scopeMeta.name, 'Usage: /skill show <name>', { kind: 'system' });
        return;
      }
      const skill = skillManager.readSkill(name);
      if (!skill) {
        pushAssistant(scopeMeta.name, `Skill not found: ${name}`, { kind: 'system' });
        return;
      }
      const preview = skill.content.length <= 3000
        ? skill.content
        : `${skill.content.slice(0, 2976)}\n...[truncated preview]`;
      const lines = [
        `Skill: $${skill.name}`,
        `source=${skill.writable ? 'local' : 'shared'}`,
        `path=${skill.path}`,
        '',
        preview
      ];
      pushAssistant(scopeMeta.name, lines.join('\n'), { kind: 'system' });
      return;
    }

    if (action === 'create') {
      const parts = parsePipeArgs(rest);
      const rawName = String(parts[0] || '').trim();
      const description = String(parts[1] || '').trim();
      const instructions = parts.slice(2).join(' | ').trim();

      if (!rawName) {
        pushAssistant(scopeMeta.name, 'Usage: /skill create <name> | <description> | <instructions>', { kind: 'system' });
        return;
      }

      try {
        const name = normalizeSkillName(rawName);
        const created = skillManager.createSkill({
          name,
          description,
          instructions
        });
        pushAssistant(scopeMeta.name, [
          `Skill created: $${created.name}`,
          `path=${created.path}`,
          '',
          `Try: /skill run ${created.name} | <your task>`,
          `Or in chat: $${created.name} <your task>`
        ].join('\n'), { kind: 'system' });
      } catch (err) {
        pushAssistant(scopeMeta.name, formatError(err), { kind: 'system' });
      }
      return;
    }

    if (action === 'delete' || action === 'remove') {
      const name = rest.trim();
      if (!name) {
        pushAssistant(scopeMeta.name, 'Usage: /skill delete <name>', { kind: 'system' });
        return;
      }
      try {
        const removed = skillManager.deleteSkill(name);
        pushAssistant(scopeMeta.name, `Skill deleted: $${removed.name}`, { kind: 'system' });
      } catch (err) {
        pushAssistant(scopeMeta.name, formatError(err), { kind: 'system' });
      }
      return;
    }

    if (action === 'run') {
      const parts = parsePipeArgs(rest);
      const requestedName = String(parts[0] || '').trim();
      const task = parts.slice(1).join(' | ').trim();
      if (!requestedName || !task) {
        pushAssistant(scopeMeta.name, 'Usage: /skill run <name> | <task>', { kind: 'system' });
        return;
      }
      const found = skillManager.getSkill(requestedName);
      if (!found) {
        pushAssistant(scopeMeta.name, `Skill not found: ${requestedName}`, { kind: 'system' });
        return;
      }

      const forcedPrompt = buildSkillRunPrompt(found.name, task);
      await runPromptTurn(scopeMeta, {
        userPrompt: forcedPrompt,
        inputItems: [{ type: 'text', text: forcedPrompt }],
        introText: `Running skill $${found.name}...`
      });
      return;
    }

    pushAssistant(scopeMeta.name, helpText, { kind: 'system' });
  }

  async function handleScheduleCommand(scopeMeta, body) {
    const helpText = [
      'Schedule command usage:',
      '/schedule list',
      '/schedule add daily HH:MM [tz=Area/City] | <prompt>',
      '/schedule add cron <m h dom mon dow> [tz=Area/City] | <prompt>',
      '/schedule add heartbeat HH:MM [tz=Area/City] | <optional heartbeat goal>',
      '/schedule pause <id>',
      '/schedule resume <id>',
      '/schedule remove <id>',
      '/schedule run <id>',
      '',
      `Default timezone: ${scheduleDefaultTimezone}`
    ].join('\n');

    if (!body) {
      pushAssistant(scopeMeta.name, helpText, { kind: 'system' });
      return;
    }

    const [actionRaw] = body.split(/\s+/);
    const action = String(actionRaw || '').toLowerCase();

    if (action === 'list') {
      const jobs = store.listScheduledJobs(scopeMeta.chatId);
      if (!jobs.length) {
        pushAssistant(scopeMeta.name, 'No schedules in this web scope yet.', { kind: 'system' });
        return;
      }

      const lines = [
        `Schedules for web:${scopeMeta.name}:`,
        ...jobs.slice(0, 40).map((job) => [
          `#${job.id} [${job.active ? 'on' : 'off'}] ${job.kind}`,
          `cron=${job.cron_expr}`,
          `next=${formatRunAt(job.next_run_at, job.timezone)}`,
          `title=${job.title}`
        ].join(' | '))
      ];
      pushAssistant(scopeMeta.name, lines.join('\n'), { kind: 'system' });
      return;
    }

    if (action === 'pause' || action === 'resume') {
      const id = Number(body.split(/\s+/)[1]);
      if (!Number.isInteger(id) || id <= 0) {
        pushAssistant(scopeMeta.name, 'Usage: /schedule pause <id> OR /schedule resume <id>', { kind: 'system' });
        return;
      }
      const ok = store.setScheduledJobActive(id, scopeMeta.chatId, action === 'resume');
      if (!ok) {
        pushAssistant(scopeMeta.name, `Schedule #${id} not found in this scope.`, { kind: 'system' });
        return;
      }
      pushAssistant(scopeMeta.name, `Schedule #${id} ${action === 'resume' ? 'resumed' : 'paused'}.`, { kind: 'system' });
      return;
    }

    if (action === 'remove' || action === 'delete') {
      const id = Number(body.split(/\s+/)[1]);
      if (!Number.isInteger(id) || id <= 0) {
        pushAssistant(scopeMeta.name, 'Usage: /schedule remove <id>', { kind: 'system' });
        return;
      }
      const ok = store.deleteScheduledJob(id, scopeMeta.chatId);
      if (!ok) {
        pushAssistant(scopeMeta.name, `Schedule #${id} not found in this scope.`, { kind: 'system' });
        return;
      }
      pushAssistant(scopeMeta.name, `Schedule #${id} removed.`, { kind: 'system' });
      return;
    }

    if (action === 'run') {
      const id = Number(body.split(/\s+/)[1]);
      if (!Number.isInteger(id) || id <= 0) {
        pushAssistant(scopeMeta.name, 'Usage: /schedule run <id>', { kind: 'system' });
        return;
      }
      const job = store.getScheduledJob(id, scopeMeta.chatId);
      if (!job) {
        pushAssistant(scopeMeta.name, `Schedule #${id} not found in this scope.`, { kind: 'system' });
        return;
      }
      pushAssistant(scopeMeta.name, `Running schedule #${id} now...`, { kind: 'system' });
      await runScheduledJobNow(job, { manual: true });
      store.touchScheduledJob(job.id);
      return;
    }

    if (action === 'add') {
      const payload = body.slice(3).trim();
      const sep = payload.indexOf('|');
      if (sep <= 0) {
        pushAssistant(scopeMeta.name, 'Usage: /schedule add daily HH:MM | <prompt>', { kind: 'system' });
        return;
      }

      let left = payload.slice(0, sep).trim();
      let prompt = payload.slice(sep + 1).trim();
      if (!left) {
        pushAssistant(scopeMeta.name, 'Missing schedule definition.', { kind: 'system' });
        return;
      }

      let kind = 'report';
      if (/^heartbeat\s+/i.test(left)) {
        kind = 'heartbeat';
        left = `daily ${left.replace(/^heartbeat\s+/i, '').trim()}`;
      }

      const tzParsed = parseTzHint(left, scheduleDefaultTimezone);
      left = tzParsed.cleaned;
      const timezone = tzParsed.timezone || scheduleDefaultTimezone;
      if (!validateTimeZone(timezone)) {
        pushAssistant(scopeMeta.name, `Invalid timezone "${timezone}". Example: tz=America/Los_Angeles`, { kind: 'system' });
        return;
      }

      if (!prompt && kind === 'heartbeat') {
        prompt = 'Check in proactively and keep me focused on my priorities.';
      }
      if (!prompt) {
        pushAssistant(scopeMeta.name, 'Prompt cannot be empty.', { kind: 'system' });
        return;
      }

      const job = createScheduleForScope(scopeMeta, {
        kind,
        scheduleSpec: left,
        timezone,
        prompt
      });

      pushAssistant(scopeMeta.name, [
        `Schedule #${job.id} created.`,
        `kind=${job.kind}`,
        `cron=${job.cron_expr}`,
        `next=${formatRunAt(job.next_run_at, job.timezone)}`,
        `scope=web:${scopeMeta.name}`
      ].join('\n'), { kind: 'system' });
      return;
    }

    pushAssistant(scopeMeta.name, helpText, { kind: 'system' });
  }

  async function handleCommand(scopeMeta, text) {
    const raw = String(text || '').trim();
    const command = raw.replace(/^\//, '').split(/\s+/)[0]?.toLowerCase() || '';

    if (command === 'help' || command === 'start') {
      pushAssistant(scopeMeta.name, [
        `${appName} dashboard commands:`,
        '/newsession - start a new memory session + thread',
        '/memory - show memory status',
        '/heartbeat - run maintenance now',
        '/schedule - manage proactive schedules',
        '/skill - create/list/run skills',
        '/restart - restart bot + codex server',
        '/autostart on|off|status - boot behavior',
        '/chatid - show current scope id'
      ].join('\n'), { kind: 'system' });
      return;
    }

    if (command === 'chatid') {
      pushAssistant(scopeMeta.name, [
        `scope=web:${scopeMeta.name}`,
        `chat_id=${scopeMeta.chatId}`,
        'topic_id=0'
      ].join('\n'), { kind: 'system' });
      return;
    }

    if (command === 'whoami') {
      pushAssistant(scopeMeta.name, [
        'dashboard_user=local',
        `scope=web:${scopeMeta.name}`,
        `chat_id=${scopeMeta.chatId}`,
        `host=${host}:${port}`
      ].join('\n'), { kind: 'system' });
      return;
    }

    if (command === 'newsession') {
      const state = store.startNewSession(scopeMeta.chatId);
      pushAssistant(scopeMeta.name, `Started session #${state.session_no}. Thread reset.`, { kind: 'system' });
      return;
    }

    if (command === 'memory') {
      pushAssistant(scopeMeta.name, buildMemoryStatusText(scopeMeta), { kind: 'system' });
      return;
    }

    if (command === 'heartbeat') {
      const result = memory.runHeartbeat();
      pushAssistant(scopeMeta.name,
        `Heartbeat complete. weekly_summaries=${result.weeklyUpdated}, contradictions=${result.contradictionCount}, chats=${result.chatCount}`,
        { kind: 'system' }
      );
      return;
    }

    if (command === 'schedule') {
      const body = raw.replace(/^\/schedule(?:@\w+)?/i, '').trim();
      await handleScheduleCommand(scopeMeta, body);
      return;
    }

    if (command === 'skill') {
      const body = raw.replace(/^\/skill(?:@\w+)?/i, '').trim();
      await handleSkillCommand(scopeMeta, body);
      return;
    }

    if (command === 'autostart') {
      if (!autostart) {
        pushAssistant(scopeMeta.name, 'Autostart manager is not configured.', { kind: 'system' });
        return;
      }

      const parts = raw.split(/\s+/).filter(Boolean);
      const arg = String(parts[1] || 'status').toLowerCase();
      if (!['on', 'off', 'status'].includes(arg)) {
        pushAssistant(scopeMeta.name, 'Usage: /autostart on | /autostart off | /autostart status', { kind: 'system' });
        return;
      }

      try {
        const status = arg === 'on'
          ? await autostart.enable()
          : arg === 'off'
            ? await autostart.disable()
            : await autostart.status();

        const state = status.loaded ? 'ENABLED' : 'DISABLED';
        const lines = [
          `Autostart: ${state}`,
          `Label: ${status.label}`,
          `Service: ${status.serviceTarget}`,
          `Plist: ${status.plistPath}`,
          `PID: ${status.pid || '(not running via launchd)'}`
        ];
        if (arg === 'on') {
          lines.unshift('Autostart enabled. Bot will come back after Mac reboot.');
        } else if (arg === 'off') {
          lines.unshift('Autostart disabled. Bot will NOT auto-start after reboot.');
        }
        pushAssistant(scopeMeta.name, lines.join('\n'), { kind: 'system' });
      } catch (err) {
        pushAssistant(scopeMeta.name, formatError(err), { kind: 'system' });
      }
      return;
    }

    if (command === 'restart') {
      if (!restartService) {
        pushAssistant(scopeMeta.name, 'Restart handler is not configured.', { kind: 'system' });
        return;
      }
      pushAssistant(scopeMeta.name, 'Restarting bot and Codex server now...', { kind: 'system' });
      await restartService({
        chatId: null,
        userId: null
      });
      return;
    }

    pushAssistant(scopeMeta.name, `Unknown command: ${command}. Use /help.`, { kind: 'system' });
  }

  async function handleChatRequest(scopeName, text, files, requestId) {
    const scopeMeta = registerScope(scopeName);
    await saveScopeRegistry().catch(() => undefined);

    const uploaded = await saveUploadedFiles(scopeMeta.name, files);
    let userPrompt = String(text || '').trim();

    if (!userPrompt && uploaded.length) {
      userPrompt = uploaded.every((f) => f.isImage)
        ? 'Analyze this image and help me.'
        : 'Analyze this attached file and help me.';
    }

    const audioUploads = uploaded.filter((f) => f.isAudio);
    const onlyAudioUploads = uploaded.length > 0 && audioUploads.length === uploaded.length;
    if (onlyAudioUploads && audioUploads.length === 1 && whisperSettings?.enabled) {
      const audio = audioUploads[0];
      pushStatus(scopeMeta.name, 'Transcribing audio...');
      try {
        const transcript = await transcribeLocalAudioFile({
          localPath: audio.path,
          inputExtension: path.extname(audio.path).replace('.', '') || 'ogg',
          languageHint: null,
          whisper: whisperSettings
        });
        if (transcript) {
          userPrompt = text
            ? `${String(text || '').trim()}\n\n[Voice transcript]\n${transcript}`
            : transcript;
        }
      } catch (err) {
        pushAssistant(scopeMeta.name, `Audio transcription failed: ${err?.message || err}`, { kind: 'system', requestId });
      }
    }

    if (!userPrompt) {
      pushAssistant(scopeMeta.name, 'Please type a message or attach a file.', { kind: 'system', requestId });
      emitToScope(scopeMeta.name, 'done', { requestId, ts: Date.now() });
      return;
    }

    if (userPrompt.startsWith('/')) {
      await handleCommand(scopeMeta, userPrompt);
      emitToScope(scopeMeta.name, 'done', { requestId, ts: Date.now() });
      return;
    }

    const imageUploads = uploaded.filter((f) => f.isImage);
    const nonImageUploads = uploaded.filter((f) => !f.isImage && !f.isAudio);

    let finalPrompt = userPrompt;
    const inputItems = [{ type: 'text', text: userPrompt }];

    for (const img of imageUploads) {
      inputItems.push({ type: 'localImage', path: img.path });
    }

    if (nonImageUploads.length === 1 && !imageUploads.length) {
      const f = nonImageUploads[0];
      finalPrompt = buildFileAwarePrompt(userPrompt, f.path, f.name, f.mime);
      inputItems[0] = { type: 'text', text: finalPrompt };
    } else if (nonImageUploads.length > 0) {
      finalPrompt = buildMultiAttachmentPrompt(userPrompt, nonImageUploads);
      inputItems[0] = { type: 'text', text: finalPrompt };
    }

    await runPromptTurn(scopeMeta, {
      userPrompt: finalPrompt,
      inputItems,
      introText: uploaded.length ? 'Thinking (with attachment)...' : 'Thinking...',
      requestId
    });
  }

  function collectConfigSummary() {
    return {
      appName,
      codex: {
        cwd: codexSettings.cwd,
        model: codexSettings.model || '(default)',
        provider: codexSettings.modelProvider || '(default)',
        sandbox: codexSettings.sandbox,
        approvalPolicy: codexSettings.approvalPolicy,
        accessProfile: codexSettings.accessProfile || '(not set)',
        multiAgent: Boolean(codexSettings.enableMultiAgent)
      },
      dashboard: {
        host,
        port,
        tokenProtected: Boolean(currentAuthToken)
      },
      schedule: {
        defaultTimezone: scheduleDefaultTimezone,
        pollIntervalMs: schedulePollMs
      },
      skills: {
        writableRoot: skillManager.writableRoot,
        readRoots: skillManager.readRoots
      }
    };
  }

  function enforceAuth(req, parsedUrl) {
    if (!isLocalRequest(req)) {
      return { ok: false, code: 403, message: 'Dashboard accepts local requests only.' };
    }

    if (!currentAuthToken) {
      return { ok: true };
    }

    const headerToken = String(req.headers['x-dashboard-token'] || '').trim();
    const queryToken = String(parsedUrl.searchParams.get('token') || '').trim();
    const cookieToken = String(req.headers.cookie || '')
      .split(';')
      .map((p) => p.trim())
      .find((p) => p.startsWith('dashboard_token='));
    const cookieValue = cookieToken ? decodeURIComponent(cookieToken.split('=').slice(1).join('=')) : '';

    const token = headerToken || queryToken || cookieValue;
    if (token && token === currentAuthToken) {
      return { ok: true };
    }

    return { ok: false, code: 401, message: 'Unauthorized dashboard token.' };
  }

  async function route(req, res) {
    const parsedUrl = new URL(req.url, `http://${host}:${port}`);
    const pathname = parsedUrl.pathname;

    if (pathname.startsWith('/api/')) {
      const auth = enforceAuth(req, parsedUrl);
      if (!auth.ok) {
        sendJson(res, auth.code || 401, { error: auth.message || 'unauthorized' });
        return;
      }
    }

    if (pathname === '/' && req.method === 'GET') {
      const headers = {
        'Content-Type': 'text/html; charset=utf-8',
        'Content-Length': Buffer.byteLength(webIndex),
        'Cache-Control': 'no-store'
      };
      if (currentAuthToken) {
        headers['Set-Cookie'] = `dashboard_token=${encodeURIComponent(currentAuthToken)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`;
      } else {
        headers['Set-Cookie'] = 'dashboard_token=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0';
      }
      res.writeHead(200, headers);
      res.end(webIndex);
      return;
    }

    if (pathname === '/app.js' && req.method === 'GET') {
      sendText(res, 200, webApp, 'text/javascript; charset=utf-8');
      return;
    }

    if (pathname === '/styles.css' && req.method === 'GET') {
      sendText(res, 200, webCss, 'text/css; charset=utf-8');
      return;
    }

    if (pathname === '/api/health' && req.method === 'GET') {
      sendJson(res, 200, {
        ok: true,
        appName,
        time: nowIso()
      });
      return;
    }

    if (pathname === '/api/state' && req.method === 'GET') {
      const requestedScope = normalizeScopeName(parsedUrl.searchParams.get('scope') || scopeRegistry.defaultScope || DEFAULT_SCOPE);
      const scopeMeta = registerScope(requestedScope);
      await saveScopeRegistry().catch(() => undefined);

      const chatState = store.getChatState(scopeMeta.chatId);
      const status = memory.getMemoryStatus(scopeMeta.chatId);
      let autostartStatus = null;
      if (autostart) {
        try {
          autostartStatus = await autostart.status();
        } catch {
          autostartStatus = null;
        }
      }

      sendJson(res, 200, {
        scope: scopeMeta,
        scopes: listScopes(),
        state: {
          sessionNo: chatState.session_no,
          threadId: chatState.thread_id,
          turnCount: status.turnCount
        },
        autostart: autostartStatus,
        config: collectConfigSummary(),
        now: Date.now()
      });
      return;
    }

    if (pathname === '/api/settings' && req.method === 'GET') {
      const schema = await getEnvSettingsSchema();
      sendJson(res, 200, {
        envPath: schema.envPath,
        fields: schema.fields
      });
      return;
    }

    if (pathname === '/api/settings/update' && req.method === 'POST') {
      const body = await readJsonBody(req);
      const updates = Array.isArray(body.updates) ? body.updates : [];
      if (updates.length > 200) {
        sendJson(res, 400, { error: 'Too many env updates in one request.' });
        return;
      }

      const changedAuthToken = updates.some((u) => String(u?.key || '').trim() === 'DASHBOARD_AUTH_TOKEN');
      const shouldRestart = Boolean(body.restart);

      try {
        const result = await applyEnvUpdates(updates);
        const payload = JSON.stringify({
          ok: true,
          changedKeys: result.changedKeys,
          restartRequired: result.restartRequired
        });

        const headers = {
          'Content-Type': 'application/json; charset=utf-8',
          'Content-Length': Buffer.byteLength(payload),
          'Cache-Control': 'no-store'
        };
        if (changedAuthToken) {
          if (currentAuthToken) {
            headers['Set-Cookie'] = `dashboard_token=${encodeURIComponent(currentAuthToken)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`;
          } else {
            headers['Set-Cookie'] = 'dashboard_token=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0';
          }
        }
        res.writeHead(200, headers);
        res.end(payload);

        if (shouldRestart && result.changedKeys.length && restartService) {
          setTimeout(() => {
            void restartService({ chatId: null, userId: null }).catch((err) => {
              console.error('[dashboard] restart after env update failed:', err?.message || err);
            });
          }, 200);
        }
      } catch (err) {
        sendJson(res, 400, { error: err?.message || String(err) });
      }
      return;
    }

    if (pathname === '/api/history' && req.method === 'GET') {
      const scopeName = normalizeScopeName(parsedUrl.searchParams.get('scope') || scopeRegistry.defaultScope || DEFAULT_SCOPE);
      const scopeMeta = registerScope(scopeName);
      await saveScopeRegistry().catch(() => undefined);
      const limit = Math.min(120, Math.max(1, Number(parsedUrl.searchParams.get('limit') || 40)));
      const rows = store.getRecentExchanges(scopeMeta.chatId, limit).reverse();

      const messages = [];
      for (const row of rows) {
        messages.push({ role: 'user', text: normalizeOutgoingText(row.user_text), ts: row.created_at });
        messages.push({ role: 'assistant', text: normalizeOutgoingText(row.assistant_text), ts: row.created_at });
      }

      sendJson(res, 200, {
        scope: scopeMeta,
        messages
      });
      return;
    }

    if (pathname === '/api/scope' && req.method === 'POST') {
      const body = await readJsonBody(req);
      const desired = normalizeScopeName(body.scope || body.name || DEFAULT_SCOPE);
      const scopeMeta = registerScope(desired);
      await saveScopeRegistry().catch(() => undefined);
      sendJson(res, 200, {
        ok: true,
        scope: scopeMeta,
        scopes: listScopes()
      });
      return;
    }

    if (pathname === '/api/chat' && req.method === 'POST') {
      const body = await readJsonBody(req);
      const scopeName = normalizeScopeName(body.scope || scopeRegistry.defaultScope || DEFAULT_SCOPE);
      const text = String(body.text || '').trim();
      const files = Array.isArray(body.files) ? body.files : [];
      const requestId = String(body.requestId || crypto.randomUUID());

      registerScope(scopeName);
      await saveScopeRegistry().catch(() => undefined);

      pushStatus(scopeName, 'Queued...');
      enqueueScope(scopeName, async () => {
        try {
          await handleChatRequest(scopeName, text, files, requestId);
        } catch (err) {
          pushAssistant(scopeName, formatError(err), { kind: 'system', requestId });
          emitToScope(scopeName, 'done', {
            requestId,
            ts: Date.now()
          });
        }
      });

      sendJson(res, 202, {
        ok: true,
        requestId,
        scope: scopeName
      });
      return;
    }

    if (pathname === '/api/autostart' && req.method === 'POST') {
      if (!autostart) {
        sendJson(res, 400, { error: 'Autostart manager is not configured.' });
        return;
      }

      const body = await readJsonBody(req);
      const action = String(body.action || 'status').toLowerCase();
      if (!['on', 'off', 'status'].includes(action)) {
        sendJson(res, 400, { error: 'action must be one of: on, off, status' });
        return;
      }

      try {
        const status = action === 'on'
          ? await autostart.enable()
          : action === 'off'
            ? await autostart.disable()
            : await autostart.status();
        sendJson(res, 200, {
          ok: true,
          status
        });
      } catch (err) {
        sendJson(res, 500, { error: err?.message || String(err) });
      }
      return;
    }

    if (pathname === '/api/restart' && req.method === 'POST') {
      if (!restartService) {
        sendJson(res, 400, { error: 'Restart handler is not configured.' });
        return;
      }

      sendJson(res, 200, {
        ok: true,
        message: 'Restarting bot and Codex server now...'
      });

      setTimeout(() => {
        void restartService({ chatId: null, userId: null }).catch((err) => {
          console.error('[dashboard] restart request failed:', err?.message || err);
        });
      }, 120);
      return;
    }

    if (pathname.startsWith('/api/file/') && req.method === 'GET') {
      const token = pathname.slice('/api/file/'.length);
      const info = fileTokenMap.get(token);
      if (!info) {
        sendJson(res, 404, { error: 'File token not found' });
        return;
      }
      if (Date.now() > Number(info.expiresAt || 0)) {
        fileTokenMap.delete(token);
        sendJson(res, 410, { error: 'File token expired' });
        return;
      }

      const filePath = String(info.path || '');
      if (!filePath || !fs.existsSync(filePath)) {
        sendJson(res, 404, { error: 'File not found' });
        return;
      }

      const stat = fs.statSync(filePath);
      const filename = path.basename(filePath);
      const ext = path.extname(filePath).toLowerCase();
      const contentType = IMAGE_EXTS.has(ext)
        ? (ext === '.svg' ? 'image/svg+xml' : `image/${ext.replace('.', '').replace('jpg', 'jpeg')}`)
        : 'application/octet-stream';

      res.writeHead(200, {
        'Content-Type': contentType,
        'Content-Length': stat.size,
        'Content-Disposition': `inline; filename="${filename}"`,
        'Cache-Control': 'private, max-age=900'
      });
      fs.createReadStream(filePath).pipe(res);
      return;
    }

    if (pathname === '/api/stream' && req.method === 'GET') {
      const scopeName = normalizeScopeName(parsedUrl.searchParams.get('scope') || scopeRegistry.defaultScope || DEFAULT_SCOPE);
      registerScope(scopeName);
      await saveScopeRegistry().catch(() => undefined);

      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive'
      });
      res.write(': connected\n\n');

      const clients = streamClientsByScope.get(scopeName) || new Set();
      clients.add(res);
      streamClientsByScope.set(scopeName, clients);

      sendSseEvent(res, 'hello', {
        scope: scopeName,
        ts: Date.now()
      });

      req.on('close', () => {
        const set = streamClientsByScope.get(scopeName);
        if (!set) return;
        set.delete(res);
        if (!set.size) {
          streamClientsByScope.delete(scopeName);
        }
      });
      return;
    }

    sendJson(res, 404, {
      error: 'Not found'
    });
  }

  async function start() {
    await loadScopeRegistry();

    const moduleDir = path.dirname(fileURLToPath(import.meta.url));
    const webRoot = path.resolve(moduleDir, '..', 'web');
    webIndex = await fsPromises.readFile(path.join(webRoot, 'index.html'), 'utf8');
    webApp = await fsPromises.readFile(path.join(webRoot, 'app.js'), 'utf8');
    webCss = await fsPromises.readFile(path.join(webRoot, 'styles.css'), 'utf8');

    server = http.createServer((req, res) => {
      void route(req, res).catch((err) => {
        console.error('[dashboard] request failed:', err);
        if (!res.headersSent) {
          sendJson(res, 500, { error: err?.message || String(err) });
        } else {
          res.end();
        }
      });
    });

    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, host, () => {
        server.off('error', reject);
        resolve();
      });
    });

    heartbeatTimer = setInterval(() => {
      for (const [token, info] of fileTokenMap.entries()) {
        if (Date.now() > Number(info.expiresAt || 0)) {
          fileTokenMap.delete(token);
        }
      }
      for (const [scope, clients] of streamClientsByScope.entries()) {
        for (const res of clients) {
          try {
            res.write(': ping\n\n');
          } catch {
            // ignore
          }
        }
        if (!clients.size) {
          streamClientsByScope.delete(scope);
        }
      }
    }, 15_000);
    heartbeatTimer.unref();

    scheduleTimer = setInterval(() => {
      void pollScheduledJobs().catch((err) => {
        console.error('[dashboard] schedule poll failed:', err?.message || err);
      });
    }, schedulePollMs);
    scheduleTimer.unref();

    void pollScheduledJobs().catch((err) => {
      console.error('[dashboard] initial schedule poll failed:', err?.message || err);
    });

    console.log(`[dashboard] listening on http://${host}:${port}`);
    if (currentAuthToken) {
      console.log('[dashboard] auth token is enabled.');
    }
  }

  async function stop() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
    if (scheduleTimer) {
      clearInterval(scheduleTimer);
      scheduleTimer = null;
    }

    for (const clients of streamClientsByScope.values()) {
      for (const res of clients) {
        try {
          res.end();
        } catch {
          // ignore
        }
      }
    }
    streamClientsByScope.clear();

    if (!server) return;
    const current = server;
    server = null;

    await new Promise((resolve) => {
      current.close(() => resolve());
    });
  }

  return {
    start,
    stop,
    deriveChatId: deriveDashboardChatId
  };
}
