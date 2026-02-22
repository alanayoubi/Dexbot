import path from 'node:path';
import process from 'node:process';
import { config as loadDotEnv } from 'dotenv';

loadDotEnv();

function must(name) {
  const value = process.env[name];
  if (!value || !value.trim()) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value.trim();
}

function bool(name, fallback = false) {
  const value = process.env[name];
  if (value == null) return fallback;
  return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase());
}

function int(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const n = Number(raw);
  return Number.isInteger(n) ? n : fallback;
}

function num(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

const cwd = process.env.CODEX_CWD ? path.resolve(process.env.CODEX_CWD) : process.cwd();
const listenUrl = process.env.CODEX_APP_SERVER_URL || 'ws://127.0.0.1:8787';
const memoryRoot = path.resolve(process.env.MEMORY_ROOT || './memory');
const defaultTz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
const codexHome = process.env.CODEX_HOME
  ? path.resolve(process.env.CODEX_HOME)
  : path.join(process.env.HOME || '', '.codex');

export const settings = {
  app: {
    name: (process.env.BOT_APP_NAME || 'Dexbot').trim() || 'Dexbot'
  },
  telegramToken: must('TELEGRAM_BOT_TOKEN'),
  allowedTelegramUserIds: (process.env.ALLOWED_TELEGRAM_USER_IDS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0),
  allowedTelegramChatIds: (process.env.ALLOWED_TELEGRAM_CHAT_IDS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n)),

  dbPath: path.resolve(process.env.DB_PATH || './data/memory.db'),

  codex: {
    cwd,
    appServerUrl: listenUrl,
    autoSpawnAppServer: bool('AUTO_SPAWN_APP_SERVER', true),
    appServerListenUrl: listenUrl,
    model: process.env.CODEX_MODEL || null,
    modelProvider: process.env.CODEX_MODEL_PROVIDER || null,
    approvalPolicy: process.env.CODEX_APPROVAL_POLICY || 'never',
    sandbox: process.env.CODEX_SANDBOX || 'danger-full-access',
    personality: process.env.CODEX_PERSONALITY || null,
    enableMultiAgent: bool('CODEX_ENABLE_MULTI_AGENT', true),
    enableChildAgentsMd: bool('CODEX_ENABLE_CHILD_AGENTS_MD', false)
  },

  telegram: {
    streamEditIntervalMs: int('TELEGRAM_STREAM_EDIT_INTERVAL_MS', 1000),
    typingIntervalMs: int('TELEGRAM_TYPING_INTERVAL_MS', 4000),
    privateOnly: bool('TELEGRAM_PRIVATE_ONLY', true),
    groupRequireMention: bool('TELEGRAM_GROUP_REQUIRE_MENTION', true),
    handlerTimeoutMs: int('TELEGRAM_HANDLER_TIMEOUT_MS', 600000)
  },

  schedule: {
    pollIntervalMs: int('SCHEDULE_POLL_INTERVAL_MS', 15_000),
    defaultTimezone: (process.env.SCHEDULE_DEFAULT_TIMEZONE || defaultTz).trim() || 'UTC'
  },

  skills: {
    root: path.resolve(process.env.SKILLS_ROOT || path.join(cwd, '.agents', 'skills')),
    includeCodexHome: bool('SKILLS_INCLUDE_CODEX_HOME', true),
    codexHomeRoot: path.resolve(process.env.SKILLS_CODEX_HOME_ROOT || path.join(codexHome, 'skills'))
  },

  whisper: {
    enabled: bool('WHISPER_ENABLED', true),
    command: (process.env.WHISPER_COMMAND || 'whisper').trim(),
    model: (process.env.WHISPER_MODEL || 'base').trim(),
    language: (process.env.WHISPER_LANGUAGE || '').trim() || null,
    task: (process.env.WHISPER_TASK || 'transcribe').trim() || 'transcribe',
    extraArgs: process.env.WHISPER_EXTRA_ARGS || '',
    timeoutMs: int('WHISPER_TIMEOUT_MS', 240000),
    persistent: bool('WHISPER_PERSISTENT', true),
    pythonCommand: (process.env.WHISPER_PYTHON_COMMAND || '').trim() || null
  },

  memory: {
    root: memoryRoot,
    embeddingDim: int('MEMORY_EMBEDDING_DIM', 96),
    sessionSummaryEveryTurns: int('MEMORY_SESSION_SUMMARY_EVERY_TURNS', 4),
    maxSessionSummaryLines: int('MEMORY_MAX_SESSION_SUMMARY_LINES', 320),
    maxFactsPerRetain: int('MEMORY_MAX_FACTS_PER_RETAIN', 10),
    maxEpisodesPerRetain: int('MEMORY_MAX_EPISODES_PER_RETAIN', 3),
    maxStableFacts: int('MEMORY_MAX_STABLE_FACTS', 8),
    maxEpisodes: int('MEMORY_MAX_EPISODES', 4),
    maxOpenLoops: int('MEMORY_MAX_OPEN_LOOPS', 6),
    maxInjectionTokens: int('MEMORY_MAX_INJECTION_TOKENS', 360),
    confidenceThreshold: num('MEMORY_CONFIDENCE_THRESHOLD', 0.6),
    curatedMinConfidence: num('MEMORY_CURATED_MIN_CONFIDENCE', 0.75),
    recencyBiasDays: int('MEMORY_RECENCY_BIAS_DAYS', 45),
    multiAgentComplexityThreshold: num('MEMORY_MULTI_AGENT_COMPLEXITY_THRESHOLD', 0.68),
    maxMemoryLines: int('MEMORY_MAX_CURATED_LINES', 300),
    heartbeatHours: int('MEMORY_HEARTBEAT_HOURS', 24),
    heartbeatCompressOlderThanDays: int('MEMORY_HEARTBEAT_COMPRESS_OLDER_THAN_DAYS', 7),
    heartbeatDecayDays: int('MEMORY_HEARTBEAT_DECAY_DAYS', 45),
    heartbeatDecayStep: num('MEMORY_HEARTBEAT_DECAY_STEP', 0.04)
  }
};

export function ensureSafeConfigWarnings() {
  if (!settings.allowedTelegramUserIds.length) {
    console.warn('[security] ALLOWED_TELEGRAM_USER_IDS is empty; any Telegram user who can reach the bot can use it.');
  }
  if (!settings.telegram.privateOnly) {
    console.warn('[security] TELEGRAM_PRIVATE_ONLY is false; bot can be used in groups/channels if user ID checks pass.');
    if (!settings.allowedTelegramChatIds.length) {
      console.warn('[security] ALLOWED_TELEGRAM_CHAT_IDS is empty while group mode is enabled. Any chat can reach access checks.');
    }
  }
  if (settings.codex.sandbox === 'danger-full-access') {
    console.warn('[security] CODEX_SANDBOX is danger-full-access. Use only on an isolated machine.');
  }
  if (settings.codex.approvalPolicy === 'never') {
    console.warn('[security] CODEX_APPROVAL_POLICY is never. Commands run without manual approvals.');
  }
  if (settings.whisper.enabled && !settings.whisper.command) {
    console.warn('[config] WHISPER_ENABLED is true but WHISPER_COMMAND is empty.');
  }
}
