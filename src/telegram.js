import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { Telegraf } from 'telegraf';
import { transcribeTelegramFile } from './transcription.js';
import { computeNextRunIso, cronFromDailyTime, parseScheduleSpec, validateTimeZone } from './schedule.js';
import {
  buildSkillRunPrompt,
  createSkillManager,
  normalizeSkillName,
  parseNaturalSkillCreateRequest
} from './skills.js';

const IMAGE_EXTS = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tif', '.tiff', '.svg']);
const URL_IMAGE_RE = /https?:\/\/[^\s"'`<>]+\.(?:png|jpe?g|gif|webp|bmp|tiff?|svg)(?:\?[^\s"'`<>]*)?/gi;
const ABS_PATH_IMAGE_RE = /\/[^\s"'`<>]+?\.(?:png|jpe?g|gif|webp|bmp|tiff?|svg)/gi;
const ABS_PATH_FILE_RE = /\/[^\s"'`<>]+?\.[a-z0-9]{1,10}/gi;

function clampTelegram(text) {
  if (!text) return '(empty response)';
  if (text.length <= 4000) return text;
  return `${text.slice(0, 4000)}\n\n[truncated for Telegram; full output available in memory/session artifacts]`;
}

function formatError(err) {
  const msg = err?.message || String(err);
  return `Error: ${msg.slice(0, 3000)}`;
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

function parseOutputMarkers(text) {
  const imageRefs = [];
  const fileRefs = [];
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
    kept.push(line);
  }
  const cleanText = kept.join('\n').replace(/\n{3,}/g, '\n\n').trim();
  return {
    cleanText,
    imageRefs: uniqueRefs(imageRefs),
    fileRefs: uniqueRefs(fileRefs)
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

function resolveLocalImagePath(ref, codexCwd) {
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
  if (normalized.startsWith('./') || normalized.startsWith('../') || normalized.includes('/')) {
    return path.resolve(codexCwd, normalized);
  }
  return null;
}

async function sendImageReference(ctx, ref, codexCwd) {
  if (isHttpUrl(ref)) {
    try {
      await ctx.replyWithPhoto(ref, topicExtra(ctx));
      return true;
    } catch {
      try {
        await ctx.replyWithDocument(ref, topicExtra(ctx));
        return true;
      } catch {
        return false;
      }
    }
  }

  const localPath = resolveLocalImagePath(ref, codexCwd);
  if (!localPath || !fs.existsSync(localPath) || !fileLooksLikeImage(localPath)) {
    return false;
  }

  try {
    await ctx.replyWithPhoto({ source: localPath }, topicExtra(ctx));
    return true;
  } catch {
    try {
      await ctx.replyWithDocument({ source: localPath }, topicExtra(ctx));
      return true;
    } catch {
      return false;
    }
  }
}

async function sendFileReference(ctx, ref, codexCwd) {
  const normalized = normalizeRef(ref);
  if (!normalized) {
    return false;
  }
  if (isHttpUrl(normalized)) {
    if (normalized.match(/\.(png|jpe?g|gif|webp|bmp|tiff?|svg)(\?|$)/i)) {
      return sendImageReference(ctx, normalized, codexCwd);
    }
    try {
      await ctx.replyWithDocument(normalized, topicExtra(ctx));
      return true;
    } catch {
      return false;
    }
  }

  const localPath = resolveLocalImagePath(normalized, codexCwd) || (
    path.isAbsolute(normalized) ? normalized : path.resolve(codexCwd, normalized)
  );

  if (!localPath || !fs.existsSync(localPath)) {
    return false;
  }
  if (fileLooksLikeImage(localPath)) {
    return sendImageReference(ctx, localPath, codexCwd);
  }
  try {
    await ctx.replyWithDocument({ source: localPath }, topicExtra(ctx));
    return true;
  } catch {
    return false;
  }
}

function guessImageExtension(urlText, mimeHint = '', contentType = '') {
  try {
    const ext = path.extname(new URL(urlText).pathname || '').toLowerCase();
    if (IMAGE_EXTS.has(ext)) return ext;
  } catch {
    // ignore URL parsing issues
  }

  const mime = `${mimeHint || ''} ${contentType || ''}`.toLowerCase();
  if (mime.includes('png')) return '.png';
  if (mime.includes('jpeg') || mime.includes('jpg')) return '.jpg';
  if (mime.includes('gif')) return '.gif';
  if (mime.includes('webp')) return '.webp';
  if (mime.includes('svg')) return '.svg';
  if (mime.includes('bmp')) return '.bmp';
  if (mime.includes('tiff')) return '.tiff';
  return '.jpg';
}

function guessFileExtension(urlText, fileNameHint = '', mimeHint = '', contentType = '') {
  const fileExt = path.extname(String(fileNameHint || '')).toLowerCase();
  if (fileExt) {
    return fileExt;
  }

  try {
    const urlExt = path.extname(new URL(urlText).pathname || '').toLowerCase();
    if (urlExt) return urlExt;
  } catch {
    // ignore URL parsing issues
  }

  const mime = `${mimeHint || ''} ${contentType || ''}`.toLowerCase();
  if (mime.includes('pdf')) return '.pdf';
  if (mime.includes('zip')) return '.zip';
  if (mime.includes('json')) return '.json';
  if (mime.includes('csv')) return '.csv';
  if (mime.includes('plain')) return '.txt';
  if (mime.includes('markdown')) return '.md';
  if (mime.includes('word')) return '.docx';
  if (mime.includes('excel') || mime.includes('spreadsheet')) return '.xlsx';
  if (mime.includes('powerpoint') || mime.includes('presentation')) return '.pptx';
  if (mime.includes('mpeg') || mime.includes('mp3')) return '.mp3';
  if (mime.includes('m4a')) return '.m4a';
  if (mime.includes('ogg')) return '.ogg';
  if (mime.includes('wav')) return '.wav';
  if (mime.includes('mp4') || mime.includes('video')) return '.mp4';
  if (mime.includes('quicktime')) return '.mov';
  if (mime.includes('png') || mime.includes('jpeg') || mime.includes('jpg') || mime.includes('gif') || mime.includes('webp') || mime.includes('svg') || mime.includes('bmp') || mime.includes('tiff')) {
    return guessImageExtension(urlText, mimeHint, contentType);
  }
  return '.bin';
}

async function downloadTelegramImageForCodex(ctx, fileId, chatId, memoryRoot, mimeHint = '') {
  const url = (await ctx.telegram.getFileLink(fileId)).toString();
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download Telegram image: HTTP ${response.status}`);
  }

  const uploadsDir = path.join(memoryRoot, 'uploads', String(chatId));
  await fsPromises.mkdir(uploadsDir, { recursive: true });

  const extension = guessImageExtension(url, mimeHint, response.headers.get('content-type') || '');
  const filename = `tg_${Date.now()}_${crypto.randomUUID().slice(0, 8)}${extension}`;
  const localPath = path.join(uploadsDir, filename);

  const bytes = Buffer.from(await response.arrayBuffer());
  await fsPromises.writeFile(localPath, bytes);
  return localPath;
}

async function downloadTelegramFileForCodex(ctx, fileId, chatId, memoryRoot, fileNameHint = '', mimeHint = '') {
  const url = (await ctx.telegram.getFileLink(fileId)).toString();
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download Telegram file: HTTP ${response.status}`);
  }

  const uploadsDir = path.join(memoryRoot, 'uploads', String(chatId));
  await fsPromises.mkdir(uploadsDir, { recursive: true });

  const extension = guessFileExtension(
    url,
    fileNameHint,
    mimeHint,
    response.headers.get('content-type') || ''
  );
  const baseName = String(fileNameHint || 'file').replace(/\.[a-zA-Z0-9]{1,10}$/, '');
  const safeBase = baseName.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 40) || 'file';
  const filename = `${safeBase}_${Date.now()}_${crypto.randomUUID().slice(0, 8)}${extension.startsWith('.') ? extension : `.${extension}`}`;
  const localPath = path.join(uploadsDir, filename);

  const bytes = Buffer.from(await response.arrayBuffer());
  await fsPromises.writeFile(localPath, bytes);
  return localPath;
}

function isImageDocument(doc) {
  if (!doc) return false;
  const mime = String(doc.mime_type || '').toLowerCase();
  if (mime.startsWith('image/')) return true;
  const ext = path.extname(String(doc.file_name || '')).toLowerCase();
  return IMAGE_EXTS.has(ext);
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

async function readRestartGuard(filePath) {
  try {
    const raw = await fsPromises.readFile(filePath, 'utf8');
    const data = JSON.parse(raw);
    return {
      lastUpdateId: Number.isInteger(data?.lastUpdateId) ? data.lastUpdateId : 0,
      lastHandledAtMs: Number.isFinite(Number(data?.lastHandledAtMs)) ? Number(data.lastHandledAtMs) : 0
    };
  } catch {
    return { lastUpdateId: 0, lastHandledAtMs: 0 };
  }
}

async function writeRestartGuard(filePath, value) {
  const dir = path.dirname(filePath);
  await fsPromises.mkdir(dir, { recursive: true });
  const tmp = `${filePath}.tmp`;
  await fsPromises.writeFile(tmp, JSON.stringify(value, null, 2), 'utf8');
  await fsPromises.rename(tmp, filePath);
}

function shouldFlushStreamingChunk(text, force = false) {
  if (force) return String(text || '').length > 0;
  const raw = String(text || '');
  if (!raw) return false;
  if (raw.length >= 260) return true;
  return raw.length >= 90 && /(?:\s|[.!?]\s|\n)$/.test(raw);
}

function splitTelegramChunks(text, maxLen = 3900) {
  const input = String(text || '');
  if (!input) return [];
  const chunks = [];
  let start = 0;

  while (start < input.length) {
    if (input.length - start <= maxLen) {
      chunks.push(input.slice(start));
      break;
    }

    const tentativeEnd = start + maxLen;
    const windowText = input.slice(start, tentativeEnd);
    let cutRel = windowText.lastIndexOf('\n\n');
    if (cutRel < Math.floor(maxLen * 0.35)) cutRel = windowText.lastIndexOf('\n');
    if (cutRel < Math.floor(maxLen * 0.35)) cutRel = windowText.lastIndexOf('. ');
    if (cutRel < Math.floor(maxLen * 0.35)) {
      cutRel = maxLen;
    } else if (windowText[cutRel] === '.') {
      cutRel += 1;
    }

    const end = Math.max(start + 1, start + cutRel);
    chunks.push(input.slice(start, end));
    start = end;
  }

  return chunks.filter((c) => c.length > 0);
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

function deriveScopedChatId(baseChatId, topicId) {
  if (!Number.isInteger(baseChatId)) return baseChatId;
  if (!Number.isInteger(topicId) || topicId <= 0) return baseChatId;
  const h = fnv1a32(`${baseChatId}:${topicId}`);
  return 7_000_000_000_000 + h;
}

function getMessageThreadId(ctx) {
  const raw = ctx?.message?.message_thread_id
    ?? ctx?.update?.message?.message_thread_id
    ?? ctx?.update?.edited_message?.message_thread_id
    ?? 0;
  const topicId = Number(raw);
  return Number.isInteger(topicId) && topicId > 0 ? topicId : 0;
}

function topicExtra(ctx, extra = {}) {
  const topicId = getMessageThreadId(ctx);
  if (!topicId) {
    return extra;
  }
  return {
    ...extra,
    message_thread_id: topicId
  };
}

async function replyText(ctx, text, extra = {}) {
  return ctx.reply(text, topicExtra(ctx, extra));
}

async function sendTyping(ctx) {
  return ctx.sendChatAction('typing', topicExtra(ctx));
}

function getConversationScope(ctx) {
  const baseChatId = Number(ctx?.chat?.id);
  const topicId = getMessageThreadId(ctx);
  const scopedChatId = deriveScopedChatId(baseChatId, topicId);
  const label = topicId
    ? `chat:${baseChatId}:topic:${topicId}`
    : `chat:${baseChatId}`;
  return {
    baseChatId,
    topicId,
    scopedChatId,
    label
  };
}

function isGroupChat(ctx) {
  const chatType = String(ctx?.chat?.type || '');
  return chatType === 'group' || chatType === 'supergroup';
}

function botUsername(ctx) {
  return String(ctx?.botInfo?.username || '').trim().toLowerCase();
}

function stripLeadingMention(text, username) {
  const raw = String(text || '');
  const user = String(username || '').trim().replace(/^@+/, '').toLowerCase();
  if (!raw || !user) return raw.trim();
  const escaped = user.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`^\\s*@${escaped}\\s*[:,\\-]?\\s*`, 'i');
  return raw.replace(re, '').trim();
}

function isReplyToBot(ctx) {
  const botId = Number(ctx?.botInfo?.id || 0);
  const replyFromId = Number(ctx?.message?.reply_to_message?.from?.id || 0);
  return botId > 0 && replyFromId > 0 && botId === replyFromId;
}

function messageMentionsBot(ctx) {
  const username = botUsername(ctx);
  if (!username) return false;

  const msg = ctx?.message || {};
  const text = String(msg.text || msg.caption || '');
  if (text && text.toLowerCase().includes(`@${username}`)) {
    return true;
  }

  const entities = [
    ...(Array.isArray(msg.entities) ? msg.entities : []),
    ...(Array.isArray(msg.caption_entities) ? msg.caption_entities : [])
  ];
  for (const entity of entities) {
    if (entity?.type !== 'mention') continue;
    const offset = Number(entity.offset || 0);
    const length = Number(entity.length || 0);
    if (length <= 0) continue;
    const token = text.slice(offset, offset + length).toLowerCase();
    if (token === `@${username}`) {
      return true;
    }
  }
  return false;
}

function shouldHandleGroupMessage(ctx, telegramSettings) {
  if (!isGroupChat(ctx)) return true;
  if (!telegramSettings?.groupRequireMention) return true;
  return isReplyToBot(ctx) || messageMentionsBot(ctx);
}

function cleanGroupPrompt(ctx, text) {
  if (!isGroupChat(ctx)) return String(text || '').trim();
  const user = botUsername(ctx);
  return stripLeadingMention(String(text || ''), user);
}

function parseHumanTimeToHHMM(rawText) {
  const text = String(rawText || '').trim().toLowerCase();
  if (!text) return null;

  const m24 = text.match(/\b([01]?\d|2[0-3]):([0-5]\d)\b/);
  if (m24) {
    return `${m24[1].padStart(2, '0')}:${m24[2]}`;
  }

  const m12 = text.match(/\b(1[0-2]|0?[1-9])(?::([0-5]\d))?\s*(am|pm)\b/);
  if (m12) {
    let hour = Number(m12[1]);
    const minute = Number(m12[2] || 0);
    const ap = m12[3];
    if (ap === 'am') {
      if (hour === 12) hour = 0;
    } else if (hour !== 12) {
      hour += 12;
    }
    return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
  }

  return null;
}

function inferDailyTimeHint(text) {
  const lowered = String(text || '').toLowerCase();
  if (/\bmorning\b/.test(lowered)) return '09:00';
  if (/\bnoon\b/.test(lowered)) return '12:00';
  if (/\bafternoon\b/.test(lowered)) return '15:00';
  if (/\bevening\b/.test(lowered)) return '18:00';
  if (/\bnight\b/.test(lowered)) return '21:00';
  return null;
}

function parsePipeArgs(text) {
  return String(text || '')
    .split('|')
    .map((p) => p.trim());
}

function extractSkillCommandBody(rawText) {
  return String(rawText || '').replace(/^\/skill(?:@\w+)?/i, '').trim();
}

function formatSkillPreview(content, maxLen = 3000) {
  const text = String(content || '').trim();
  if (text.length <= maxLen) {
    return text;
  }
  return `${text.slice(0, maxLen - 24)}\n\n...[truncated preview]`;
}

function extractSchedulePromptFromText(text, kind) {
  const raw = String(text || '').trim();
  const lower = raw.toLowerCase();

  const toIdx = lower.indexOf(' to ');
  if (toIdx > 0) {
    const p = raw.slice(toIdx + 4).trim();
    if (p) return p;
  }

  const sendIdx = lower.indexOf('send me ');
  if (sendIdx >= 0) {
    return raw.slice(sendIdx).trim();
  }

  const remindIdx = lower.indexOf('remind me ');
  if (remindIdx >= 0) {
    return raw.slice(remindIdx).trim();
  }

  if (kind === 'heartbeat') {
    return 'Check in proactively and keep me focused on priorities.';
  }

  return 'Send a concise proactive report based on current context, open loops, and recent decisions.';
}

function parseNaturalScheduleRequest(text, defaultTimezone) {
  const input = String(text || '').trim();
  if (!input) return null;
  const lowered = input.toLowerCase();

  const hasIntent = /\b(?:set|create|add|setup|schedule)\b/.test(lowered)
    && /\b(?:schedule|reminder|heartbeat|report)\b/.test(lowered);
  const hasRecurring = /\b(?:every|daily|weekday|weekdays|cron)\b/.test(lowered);
  if (!hasIntent && !hasRecurring) {
    return null;
  }

  const kind = /\bheartbeat\b/.test(lowered) ? 'heartbeat' : 'report';

  let timezone = defaultTimezone;
  const tzMatch = input.match(/\btz=([A-Za-z_+\-/]+)\b/i);
  if (tzMatch) {
    timezone = String(tzMatch[1] || '').trim();
  }

  let cronExpr = null;
  let scheduleSpec = null;

  const cronPrefix = input.match(/\bcron\s+([^\n|]+)$/i);
  if (cronPrefix) {
    const cronBody = cronPrefix[1].trim().replace(/\s+/g, ' ');
    const parts = cronBody.split(' ').slice(0, 5);
    if (parts.length === 5) {
      cronExpr = parts.join(' ');
      scheduleSpec = `cron ${cronExpr}`;
    }
  }

  if (!cronExpr) {
    const dailyToken = input.match(/\bdaily\s+([^\s|]+)\b/i);
    let hhmm = dailyToken ? parseHumanTimeToHHMM(dailyToken[1]) : null;
    if (!hhmm) {
      const atMatch = input.match(/\bat\s+([^\n|]+?)\b(?:\s+to\s+|\s*$)/i);
      hhmm = atMatch ? parseHumanTimeToHHMM(atMatch[1]) : null;
    }
    if (!hhmm) {
      hhmm = inferDailyTimeHint(input);
    }

    if (hhmm) {
      if (/\bweekdays?\b/.test(lowered)) {
        const [h, m] = hhmm.split(':').map(Number);
        cronExpr = `${m} ${h} * * 1-5`;
        scheduleSpec = `cron ${cronExpr}`;
      } else if (/\bevery\s+(mon|tue|wed|thu|fri|sat|sun)\b/i.test(lowered)) {
        const day = lowered.match(/\bevery\s+(mon|tue|wed|thu|fri|sat|sun)\b/i)?.[1]?.toLowerCase();
        const dayMap = { mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6, sun: 0 };
        const [h, m] = hhmm.split(':').map(Number);
        cronExpr = `${m} ${h} * * ${dayMap[day]}`;
        scheduleSpec = `cron ${cronExpr}`;
      } else if (/\bevery\b|\bdaily\b|\bmorning\b|\bevening\b|\bnight\b|\bnoon\b/i.test(lowered)) {
        cronExpr = cronFromDailyTime(hhmm);
        scheduleSpec = `daily ${hhmm}`;
      }
    }
  }

  if (!cronExpr || !scheduleSpec) {
    return null;
  }

  const prompt = extractSchedulePromptFromText(input, kind);
  return {
    kind,
    scheduleSpec,
    timezone,
    prompt
  };
}

export function createTelegramBot({
  appSettings,
  token,
  allowedUserIds,
  allowedChatIds = [],
  store,
  memory,
  codex,
  telegramSettings,
  scheduleSettings,
  skillSettings,
  whisperSettings,
  codexCwd,
  restartService,
  autostart
}) {
  const bot = new Telegraf(token, {
    handlerTimeout: Number(telegramSettings?.handlerTimeoutMs || 600000)
  });
  const appName = String(appSettings?.name || 'Dexbot').trim() || 'Dexbot';
  const queueByChat = new Map();
  const processStartedAtSec = Math.floor(Date.now() / 1000);
  const restartGuardPath = path.join(memory.paths.root, 'system', 'restart-guard.json');
  let restartInProgress = false;
  let scheduleTimer = null;
  const scheduleRunningJobIds = new Set();
  const schedulePollMs = Math.max(5_000, Number(scheduleSettings?.pollIntervalMs || 15_000));
  const scheduleDefaultTimezone = (
    String(scheduleSettings?.defaultTimezone || Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC').trim()
      || 'UTC'
  );
  const skillManager = createSkillManager({
    writableRoot: skillSettings?.root || path.join(codexCwd, '.agents', 'skills'),
    readRoots: (skillSettings?.includeCodexHome && skillSettings?.codexHomeRoot)
      ? [skillSettings.codexHomeRoot]
      : []
  });

  function evaluateAccess(ctx) {
    const chatType = String(ctx.chat?.type || '');
    if (telegramSettings?.privateOnly && chatType !== 'private') {
      return { allowed: false, reason: 'private_only' };
    }
    const chatId = ctx.chat?.id;
    if (Array.isArray(allowedChatIds) && allowedChatIds.length) {
      if (!Number.isInteger(chatId) || !allowedChatIds.includes(chatId)) {
        return { allowed: false, reason: 'chat_not_allowed' };
      }
    }
    if (!allowedUserIds.length) return { allowed: true, reason: 'ok' };
    const id = ctx.from?.id;
    if (Number.isInteger(id) && allowedUserIds.includes(id)) {
      return { allowed: true, reason: 'ok' };
    }
    return { allowed: false, reason: 'user_not_allowed' };
  }

  function isAllowed(ctx) {
    return evaluateAccess(ctx).allowed;
  }

  bot.use(async (ctx, next) => {
    const access = evaluateAccess(ctx);
    if (access.allowed) {
      return next();
    }
    const chatType = String(ctx.chat?.type || '');
    if (chatType === 'private') {
      await replyText(ctx, 'Access denied.');
      return undefined;
    }
    if (access.reason === 'chat_not_allowed' || access.reason === 'private_only') {
      await ctx.leaveChat().catch(() => undefined);
      return undefined;
    }
    if (access.reason === 'user_not_allowed') {
      const senderChatId = Number(ctx?.message?.sender_chat?.id || 0);
      const currentChatId = Number(ctx?.chat?.id || 0);
      const senderLooksAnonymous = senderChatId !== 0 && senderChatId === currentChatId;
      if (ctx?.message?.sender_chat) {
        const sc = ctx.message.sender_chat;
        const title = String(sc.title || sc.username || sc.id || 'unknown');
        await replyText(
          ctx,
          `I cannot verify your user ID because this message is sent as "${title}" (sender_chat). Send as your personal account and try again.`
        ).catch(() => undefined);
        return undefined;
      }
      if (senderLooksAnonymous) {
        await replyText(
          ctx,
          'I can only respond when your personal user identity is visible. Disable "Send as channel/anonymous admin" for this group and try again.'
        ).catch(() => undefined);
      }
    }
    // Group is allowed but sender is not in ALLOWED_TELEGRAM_USER_IDS.
    // Ignore silently to avoid noise and avoid leaving the group.
    return undefined;
  });

  async function queue(chatId, task) {
    const prev = queueByChat.get(chatId) || Promise.resolve();
    const next = prev
      .catch(() => undefined)
      .then(task)
      .finally(() => {
        if (queueByChat.get(chatId) === next) queueByChat.delete(chatId);
      });
    queueByChat.set(chatId, next);
    return next;
  }

  function enqueue(chatId, task) {
    void queue(chatId, task).catch((err) => {
      console.error('[telegram] queued task failed:', err);
    });
  }

  function scheduledExtra(job) {
    if (Number(job?.topic_id || 0) > 0) {
      return { message_thread_id: Number(job.topic_id) };
    }
    return {};
  }

  function parseTzHint(text) {
    const input = String(text || '').trim();
    const m = input.match(/\btz=([A-Za-z_+\-/]+)\b/i);
    if (!m) {
      return { cleaned: input, timezone: scheduleDefaultTimezone };
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

  async function sendScheduledText(job, text) {
    const chunks = splitTelegramChunks(String(text || ''));
    for (const chunk of chunks) {
      if (!chunk.trim()) continue;
      await bot.telegram.sendMessage(
        job.base_chat_id,
        clampTelegram(chunk),
        scheduledExtra(job)
      );
    }
  }

  function createScheduleForScope(conversation, {
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

    store.getChatState(conversation.scopedChatId);
    return store.createScheduledJob({
      baseChatId: conversation.baseChatId,
      scopedChatId: conversation.scopedChatId,
      topicId: conversation.topicId || 0,
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

  async function runScheduledJobNow(job, { manual = false } = {}) {
    const chatId = Number(job.scoped_chat_id);
    const effectivePrompt = buildScheduledPrompt(job);
    const prepared = memory.prepareTurn(chatId, effectivePrompt);
    const threadId = await codex.ensureThread(prepared.state.thread_id || null, {
      developerInstructions: prepared.developerInstructions
    });
    if (threadId !== prepared.state.thread_id) {
      store.setThreadId(chatId, threadId);
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
      ? `💓 ${prefix} heartbeat: ${job.title}`
      : `⏰ ${prefix} report: ${job.title}`;

    await sendScheduledText(job, `${heading}\n\n${finalText}`);

    memory.retainReflectIndex({
      chatId,
      state: prepared.state,
      userText: `[scheduled:${job.kind}] ${job.prompt}`,
      assistantText: finalText,
      workingMemory: prepared.workingMemory
    });
  }

  function retainLocalTurn(chatId, userText, assistantText) {
    try {
      const prepared = memory.prepareTurn(chatId, userText);
      memory.retainReflectIndex({
        chatId,
        state: prepared.state,
        userText,
        assistantText,
        workingMemory: prepared.workingMemory
      });
    } catch (err) {
      console.error('[memory] retainLocalTurn failed:', err?.message || err);
    }
  }

  async function pollScheduledJobs() {
    const now = new Date();
    const nowIso = now.toISOString();
    const due = store.listDueScheduledJobs(nowIso, 12);
    for (const job of due) {
      if (scheduleRunningJobIds.has(job.id)) {
        continue;
      }
      scheduleRunningJobIds.add(job.id);
      try {
        const timezone = validateTimeZone(job.timezone) ? job.timezone : scheduleDefaultTimezone;
        const nextRunAt = computeNextRunIso({
          cronExpr: job.cron_expr,
          timezone,
          from: now
        });
        store.markScheduledJobRun(job.id, {
          lastRunAt: nowIso,
          nextRunAt
        });
        enqueue(job.scoped_chat_id, async () => {
          try {
            await runScheduledJobNow({
              ...job,
              timezone
            });
          } catch (err) {
            await bot.telegram.sendMessage(
              job.base_chat_id,
              formatError(err),
              scheduledExtra(job)
            ).catch(() => undefined);
          } finally {
            scheduleRunningJobIds.delete(job.id);
          }
        });
      } catch (err) {
        store.setScheduledJobActive(job.id, job.scoped_chat_id, false);
        scheduleRunningJobIds.delete(job.id);
        await bot.telegram.sendMessage(
          job.base_chat_id,
          `Paused schedule #${job.id} due to config error: ${err?.message || err}`,
          scheduledExtra(job)
        ).catch(() => undefined);
      }
    }
  }

  async function runPromptTurn(ctx, {
    scope = null,
    userPrompt,
    inputItems = null,
    introText = 'Thinking...'
  }) {
    const conversation = scope || getConversationScope(ctx);
    const chatId = conversation.scopedChatId;
    await sendTyping(ctx);
    const statusMsg = await replyText(ctx, introText);
    let statusCleared = false;
    let closed = false;
    let flushInterval = null;
    let ticker = null;
    let flushBusy = false;

    try {
      const prepared = memory.prepareTurn(chatId, userPrompt);
      const threadId = await codex.ensureThread(prepared.state.thread_id || null, {
        developerInstructions: prepared.developerInstructions
      });
      if (threadId !== prepared.state.thread_id) {
        store.setThreadId(chatId, threadId);
        prepared.state.thread_id = threadId;
      }

      let liveRaw = '';
      let commandOutput = '';
      let sentLength = 0;
      let pendingChunk = '';
      let sentAnyContent = false;
      let lastChunkSentAt = Date.now();

      const clearStatus = async () => {
        if (statusCleared) return;
        statusCleared = true;
        await ctx.telegram.deleteMessage(ctx.chat.id, statusMsg.message_id).catch(() => undefined);
      };

      const emitTextChunk = async (text) => {
        const chunks = splitTelegramChunks(text);
        if (!chunks.length) return;
        await clearStatus();
        for (const chunk of chunks) {
          if (!chunk.trim()) continue;
          await replyText(ctx, clampTelegram(chunk));
          sentAnyContent = true;
        }
      };

      const flushPending = async (force = false) => {
        if (flushBusy) return;
        if (!shouldFlushStreamingChunk(pendingChunk, force)) return;

        flushBusy = true;
        try {
          const out = pendingChunk;
          await emitTextChunk(out);
          sentLength += out.length;
          pendingChunk = '';
          lastChunkSentAt = Date.now();
        } finally {
          flushBusy = false;
        }
      };

      ticker = setInterval(async () => {
        if (closed) return;
        await sendTyping(ctx).catch(() => undefined);
      }, telegramSettings.typingIntervalMs);

      flushInterval = setInterval(async () => {
        if (closed) return;
        if (!liveRaw) return;
        const cleanLive = parseOutputMarkers(liveRaw).cleanText;
        if (!cleanLive) return;
        const consumed = sentLength + pendingChunk.length;
        if (cleanLive.length <= consumed) return;
        pendingChunk += cleanLive.slice(consumed);
        const silentMs = Date.now() - lastChunkSentAt;
        const forceBySilence = pendingChunk.length >= 40 && silentMs >= 2200;
        await flushPending(forceBySilence);
      }, telegramSettings.streamEditIntervalMs);

      const result = await codex.runTurn({
        threadId,
        userPrompt,
        inputItems,
        onDelta: (delta) => {
          liveRaw += delta;
        },
        onCommandDelta: (delta) => {
          commandOutput += delta;
        }
      });

      closed = true;
      if (flushInterval) clearInterval(flushInterval);
      if (ticker) clearInterval(ticker);

      const rawFinal = (result.finalText || liveRaw || '').trim();
      const parsedFinal = parseOutputMarkers(rawFinal);
      const fallbackRefs = extractImageRefsFallback(`${rawFinal}\n${commandOutput}`);
      const fallbackFileRefs = extractFileRefsFallback(commandOutput);
      const imageRefs = uniqueRefs([...parsedFinal.imageRefs, ...fallbackRefs]).slice(0, 6);
      const fileRefs = uniqueRefs([...parsedFinal.fileRefs, ...fallbackFileRefs]).slice(0, 6);

      const finalText = parsedFinal.cleanText || ((imageRefs.length || fileRefs.length) ? 'Done. Sending file output.' : '(empty response)');

      const consumed = sentLength + pendingChunk.length;
      if (finalText.length > consumed) {
        pendingChunk += finalText.slice(consumed);
      }

      await flushPending(true);

      if (!sentAnyContent && finalText) {
        await clearStatus();
        await replyText(ctx, clampTelegram(finalText));
        sentAnyContent = true;
      } else {
        await clearStatus();
      }

      for (const ref of imageRefs) {
        await sendImageReference(ctx, ref, codexCwd).catch(() => false);
      }
      for (const ref of fileRefs) {
        await sendFileReference(ctx, ref, codexCwd).catch(() => false);
      }

      const assistantForMemory = (imageRefs.length || fileRefs.length)
        ? `${finalText}\n[image_outputs: ${imageRefs.join(', ')}]\n[file_outputs: ${fileRefs.join(', ')}]`
        : finalText;

      memory.retainReflectIndex({
        chatId,
        state: prepared.state,
        userText: userPrompt,
        assistantText: assistantForMemory,
        workingMemory: prepared.workingMemory
      });
    } catch (err) {
      if (!statusCleared) {
        await ctx.telegram.deleteMessage(ctx.chat.id, statusMsg.message_id).catch(() => undefined);
        statusCleared = true;
      }
      await replyText(ctx, formatError(err)).catch(() => undefined);
    } finally {
      closed = true;
      if (flushInterval) clearInterval(flushInterval);
      if (ticker) clearInterval(ticker);
    }
  }

  async function handleAudioLikeMessage(ctx, fileId, inputExtension = 'ogg') {
    if (!whisperSettings?.enabled) {
      await replyText(ctx, 'Voice transcription is disabled. Set WHISPER_ENABLED=true to enable it.');
      return;
    }

    const conversation = getConversationScope(ctx);
    enqueue(conversation.scopedChatId, async () => {
      await sendTyping(ctx);
      const statusMsg = await replyText(ctx, 'Transcribing voice message...');
      try {
        const transcript = await transcribeTelegramFile({
          ctx,
          fileId,
          inputExtension,
          languageHint: ctx.from?.language_code || null,
          whisper: whisperSettings
        });

        await ctx.telegram
          .deleteMessage(ctx.chat.id, statusMsg.message_id)
          .catch(() => undefined);

        await runPromptTurn(ctx, {
          scope: conversation,
          userPrompt: transcript,
          introText: 'Thinking...'
        });
      } catch (err) {
        await ctx.telegram
          .editMessageText(ctx.chat.id, statusMsg.message_id, undefined, formatError(err))
          .catch(() => undefined);
      }
    });
  }

  bot.start(async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    await replyText(ctx, [
      `${appName} is active with layered memory.`,
      'You can send text, images, documents, videos, voice notes, or audio files.',
      'Commands:',
      '/newsession - start a new session memory + new Codex thread',
      '/memory - show memory status + key artifacts',
      '/heartbeat - run memory maintenance now',
      '/schedule - manage proactive cron jobs',
      '/skill - create/list/show/run/delete skills',
      'Natural skill creation also works (e.g. "create a skill called sales-page for ...").',
      '/restart - restart bot + Codex app-server',
      '/autostart [on|off|status] - control start on Mac reboot',
      '/chatid - show current chat id and topic id',
      '/whoami - show your Telegram user ID'
    ].join('\n'));
  });

  bot.command('chatid', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    const conversation = getConversationScope(ctx);
    const lines = [
      `chat_id=${conversation.baseChatId}`,
      `topic_id=${conversation.topicId || 0}`,
      `scope=${conversation.label}`,
      `chat_type=${ctx.chat?.type || 'unknown'}`
    ];
    await replyText(ctx, lines.join('\n'));
  });

  bot.command('whoami', async (ctx) => {
    const senderChat = ctx?.message?.sender_chat;
    const lines = [
      `telegram_user_id=${ctx.from?.id || 'unknown'}`,
      `chat_id=${ctx.chat?.id || 'unknown'}`,
      `chat_type=${ctx.chat?.type || 'unknown'}`,
      `topic_id=${getMessageThreadId(ctx) || 0}`,
      `sender_chat_id=${senderChat?.id || 0}`,
      `sender_chat_title=${senderChat?.title || senderChat?.username || '(none)'}`
    ];
    await replyText(ctx, lines.join('\n'));
  });

  bot.command('newsession', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    const conversation = getConversationScope(ctx);
    const state = store.startNewSession(conversation.scopedChatId);
    if (conversation.topicId) {
      await replyText(ctx, `Started session #${state.session_no} for topic ${conversation.topicId}. Thread reset.`);
    } else {
      await replyText(ctx, `Started session #${state.session_no}. Thread reset.`);
    }
  });

  bot.command('heartbeat', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    const result = memory.runHeartbeat();
    await replyText(ctx,
      `Heartbeat complete. weekly_summaries=${result.weeklyUpdated}, contradictions=${result.contradictionCount}, chats=${result.chatCount}`
    );
  });

  bot.command('schedule', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }

    const conversation = getConversationScope(ctx);
    const raw = String(ctx.message?.text || '').trim();
    const body = raw.replace(/^\/schedule(?:@\w+)?/i, '').trim();
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
      await replyText(ctx, helpText);
      return;
    }

    const [actionRaw] = body.split(/\s+/);
    const action = String(actionRaw || '').toLowerCase();

    if (action === 'list') {
      const jobs = store.listScheduledJobs(conversation.scopedChatId);
      if (!jobs.length) {
        await replyText(ctx, 'No schedules in this chat/topic scope yet.');
        return;
      }
      const lines = [
        `Schedules for ${conversation.label}:`,
        ...jobs.slice(0, 30).map((job) => [
          `#${job.id} [${job.active ? 'on' : 'off'}] ${job.kind}`,
          `cron=${job.cron_expr}`,
          `next=${formatRunAt(job.next_run_at, job.timezone)}`,
          `title=${job.title}`
        ].join(' | '))
      ];
      await replyText(ctx, clampTelegram(lines.join('\n')));
      return;
    }

    if (action === 'pause' || action === 'resume') {
      const id = Number(body.split(/\s+/)[1]);
      if (!Number.isInteger(id) || id <= 0) {
        await replyText(ctx, 'Usage: /schedule pause <id> OR /schedule resume <id>');
        return;
      }
      const ok = store.setScheduledJobActive(id, conversation.scopedChatId, action === 'resume');
      if (!ok) {
        await replyText(ctx, `Schedule #${id} not found in this scope.`);
        return;
      }
      await replyText(ctx, `Schedule #${id} ${action === 'resume' ? 'resumed' : 'paused'}.`);
      return;
    }

    if (action === 'remove' || action === 'delete') {
      const id = Number(body.split(/\s+/)[1]);
      if (!Number.isInteger(id) || id <= 0) {
        await replyText(ctx, 'Usage: /schedule remove <id>');
        return;
      }
      const ok = store.deleteScheduledJob(id, conversation.scopedChatId);
      if (!ok) {
        await replyText(ctx, `Schedule #${id} not found in this scope.`);
        return;
      }
      await replyText(ctx, `Schedule #${id} removed.`);
      return;
    }

    if (action === 'run') {
      const id = Number(body.split(/\s+/)[1]);
      if (!Number.isInteger(id) || id <= 0) {
        await replyText(ctx, 'Usage: /schedule run <id>');
        return;
      }
      const job = store.getScheduledJob(id, conversation.scopedChatId);
      if (!job) {
        await replyText(ctx, `Schedule #${id} not found in this scope.`);
        return;
      }
      enqueue(conversation.scopedChatId, async () => {
        try {
          await runScheduledJobNow(job, { manual: true });
          store.touchScheduledJob(job.id);
        } catch (err) {
          await replyText(ctx, formatError(err)).catch(() => undefined);
        }
      });
      await replyText(ctx, `Running schedule #${id} now...`);
      return;
    }

    if (action === 'add') {
      const payload = body.slice(3).trim();
      const sep = payload.indexOf('|');
      if (sep <= 0) {
        await replyText(ctx, 'Usage: /schedule add daily HH:MM | <prompt>');
        return;
      }

      let left = payload.slice(0, sep).trim();
      let prompt = payload.slice(sep + 1).trim();
      if (!left) {
        await replyText(ctx, 'Missing schedule definition.');
        return;
      }

      let kind = 'report';
      if (/^heartbeat\s+/i.test(left)) {
        kind = 'heartbeat';
        left = `daily ${left.replace(/^heartbeat\s+/i, '').trim()}`;
      }

      const tzParsed = parseTzHint(left);
      left = tzParsed.cleaned;
      const timezone = tzParsed.timezone || scheduleDefaultTimezone;
      if (!validateTimeZone(timezone)) {
        await replyText(ctx, `Invalid timezone "${timezone}". Example: tz=America/Los_Angeles`);
        return;
      }
      if (!prompt && kind === 'heartbeat') {
        prompt = 'Check in proactively and keep me focused on my priorities.';
      }
      if (!prompt) {
        await replyText(ctx, 'Prompt cannot be empty.');
        return;
      }
      const job = createScheduleForScope(conversation, {
        kind,
        scheduleSpec: left,
        timezone,
        prompt
      });

      await replyText(ctx, [
        `Schedule #${job.id} created.`,
        `kind=${job.kind}`,
        `cron=${job.cron_expr}`,
        `next=${formatRunAt(job.next_run_at, job.timezone)}`,
        `scope=${conversation.label}`
      ].join('\n'));
      return;
    }

    await replyText(ctx, helpText);
  });

  bot.command('skill', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }

    const conversation = getConversationScope(ctx);
    const raw = String(ctx.message?.text || '').trim();
    const body = extractSkillCommandBody(raw);
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
      '- Natural creation works too: "create a skill called <name> for ...".'
    ].join('\n');

    if (!body) {
      await replyText(ctx, helpText);
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
      await replyText(ctx, lines.join('\n'));
      return;
    }

    if (action === 'list') {
      const skills = skillManager.listSkills();
      if (!skills.length) {
        await replyText(ctx, 'No skills found yet. Create one with /skill create ...');
        return;
      }
      const lines = [
        `Skills (${skills.length}):`,
        ...skills.slice(0, 80).map((s) => `- $${s.name} [${s.writable ? 'local' : 'shared'}]`)
      ];
      await replyText(ctx, clampTelegram(lines.join('\n')));
      return;
    }

    if (action === 'show') {
      const name = rest.trim();
      if (!name) {
        await replyText(ctx, 'Usage: /skill show <name>');
        return;
      }
      const skill = skillManager.readSkill(name);
      if (!skill) {
        await replyText(ctx, `Skill not found: ${name}`);
        return;
      }
      const lines = [
        `Skill: $${skill.name}`,
        `source=${skill.writable ? 'local' : 'shared'}`,
        `path=${skill.path}`,
        '',
        formatSkillPreview(skill.content)
      ];
      await replyText(ctx, clampTelegram(lines.join('\n')));
      return;
    }

    if (action === 'create') {
      const parts = parsePipeArgs(rest);
      const rawName = String(parts[0] || '').trim();
      const description = String(parts[1] || '').trim();
      const instructions = parts.slice(2).join(' | ').trim();

      if (!rawName) {
        await replyText(ctx, 'Usage: /skill create <name> | <description> | <instructions>');
        return;
      }

      try {
        const name = normalizeSkillName(rawName);
        const created = skillManager.createSkill({
          name,
          description,
          instructions
        });
        await replyText(ctx, [
          `Skill created: $${created.name}`,
          `path=${created.path}`,
          '',
          `Try: /skill run ${created.name} | <your task>`,
          `Or in chat: $${created.name} <your task>`
        ].join('\n'));
      } catch (err) {
        await replyText(ctx, formatError(err));
      }
      return;
    }

    if (action === 'delete' || action === 'remove') {
      const name = rest.trim();
      if (!name) {
        await replyText(ctx, 'Usage: /skill delete <name>');
        return;
      }
      try {
        const removed = skillManager.deleteSkill(name);
        await replyText(ctx, `Skill deleted: $${removed.name}`);
      } catch (err) {
        await replyText(ctx, formatError(err));
      }
      return;
    }

    if (action === 'run') {
      const parts = parsePipeArgs(rest);
      const requestedName = String(parts[0] || '').trim();
      const task = parts.slice(1).join(' | ').trim();
      if (!requestedName || !task) {
        await replyText(ctx, 'Usage: /skill run <name> | <task>');
        return;
      }
      const found = skillManager.getSkill(requestedName);
      if (!found) {
        await replyText(ctx, `Skill not found: ${requestedName}`);
        return;
      }
      const forcedPrompt = buildSkillRunPrompt(found.name, task);
      enqueue(conversation.scopedChatId, async () => {
        await runPromptTurn(ctx, {
          scope: conversation,
          userPrompt: forcedPrompt,
          inputItems: [{ type: 'text', text: forcedPrompt }],
          introText: `Running skill $${found.name}...`
        });
      });
      return;
    }

    await replyText(ctx, helpText);
  });

  bot.command('restart', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    if (!restartService) {
      await replyText(ctx, 'Restart handler is not configured.');
      return;
    }

    if (restartInProgress) {
      await replyText(ctx, 'Restart is already in progress.');
      return;
    }

    const updateId = Number(ctx.update?.update_id || 0);
    const messageDateSec = Number(ctx.message?.date || 0);

    if (messageDateSec && messageDateSec < (processStartedAtSec - 2)) {
      return;
    }

    const guard = await readRestartGuard(restartGuardPath);
    if (updateId && guard.lastUpdateId && updateId <= guard.lastUpdateId) {
      return;
    }

    const nowMs = Date.now();
    if (guard.lastHandledAtMs && (nowMs - guard.lastHandledAtMs) < 15_000) {
      return;
    }

    restartInProgress = true;
    try {
      await writeRestartGuard(restartGuardPath, {
        lastUpdateId: updateId || guard.lastUpdateId || 0,
        lastHandledAtMs: nowMs
      });
      await replyText(ctx, 'Restarting bot and Codex server now...');
      await restartService({
        chatId: ctx.chat.id,
        userId: ctx.from?.id
      });
    } catch (err) {
      restartInProgress = false;
      await replyText(ctx, formatError(err));
    }
  });

  bot.command('autostart', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    if (!autostart) {
      await replyText(ctx, 'Autostart manager is not configured.');
      return;
    }

    const text = String(ctx.message?.text || '').trim();
    const parts = text.split(/\s+/).filter(Boolean);
    const arg = String(parts[1] || 'status').toLowerCase();

    if (!['on', 'off', 'status'].includes(arg)) {
      await replyText(ctx, 'Usage: /autostart on | /autostart off | /autostart status');
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
      await replyText(ctx, lines.join('\n'));
    } catch (err) {
      await replyText(ctx, formatError(err));
    }
  });

  bot.command('memory', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    const conversation = getConversationScope(ctx);
    const status = memory.getMemoryStatus(conversation.scopedChatId);
    const lines = [
      `Scope: ${conversation.label}`,
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
    await replyText(ctx, clampTelegram(lines.join('\n')));
  });

  bot.on('message', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }

    const text = typeof ctx.message?.text === 'string' ? ctx.message.text : '';
    if (!text) {
      return;
    }
    if (text.startsWith('/')) {
      return;
    }
    if (!shouldHandleGroupMessage(ctx, telegramSettings)) {
      return;
    }

    const conversation = getConversationScope(ctx);
    const userPrompt = cleanGroupPrompt(ctx, text);
    if (!userPrompt) {
      return;
    }

    const naturalSkillCreate = parseNaturalSkillCreateRequest(userPrompt);
    if (naturalSkillCreate) {
      try {
        const normalized = normalizeSkillName(naturalSkillCreate.nameCandidate);
        const created = skillManager.createSkill({
          name: normalized,
          description: naturalSkillCreate.description,
          instructions: naturalSkillCreate.instructions
        });
        const message = [
          `Skill created from conversation: $${created.name}`,
          `path=${created.path}`,
          '',
          `Use it with: $${created.name} <task>`,
          `Or: /skill run ${created.name} | <task>`
        ].join('\n');
        await replyText(ctx, message);
        retainLocalTurn(conversation.scopedChatId, userPrompt, message);
      } catch (err) {
        const txt = String(err?.message || err);
        if (/already exists/i.test(txt)) {
          const msg = `Skill already exists: $${normalizeSkillName(naturalSkillCreate.nameCandidate)}. Use /skill show ${normalizeSkillName(naturalSkillCreate.nameCandidate)} or run it directly.`;
          await replyText(ctx, msg);
          retainLocalTurn(conversation.scopedChatId, userPrompt, msg);
        } else {
          await replyText(ctx, formatError(err)).catch(() => undefined);
        }
      }
      return;
    }

    const naturalSchedule = parseNaturalScheduleRequest(userPrompt, scheduleDefaultTimezone);
    if (naturalSchedule) {
      try {
        const tz = naturalSchedule.timezone || scheduleDefaultTimezone;
        if (!validateTimeZone(tz)) {
          await replyText(ctx, `Invalid timezone "${tz}". Example: tz=America/Los_Angeles`);
          return;
        }
        const job = createScheduleForScope(conversation, {
          kind: naturalSchedule.kind,
          scheduleSpec: naturalSchedule.scheduleSpec,
          timezone: tz,
          prompt: naturalSchedule.prompt
        });
        await replyText(ctx, [
          `Scheduled via natural request. #${job.id}`,
          `kind=${job.kind}`,
          `cron=${job.cron_expr}`,
          `next=${formatRunAt(job.next_run_at, job.timezone)}`
        ].join('\n'));
      } catch (err) {
        await replyText(ctx, formatError(err)).catch(() => undefined);
      }
      return;
    }

    enqueue(conversation.scopedChatId, async () => {
      await runPromptTurn(ctx, {
        scope: conversation,
        userPrompt,
        inputItems: [{ type: 'text', text: userPrompt }]
      });
    });
  });

  bot.on('photo', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }

    const conversation = getConversationScope(ctx);
    const photos = ctx.message?.photo || [];
    const best = photos[photos.length - 1];
    if (!best?.file_id) {
      await replyText(ctx, 'Could not read this image payload.');
      return;
    }
    if (!shouldHandleGroupMessage(ctx, telegramSettings)) {
      return;
    }

    enqueue(conversation.scopedChatId, async () => {
      const userPrompt = cleanGroupPrompt(ctx, String(ctx.message?.caption || '').trim()) || 'Analyze this image and help me.';
      const localImagePath = await downloadTelegramImageForCodex(
        ctx,
        best.file_id,
        conversation.scopedChatId,
        memory.paths.root,
        'image/jpeg'
      );

      await runPromptTurn(ctx, {
        scope: conversation,
        userPrompt,
        inputItems: [
          { type: 'text', text: userPrompt },
          { type: 'localImage', path: localImagePath }
        ],
        introText: 'Thinking (with image)...'
      });
    });
  });

  bot.on('document', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }

    const doc = ctx.message?.document;
    if (!doc?.file_id) {
      await replyText(ctx, 'Could not read this file payload.');
      return;
    }
    if (!shouldHandleGroupMessage(ctx, telegramSettings)) {
      return;
    }

    const conversation = getConversationScope(ctx);
    enqueue(conversation.scopedChatId, async () => {
      const basePrompt = cleanGroupPrompt(ctx, String(ctx.message?.caption || '').trim());
      if (isImageDocument(doc)) {
        const userPrompt = basePrompt || 'Analyze this image and help me.';
        const localImagePath = await downloadTelegramImageForCodex(
          ctx,
          doc.file_id,
          conversation.scopedChatId,
          memory.paths.root,
          String(doc.mime_type || '')
        );

        await runPromptTurn(ctx, {
          scope: conversation,
          userPrompt,
          inputItems: [
            { type: 'text', text: userPrompt },
            { type: 'localImage', path: localImagePath }
          ],
          introText: 'Thinking (with image)...'
        });
        return;
      }

      const localPath = await downloadTelegramFileForCodex(
        ctx,
        doc.file_id,
        conversation.scopedChatId,
        memory.paths.root,
        String(doc.file_name || ''),
        String(doc.mime_type || '')
      );

      const prompt = buildFileAwarePrompt(
        basePrompt || 'Analyze this attached file and help me.',
        localPath,
        String(doc.file_name || ''),
        String(doc.mime_type || '')
      );

      await runPromptTurn(ctx, {
        scope: conversation,
        userPrompt: prompt,
        inputItems: [{ type: 'text', text: prompt }],
        introText: 'Thinking (with file)...'
      });
    });
  });

  bot.on('video', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    const video = ctx.message?.video;
    if (!video?.file_id) {
      await replyText(ctx, 'Could not read this video payload.');
      return;
    }
    if (!shouldHandleGroupMessage(ctx, telegramSettings)) {
      return;
    }
    const conversation = getConversationScope(ctx);
    enqueue(conversation.scopedChatId, async () => {
      const localPath = await downloadTelegramFileForCodex(
        ctx,
        video.file_id,
        conversation.scopedChatId,
        memory.paths.root,
        String(video.file_name || ''),
        String(video.mime_type || '')
      );
      const prompt = buildFileAwarePrompt(
        cleanGroupPrompt(ctx, String(ctx.message?.caption || '').trim()) || 'Analyze this attached video and help me.',
        localPath,
        String(video.file_name || ''),
        String(video.mime_type || '')
      );
      await runPromptTurn(ctx, {
        scope: conversation,
        userPrompt: prompt,
        inputItems: [{ type: 'text', text: prompt }],
        introText: 'Thinking (with file)...'
      });
    });
  });

  bot.on('animation', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    const anim = ctx.message?.animation;
    if (!anim?.file_id) {
      await replyText(ctx, 'Could not read this animation payload.');
      return;
    }
    if (!shouldHandleGroupMessage(ctx, telegramSettings)) {
      return;
    }
    const conversation = getConversationScope(ctx);
    enqueue(conversation.scopedChatId, async () => {
      const localPath = await downloadTelegramFileForCodex(
        ctx,
        anim.file_id,
        conversation.scopedChatId,
        memory.paths.root,
        String(anim.file_name || ''),
        String(anim.mime_type || '')
      );
      const prompt = buildFileAwarePrompt(
        cleanGroupPrompt(ctx, String(ctx.message?.caption || '').trim()) || 'Analyze this attached file and help me.',
        localPath,
        String(anim.file_name || ''),
        String(anim.mime_type || '')
      );
      await runPromptTurn(ctx, {
        scope: conversation,
        userPrompt: prompt,
        inputItems: [{ type: 'text', text: prompt }],
        introText: 'Thinking (with file)...'
      });
    });
  });

  bot.on('voice', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    const fileId = ctx.message?.voice?.file_id;
    if (!fileId) {
      await replyText(ctx, 'Could not find a valid voice file in this message.');
      return;
    }
    if (!shouldHandleGroupMessage(ctx, telegramSettings)) {
      return;
    }
    void handleAudioLikeMessage(ctx, fileId, 'ogg');
  });

  bot.on('audio', async (ctx) => {
    if (!isAllowed(ctx)) {
      await replyText(ctx, 'Access denied.');
      return;
    }
    const fileId = ctx.message?.audio?.file_id;
    if (!fileId) {
      await replyText(ctx, 'Could not find a valid audio file in this message.');
      return;
    }
    if (!shouldHandleGroupMessage(ctx, telegramSettings)) {
      return;
    }
    const mime = String(ctx.message?.audio?.mime_type || '');
    const inputExtension = mime.includes('mpeg') || mime.includes('mp3') ? 'mp3' : 'm4a';
    void handleAudioLikeMessage(ctx, fileId, inputExtension);
  });

  bot.startScheduleLoop = () => {
    if (scheduleTimer) {
      return;
    }
    scheduleTimer = setInterval(() => {
      void pollScheduledJobs().catch((err) => {
        console.error('[schedule] poll failed:', err);
      });
    }, schedulePollMs);
    scheduleTimer.unref();
    void pollScheduledJobs().catch((err) => {
      console.error('[schedule] initial poll failed:', err);
    });
  };

  bot.stopScheduleLoop = () => {
    if (!scheduleTimer) {
      return;
    }
    clearInterval(scheduleTimer);
    scheduleTimer = null;
  };

  bot.catch((err, ctx) => {
    const uid = ctx?.update?.update_id;
    console.error(`[telegram] unhandled error on update ${uid ?? 'unknown'}:`, err);
  });

  return bot;
}
