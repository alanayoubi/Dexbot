import process from 'node:process';
import { spawn } from 'node:child_process';
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { ensureSafeConfigWarnings, settings } from './config.js';
import { CodexRpcClient } from './codex-client.js';
import { MemoryStore } from './db.js';
import { MemoryEngine } from './memory.js';
import { createTelegramBot } from './telegram.js';
import { createAutostartManager } from './autostart.js';
import { createDashboardServer } from './dashboard.js';

const TELEGRAM_COMMANDS = [
  { command: 'newsession', description: 'Start a new memory session' },
  { command: 'memory', description: 'Show memory status' },
  { command: 'heartbeat', description: 'Run memory maintenance now' },
  { command: 'schedule', description: 'Manage proactive cron jobs' },
  { command: 'skill', description: 'Manage Codex skills' },
  { command: 'restart', description: 'Restart bot + Codex app-server' },
  { command: 'autostart', description: 'Toggle start-on-boot: on|off|status' },
  { command: 'chatid', description: 'Show current chat id/topic id' },
  { command: 'whoami', description: 'Show your Telegram user ID' }
];
const RESTART_NOTICE_MAX_AGE_MS = 10 * 60 * 1000;
const TELEGRAM_BOOT_TIMEOUT_MS = 45_000;
const TELEGRAM_API_TIMEOUT_MS = 15_000;

async function withTimeout(label, promise, timeoutMs) {
  let timer = null;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      })
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

async function syncTelegramCommands(bot, {
  allowedUserIds = [],
  allowedChatIds = []
} = {}) {
  const scopes = [];

  // Always clear any previously registered global commands to avoid leaking menus.
  await withTimeout(
    'deleteMyCommands(global)',
    bot.telegram.deleteMyCommands().catch(() => undefined),
    TELEGRAM_API_TIMEOUT_MS
  );

  for (const chatId of allowedChatIds || []) {
    if (Number.isInteger(chatId)) {
      scopes.push({ type: 'chat', chat_id: chatId });
    }
  }

  // Backward-safe fallback when explicit chat allowlist is not configured.
  if (!scopes.length) {
    scopes.push({ type: 'all_private_chats' });
  }

  for (const userId of allowedUserIds || []) {
    if (Number.isInteger(userId) && userId > 0) {
      scopes.push({ type: 'chat', chat_id: userId });
    }
  }

  const seen = new Set();
  for (const scope of scopes) {
    const key = `${scope.type}:${scope.chat_id ?? '*'}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    const args = scope ? { scope } : undefined;
    const scopeLabel = scope?.type || 'default';
    await withTimeout(
      `deleteMyCommands(${scopeLabel})`,
      bot.telegram.deleteMyCommands(args).catch(() => undefined),
      TELEGRAM_API_TIMEOUT_MS
    );
    await withTimeout(
      `setMyCommands(${scopeLabel})`,
      bot.telegram.setMyCommands(TELEGRAM_COMMANDS, args),
      TELEGRAM_API_TIMEOUT_MS
    );
  }
}

async function readJsonFile(filePath, fallback = null) {
  try {
    const raw = await fsPromises.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function writeJsonFileAtomic(filePath, value) {
  await fsPromises.mkdir(path.dirname(filePath), { recursive: true });
  const tmpPath = `${filePath}.tmp`;
  await fsPromises.writeFile(tmpPath, JSON.stringify(value, null, 2), 'utf8');
  await fsPromises.rename(tmpPath, filePath);
}

async function removeFileIfExists(filePath) {
  await fsPromises.unlink(filePath).catch(() => undefined);
}

async function main() {
  ensureSafeConfigWarnings();

  const store = new MemoryStore(settings.dbPath);
  const memory = new MemoryEngine({
    store,
    config: settings.memory
  });
  memory.init();

  const codex = new CodexRpcClient(settings.codex);
  const autostart = createAutostartManager({
    appCwd: settings.codex.cwd
  });

  codex.on('server-log', (line) => {
    const txt = line.trim();
    if (txt) console.log(`[codex] ${txt}`);
  });

  codex.on('server-exit', ({ code, signal }) => {
    console.error(`[codex] app-server exited (code=${code}, signal=${signal})`);
  });

  codex.on('connection-lost', (err) => {
    console.error(`[codex] connection lost: ${err?.message || err}`);
  });

  codex.on('reconnect-failed', (err) => {
    console.error(`[codex] reconnect attempt failed: ${err?.message || err}`);
  });

  codex.on('reconnected', () => {
    console.log('[codex] self-healing reconnect succeeded.');
  });

  await codex.start();
  const restartNoticePath = path.join(settings.memory.root, 'system', 'restart-notify.json');
  let heartbeatTimer = null;
  let dashboard = null;

  let shuttingDown = false;
  let shutdownPromise = null;

  const shutdown = async (reason = 'SIGTERM') => {
    if (shutdownPromise) {
      return shutdownPromise;
    }
    shuttingDown = true;
    shutdownPromise = (async () => {
      try {
        console.log(`Shutting down (${reason})...`);
        if (heartbeatTimer) {
          clearInterval(heartbeatTimer);
          heartbeatTimer = null;
        }
        if (dashboard) {
          await dashboard.stop().catch((err) => {
            console.error('[dashboard] stop failed:', err?.message || err);
          });
          dashboard = null;
        }
        if (typeof bot.stopScheduleLoop === 'function') {
          bot.stopScheduleLoop();
        }
        bot.stop('SIGTERM');
        await codex.shutdown();
        store.close();
        process.exit(0);
      } catch (err) {
        console.error('Shutdown failed:', err);
        process.exit(1);
      }
    })();
    return shutdownPromise;
  };

  const restartService = async ({ chatId = null, userId = null } = {}) => {
    if (shuttingDown) {
      return;
    }
    if (Number.isInteger(chatId)) {
      await writeJsonFileAtomic(restartNoticePath, {
        chatId,
        userId: Number.isInteger(userId) ? userId : null,
        requestedAtMs: Date.now()
      }).catch((err) => {
        console.error('[restart] failed to persist restart notification target:', err?.message || err);
      });
    }
    const child = spawn(process.execPath, process.argv.slice(1), {
      cwd: process.cwd(),
      env: process.env,
      detached: true,
      stdio: 'ignore'
    });
    if (!child.pid) {
      throw new Error('Failed to spawn replacement process.');
    }
    child.unref();
    console.log(`[restart] spawned replacement pid=${child.pid}`);
    await shutdown('restart');
  };

  const bot = createTelegramBot({
    appSettings: settings.app,
    token: settings.telegramToken,
    allowedUserIds: settings.allowedTelegramUserIds,
    allowedChatIds: settings.allowedTelegramChatIds,
    store,
    memory,
    codex,
    telegramSettings: settings.telegram,
    scheduleSettings: settings.schedule,
    skillSettings: settings.skills,
    whisperSettings: settings.whisper,
    codexCwd: settings.codex.cwd,
    restartService,
    autostart
  });

  if (settings.dashboard?.enabled) {
    dashboard = createDashboardServer({
      appSettings: settings.app,
      codexSettings: settings.codex,
      dashboardSettings: settings.dashboard,
      scheduleSettings: settings.schedule,
      skillSettings: settings.skills,
      whisperSettings: settings.whisper,
      codex,
      memory,
      store,
      restartService,
      autostart
    });
    await dashboard.start();
  }

  const launchPromise = bot.launch();
  let launchResolvedInTime = false;
  try {
    await withTimeout('bot.launch', launchPromise, TELEGRAM_BOOT_TIMEOUT_MS);
    launchResolvedInTime = true;
  } catch (err) {
    console.warn(
      `[telegram] launch did not resolve within ${TELEGRAM_BOOT_TIMEOUT_MS}ms; continuing because polling may already be active.`
    );
    console.warn(`[telegram] launch timeout detail: ${err?.message || err}`);
  }
  if (!launchResolvedInTime) {
    launchPromise
      .then(() => {
        console.log('[telegram] launch eventually resolved.');
      })
      .catch((err) => {
        console.error('[telegram] launch eventually failed:', err?.message || err);
      });
  }

  await syncTelegramCommands(bot, {
    allowedUserIds: settings.allowedTelegramUserIds,
    allowedChatIds: settings.allowedTelegramChatIds
  }).catch((err) => {
    console.error('[telegram] command sync failed:', err?.message || err);
  });

  if (typeof bot.startScheduleLoop === 'function') {
    bot.startScheduleLoop();
  }

  const pendingNotice = await readJsonFile(restartNoticePath, null);
  if (pendingNotice && Number.isInteger(pendingNotice.chatId)) {
    const ageMs = Date.now() - Number(pendingNotice.requestedAtMs || 0);
    if (ageMs >= 0 && ageMs <= RESTART_NOTICE_MAX_AGE_MS) {
      try {
        await withTimeout(
          'send restart complete notice',
          bot.telegram.sendMessage(
            pendingNotice.chatId,
            'Restart complete. I am back online.'
          ),
          TELEGRAM_API_TIMEOUT_MS
        );
        await removeFileIfExists(restartNoticePath);
      } catch (err) {
        console.error('[restart] failed to send back-online notice:', err?.message || err);
      }
    } else {
      await removeFileIfExists(restartNoticePath);
    }
  }

  console.log('Telegram bridge running.');

  const heartbeatMs = settings.memory.heartbeatHours * 60 * 60 * 1000;
  heartbeatTimer = setInterval(() => {
    try {
      const res = memory.runHeartbeat();
      console.log(
        `[heartbeat] weekly_summaries=${res.weeklyUpdated} contradictions=${res.contradictionCount} chats=${res.chatCount}`
      );
    } catch (err) {
      console.error('[heartbeat] failed', err);
    }
  }, heartbeatMs);
  heartbeatTimer.unref();
  process.once('SIGINT', () => {
    void shutdown('SIGINT');
  });
  process.once('SIGTERM', () => {
    void shutdown('SIGTERM');
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
