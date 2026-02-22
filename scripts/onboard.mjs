#!/usr/bin/env node
import fs from 'node:fs/promises';
import { constants as fsConstants } from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import readline from 'node:readline/promises';
import { spawnSync } from 'node:child_process';
import { upsertEnvText } from '../src/env-file.js';

async function exists(filePath) {
  try {
    await fs.access(filePath, fsConstants.F_OK);
    return true;
  } catch {
    return false;
  }
}

function runCapture(cmd, args = []) {
  const out = spawnSync(cmd, args, { encoding: 'utf8' });
  const combined = `${String(out.stdout || '')}\n${String(out.stderr || '')}`.trim();
  return {
    ok: out.status === 0,
    status: out.status,
    stdout: String(out.stdout || '').trim(),
    stderr: String(out.stderr || '').trim(),
    text: combined
  };
}

function runInherit(cmd, args = []) {
  const out = spawnSync(cmd, args, { stdio: 'inherit' });
  return out.status === 0;
}

function parseCurrentValue(envText, key, fallback = '') {
  const m = String(envText || '').match(new RegExp(`^\\s*${key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*=\\s*(.*)$`, 'm'));
  if (!m) return fallback;
  const v = String(m[1] || '').trim();
  if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
    return v.slice(1, -1);
  }
  return v;
}

function parseBoolish(input, fallback = true) {
  const raw = String(input || '').trim().toLowerCase();
  if (!raw) return fallback;
  if (['y', 'yes', 'true', '1', 'on'].includes(raw)) return true;
  if (['n', 'no', 'false', '0', 'off'].includes(raw)) return false;
  return fallback;
}

function parseCsvCount(text) {
  return String(text || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean).length;
}

function maskToken(value) {
  const raw = String(value || '').trim();
  if (!raw) return '(missing)';
  if (raw.length <= 10) return `${raw.slice(0, 2)}***`;
  return `${raw.slice(0, 6)}...${raw.slice(-4)}`;
}

function boolLabel(value, fallback = false) {
  return parseBoolish(value, fallback) ? 'true' : 'false';
}

function inferAccessProfile(sandbox, approval) {
  const s = String(sandbox || '').trim();
  const a = String(approval || '').trim();
  if (s === 'danger-full-access' && a === 'never') {
    return 'full';
  }
  if (s === 'workspace-write' && a === 'on-request') {
    return 'partial';
  }
  if (s === 'read-only' && (a === 'on-request' || a === 'untrusted')) {
    return 'readonly';
  }
  return 'custom';
}

function accessPreset(profile) {
  if (profile === 'full') {
    return {
      sandbox: 'danger-full-access',
      approval: 'never'
    };
  }
  if (profile === 'partial') {
    return {
      sandbox: 'workspace-write',
      approval: 'on-request'
    };
  }
  if (profile === 'readonly') {
    return {
      sandbox: 'read-only',
      approval: 'on-request'
    };
  }
  return null;
}

function accessProfileLabel(profile) {
  if (profile === 'full') return 'full';
  if (profile === 'partial') return 'partial';
  if (profile === 'readonly') return 'read-only';
  return 'custom';
}

function configSnapshot(envText, cwd) {
  const sandbox = parseCurrentValue(envText, 'CODEX_SANDBOX', 'danger-full-access');
  const approval = parseCurrentValue(envText, 'CODEX_APPROVAL_POLICY', 'never');
  const explicitProfile = parseCurrentValue(envText, 'CODEX_ACCESS_PROFILE', '').toLowerCase();
  const inferred = inferAccessProfile(sandbox, approval);
  return {
    telegramToken: parseCurrentValue(envText, 'TELEGRAM_BOT_TOKEN', ''),
    allowedUsers: parseCurrentValue(envText, 'ALLOWED_TELEGRAM_USER_IDS', ''),
    allowedChats: parseCurrentValue(envText, 'ALLOWED_TELEGRAM_CHAT_IDS', ''),
    privateOnly: parseCurrentValue(envText, 'TELEGRAM_PRIVATE_ONLY', 'true'),
    requireMention: parseCurrentValue(envText, 'TELEGRAM_GROUP_REQUIRE_MENTION', 'true'),
    codexCwd: parseCurrentValue(envText, 'CODEX_CWD', cwd),
    sandbox,
    approval,
    accessProfile: explicitProfile || inferred,
    botName: parseCurrentValue(envText, 'BOT_APP_NAME', 'Dexbot')
  };
}

function isConfigComplete(snapshot) {
  if (!snapshot.telegramToken || !snapshot.allowedUsers) {
    return false;
  }
  const privateOnly = parseBoolish(snapshot.privateOnly, true);
  if (!privateOnly && !snapshot.allowedChats.trim()) {
    return false;
  }
  return true;
}

function printConfigSummary(snapshot) {
  const privateOnly = parseBoolish(snapshot.privateOnly, true);
  const userCount = parseCsvCount(snapshot.allowedUsers);
  const chatCount = parseCsvCount(snapshot.allowedChats);
  console.log('\nCurrent .env configuration:');
  console.log(`- BOT_APP_NAME: ${snapshot.botName || '(missing)'}`);
  console.log(`- TELEGRAM_BOT_TOKEN: ${maskToken(snapshot.telegramToken)}`);
  console.log(`- ALLOWED_TELEGRAM_USER_IDS: ${userCount > 0 ? `${userCount} configured` : '(missing)'}`);
  if (privateOnly) {
    console.log(`- TELEGRAM_PRIVATE_ONLY: true`);
    console.log('- ALLOWED_TELEGRAM_CHAT_IDS: not required in private-only mode');
  } else {
    console.log('- TELEGRAM_PRIVATE_ONLY: false');
    console.log(`- ALLOWED_TELEGRAM_CHAT_IDS: ${chatCount > 0 ? `${chatCount} configured` : '(missing for group mode)'}`);
  }
  console.log(`- TELEGRAM_GROUP_REQUIRE_MENTION: ${boolLabel(snapshot.requireMention, true)}`);
  console.log(`- CODEX_CWD: ${snapshot.codexCwd || '(missing)'}`);
  console.log(`- Access profile: ${accessProfileLabel(snapshot.accessProfile)}`);
  console.log(`- CODEX_SANDBOX: ${snapshot.sandbox || '(missing)'}`);
  console.log(`- CODEX_APPROVAL_POLICY: ${snapshot.approval || '(missing)'}`);
}

async function main() {
  const cwd = process.cwd();
  const envPath = path.join(cwd, '.env');
  const envExamplePath = path.join(cwd, '.env.example');

  console.log('Dexbot onboarding');
  console.log(`workspace: ${cwd}`);

  let envText = '';
  if (await exists(envPath)) {
    envText = await fs.readFile(envPath, 'utf8');
  } else if (await exists(envExamplePath)) {
    envText = await fs.readFile(envExamplePath, 'utf8');
    await fs.writeFile(envPath, envText, 'utf8');
    console.log('Created .env from .env.example');
  } else {
    await fs.writeFile(envPath, '', 'utf8');
    console.log('Created empty .env');
  }

  const codexVersion = runCapture('codex', ['--version']);
  if (!codexVersion.ok) {
    console.error('Codex CLI not found. Install Codex first, then run onboarding again.');
    process.exit(1);
  }
  console.log(`codex: ${codexVersion.stdout || '(version unknown)'}`);

  let loggedIn = false;
  const status = runCapture('codex', ['login', 'status']);
  if (status.ok && /logged in/i.test(status.text)) {
    loggedIn = true;
    console.log(`auth: already authenticated (${status.text})`);
  } else {
    console.log('auth: not authenticated yet');
  }

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  try {
    if (!loggedIn) {
      const doLogin = await rl.question('Open Codex login now? [y/N]: ');
      if (parseBoolish(doLogin, false)) {
        const ok = runInherit('codex', ['login']);
        if (!ok) {
          console.error('Codex login failed. You can rerun: codex login');
        }
        const post = runCapture('codex', ['login', 'status']);
        if (post.ok && /logged in/i.test(post.text)) {
          loggedIn = true;
          console.log(`auth: already authenticated (${post.text})`);
        }
      }
    }

    const existing = configSnapshot(envText, cwd);
    const hasExistingConfig = Boolean(
      existing.telegramToken.trim()
      || existing.allowedUsers.trim()
      || existing.allowedChats.trim()
    );

    let mode = 'update';
    if (hasExistingConfig) {
      printConfigSummary(existing);
      if (isConfigComplete(existing)) {
        console.log('\nStatus: already configured.');
      } else {
        console.log('\nStatus: partial configuration detected.');
      }

      const modeRaw = await rl.question(
        'Choose onboarding mode: [K]eep current, [U]pdate existing, [R]eset from scratch [U]: '
      );
      const first = String(modeRaw || '').trim().toLowerCase()[0] || 'u';
      if (first === 'k') mode = 'keep';
      else if (first === 'r') mode = 'reset';
      else mode = 'update';
    }

    if (mode === 'keep') {
      await fs.chmod(envPath, 0o600).catch(() => undefined);
      console.log('\nOnboarding complete (kept existing configuration).');
      if (!loggedIn) {
        console.log('Note: Codex login is still missing. Run: codex login');
      }
      console.log('Next steps:');
      console.log('1) npm install');
      console.log('2) npm run start');
      console.log('3) In Telegram, send /start to your bot');
      return;
    }

    const defaults = mode === 'reset'
      ? {
        telegramToken: '',
        allowedUsers: '',
        allowedChats: '',
        privateOnly: 'true',
        requireMention: 'true',
        codexCwd: cwd,
        sandbox: 'danger-full-access',
        approval: 'never',
        accessProfile: 'full',
        botName: 'Dexbot'
      }
      : existing;

    if (mode === 'reset') {
      console.log('\nReset mode: starting fresh prompts for all required fields.');
    }

    const token = (await rl.question(`Telegram bot token [${defaults.telegramToken || 'required'}]: `)).trim()
      || defaults.telegramToken;
    if (!token) {
      console.error('TELEGRAM_BOT_TOKEN is required.');
      process.exit(1);
    }

    const users = (await rl.question(`Allowed Telegram user IDs (comma-separated) [${defaults.allowedUsers || 'required'}]: `)).trim()
      || defaults.allowedUsers;
    if (!users) {
      console.error('ALLOWED_TELEGRAM_USER_IDS is required for secure setup.');
      process.exit(1);
    }

    const privateOnlyRaw = await rl.question(`Private only mode? (true/false) [${defaults.privateOnly}]: `);
    const privateOnly = parseBoolish(privateOnlyRaw, parseBoolish(defaults.privateOnly, true));

    let allowedChats = defaults.allowedChats;
    if (!privateOnly) {
      allowedChats = (await rl.question(`Allowed Telegram chat IDs (comma-separated, include group IDs) [${defaults.allowedChats || 'optional'}]: `)).trim()
        || defaults.allowedChats;
    }

    const mentionRaw = await rl.question(`Require @mention in groups? (true/false) [${defaults.requireMention}]: `);
    const requireMention = parseBoolish(mentionRaw, parseBoolish(defaults.requireMention, true));

    const codexCwd = (await rl.question(`CODEX_CWD [${defaults.codexCwd}]: `)).trim() || defaults.codexCwd;

    const defaultProfileLetter = (
      defaults.accessProfile === 'full' ? 'f'
        : defaults.accessProfile === 'partial' ? 'p'
          : defaults.accessProfile === 'readonly' ? 'r'
            : 'c'
    );
    console.log('\nSecurity warning:');
    console.log('- Full access allows Codex to execute machine-level commands with minimal restrictions.');
    console.log('- Use full access only on an isolated machine you control.');
    console.log('- You accept all operational and security risk when enabling full access.');
    const profileRaw = await rl.question(
      `Access level: [F]ull (recommended only on isolated machine), [P]artial, [R]ead-only, [C]ustom [${defaultProfileLetter.toUpperCase()}]: `
    );
    const profileLetter = String(profileRaw || '').trim().toLowerCase()[0] || defaultProfileLetter;
    let selectedProfile = 'custom';
    if (profileLetter === 'f') selectedProfile = 'full';
    else if (profileLetter === 'p') selectedProfile = 'partial';
    else if (profileLetter === 'r') selectedProfile = 'readonly';

    let sandbox = defaults.sandbox;
    let approval = defaults.approval;
    const preset = accessPreset(selectedProfile);
    if (preset) {
      sandbox = preset.sandbox;
      approval = preset.approval;
      if (selectedProfile === 'full') {
        const ack = (await rl.question('Type FULL to confirm high-risk full access [cancel]: ')).trim();
        if (ack !== 'FULL') {
          console.log('Full access not confirmed. Falling back to partial profile.');
          const fallback = accessPreset('partial');
          sandbox = fallback.sandbox;
          approval = fallback.approval;
          selectedProfile = 'partial';
        }
      }
    } else {
      sandbox = (await rl.question(`CODEX_SANDBOX (read-only/workspace-write/danger-full-access) [${defaults.sandbox}]: `)).trim() || defaults.sandbox;
      approval = (await rl.question(`CODEX_APPROVAL_POLICY (untrusted/on-request/never) [${defaults.approval}]: `)).trim() || defaults.approval;
    }
    const botName = (await rl.question(`BOT_APP_NAME [${defaults.botName || 'Dexbot'}]: `)).trim() || defaults.botName || 'Dexbot';

    envText = await fs.readFile(envPath, 'utf8').catch(() => '');
    const next = upsertEnvText(envText, {
      TELEGRAM_BOT_TOKEN: token,
      ALLOWED_TELEGRAM_USER_IDS: users,
      ALLOWED_TELEGRAM_CHAT_IDS: allowedChats,
      TELEGRAM_PRIVATE_ONLY: String(privateOnly),
      TELEGRAM_GROUP_REQUIRE_MENTION: String(requireMention),
      CODEX_CWD: codexCwd,
      CODEX_SANDBOX: sandbox,
      CODEX_APPROVAL_POLICY: approval,
      CODEX_ACCESS_PROFILE: selectedProfile,
      AUTO_SPAWN_APP_SERVER: 'true',
      SKILLS_ROOT: './.agents/skills',
      SKILLS_INCLUDE_CODEX_HOME: 'true',
      BOT_APP_NAME: botName
    });

    await fs.writeFile(envPath, next, 'utf8');
    await fs.chmod(envPath, 0o600).catch(() => undefined);

    console.log('\nOnboarding complete.');
    if (!loggedIn) {
      console.log('Note: Codex login is still missing. Run: codex login');
    }
    console.log('Next steps:');
    console.log('1) npm install');
    console.log('2) npm run start');
    console.log('3) In Telegram, send /start to your bot');
  } finally {
    rl.close();
  }
}

main().catch((err) => {
  console.error('Onboarding failed:', err?.message || err);
  process.exit(1);
});
