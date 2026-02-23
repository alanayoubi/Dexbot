import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import readline from 'node:readline';
import { fileURLToPath } from 'node:url';
import { spawn, spawnSync } from 'node:child_process';

const WORKER_SCRIPT_PATH = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  'whisper-worker.py'
);

let persistentClient = null;

function splitShellWords(value) {
  const text = String(value || '').trim();
  if (!text) return [];
  const parts = text.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) || [];
  return parts.map((token) => token.replace(/^["']|["']$/g, ''));
}

function normalizeTranscript(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeLanguageHint(languageHint) {
  const raw = String(languageHint || '').trim().toLowerCase();
  if (!raw) return null;
  const candidate = raw.split(/[-_]/)[0];
  if (!candidate || !/^[a-z]{2,3}$/.test(candidate)) {
    return null;
  }
  return candidate;
}

async function downloadTelegramFile(ctx, fileId, destinationPath) {
  const url = await ctx.telegram.getFileLink(fileId);
  const response = await fetch(String(url));
  if (!response.ok) {
    throw new Error(`Failed to download Telegram file: HTTP ${response.status}`);
  }
  const data = Buffer.from(await response.arrayBuffer());
  await fsPromises.writeFile(destinationPath, data);
}

function runCommand(executable, args, timeoutMs) {
  return new Promise((resolve, reject) => {
    let stderr = '';
    let timedOut = false;

    const child = spawn(executable, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env
    });

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
    }, Math.max(1000, timeoutMs || 240000));

    child.stderr.on('data', (buf) => {
      stderr += buf.toString();
    });
    child.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
    child.on('exit', (code) => {
      clearTimeout(timer);
      if (timedOut) {
        reject(new Error('Whisper transcription timed out.'));
        return;
      }
      if (code !== 0) {
        const tail = stderr.trim().split('\n').slice(-4).join('\n');
        reject(new Error(`Whisper failed (exit ${code}). ${tail || 'No error output.'}`));
        return;
      }
      resolve();
    });
  });
}

function resolveExecutablePath(executable) {
  if (!executable) return null;

  if (path.isAbsolute(executable) || executable.includes('/')) {
    return executable;
  }

  const found = spawnSync('which', [executable], { encoding: 'utf8' });
  if (found.status === 0) {
    const out = String(found.stdout || '').trim();
    return out || null;
  }
  return null;
}

function extractShebangInterpreterWords(scriptPath) {
  try {
    const firstLine = fs.readFileSync(scriptPath, 'utf8').split('\n')[0].trim();
    if (!firstLine.startsWith('#!')) {
      return null;
    }
    const shebang = firstLine.slice(2).trim();
    const words = splitShellWords(shebang);
    return words.length ? words : null;
  } catch {
    return null;
  }
}

function resolvePythonCommandWords(whisper) {
  const explicit = splitShellWords(whisper.pythonCommand || '');
  if (explicit.length) {
    return explicit;
  }

  const commandWords = splitShellWords(whisper.command || 'whisper');
  if (!commandWords.length) {
    return null;
  }

  const executable = commandWords[0];
  const resolved = resolveExecutablePath(executable);
  if (!resolved) {
    return null;
  }
  return extractShebangInterpreterWords(resolved);
}

class PersistentWhisperClient {
  constructor(whisper) {
    this.whisper = whisper;
    this.proc = null;
    this.rl = null;
    this.nextId = 1;
    this.pending = new Map();
    this.readyPromise = null;
    this.readyResolve = null;
    this.readyReject = null;
    this.stderrTail = [];
    this.signature = JSON.stringify({
      command: whisper.command,
      pythonCommand: whisper.pythonCommand,
      model: whisper.model,
      task: whisper.task,
      language: whisper.language
    });
  }

  hasSameSignature(whisper) {
    const nextSig = JSON.stringify({
      command: whisper.command,
      pythonCommand: whisper.pythonCommand,
      model: whisper.model,
      task: whisper.task,
      language: whisper.language
    });
    return nextSig === this.signature;
  }

  captureStderr(line) {
    if (!line) return;
    this.stderrTail.push(line);
    if (this.stderrTail.length > 20) {
      this.stderrTail.shift();
    }
  }

  tailStderr() {
    return this.stderrTail.slice(-4).join('\n');
  }

  async ensureReady() {
    if (this.readyPromise) {
      return this.readyPromise;
    }
    this.readyPromise = new Promise((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
    });

    const pythonWords = resolvePythonCommandWords(this.whisper);
    if (!pythonWords?.length) {
      const err = new Error('Could not detect Whisper Python interpreter from WHISPER_COMMAND. Set WHISPER_PYTHON_COMMAND.');
      this.readyReject?.(err);
      this.readyPromise = null;
      throw err;
    }

    const [pythonExec, ...pythonArgs] = pythonWords;
    const args = [
      ...pythonArgs,
      WORKER_SCRIPT_PATH,
      '--model', this.whisper.model || 'base',
      '--task', this.whisper.task || 'transcribe'
    ];
    if (this.whisper.language) {
      args.push('--language', this.whisper.language);
    }

    this.proc = spawn(pythonExec, args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: process.env
    });

    this.proc.stderr.on('data', (buf) => {
      for (const line of String(buf).split('\n')) {
        this.captureStderr(line.trim());
      }
    });

    this.proc.on('error', (err) => {
      this.failAllPending(new Error(`Whisper worker failed to start: ${err.message}`));
    });

    this.proc.on('exit', (code, signal) => {
      const message = `Whisper worker exited (code=${code}, signal=${signal}). ${this.tailStderr()}`.trim();
      this.failAllPending(new Error(message));
      this.cleanupProcessRefs();
    });

    this.rl = readline.createInterface({ input: this.proc.stdout });
    this.rl.on('line', (line) => {
      let msg = null;
      try {
        msg = JSON.parse(line);
      } catch {
        this.captureStderr(`worker-non-json: ${line}`);
        return;
      }

      if (msg?.type === 'ready') {
        this.readyResolve?.();
        return;
      }
      if (msg?.type === 'fatal') {
        const err = new Error(msg.error || 'Whisper worker fatal error');
        this.readyReject?.(err);
        this.failAllPending(err);
        this.proc?.kill('SIGKILL');
        return;
      }

      const id = msg?.id;
      if (id == null) {
        return;
      }
      const pending = this.pending.get(id);
      if (!pending) {
        return;
      }
      this.pending.delete(id);
      clearTimeout(pending.timer);
      if (msg.error) {
        pending.reject(new Error(msg.error));
        return;
      }
      pending.resolve(normalizeTranscript(msg.text));
    });

    return this.readyPromise;
  }

  cleanupProcessRefs() {
    if (this.rl) {
      this.rl.close();
      this.rl = null;
    }
    this.proc = null;
    this.readyPromise = null;
    this.readyResolve = null;
    this.readyReject = null;
  }

  failAllPending(err) {
    if (this.readyReject) {
      this.readyReject(err);
    }
    for (const [, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(err);
    }
    this.pending.clear();
  }

  async transcribe(audioPath, { language = null, task = null, timeoutMs = 240000 } = {}) {
    await this.ensureReady();
    if (!this.proc || !this.proc.stdin.writable) {
      throw new Error('Whisper worker is not ready.');
    }

    const id = this.nextId++;
    const payload = {
      id,
      audio_path: audioPath,
      task: task || this.whisper.task || 'transcribe'
    };
    if (language) {
      payload.language = language;
    }

    const result = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error('Whisper worker timed out.'));
        this.proc?.kill('SIGKILL');
      }, Math.max(1000, timeoutMs || 240000));

      this.pending.set(id, { resolve, reject, timer });
    });

    this.proc.stdin.write(`${JSON.stringify(payload)}\n`);
    return result;
  }
}

function getPersistentClient(whisper) {
  if (!persistentClient || !persistentClient.hasSameSignature(whisper)) {
    persistentClient = new PersistentWhisperClient(whisper);
  }
  return persistentClient;
}

async function transcribeWithCli({
  inputPath,
  outputDir,
  whisper,
  language
}) {
  const commandWords = splitShellWords(whisper.command || 'whisper');
  if (!commandWords.length) {
    throw new Error('Whisper command is empty.');
  }

  const [executable, ...prefixedArgs] = commandWords;
  const extraArgs = splitShellWords(whisper.extraArgs || '');
  const args = [
    ...prefixedArgs,
    inputPath,
    '--model', whisper.model || 'base',
    '--task', whisper.task || 'transcribe',
    '--output_dir', outputDir,
    '--output_format', 'txt',
    '--verbose', 'False',
    '--temperature', '0',
    '--best_of', '1',
    '--beam_size', '1',
    '--condition_on_previous_text', 'False',
    ...extraArgs
  ];

  if (language) {
    args.push('--language', language);
  }

  await runCommand(executable, args, whisper.timeoutMs);

  const transcriptPath = path.join(outputDir, 'input.txt');
  const raw = await fsPromises.readFile(transcriptPath, 'utf8');
  const text = normalizeTranscript(raw);
  if (!text) {
    throw new Error('Whisper returned an empty transcript.');
  }
  return text;
}

export async function transcribeTelegramFile({
  ctx,
  fileId,
  inputExtension = 'ogg',
  languageHint = null,
  whisper
}) {
  if (!whisper?.enabled) {
    throw new Error('Voice transcription is disabled (WHISPER_ENABLED=false).');
  }

  const tempRoot = await fsPromises.mkdtemp(path.join(os.tmpdir(), 'dexbot-whisper-'));
  const safeExt = String(inputExtension || 'ogg').replace(/[^a-z0-9]/gi, '') || 'ogg';
  const inputPath = path.join(tempRoot, `input.${safeExt}`);
  const outputDir = path.join(tempRoot, 'out');
  await fsPromises.mkdir(outputDir, { recursive: true });

  const normalizedHint = normalizeLanguageHint(languageHint);
  const language = whisper.language || normalizedHint || null;

  try {
    await downloadTelegramFile(ctx, fileId, inputPath);

    if (whisper.persistent) {
      try {
        const worker = getPersistentClient(whisper);
        const text = await worker.transcribe(inputPath, {
          language,
          task: whisper.task,
          timeoutMs: whisper.timeoutMs
        });
        if (text) return text;
        throw new Error('Whisper worker returned an empty transcript.');
      } catch (err) {
        console.warn(`[whisper] persistent mode failed, falling back to CLI: ${err.message}`);
      }
    }

    return await transcribeWithCli({
      inputPath,
      outputDir,
      whisper,
      language
    });
  } finally {
    await fsPromises.rm(tempRoot, { recursive: true, force: true }).catch(() => undefined);
  }
}

export async function transcribeLocalAudioFile({
  localPath,
  inputExtension = '',
  languageHint = null,
  whisper
}) {
  if (!whisper?.enabled) {
    throw new Error('Voice transcription is disabled (WHISPER_ENABLED=false).');
  }

  const tempRoot = await fsPromises.mkdtemp(path.join(os.tmpdir(), 'dexbot-whisper-local-'));
  const extCandidate = inputExtension || path.extname(String(localPath || '')).replace('.', '') || 'ogg';
  const safeExt = String(extCandidate).replace(/[^a-z0-9]/gi, '') || 'ogg';
  const inputPath = path.join(tempRoot, `input.${safeExt}`);
  const outputDir = path.join(tempRoot, 'out');
  await fsPromises.mkdir(outputDir, { recursive: true });

  const normalizedHint = normalizeLanguageHint(languageHint);
  const language = whisper.language || normalizedHint || null;

  try {
    await fsPromises.copyFile(localPath, inputPath);

    if (whisper.persistent) {
      try {
        const worker = getPersistentClient(whisper);
        const text = await worker.transcribe(inputPath, {
          language,
          task: whisper.task,
          timeoutMs: whisper.timeoutMs
        });
        if (text) return text;
        throw new Error('Whisper worker returned an empty transcript.');
      } catch (err) {
        console.warn(`[whisper] persistent mode failed, falling back to CLI: ${err.message}`);
      }
    }

    return await transcribeWithCli({
      inputPath,
      outputDir,
      whisper,
      language
    });
  } finally {
    await fsPromises.rm(tempRoot, { recursive: true, force: true }).catch(() => undefined);
  }
}
