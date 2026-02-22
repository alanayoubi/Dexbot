import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';
import { execFile } from 'node:child_process';

function xmlEscape(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

function execFileAsync(file, args) {
  return new Promise((resolve, reject) => {
    execFile(file, args, { encoding: 'utf8' }, (err, stdout, stderr) => {
      if (err) {
        const wrapped = new Error(stderr?.trim() || err.message || 'command failed');
        wrapped.code = err.code;
        wrapped.stdout = stdout || '';
        wrapped.stderr = stderr || '';
        reject(wrapped);
        return;
      }
      resolve({
        stdout: stdout || '',
        stderr: stderr || ''
      });
    });
  });
}

export class AutostartManager {
  constructor({ appCwd, label = 'com.dexbot.telegram' }) {
    this.appCwd = appCwd;
    this.label = label;
    this.launchAgentsDir = path.join(os.homedir(), 'Library', 'LaunchAgents');
    this.plistPath = path.join(this.launchAgentsDir, `${this.label}.plist`);
    this.logDir = path.join(this.appCwd, 'memory', 'system');
    const uid = typeof process.getuid === 'function'
      ? process.getuid()
      : Number(process.env.UID || 0);
    this.domain = `gui/${uid}`;
    this.serviceTarget = `${this.domain}/${this.label}`;
  }

  async ensurePlist() {
    await fs.mkdir(this.launchAgentsDir, { recursive: true });
    await fs.mkdir(this.logDir, { recursive: true });

    const outLog = path.join(this.logDir, 'launchd.out.log');
    const errLog = path.join(this.logDir, 'launchd.err.log');
    const entrypoint = path.join(this.appCwd, 'src', 'index.js');

    const plist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${xmlEscape(this.label)}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${xmlEscape(process.execPath)}</string>
    <string>${xmlEscape(entrypoint)}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${xmlEscape(this.appCwd)}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${xmlEscape(outLog)}</string>
  <key>StandardErrorPath</key>
  <string>${xmlEscape(errLog)}</string>
</dict>
</plist>
`;
    await fs.writeFile(this.plistPath, plist, 'utf8');
    return this.plistPath;
  }

  async bootoutIfLoaded() {
    try {
      await execFileAsync('/bin/launchctl', ['bootout', this.serviceTarget]);
      return;
    } catch {
      // try with explicit plist target, then ignore failures
    }
    try {
      await execFileAsync('/bin/launchctl', ['bootout', this.domain, this.plistPath]);
    } catch {
      // no-op
    }
  }

  async enable() {
    await this.ensurePlist();
    await this.bootoutIfLoaded();
    await execFileAsync('/bin/launchctl', ['bootstrap', this.domain, this.plistPath]);
    await execFileAsync('/bin/launchctl', ['kickstart', '-k', this.serviceTarget]);
    return this.status();
  }

  async disable() {
    await this.bootoutIfLoaded();
    return this.status();
  }

  async status() {
    let loaded = false;
    let pid = null;
    let rawStatus = '';
    let configured = false;

    try {
      await fs.access(this.plistPath);
      configured = true;
    } catch {
      configured = false;
    }

    try {
      const res = await execFileAsync('/bin/launchctl', ['print', this.serviceTarget]);
      rawStatus = res.stdout || '';
      loaded = true;
      const m = rawStatus.match(/\bpid\s*=\s*(\d+)/);
      if (m) pid = Number(m[1]);
    } catch (err) {
      rawStatus = err?.stdout || '';
      loaded = false;
    }

    return {
      label: this.label,
      plistPath: this.plistPath,
      serviceTarget: this.serviceTarget,
      configured,
      loaded,
      pid
    };
  }
}

export function createAutostartManager(opts) {
  return new AutostartManager(opts);
}
