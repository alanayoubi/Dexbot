import { EventEmitter } from 'node:events';
import { spawn } from 'node:child_process';
import process from 'node:process';
import WebSocket from 'ws';

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class CodexRpcClient extends EventEmitter {
  constructor(settings) {
    super();
    this.settings = settings;
    this.ws = null;
    this.nextId = 1;
    this.pending = new Map();
    this.initialized = false;
    this.serverProc = null;

    this.stopping = false;
    this.connectingPromise = null;
    this.reconnectTimer = null;
    this.reconnectInProgress = false;

    this.requestTimeoutMs = 60_000;
    this.turnTimeoutMs = 15 * 60_000;
  }

  async start() {
    this.stopping = false;
    await this.ensureConnected({ retries: 6, spawnOnFailure: true });
  }

  spawnServer() {
    if (this.stopping || this.serverProc) {
      return;
    }

    const args = ['app-server', '--listen', this.settings.appServerListenUrl];
    if (this.settings.enableMultiAgent) {
      args.push('--enable', 'multi_agent');
    }
    if (this.settings.enableChildAgentsMd) {
      args.push('--enable', 'child_agents_md');
    }
    this.serverProc = spawn('codex', args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env
    });

    this.serverProc.stdout.on('data', (buf) => {
      this.emit('server-log', buf.toString());
    });

    this.serverProc.stderr.on('data', (buf) => {
      this.emit('server-log', buf.toString());
    });

    this.serverProc.on('exit', (code, signal) => {
      this.emit('server-exit', { code, signal });
      this.serverProc = null;
      if (!this.stopping && this.settings.autoSpawnAppServer) {
        this.scheduleReconnect(800);
      }
    });
  }

  async connect() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }
    if (this.connectingPromise) {
      await this.connectingPromise;
      return;
    }

    this.connectingPromise = new Promise((resolve, reject) => {
      let settled = false;
      let timeoutId = null;

      const ws = new WebSocket(this.settings.appServerUrl, {
        perMessageDeflate: false
      });

      const fail = (err) => {
        if (settled) {
          return;
        }
        settled = true;
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        try {
          ws.close();
        } catch {
          // ignore close errors
        }
        reject(err);
      };

      timeoutId = setTimeout(() => {
        fail(new Error(`Timed out connecting to Codex app-server at ${this.settings.appServerUrl}`));
      }, 5_000);

      ws.on('open', () => {
        if (settled) {
          return;
        }
        settled = true;
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        this.attachSocket(ws);
        resolve();
      });

      ws.on('error', (err) => {
        fail(err);
      });

      ws.on('close', () => {
        if (!settled) {
          fail(new Error(`Socket closed while connecting to ${this.settings.appServerUrl}`));
        }
      });
    });

    try {
      await this.connectingPromise;
    } finally {
      this.connectingPromise = null;
    }
  }

  attachSocket(ws) {
    this.ws = ws;

    ws.on('message', (data) => {
      this.handleMessage(data.toString());
    });

    ws.on('error', (err) => {
      this.emit('connection-error', err);
    });

    ws.on('close', (code, reasonBuf) => {
      const reason = reasonBuf ? reasonBuf.toString() : '';
      const err = new Error(
        `Codex app-server socket closed${code ? ` (${code}${reason ? `: ${reason}` : ''})` : ''}`
      );
      this.initialized = false;
      this.ws = null;
      this.rejectPendingRequests(err);
      this.emit('connection-lost', err);
      if (!this.stopping) {
        this.scheduleReconnect(800);
      }
    });
  }

  rejectPendingRequests(err) {
    for (const [id, waiter] of this.pending.entries()) {
      this.pending.delete(id);
      if (waiter.timer) {
        clearTimeout(waiter.timer);
      }
      waiter.reject(err);
    }
  }

  async ensureConnected({ retries = 3, spawnOnFailure = true } = {}) {
    let lastError = null;

    for (let attempt = 0; attempt <= retries; attempt += 1) {
      if (this.ws && this.ws.readyState === WebSocket.OPEN && this.initialized) {
        return;
      }

      try {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
          await this.connect();
        }
        if (!this.initialized) {
          await this.initialize();
        }
        return;
      } catch (err) {
        lastError = err;
        this.initialized = false;
        if (spawnOnFailure && this.settings.autoSpawnAppServer) {
          this.spawnServer();
        }
        if (attempt < retries) {
          await delay(Math.min(400 * (2 ** attempt), 3_000));
        }
      }
    }

    throw new Error(
      `Unable to connect to Codex app-server at ${this.settings.appServerUrl}: ${lastError?.message || 'unknown error'}`
    );
  }

  scheduleReconnect(delayMs = 1000) {
    if (this.stopping || this.reconnectTimer || this.reconnectInProgress) {
      return;
    }

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      if (this.stopping || this.reconnectInProgress) {
        return;
      }

      this.reconnectInProgress = true;
      try {
        await this.ensureConnected({ retries: 5, spawnOnFailure: true });
        this.emit('reconnected');
      } catch (err) {
        this.emit('reconnect-failed', err);
        if (!this.stopping) {
          this.scheduleReconnect(Math.min(delayMs * 2, 8_000));
        }
      } finally {
        this.reconnectInProgress = false;
      }
    }, delayMs);
  }

  async initialize() {
    if (this.initialized) {
      return;
    }
    await this.sendRequestUnsafe('initialize', {
      clientInfo: {
        name: 'codex-telegram-bridge',
        title: 'Codex Telegram Bridge',
        version: '0.1.0'
      },
      capabilities: {
        experimentalApi: true
      }
    });
    this.initialized = true;
  }

  handleMessage(raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.id != null && this.pending.has(msg.id)) {
      const waiter = this.pending.get(msg.id);
      this.pending.delete(msg.id);
      if (waiter?.timer) {
        clearTimeout(waiter.timer);
      }
      if (msg.error) {
        waiter.reject(new Error(msg.error.message || 'JSON-RPC error'));
      } else {
        waiter.resolve(msg.result);
      }
      return;
    }

    if (msg.method) {
      this.emit('notification', msg);
    }
  }

  sendRequestUnsafe(method, params) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('Codex app-server socket is not connected');
    }

    const id = this.nextId++;
    const payload = JSON.stringify({ jsonrpc: '2.0', id, method, params });

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Request timeout for method: ${method}`));
      }, this.requestTimeoutMs);

      this.pending.set(id, { resolve, reject, timer });

      try {
        this.ws.send(payload, (err) => {
          if (!err) {
            return;
          }
          const waiter = this.pending.get(id);
          this.pending.delete(id);
          if (waiter?.timer) {
            clearTimeout(waiter.timer);
          }
          reject(err);
        });
      } catch (err) {
        const waiter = this.pending.get(id);
        this.pending.delete(id);
        if (waiter?.timer) {
          clearTimeout(waiter.timer);
        }
        reject(err);
      }
    });
  }

  async request(method, params) {
    let lastError = null;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        await this.ensureConnected({ retries: 4, spawnOnFailure: true });
        return await this.sendRequestUnsafe(method, params);
      } catch (err) {
        lastError = err;
        this.initialized = false;
        if (attempt === 1) {
          break;
        }
        await delay(350);
      }
    }
    throw lastError;
  }

  async ensureThread(threadId, overrides = {}) {
    const threadConfig = {};
    if (this.settings.enableMultiAgent) {
      threadConfig.features = {
        multi_agent: true
      };
    }
    if (this.settings.enableChildAgentsMd) {
      threadConfig.features = {
        ...(threadConfig.features || {}),
        child_agents_md: true
      };
    }

    const base = {
      cwd: this.settings.cwd,
      approvalPolicy: this.settings.approvalPolicy,
      sandbox: this.settings.sandbox,
      personality: this.settings.personality,
      model: this.settings.model,
      modelProvider: this.settings.modelProvider,
      developerInstructions: overrides.developerInstructions ?? null,
      baseInstructions: overrides.baseInstructions ?? null,
      config: Object.keys(threadConfig).length ? threadConfig : null
    };

    const cleanBase = Object.fromEntries(Object.entries(base).filter(([, v]) => v != null));

    if (threadId) {
      try {
        const resumed = await this.request('thread/resume', {
          ...cleanBase,
          threadId
        });
        return resumed.thread.id;
      } catch {
        // fall back to a new thread
      }
    }

    const started = await this.request('thread/start', cleanBase);
    return started.thread.id;
  }

  async runTurn({ threadId, userPrompt, inputItems = null, onDelta, onCommandDelta }) {
    let streamedText = '';

    let onNotif = null;
    let onLost = null;
    let timeoutId = null;

    const cleanup = () => {
      if (onNotif) {
        this.off('notification', onNotif);
      }
      if (onLost) {
        this.off('connection-lost', onLost);
      }
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    };

    const finished = new Promise((resolve, reject) => {
      onNotif = (notif) => {
        try {
          const method = notif.method;
          const p = notif.params || {};

          if (method === 'item/agentMessage/delta' && p.threadId === threadId) {
            const delta = p.delta || '';
            streamedText += delta;
            onDelta?.(delta);
          }

          if (method === 'item/commandExecution/outputDelta' && p.threadId === threadId) {
            onCommandDelta?.(p.delta || '');
          }

          if (method === 'turn/completed' && p.threadId === threadId) {
            const turn = p.turn;
            const agentMessages = [];
            if (turn?.items?.length) {
              for (const item of turn.items) {
                if (item.type === 'agentMessage' && item.text) {
                  if (agentMessages[agentMessages.length - 1] !== item.text) {
                    agentMessages.push(item.text);
                  }
                }
              }
            }

            let finalText = agentMessages.length ? agentMessages[agentMessages.length - 1] : '';
            if (!finalText && streamedText) {
              finalText = streamedText;
            }

            cleanup();
            resolve({ finalText, turn });
          }
        } catch (err) {
          cleanup();
          reject(err);
        }
      };

      onLost = (err) => {
        cleanup();
        reject(err || new Error('Connection lost during turn'));
      };

      timeoutId = setTimeout(() => {
        cleanup();
        reject(new Error('Turn timed out waiting for completion'));
      }, this.turnTimeoutMs);

      this.on('notification', onNotif);
      this.on('connection-lost', onLost);
    });

    try {
      const input = Array.isArray(inputItems) && inputItems.length
        ? inputItems
        : [{ type: 'text', text: userPrompt }];
      await this.request('turn/start', {
        threadId,
        input
      });
    } catch (err) {
      cleanup();
      throw err;
    }

    return finished;
  }

  async shutdown() {
    this.stopping = true;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    this.rejectPendingRequests(new Error('Codex client is shutting down'));

    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        // ignore close errors
      }
      this.ws = null;
    }

    if (this.serverProc) {
      this.serverProc.kill('SIGTERM');
      this.serverProc = null;
    }
  }
}
