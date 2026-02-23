const state = {
  scope: localStorage.getItem('dexbot_scope') || 'main',
  scopes: [],
  eventSource: null,
  pendingFiles: [],
  envFields: [],
  envInitialValues: {}
};

const els = {
  scopeList: document.getElementById('scope-list'),
  scopeTitle: document.getElementById('scope-title'),
  statusStrip: document.getElementById('status-strip'),
  chatLog: document.getElementById('chat-log'),
  composer: document.getElementById('composer'),
  chatInput: document.getElementById('chat-input'),
  fileInput: document.getElementById('file-input'),
  fileChipRow: document.getElementById('file-chip-row'),
  settingsBtn: document.getElementById('settings-btn'),
  closeSettingsBtn: document.getElementById('close-settings-btn'),
  settingsDrawer: document.getElementById('settings-drawer'),
  refreshBtn: document.getElementById('refresh-btn'),
  newScopeBtn: document.getElementById('new-scope-btn'),
  runtimeSummary: document.getElementById('runtime-summary'),
  autostartSummary: document.getElementById('autostart-summary'),
  restartBtn: document.getElementById('restart-btn'),
  envFields: document.getElementById('env-fields'),
  reloadEnvBtn: document.getElementById('reload-env-btn'),
  saveEnvBtn: document.getElementById('save-env-btn'),
  restartAfterSave: document.getElementById('restart-after-save')
};

function normalizeScope(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64) || 'main';
}

function fmtNow() {
  return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function setStatus(text = '') {
  els.statusStrip.textContent = text ? `${text} (${fmtNow()})` : '';
}

async function api(path, options = {}) {
  const headers = {
    'Content-Type': 'application/json',
    ...(options.headers || {})
  };

  const response = await fetch(path, {
    ...options,
    headers,
    credentials: 'same-origin'
  });

  const json = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = json?.error || `HTTP ${response.status}`;
    const err = new Error(message);
    err.status = response.status;
    throw err;
  }
  return json;
}

function appendMessage(role, text, extra = {}) {
  const msg = document.createElement('article');
  msg.className = `message ${role}`;
  msg.textContent = text || '';

  const meta = document.createElement('span');
  meta.className = 'meta';
  meta.textContent = extra.meta || fmtNow();
  msg.appendChild(meta);

  els.chatLog.appendChild(msg);
  els.chatLog.scrollTop = els.chatLog.scrollHeight;
}

function appendAttachment(payload) {
  const wrap = document.createElement('article');
  wrap.className = 'attachment-card';

  const link = document.createElement('a');
  link.href = payload.url;
  link.target = '_blank';
  link.rel = 'noreferrer';
  link.textContent = `${payload.kind === 'image' ? 'Image' : 'File'}: ${payload.label || payload.url}`;
  wrap.appendChild(link);

  if (payload.kind === 'image') {
    const img = document.createElement('img');
    img.className = 'attachment-image';
    img.loading = 'lazy';
    img.src = payload.url;
    img.alt = payload.label || 'Generated image';
    wrap.appendChild(img);
  }

  els.chatLog.appendChild(wrap);
  els.chatLog.scrollTop = els.chatLog.scrollHeight;
}

function clearChat() {
  els.chatLog.innerHTML = '';
}

function renderScopes() {
  els.scopeList.innerHTML = '';
  for (const scope of state.scopes) {
    const li = document.createElement('li');
    li.className = 'scope-item';
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.textContent = scope.name;
    if (scope.name === state.scope) {
      btn.classList.add('active');
    }
    btn.addEventListener('click', () => switchScope(scope.name));
    li.appendChild(btn);
    els.scopeList.appendChild(li);
  }
}

function renderRuntimeSummary(payload) {
  const lines = [
    `App: ${payload?.appName || 'Dexbot'}`,
    `CWD: ${payload?.codex?.cwd || '(unknown)'}`,
    `Model: ${payload?.codex?.provider || '(default)'}/${payload?.codex?.model || '(default)'}`,
    `Access: ${payload?.codex?.accessProfile || '(unset)'}`,
    `Sandbox: ${payload?.codex?.sandbox || '(unset)'}`,
    `Approval: ${payload?.codex?.approvalPolicy || '(unset)'}`,
    `Multi-Agent: ${payload?.codex?.multiAgent ? 'enabled' : 'disabled'}`,
    `Dashboard: ${payload?.dashboard?.host || '127.0.0.1'}:${payload?.dashboard?.port || ''}`,
    `Token protected: ${payload?.dashboard?.tokenProtected ? 'yes' : 'no'}`,
    `Timezone: ${payload?.schedule?.defaultTimezone || '(unknown)'}`,
    `Skills root: ${payload?.skills?.writableRoot || '(unknown)'}`
  ];
  els.runtimeSummary.textContent = lines.join('\n');
}

function renderAutostartSummary(data) {
  if (!data) {
    els.autostartSummary.textContent = 'Autostart status unavailable.';
    return;
  }
  const lines = [
    `Loaded: ${data.loaded ? 'yes' : 'no'}`,
    `Configured: ${data.configured ? 'yes' : 'no'}`,
    `Service: ${data.serviceTarget || '(n/a)'}`,
    `PID: ${data.pid || '(none)'}`,
    `Plist: ${data.plistPath || '(n/a)'}`
  ];
  els.autostartSummary.textContent = lines.join('\n');
}

function renderEnvFields(fields) {
  els.envFields.innerHTML = '';
  state.envInitialValues = {};

  const list = Array.isArray(fields) ? fields : [];
  state.envFields = list;

  for (const field of list) {
    const row = document.createElement('div');
    row.className = 'env-row';

    const top = document.createElement('div');
    top.className = 'env-row-top';

    const key = document.createElement('span');
    key.className = 'env-key';
    key.textContent = field.key;

    const badge = document.createElement('span');
    badge.className = 'env-badge';
    badge.textContent = field.sensitive ? 'secret' : 'plain';

    top.appendChild(key);
    top.appendChild(badge);

    const desc = document.createElement('p');
    desc.className = 'env-desc';
    desc.textContent = field.description || '';

    const input = document.createElement('input');
    input.type = field.sensitive ? 'password' : 'text';
    input.setAttribute('data-env-key', field.key);
    input.setAttribute('data-sensitive', field.sensitive ? '1' : '0');

    if (field.sensitive) {
      input.placeholder = field.present
        ? `Current: ${field.masked || '(set)'}`
        : '(not set)';
      input.value = '';
      state.envInitialValues[field.key] = '';
    } else {
      const value = String(field.value || '');
      input.value = value;
      state.envInitialValues[field.key] = value;
    }

    const meta = document.createElement('div');
    meta.className = 'env-meta';
    if (field.sensitive) {
      meta.textContent = field.present ? 'Saved value exists and is hidden.' : 'No value set yet.';
    } else {
      meta.textContent = field.present ? 'Current value loaded.' : 'No value set yet.';
    }

    row.appendChild(top);
    row.appendChild(desc);
    row.appendChild(input);
    row.appendChild(meta);

    els.envFields.appendChild(row);
  }
}

async function loadEnvSettings() {
  const payload = await api('/api/settings');
  renderEnvFields(payload.fields || []);
}

async function loadState() {
  const payload = await api(`/api/state?scope=${encodeURIComponent(state.scope)}`);
  state.scope = normalizeScope(payload.scope?.name || state.scope || 'main');
  localStorage.setItem('dexbot_scope', state.scope);
  state.scopes = Array.isArray(payload.scopes) ? payload.scopes : [];

  renderScopes();
  els.scopeTitle.textContent = state.scope;
  renderRuntimeSummary(payload.config || {});
  renderAutostartSummary(payload.autostart || null);
}

async function loadHistory() {
  const payload = await api(`/api/history?scope=${encodeURIComponent(state.scope)}&limit=80`);
  clearChat();
  for (const msg of payload.messages || []) {
    const role = msg.role === 'user' ? 'user' : 'assistant';
    appendMessage(role, msg.text || '', {
      meta: msg.ts ? new Date(msg.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : fmtNow()
    });
  }
}

function closeStream() {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
}

function connectStream() {
  closeStream();
  const es = new EventSource(`/api/stream?scope=${encodeURIComponent(state.scope)}`);
  state.eventSource = es;

  es.addEventListener('hello', () => {
    setStatus('Live');
  });

  es.addEventListener('status', (event) => {
    try {
      const payload = JSON.parse(event.data);
      setStatus(payload.text || 'Working...');
    } catch {
      setStatus('Working...');
    }
  });

  es.addEventListener('assistant_chunk', (event) => {
    try {
      const payload = JSON.parse(event.data);
      if (payload.text) {
        appendMessage('assistant', payload.text);
      }
    } catch {
      // ignore malformed events
    }
  });

  es.addEventListener('assistant', (event) => {
    try {
      const payload = JSON.parse(event.data);
      const role = payload.kind === 'system' ? 'system' : 'assistant';
      appendMessage(role, payload.text || '');
    } catch {
      // ignore malformed events
    }
  });

  es.addEventListener('attachment', (event) => {
    try {
      const payload = JSON.parse(event.data);
      appendAttachment(payload);
    } catch {
      // ignore malformed events
    }
  });

  es.addEventListener('done', () => {
    setStatus('Ready');
  });

  es.onerror = () => {
    setStatus('Reconnecting stream...');
    setTimeout(() => {
      if (state.eventSource === es) {
        connectStream();
      }
    }, 1200);
  };
}

async function ensureScope(name) {
  const scope = normalizeScope(name);
  const payload = await api('/api/scope', {
    method: 'POST',
    body: JSON.stringify({ scope })
  });
  state.scopes = payload.scopes || state.scopes;
  state.scope = normalizeScope(payload.scope?.name || scope);
  localStorage.setItem('dexbot_scope', state.scope);
  renderScopes();
  els.scopeTitle.textContent = state.scope;
}

async function switchScope(name) {
  state.scope = normalizeScope(name);
  localStorage.setItem('dexbot_scope', state.scope);
  setStatus(`Switching to ${state.scope}...`);
  await loadState();
  await loadHistory();
  connectStream();
}

function renderFileChips() {
  els.fileChipRow.innerHTML = '';
  for (const file of state.pendingFiles) {
    const chip = document.createElement('span');
    chip.className = 'file-chip';
    chip.textContent = `${file.name} (${Math.max(1, Math.round(file.size / 1024))}KB)`;
    els.fileChipRow.appendChild(chip);
  }
}

function readFileAsBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || '');
      const base64 = result.includes(',') ? result.split(',')[1] : result;
      resolve({
        name: file.name,
        mime: file.type || 'application/octet-stream',
        size: file.size,
        dataBase64: base64
      });
    };
    reader.onerror = () => reject(new Error(`Failed to read ${file.name}`));
    reader.readAsDataURL(file);
  });
}

async function sendMessage(text, files) {
  const trimmed = String(text || '').trim();
  if (!trimmed && !files.length) {
    return;
  }

  if (trimmed) {
    appendMessage('user', trimmed);
  } else if (files.length) {
    appendMessage('user', `[${files.length} attachment${files.length > 1 ? 's' : ''}]`);
  }

  setStatus('Queued...');
  await api('/api/chat', {
    method: 'POST',
    body: JSON.stringify({
      scope: state.scope,
      text: trimmed,
      files
    })
  });

  els.chatInput.value = '';
  state.pendingFiles = [];
  renderFileChips();
}

async function handleAutostart(action) {
  const payload = await api('/api/autostart', {
    method: 'POST',
    body: JSON.stringify({ action })
  });
  renderAutostartSummary(payload.status || null);
  appendMessage('system', `Autostart action applied: ${action}`);
}

async function handleRestart() {
  const proceed = window.confirm('Restart bot + Codex app-server now?');
  if (!proceed) return;
  const payload = await api('/api/restart', {
    method: 'POST',
    body: JSON.stringify({})
  });
  appendMessage('system', payload.message || 'Restarting...');
  setStatus('Restart requested...');
}

async function saveEnvironmentSettings() {
  const inputs = Array.from(els.envFields.querySelectorAll('input[data-env-key]'));
  const updates = [];

  for (const input of inputs) {
    const key = String(input.getAttribute('data-env-key') || '').trim();
    const sensitive = input.getAttribute('data-sensitive') === '1';
    const next = String(input.value || '');

    if (!key) continue;

    if (sensitive) {
      if (!next) continue;
      updates.push({ key, value: next });
      continue;
    }

    const prev = String(state.envInitialValues[key] || '');
    if (next !== prev) {
      updates.push({ key, value: next });
    }
  }

  if (!updates.length) {
    appendMessage('system', 'No environment changes to save.');
    return;
  }

  const restart = Boolean(els.restartAfterSave?.checked);
  const payload = await api('/api/settings/update', {
    method: 'POST',
    body: JSON.stringify({ updates, restart })
  });

  appendMessage('system', `Saved ${payload.changedKeys.length} environment key(s).`);
  await loadEnvSettings();
  await loadState();

  if (restart) {
    appendMessage('system', 'Restart requested to apply changes. Dashboard will reconnect automatically.');
  }
}

async function openSettings() {
  els.settingsDrawer.classList.add('open');
  els.settingsDrawer.setAttribute('aria-hidden', 'false');
  try {
    await loadEnvSettings();
  } catch (err) {
    appendMessage('system', `Could not load env settings: ${err.message || err}`);
  }
}

function closeSettings() {
  els.settingsDrawer.classList.remove('open');
  els.settingsDrawer.setAttribute('aria-hidden', 'true');
}

async function bootstrap() {
  try {
    await loadState();
    await loadHistory();
    connectStream();
    setStatus('Ready');
  } catch (err) {
    if (err.status === 401) {
      appendMessage('system', 'Dashboard authorization failed. Hard refresh once. If still blocked, restart bot.');
    } else {
      appendMessage('system', `Bootstrap error: ${err.message || err}`);
    }
  }
}

els.composer.addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    await sendMessage(els.chatInput.value, state.pendingFiles);
  } catch (err) {
    appendMessage('system', `Send failed: ${err.message || err}`);
    setStatus('Send failed');
  }
});

els.fileInput.addEventListener('change', async () => {
  const files = Array.from(els.fileInput.files || []);
  if (!files.length) return;

  try {
    const encoded = [];
    for (const file of files) {
      encoded.push(await readFileAsBase64(file));
    }
    state.pendingFiles = [...state.pendingFiles, ...encoded].slice(0, 10);
    renderFileChips();
  } catch (err) {
    appendMessage('system', `Attachment failed: ${err.message || err}`);
  } finally {
    els.fileInput.value = '';
  }
});

document.querySelectorAll('[data-command]').forEach((button) => {
  button.addEventListener('click', async () => {
    const cmd = button.getAttribute('data-command');
    if (!cmd) return;
    try {
      await sendMessage(cmd, []);
    } catch (err) {
      appendMessage('system', `Command failed: ${err.message || err}`);
    }
  });
});

document.querySelectorAll('[data-autostart]').forEach((button) => {
  button.addEventListener('click', async () => {
    const action = button.getAttribute('data-autostart');
    if (!action) return;
    try {
      await handleAutostart(action);
    } catch (err) {
      appendMessage('system', `Autostart failed: ${err.message || err}`);
    }
  });
});

els.restartBtn.addEventListener('click', async () => {
  try {
    await handleRestart();
  } catch (err) {
    appendMessage('system', `Restart failed: ${err.message || err}`);
  }
});

els.reloadEnvBtn.addEventListener('click', async () => {
  try {
    await loadEnvSettings();
    appendMessage('system', 'Environment fields reloaded.');
  } catch (err) {
    appendMessage('system', `Reload env failed: ${err.message || err}`);
  }
});

els.saveEnvBtn.addEventListener('click', async () => {
  try {
    await saveEnvironmentSettings();
  } catch (err) {
    appendMessage('system', `Save env failed: ${err.message || err}`);
  }
});

els.settingsBtn.addEventListener('click', () => {
  void openSettings();
});
els.closeSettingsBtn.addEventListener('click', closeSettings);

els.refreshBtn.addEventListener('click', async () => {
  try {
    await loadState();
    await loadHistory();
    appendMessage('system', 'Dashboard refreshed.');
  } catch (err) {
    appendMessage('system', `Refresh failed: ${err.message || err}`);
  }
});

els.newScopeBtn.addEventListener('click', async () => {
  const input = window.prompt('New scope name (e.g., sales, research, ops):', 'main');
  if (input == null) return;
  const scope = normalizeScope(input);
  try {
    await ensureScope(scope);
    await switchScope(scope);
  } catch (err) {
    appendMessage('system', `Scope creation failed: ${err.message || err}`);
  }
});

window.addEventListener('beforeunload', () => {
  closeStream();
});

bootstrap();
