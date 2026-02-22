function formatEnvValue(value) {
  const raw = String(value ?? '');
  if (raw === '') {
    return '';
  }
  if (/^[A-Za-z0-9_./:@,+-]+$/.test(raw)) {
    return raw;
  }
  const escaped = raw
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n');
  return `"${escaped}"`;
}

export function upsertEnvText(existingText, updates) {
  const text = String(existingText || '');
  const lines = text === '' ? [] : text.split('\n');
  const keys = Object.keys(updates || {}).filter((k) => Object.prototype.hasOwnProperty.call(updates, k));
  const seen = new Set();

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line || /^\s*#/.test(line)) {
      continue;
    }
    const eq = line.indexOf('=');
    if (eq <= 0) {
      continue;
    }
    const key = line.slice(0, eq).trim();
    if (!Object.prototype.hasOwnProperty.call(updates, key)) {
      continue;
    }
    lines[i] = `${key}=${formatEnvValue(updates[key])}`;
    seen.add(key);
  }

  for (const key of keys) {
    if (seen.has(key)) {
      continue;
    }
    lines.push(`${key}=${formatEnvValue(updates[key])}`);
  }

  return `${lines.join('\n').replace(/\n{3,}/g, '\n\n').trimEnd()}\n`;
}
