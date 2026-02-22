const DOW_ALIASES = {
  sun: 0,
  mon: 1,
  tue: 2,
  wed: 3,
  thu: 4,
  fri: 5,
  sat: 6
};

const WEEKDAY_PARTS = {
  sun: 0,
  mon: 1,
  tue: 2,
  wed: 3,
  thu: 4,
  fri: 5,
  sat: 6
};

const formattersByTz = new Map();

function getFormatter(timezone) {
  if (formattersByTz.has(timezone)) {
    return formattersByTz.get(timezone);
  }
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    hourCycle: 'h23'
  });
  formattersByTz.set(timezone, fmt);
  return fmt;
}

function parseIntStrict(value) {
  if (!/^-?\d+$/.test(String(value))) {
    return null;
  }
  return Number(value);
}

function expandRangeToken(token, min, max, aliases = null) {
  const text = String(token || '').trim().toLowerCase();
  if (!text) {
    throw new Error('Empty cron token.');
  }

  const toNumber = (raw) => {
    if (aliases && Object.prototype.hasOwnProperty.call(aliases, raw)) {
      return aliases[raw];
    }
    const n = parseIntStrict(raw);
    if (n == null) {
      throw new Error(`Invalid cron value "${raw}".`);
    }
    return n;
  };

  let step = 1;
  let base = text;
  if (text.includes('/')) {
    const parts = text.split('/');
    if (parts.length !== 2) {
      throw new Error(`Invalid cron step token "${text}".`);
    }
    base = parts[0];
    step = parseIntStrict(parts[1]);
    if (!Number.isInteger(step) || step <= 0) {
      throw new Error(`Invalid cron step "${parts[1]}".`);
    }
  }

  let start;
  let end;

  if (base === '*') {
    start = min;
    end = max;
  } else if (base.includes('-')) {
    const parts = base.split('-');
    if (parts.length !== 2) {
      throw new Error(`Invalid cron range "${base}".`);
    }
    start = toNumber(parts[0]);
    end = toNumber(parts[1]);
  } else {
    start = toNumber(base);
    end = start;
  }

  if (start < min || start > max || end < min || end > max || start > end) {
    throw new Error(`Cron value out of bounds "${text}" for range ${min}-${max}.`);
  }

  const out = [];
  for (let n = start; n <= end; n += step) {
    out.push(n);
  }
  return out;
}

function parseField(field, min, max, aliases = null, normalize = null) {
  const text = String(field || '').trim().toLowerCase();
  if (!text) {
    throw new Error('Cron field cannot be empty.');
  }
  const wildcard = text === '*';
  const values = new Set();
  if (wildcard) {
    for (let n = min; n <= max; n += 1) {
      values.add(n);
    }
    return { wildcard: true, values };
  }

  for (const rawToken of text.split(',')) {
    const expanded = expandRangeToken(rawToken, min, max, aliases);
    for (let n of expanded) {
      if (normalize) {
        n = normalize(n);
      }
      values.add(n);
    }
  }

  return { wildcard: false, values };
}

export function validateTimeZone(timezone) {
  const tz = String(timezone || '').trim();
  if (!tz) {
    return false;
  }
  try {
    // Throws RangeError for invalid names.
    new Intl.DateTimeFormat('en-US', { timeZone: tz }).format(new Date());
    return true;
  } catch {
    return false;
  }
}

export function cronFromDailyTime(hhmm) {
  const m = String(hhmm || '').trim().match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  if (!m) {
    throw new Error('Daily time must be HH:MM (24h).');
  }
  const hour = Number(m[1]);
  const minute = Number(m[2]);
  return `${minute} ${hour} * * *`;
}

export function parseCronExpression(expr) {
  const parts = String(expr || '').trim().split(/\s+/).filter(Boolean);
  if (parts.length !== 5) {
    throw new Error('Cron expression must have 5 fields: minute hour day month weekday');
  }

  const minute = parseField(parts[0], 0, 59);
  const hour = parseField(parts[1], 0, 23);
  const day = parseField(parts[2], 1, 31);
  const month = parseField(parts[3], 1, 12);
  const weekday = parseField(parts[4], 0, 7, DOW_ALIASES, (n) => (n === 7 ? 0 : n));

  return {
    minute,
    hour,
    day,
    month,
    weekday
  };
}

function zonedDateParts(date, timezone) {
  const parts = getFormatter(timezone).formatToParts(date);
  const map = {};
  for (const p of parts) {
    map[p.type] = p.value;
  }

  const weekday = WEEKDAY_PARTS[String(map.weekday || '').slice(0, 3).toLowerCase()];
  if (weekday == null) {
    throw new Error(`Could not parse weekday for timezone ${timezone}.`);
  }

  return {
    minute: Number(map.minute),
    hour: Number(map.hour),
    day: Number(map.day),
    month: Number(map.month),
    weekday
  };
}

function cronMatches(rule, date, timezone) {
  const z = zonedDateParts(date, timezone);
  if (!rule.minute.values.has(z.minute)) return false;
  if (!rule.hour.values.has(z.hour)) return false;
  if (!rule.month.values.has(z.month)) return false;

  const dayMatches = rule.day.values.has(z.day);
  const weekdayMatches = rule.weekday.values.has(z.weekday);
  if (rule.day.wildcard && rule.weekday.wildcard) {
    return true;
  }
  if (rule.day.wildcard) {
    return weekdayMatches;
  }
  if (rule.weekday.wildcard) {
    return dayMatches;
  }
  return dayMatches || weekdayMatches;
}

export function computeNextRunIso({ cronExpr, timezone, from = new Date() }) {
  if (!validateTimeZone(timezone)) {
    throw new Error(`Invalid timezone "${timezone}".`);
  }
  const rule = parseCronExpression(cronExpr);

  const start = new Date(from);
  start.setUTCSeconds(0, 0);
  let cursor = new Date(start.getTime() + 60_000);

  // Search up to ~400 days.
  const maxSteps = 400 * 24 * 60;
  for (let i = 0; i < maxSteps; i += 1) {
    if (cronMatches(rule, cursor, timezone)) {
      return cursor.toISOString();
    }
    cursor = new Date(cursor.getTime() + 60_000);
  }
  throw new Error('Could not resolve next cron run in search window.');
}

export function parseScheduleSpec(input) {
  const text = String(input || '').trim();
  if (!text) {
    throw new Error('Missing schedule spec.');
  }

  const daily = text.match(/^daily\s+([01]?\d|2[0-3]):([0-5]\d)$/i);
  if (daily) {
    return {
      mode: 'daily',
      cronExpr: cronFromDailyTime(`${daily[1]}:${daily[2]}`),
      normalizedSpec: `daily ${daily[1].padStart(2, '0')}:${daily[2]}`
    };
  }

  const cronPrefixed = text.match(/^cron\s+(.+)$/i);
  if (cronPrefixed) {
    const expr = cronPrefixed[1].trim();
    parseCronExpression(expr);
    return {
      mode: 'cron',
      cronExpr: expr,
      normalizedSpec: `cron ${expr}`
    };
  }

  // Bare 5-field cron is accepted.
  parseCronExpression(text);
  return {
    mode: 'cron',
    cronExpr: text,
    normalizedSpec: `cron ${text}`
  };
}

