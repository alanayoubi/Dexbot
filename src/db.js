import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';

function nowIso() {
  return new Date().toISOString();
}

function normalizeFactKey(subject, predicate, object) {
  const n = (v) => String(v || '').trim().toLowerCase().replace(/\s+/g, ' ');
  return `${n(subject)}|${n(predicate)}|${n(object)}`;
}

function safeJson(value, fallback) {
  if (value == null || value === '') {
    return fallback;
  }
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function buildFtsQuery(text) {
  const tokens = (String(text || '').toLowerCase().match(/[a-z0-9_\/-]{3,}/g) || [])
    .slice(0, 12)
    .map((t) => t.replace(/"/g, ''));
  if (!tokens.length) {
    return 'memory';
  }
  return tokens.map((t) => `"${t}"`).join(' OR ');
}

function cosineSimilarity(vecA, vecB) {
  if (!Array.isArray(vecA) || !Array.isArray(vecB) || !vecA.length || !vecB.length) {
    return 0;
  }
  const len = Math.min(vecA.length, vecB.length);
  let dot = 0;
  let aNorm = 0;
  let bNorm = 0;
  for (let i = 0; i < len; i += 1) {
    const a = Number(vecA[i] || 0);
    const b = Number(vecB[i] || 0);
    dot += a * b;
    aNorm += a * a;
    bNorm += b * b;
  }
  if (!aNorm || !bNorm) {
    return 0;
  }
  return dot / (Math.sqrt(aNorm) * Math.sqrt(bNorm));
}

export class MemoryStore {
  constructor(dbPath) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('foreign_keys = ON');
    this.runMigrations();
    this.prepareStatements();
  }

  runMigrations() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
      );
    `);

    const applied = new Set(
      this.db
        .prepare('SELECT version FROM schema_migrations ORDER BY version')
        .all()
        .map((r) => r.version)
    );

    const migrations = [
      {
        version: 1,
        sql: `
          CREATE TABLE IF NOT EXISTS chats (
            chat_id INTEGER PRIMARY KEY,
            thread_id TEXT,
            session_no INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
          );

          CREATE TABLE IF NOT EXISTS session_state (
            chat_id INTEGER NOT NULL,
            session_no INTEGER NOT NULL,
            turn_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, session_no),
            FOREIGN KEY (chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
          );

          CREATE TABLE IF NOT EXISTS exchanges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            session_no INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            user_text TEXT NOT NULL,
            assistant_text TEXT NOT NULL,
            FOREIGN KEY (chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
          );

          CREATE INDEX IF NOT EXISTS idx_exchanges_chat_session ON exchanges(chat_id, session_no, id);
          CREATE INDEX IF NOT EXISTS idx_exchanges_created ON exchanges(created_at);
        `
      },
      {
        version: 2,
        sql: `
          CREATE TABLE IF NOT EXISTS facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT NOT NULL,
            confidence REAL NOT NULL,
            tags_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            last_confirmed_at TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_excerpt TEXT NOT NULL,
            fact_key TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE,
            UNIQUE(chat_id, fact_key)
          );

          CREATE TABLE IF NOT EXISTS episodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            summary TEXT NOT NULL,
            entities_json TEXT NOT NULL DEFAULT '[]',
            tags_json TEXT NOT NULL DEFAULT '[]',
            salience REAL NOT NULL,
            time_range_start TEXT NOT NULL,
            time_range_end TEXT NOT NULL,
            source_files_json TEXT NOT NULL DEFAULT '[]',
            embedding_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
          );

          CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            text TEXT NOT NULL,
            embedding_json TEXT,
            created_at TEXT NOT NULL,
            tags_json TEXT NOT NULL DEFAULT '[]',
            FOREIGN KEY (chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE,
            UNIQUE(chat_id, path, chunk_index)
          );

          CREATE TABLE IF NOT EXISTS open_loops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            tags_json TEXT NOT NULL DEFAULT '[]',
            confidence REAL NOT NULL DEFAULT 0.7,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL,
            resolved_at TEXT,
            source_file TEXT NOT NULL,
            UNIQUE(chat_id, text, status),
            FOREIGN KEY (chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
          );

          CREATE TABLE IF NOT EXISTS contradictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            objects_json TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            resolved_at TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            UNIQUE(chat_id, subject, predicate, status),
            FOREIGN KEY (chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
          );

          CREATE INDEX IF NOT EXISTS idx_facts_chat_predicate ON facts(chat_id, subject, predicate);
          CREATE INDEX IF NOT EXISTS idx_facts_confidence ON facts(chat_id, confidence DESC);
          CREATE INDEX IF NOT EXISTS idx_episodes_chat_time ON episodes(chat_id, time_range_end DESC);
          CREATE INDEX IF NOT EXISTS idx_open_loops_status ON open_loops(chat_id, status, created_at DESC);
        `
      },
      {
        version: 3,
        sql: `
          CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5(
            subject,
            predicate,
            object,
            source_excerpt,
            tokenize='porter unicode61'
          );

          CREATE VIRTUAL TABLE IF NOT EXISTS episodes_fts USING fts5(
            summary,
            tokenize='porter unicode61'
          );

          CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
            path,
            text,
            tokenize='porter unicode61'
          );
        `
      }
    ];

    for (const migration of migrations) {
      if (applied.has(migration.version)) {
        continue;
      }
      const tx = this.db.transaction(() => {
        this.db.exec(migration.sql);
        this.db
          .prepare('INSERT INTO schema_migrations(version, applied_at) VALUES(?, ?)')
          .run(migration.version, nowIso());
      });
      tx();
    }

    if (!applied.has(4)) {
      const tx = this.db.transaction(() => {
        const columns = this.db.prepare('PRAGMA table_info(exchanges)').all();
        const hasDateKey = columns.some((c) => c.name === 'date_key');
        if (!hasDateKey) {
          this.db.exec('ALTER TABLE exchanges ADD COLUMN date_key TEXT');
        }
        this.db.exec(`
          UPDATE exchanges
          SET date_key = COALESCE(NULLIF(date_key, ''), substr(created_at, 1, 10))
          WHERE date_key IS NULL OR date_key = '';
        `);
        this.db.exec('CREATE INDEX IF NOT EXISTS idx_exchanges_chat_date ON exchanges(chat_id, date_key, id);');
        this.db
          .prepare('INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(?, ?)')
          .run(4, nowIso());
      });
      tx();
    }

    if (!applied.has(5)) {
      const tx = this.db.transaction(() => {
        this.db.exec(`
          CREATE TABLE IF NOT EXISTS scheduled_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            base_chat_id INTEGER NOT NULL,
            scoped_chat_id INTEGER NOT NULL,
            topic_id INTEGER NOT NULL DEFAULT 0,
            title TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'report',
            prompt TEXT NOT NULL,
            cron_expr TEXT NOT NULL,
            timezone TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            last_run_at TEXT,
            next_run_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
          );

          CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_scope ON scheduled_jobs(scoped_chat_id, active, next_run_at);
          CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due ON scheduled_jobs(active, next_run_at);
        `);
        this.db
          .prepare('INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(?, ?)')
          .run(5, nowIso());
      });
      tx();
    }
  }

  prepareStatements() {
    this.stmts = {
      getChat: this.db.prepare('SELECT * FROM chats WHERE chat_id = ?'),
      insertChat: this.db.prepare(
        'INSERT INTO chats(chat_id, thread_id, session_no, created_at, updated_at) VALUES(?, NULL, 1, ?, ?)'
      ),
      updateChatThread: this.db.prepare('UPDATE chats SET thread_id = ?, updated_at = ? WHERE chat_id = ?'),
      incrementSession: this.db.prepare(
        'UPDATE chats SET session_no = session_no + 1, thread_id = NULL, updated_at = ? WHERE chat_id = ?'
      ),
      getSessionState: this.db.prepare(
        'SELECT * FROM session_state WHERE chat_id = ? AND session_no = ?'
      ),
      insertSessionState: this.db.prepare(
        `INSERT INTO session_state(chat_id, session_no, turn_count, created_at, updated_at)
         VALUES(?, ?, 0, ?, ?)`
      ),
      bumpSessionTurn: this.db.prepare(
        `UPDATE session_state
         SET turn_count = turn_count + 1, updated_at = ?
         WHERE chat_id = ? AND session_no = ?`
      ),
      insertExchange: this.db.prepare(
        `INSERT INTO exchanges(chat_id, session_no, date_key, created_at, user_text, assistant_text)
         VALUES(?, ?, ?, ?, ?, ?)`
      ),
      recentExchanges: this.db.prepare(
        `SELECT user_text, assistant_text, created_at
         FROM exchanges
         WHERE chat_id = ?
         ORDER BY id DESC
         LIMIT ?`
      ),
      recentSessionExchanges: this.db.prepare(
        `SELECT user_text, assistant_text, created_at
         FROM exchanges
         WHERE chat_id = ? AND session_no = ?
         ORDER BY id DESC
         LIMIT ?`
      ),
      upsertFactInsert: this.db.prepare(
        `INSERT INTO facts(
          chat_id, subject, predicate, object, confidence, tags_json,
          created_at, last_confirmed_at, source_file, source_excerpt,
          fact_key, active
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`
      ),
      findFactByKey: this.db.prepare('SELECT * FROM facts WHERE chat_id = ? AND fact_key = ?'),
      updateFactById: this.db.prepare(
        `UPDATE facts
         SET confidence = ?,
             tags_json = ?,
             last_confirmed_at = ?,
             source_file = ?,
             source_excerpt = ?,
             active = 1
         WHERE id = ?`
      ),
      topFacts: this.db.prepare(
        `SELECT * FROM facts
         WHERE chat_id = ? AND active = 1
         ORDER BY confidence DESC, last_confirmed_at DESC
         LIMIT ?`
      ),
      keywordFacts: this.db.prepare(
        `SELECT f.*, bm25(facts_fts) AS rank
         FROM facts_fts
         JOIN facts f ON f.id = facts_fts.rowid
         WHERE f.chat_id = ? AND f.active = 1 AND facts_fts MATCH ?
         ORDER BY rank
         LIMIT ?`
      ),
      exactFacts: this.db.prepare(
        `SELECT * FROM facts
         WHERE chat_id = ? AND active = 1
           AND (
             subject IN (SELECT value FROM json_each(?))
             OR predicate IN (SELECT value FROM json_each(?))
             OR EXISTS (
               SELECT 1 FROM json_each(tags_json)
               WHERE value IN (SELECT value FROM json_each(?))
             )
           )
         ORDER BY confidence DESC, last_confirmed_at DESC
         LIMIT ?`
      ),
      insertEpisode: this.db.prepare(
        `INSERT INTO episodes(
          chat_id, summary, entities_json, tags_json, salience,
          time_range_start, time_range_end, source_files_json,
          embedding_json, created_at, updated_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ),
      keywordEpisodes: this.db.prepare(
        `SELECT e.*, bm25(episodes_fts) AS rank
         FROM episodes_fts
         JOIN episodes e ON e.id = episodes_fts.rowid
         WHERE e.chat_id = ? AND episodes_fts MATCH ?
         ORDER BY rank
         LIMIT ?`
      ),
      episodeCandidatesForVector: this.db.prepare(
        `SELECT * FROM episodes
         WHERE chat_id = ?
         ORDER BY time_range_end DESC
         LIMIT ?`
      ),
      exactEpisodes: this.db.prepare(
        `SELECT * FROM episodes
         WHERE chat_id = ?
           AND (
             EXISTS (
               SELECT 1 FROM json_each(entities_json)
               WHERE value IN (SELECT value FROM json_each(?))
             )
             OR EXISTS (
               SELECT 1 FROM json_each(tags_json)
               WHERE value IN (SELECT value FROM json_each(?))
             )
           )
         ORDER BY salience DESC, time_range_end DESC
         LIMIT ?`
      ),
      upsertDocument: this.db.prepare(
        `INSERT INTO documents(chat_id, path, chunk_index, text, embedding_json, created_at, tags_json)
         VALUES(?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(chat_id, path, chunk_index)
         DO UPDATE SET
           text = excluded.text,
           embedding_json = excluded.embedding_json,
           created_at = excluded.created_at,
           tags_json = excluded.tags_json`
      ),
      keywordDocuments: this.db.prepare(
        `SELECT d.*, bm25(documents_fts) AS rank
         FROM documents_fts
         JOIN documents d ON d.id = documents_fts.rowid
         WHERE d.chat_id = ? AND documents_fts MATCH ?
         ORDER BY rank
         LIMIT ?`
      ),
      insertOpenLoop: this.db.prepare(
        `INSERT INTO open_loops(chat_id, text, tags_json, confidence, status, created_at, source_file)
         VALUES(?, ?, ?, ?, 'open', ?, ?)
         ON CONFLICT(chat_id, text, status)
         DO UPDATE SET
           tags_json = excluded.tags_json,
           confidence = MAX(open_loops.confidence, excluded.confidence),
           created_at = excluded.created_at,
           source_file = excluded.source_file`
      ),
      resolveOpenLoop: this.db.prepare(
        `UPDATE open_loops SET status = 'resolved', resolved_at = ?
         WHERE chat_id = ? AND status = 'open' AND text = ?`
      ),
      listOpenLoops: this.db.prepare(
        `SELECT * FROM open_loops
         WHERE chat_id = ? AND status = 'open'
         ORDER BY confidence DESC, created_at DESC
         LIMIT ?`
      ),
      listOpenLoopsByKeyword: this.db.prepare(
        `SELECT * FROM open_loops
         WHERE chat_id = ? AND status = 'open' AND text LIKE ?
         ORDER BY confidence DESC, created_at DESC
         LIMIT ?`
      ),
      insertContradiction: this.db.prepare(
        `INSERT INTO contradictions(chat_id, subject, predicate, objects_json, detected_at, status)
         VALUES(?, ?, ?, ?, ?, 'open')
         ON CONFLICT(chat_id, subject, predicate, status)
         DO UPDATE SET
           objects_json = excluded.objects_json,
           detected_at = excluded.detected_at`
      ),
      listContradictions: this.db.prepare(
        `SELECT * FROM contradictions
         WHERE chat_id = ? AND status = 'open'
         ORDER BY detected_at DESC`
      ),
      contradictionCandidates: this.db.prepare(
        `SELECT subject, predicate,
                json_group_array(object) AS objects_json,
                COUNT(DISTINCT object) AS object_count
         FROM facts
         WHERE chat_id = ? AND active = 1 AND confidence >= ?
         GROUP BY subject, predicate
         HAVING COUNT(DISTINCT object) > 1`
      ),
      decayFacts: this.db.prepare(
        `UPDATE facts
         SET confidence = MAX(0.1, confidence - ?)
         WHERE chat_id = ?
           AND active = 1
           AND julianday('now') - julianday(last_confirmed_at) > ?`
      ),
      duplicateFactRows: this.db.prepare(
        `SELECT chat_id, fact_key, GROUP_CONCAT(id) AS ids, COUNT(*) AS n
         FROM facts
         GROUP BY chat_id, fact_key
         HAVING COUNT(*) > 1`
      ),
      deleteFactById: this.db.prepare('DELETE FROM facts WHERE id = ?'),
      allChats: this.db.prepare('SELECT chat_id FROM chats ORDER BY chat_id ASC'),
      insertScheduledJob: this.db.prepare(
        `INSERT INTO scheduled_jobs(
          base_chat_id, scoped_chat_id, topic_id, title, kind, prompt,
          cron_expr, timezone, active, last_run_at, next_run_at, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)`
      ),
      listScheduledJobsByScope: this.db.prepare(
        `SELECT * FROM scheduled_jobs
         WHERE scoped_chat_id = ?
         ORDER BY id DESC`
      ),
      getScheduledJobByScope: this.db.prepare(
        `SELECT * FROM scheduled_jobs
         WHERE id = ? AND scoped_chat_id = ?`
      ),
      setScheduledJobActiveByScope: this.db.prepare(
        `UPDATE scheduled_jobs
         SET active = ?, updated_at = ?
         WHERE id = ? AND scoped_chat_id = ?`
      ),
      deleteScheduledJobByScope: this.db.prepare(
        `DELETE FROM scheduled_jobs
         WHERE id = ? AND scoped_chat_id = ?`
      ),
      listDueScheduledJobs: this.db.prepare(
        `SELECT * FROM scheduled_jobs
         WHERE active = 1 AND next_run_at <= ?
         ORDER BY next_run_at ASC, id ASC
         LIMIT ?`
      ),
      markScheduledJobRun: this.db.prepare(
        `UPDATE scheduled_jobs
         SET last_run_at = ?, next_run_at = ?, updated_at = ?
         WHERE id = ?`
      ),
      touchScheduledJobUpdatedAt: this.db.prepare(
        `UPDATE scheduled_jobs
         SET updated_at = ?
         WHERE id = ?`
      )
    };

    this.fts = {
      upsertFact: this.db.prepare(
        'INSERT OR REPLACE INTO facts_fts(rowid, subject, predicate, object, source_excerpt) VALUES(?, ?, ?, ?, ?)'
      ),
      upsertEpisode: this.db.prepare(
        'INSERT OR REPLACE INTO episodes_fts(rowid, summary) VALUES(?, ?)'
      ),
      upsertDocument: this.db.prepare(
        'INSERT OR REPLACE INTO documents_fts(rowid, path, text) VALUES(?, ?, ?)'
      )
    };
  }

  close() {
    this.db.close();
  }

  getChatIds() {
    return this.stmts.allChats.all().map((r) => r.chat_id);
  }

  ensureChat(chatId) {
    let row = this.stmts.getChat.get(chatId);
    if (!row) {
      const now = nowIso();
      this.stmts.insertChat.run(chatId, now, now);
      row = this.stmts.getChat.get(chatId);
    }
    return row;
  }

  ensureSessionState(chatId, sessionNo) {
    let row = this.stmts.getSessionState.get(chatId, sessionNo);
    if (!row) {
      const now = nowIso();
      this.stmts.insertSessionState.run(chatId, sessionNo, now, now);
      row = this.stmts.getSessionState.get(chatId, sessionNo);
    }
    return row;
  }

  getChatState(chatId) {
    const state = this.ensureChat(chatId);
    this.ensureSessionState(chatId, state.session_no);
    return state;
  }

  startNewSession(chatId) {
    const state = this.ensureChat(chatId);
    this.ensureSessionState(chatId, state.session_no);
    this.stmts.incrementSession.run(nowIso(), chatId);
    const next = this.stmts.getChat.get(chatId);
    this.ensureSessionState(chatId, next.session_no);
    return next;
  }

  setThreadId(chatId, threadId) {
    this.stmts.updateChatThread.run(threadId, nowIso(), chatId);
  }

  recordExchange(chatId, sessionNo, userText, assistantText) {
    const now = nowIso();
    const dateKey = now.slice(0, 10);
    this.stmts.insertExchange.run(chatId, sessionNo, dateKey, now, userText, assistantText);
    this.ensureSessionState(chatId, sessionNo);
    this.stmts.bumpSessionTurn.run(now, chatId, sessionNo);
  }

  getSessionTurnCount(chatId, sessionNo) {
    return this.ensureSessionState(chatId, sessionNo).turn_count;
  }

  getRecentExchanges(chatId, limit = 8, sessionNo = null) {
    if (sessionNo == null) {
      return this.stmts.recentExchanges.all(chatId, limit);
    }
    return this.stmts.recentSessionExchanges.all(chatId, sessionNo, limit);
  }

  upsertFact({
    chatId,
    subject,
    predicate,
    object,
    confidence,
    tags = [],
    createdAt,
    sourceFile,
    sourceExcerpt
  }) {
    const factKey = normalizeFactKey(subject, predicate, object);
    const found = this.stmts.findFactByKey.get(chatId, factKey);
    const now = createdAt || nowIso();
    const tagsJson = JSON.stringify(Array.from(new Set(tags.map((t) => String(t).trim()).filter(Boolean))));

    if (!found) {
      const info = this.stmts.upsertFactInsert.run(
        chatId,
        subject,
        predicate,
        object,
        confidence,
        tagsJson,
        now,
        now,
        sourceFile,
        sourceExcerpt,
        factKey
      );
      const id = Number(info.lastInsertRowid);
      this.fts.upsertFact.run(id, subject, predicate, object, sourceExcerpt || '');
      return { id, inserted: true, updated: false };
    }

    const nextConfidence = Math.max(Number(found.confidence || 0), Number(confidence || 0));
    const mergedTags = Array.from(
      new Set([...safeJson(found.tags_json, []), ...safeJson(tagsJson, [])])
    );

    this.stmts.updateFactById.run(
      nextConfidence,
      JSON.stringify(mergedTags),
      now,
      sourceFile || found.source_file,
      sourceExcerpt || found.source_excerpt,
      found.id
    );
    this.fts.upsertFact.run(found.id, subject, predicate, object, sourceExcerpt || found.source_excerpt || '');
    return { id: found.id, inserted: false, updated: true };
  }

  getTopFacts(chatId, limit = 20) {
    return this.stmts.topFacts.all(chatId, limit).map((row) => ({
      ...row,
      tags: safeJson(row.tags_json, [])
    }));
  }

  searchFactsKeyword(chatId, queryText, limit = 20) {
    const q = buildFtsQuery(queryText);
    return this.stmts.keywordFacts.all(chatId, q, limit).map((row) => ({
      ...row,
      tags: safeJson(row.tags_json, []),
      fts_rank: row.rank
    }));
  }

  searchFactsExact(chatId, entities = [], predicates = [], tags = [], limit = 20) {
    if (!entities.length && !predicates.length && !tags.length) {
      return [];
    }
    return this.stmts
      .exactFacts
      .all(chatId, JSON.stringify(entities), JSON.stringify(predicates), JSON.stringify(tags), limit)
      .map((row) => ({
        ...row,
        tags: safeJson(row.tags_json, [])
      }));
  }

  insertEpisode({
    chatId,
    summary,
    entities = [],
    tags = [],
    salience = 0.7,
    timeRangeStart,
    timeRangeEnd,
    sourceFiles = [],
    embedding = []
  }) {
    const now = nowIso();
    const info = this.stmts.insertEpisode.run(
      chatId,
      summary,
      JSON.stringify(Array.from(new Set(entities))),
      JSON.stringify(Array.from(new Set(tags))),
      salience,
      timeRangeStart || now,
      timeRangeEnd || now,
      JSON.stringify(Array.from(new Set(sourceFiles))),
      JSON.stringify(embedding),
      now,
      now
    );
    const id = Number(info.lastInsertRowid);
    this.fts.upsertEpisode.run(id, summary);
    return id;
  }

  searchEpisodesKeyword(chatId, queryText, limit = 20) {
    const q = buildFtsQuery(queryText);
    return this.stmts.keywordEpisodes.all(chatId, q, limit).map((row) => ({
      ...row,
      entities: safeJson(row.entities_json, []),
      tags: safeJson(row.tags_json, []),
      source_files: safeJson(row.source_files_json, []),
      embedding: safeJson(row.embedding_json, []),
      fts_rank: row.rank
    }));
  }

  searchEpisodesExact(chatId, entities = [], tags = [], limit = 20) {
    if (!entities.length && !tags.length) {
      return [];
    }
    return this.stmts
      .exactEpisodes
      .all(chatId, JSON.stringify(entities), JSON.stringify(tags), limit)
      .map((row) => ({
        ...row,
        entities: safeJson(row.entities_json, []),
        tags: safeJson(row.tags_json, []),
        source_files: safeJson(row.source_files_json, [])
      }));
  }

  searchEpisodesVector(chatId, queryEmbedding, limit = 20, candidateLimit = 400) {
    const rows = this.stmts.episodeCandidatesForVector.all(chatId, candidateLimit).map((row) => ({
      ...row,
      entities: safeJson(row.entities_json, []),
      tags: safeJson(row.tags_json, []),
      source_files: safeJson(row.source_files_json, []),
      embedding: safeJson(row.embedding_json, [])
    }));

    return rows
      .map((row) => ({
        ...row,
        vector_score: cosineSimilarity(queryEmbedding, row.embedding)
      }))
      .sort((a, b) => b.vector_score - a.vector_score)
      .slice(0, limit);
  }

  upsertDocumentChunk({ chatId, path: docPath, chunkIndex, text, embedding = null, tags = [] }) {
    const now = nowIso();
    this.stmts.upsertDocument.run(
      chatId,
      docPath,
      chunkIndex,
      text,
      embedding ? JSON.stringify(embedding) : null,
      now,
      JSON.stringify(Array.from(new Set(tags)))
    );

    const row = this.db
      .prepare('SELECT id FROM documents WHERE chat_id = ? AND path = ? AND chunk_index = ?')
      .get(chatId, docPath, chunkIndex);
    if (row?.id) {
      this.fts.upsertDocument.run(row.id, docPath, text);
    }
  }

  searchDocumentsKeyword(chatId, queryText, limit = 20) {
    const q = buildFtsQuery(queryText);
    return this.stmts.keywordDocuments.all(chatId, q, limit).map((row) => ({
      ...row,
      tags: safeJson(row.tags_json, []),
      embedding: safeJson(row.embedding_json, null),
      fts_rank: row.rank
    }));
  }

  upsertOpenLoop({ chatId, text, tags = [], confidence = 0.7, sourceFile }) {
    this.stmts.insertOpenLoop.run(
      chatId,
      text,
      JSON.stringify(Array.from(new Set(tags))),
      confidence,
      nowIso(),
      sourceFile
    );
  }

  resolveOpenLoop(chatId, text) {
    this.stmts.resolveOpenLoop.run(nowIso(), chatId, text);
  }

  getOpenLoops(chatId, limit = 10, queryText = '') {
    if (queryText && queryText.trim()) {
      const like = `%${queryText.trim().slice(0, 80)}%`;
      const rows = this.stmts.listOpenLoopsByKeyword.all(chatId, like, limit);
      if (rows.length) {
        return rows.map((r) => ({ ...r, tags: safeJson(r.tags_json, []) }));
      }
    }
    return this.stmts.listOpenLoops.all(chatId, limit).map((r) => ({
      ...r,
      tags: safeJson(r.tags_json, [])
    }));
  }

  detectContradictions(chatId, minConfidence = 0.6) {
    const rows = this.stmts.contradictionCandidates.all(chatId, minConfidence);
    const detectedAt = nowIso();
    const found = [];
    for (const row of rows) {
      const objects = Array.from(new Set(safeJson(row.objects_json, []).map((v) => String(v))));
      if (objects.length < 2) {
        continue;
      }
      this.stmts.insertContradiction.run(chatId, row.subject, row.predicate, JSON.stringify(objects), detectedAt);
      found.push({ subject: row.subject, predicate: row.predicate, objects });
    }
    return found;
  }

  listContradictions(chatId) {
    return this.stmts.listContradictions.all(chatId).map((row) => ({
      ...row,
      objects: safeJson(row.objects_json, [])
    }));
  }

  dedupeFacts() {
    const groups = this.stmts.duplicateFactRows.all();
    for (const group of groups) {
      const ids = String(group.ids)
        .split(',')
        .map((x) => Number(x))
        .filter((n) => Number.isInteger(n));
      if (ids.length < 2) {
        continue;
      }
      const rows = this.db
        .prepare(`SELECT * FROM facts WHERE id IN (${ids.map(() => '?').join(',')}) ORDER BY confidence DESC, last_confirmed_at DESC`)
        .all(...ids);
      const keep = rows[0];
      for (let i = 1; i < rows.length; i += 1) {
        this.stmts.deleteFactById.run(rows[i].id);
      }
      if (keep) {
        this.fts.upsertFact.run(keep.id, keep.subject, keep.predicate, keep.object, keep.source_excerpt || '');
      }
    }
  }

  decayFactConfidence(chatId, daysOld, decayStep) {
    this.stmts.decayFacts.run(decayStep, chatId, daysOld);
  }

  normalizeScheduledJobRow(row) {
    if (!row) return null;
    return {
      ...row,
      id: Number(row.id),
      base_chat_id: Number(row.base_chat_id),
      scoped_chat_id: Number(row.scoped_chat_id),
      topic_id: Number(row.topic_id || 0),
      active: Number(row.active || 0) === 1
    };
  }

  createScheduledJob({
    baseChatId,
    scopedChatId,
    topicId = 0,
    title,
    kind = 'report',
    prompt,
    cronExpr,
    timezone,
    active = true,
    nextRunAt
  }) {
    const now = nowIso();
    const info = this.stmts.insertScheduledJob.run(
      baseChatId,
      scopedChatId,
      topicId,
      title,
      kind,
      prompt,
      cronExpr,
      timezone,
      active ? 1 : 0,
      nextRunAt,
      now,
      now
    );
    const id = Number(info.lastInsertRowid);
    return this.normalizeScheduledJobRow(
      this.db.prepare('SELECT * FROM scheduled_jobs WHERE id = ?').get(id)
    );
  }

  listScheduledJobs(scopedChatId) {
    return this.stmts.listScheduledJobsByScope
      .all(scopedChatId)
      .map((row) => this.normalizeScheduledJobRow(row));
  }

  getScheduledJob(id, scopedChatId) {
    return this.normalizeScheduledJobRow(
      this.stmts.getScheduledJobByScope.get(id, scopedChatId)
    );
  }

  setScheduledJobActive(id, scopedChatId, active) {
    const res = this.stmts.setScheduledJobActiveByScope.run(
      active ? 1 : 0,
      nowIso(),
      id,
      scopedChatId
    );
    return Number(res.changes || 0) > 0;
  }

  deleteScheduledJob(id, scopedChatId) {
    const res = this.stmts.deleteScheduledJobByScope.run(id, scopedChatId);
    return Number(res.changes || 0) > 0;
  }

  listDueScheduledJobs(nowIsoValue, limit = 10) {
    return this.stmts.listDueScheduledJobs
      .all(nowIsoValue, limit)
      .map((row) => this.normalizeScheduledJobRow(row));
  }

  markScheduledJobRun(id, {
    lastRunAt,
    nextRunAt
  }) {
    this.stmts.markScheduledJobRun.run(lastRunAt, nextRunAt, nowIso(), id);
  }

  touchScheduledJob(id) {
    this.stmts.touchScheduledJobUpdatedAt.run(nowIso(), id);
  }
}
