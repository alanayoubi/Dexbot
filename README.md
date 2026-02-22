# Dexbot: Codex for Everyday Automation

Bring the power of Codex to be your own, the most powerful AI assistant you can have.

Dexbot brings Codex power into daily life and business workflows through Telegram.
It gives you a practical way to run a full Codex agent from chat, with memory, skills, scheduling, files, voice, and topic-scoped sessions.

Why this project exists:
- Codex is one of the most capable coding/automation agents available.
- Existing Telegram agent stacks are often not optimized for Codex-native workflows.
- Dexbot is designed to make Codex usable as a real, always-available automation operator.

Important:
- Users bring their own Codex account/subscription (`codex login` on their machine).
- Users bring their own Telegram bot token.
- This project is a bridge/runtime, not a shared hosted service.

## Quick Start

```bash
git clone https://github.com/alanayoubi/Dexbot.git
cd Dexbot
npm install
npm run onboard
npm run start
```

## What Is Implemented

### Memory layers
- Working memory (ephemeral, per turn):
  - current goal, plan, assumptions, active constraints, tool result summary
  - generated in runtime only, not persisted
- Session memory (persisted per chat/session):
  - `memory/sessions/<chat>/<session>/session_summary.md`
  - `memory/sessions/<chat>/<session>/session_key_facts.json`
- Long-term canonical memory (human-auditable markdown):
  - `memory/daily/YYYY-MM-DD.md` (append-only)
  - `memory/MEMORY.md`
  - `memory/USER.md`
  - `memory/IDENTITY.md`
  - `memory/SOUL.md`
  - `memory/HEARTBEAT.md`
  - `memory/TOOLS.md`
- Long-term derived index (disposable/rebuildable):
  - SQLite tables: `facts`, `episodes`, `documents`, `open_loops`, `contradictions`
  - FTS/BM25: `facts_fts`, `episodes_fts`, `documents_fts`
  - vector embeddings (deterministic local embeddings) for episode/document hybrid retrieval

### Pipelines
- Write pipeline (`retain -> reflect -> index`) on every turn:
  - append daily journal entry
  - extract important facts (max configured)
  - extract major episode summaries (max configured)
  - update session summary/key facts
  - update curated memory files (`MEMORY.md`, `USER.md`)
  - index files/chunks into `documents` + FTS + embeddings
- Retrieval pipeline (`hybrid -> filters -> rerank -> capped injection`):
  - vector search on episodes
  - keyword FTS search on facts/episodes/documents
  - exact metadata/entity/tag matching
  - confidence filter + recency bias + project bias
  - reranked final memory injection with strict section caps and token cap
- Always-on personality/identity:
  - `memory/IDENTITY.md` and `memory/SOUL.md` are silently applied on every turn via developer instructions
  - never shown to the user unless explicitly requested
- Heartbeat maintenance (scheduled, default every 24h):
  - compress old daily logs into weekly summaries
  - fact dedupe
  - confidence decay for stale facts
  - contradiction detection
  - keep curated files compact
- Self-healing transport:
  - automatic reconnect when app-server socket drops
  - auto-respawn of `codex app-server` when unavailable
  - pending request recovery + retry path for transient disconnects

## Setup

```bash
cp .env.example .env
npm install
npm run start
```

For guided onboarding (recommended for new users):

```bash
npm run onboard
```

The onboarding wizard:
- checks `codex` is installed
- checks `codex login status` and reports if you're already authenticated
- can run `codex login` only when you choose it
- detects existing `.env` setup and shows current Telegram/security config
- offers modes: keep current, update existing, or reset from scratch
- collects Telegram settings
- writes `.env` and sets secure permissions (`chmod 600`)

Images are supported in both directions:
- send photo/image documents to the bot (vision input)
- bot can send image outputs back to Telegram when generated/fetched by Codex

Voice messages are supported with local Whisper. Default config expects `whisper` on your PATH.
You can tune model/language/timeouts with `WHISPER_*` env vars in `.env`.
For faster turnaround, persistent mode keeps the Whisper model loaded between messages (`WHISPER_PERSISTENT=true`).

## Commands

- `/start`
- `/newsession`
- `/memory`
- `/heartbeat`
- `/schedule`
- `/skill`
- `/restart`
- `/autostart on|off|status`
- `/chatid`
- `/whoami`

Also supported:
- Send a normal text message
- Send a photo or image document (optionally with caption prompt)
- Send a Telegram voice note (`voice`) or audio file (`audio`); bot transcribes locally and responds without echoing transcript text

## Skills

Skills are Codex-style capability packs stored as:

```text
<skills-root>/<skill-name>/SKILL.md
```

Format matches modern skill conventions (OpenAI/Anthropic style): required `SKILL.md` with YAML frontmatter (`name`, `description`) and optional `scripts/`, `references/`, `assets/` folders.

Default skills root is:

```text
.agents/skills
```

Telegram command examples:

```text
/skill list
/skill paths
/skill create sales-page | Build persuasive landing pages | Gather offer details, draft sections, include CTA options.
/skill show sales-page
/skill run sales-page | Create a hero + offer section for a fintech B2B page.
/skill delete sales-page
```

You can also trigger a skill directly in normal chat with:

```text
$sales-page <task>
```

Natural creation is also supported in plain conversation, for example:

```text
Create a skill called sales-page for writing high-conversion landing pages.
Create a cold-email skill for outbound campaigns: ask for ICP, offer, CTA, then draft 3 variants.
```

## Proactive Cron + Heartbeats

Use `/schedule` to make the bot proactive in the current chat/topic scope.

Examples:

```text
/schedule add daily 09:00 | Send my morning report: priorities, open loops, and blockers.
/schedule add cron 0 18 * * 1-5 | Send end-of-day summary with decisions and pending items.
/schedule add heartbeat 13:30 | Check in with me and ask for my top focus right now.
/schedule list
/schedule pause 3
/schedule resume 3
/schedule run 3
/schedule remove 3
```

Notes:
- Schedule scope is topic-aware (`topic_id`): each topic has isolated jobs.
- Default timezone is local machine timezone (override per job with `tz=Area/City`).
- `/heartbeat` remains memory-maintenance; proactive pings use `/schedule ...`.
- Natural-language schedule intents are supported (e.g., "set a daily schedule at 9am to send my morning report").

## Voice Performance Tips

- Set `WHISPER_PERSISTENT=true` to avoid model reload on every clip
- Use a smaller/faster model like `WHISPER_MODEL=base` or `WHISPER_MODEL=tiny`
- Set `WHISPER_LANGUAGE=en` (or your language) to skip repeated language detection

## Full-access mode

Default env includes:
- `CODEX_APPROVAL_POLICY=never`
- `CODEX_SANDBOX=danger-full-access`

Use only on an isolated machine.

## Sharing With Friends

Yes, each person can use their own Codex account/subscription with this project.

How auth works:
- This bridge uses the local Codex CLI on that machine.
- Each user authenticates locally with `codex login` (ChatGPT login flow or API key flow).
- The bridge then uses that local logged-in Codex identity when running `codex app-server`.

Branding:
- Set `BOT_APP_NAME` (default `Dexbot`) to customize bot-facing app name text.

Recommended distribution flow:
1. Publish this repo to GitHub (without `.env`).
2. User clones the repo.
3. User runs `npm install`.
4. User runs `npm run onboard`.
5. User runs `npm run start`.

Publish to GitHub (maintainer flow):

```bash
brew install gh
gh auth login --web
bash scripts/publish-github.sh <owner/repo> public
```

Or with npm:

```bash
npm run publish:github -- <owner/repo> public
```

Security reminder for shared installs:
- Keep `.env` out of git.
- Use strict allowlists (`ALLOWED_TELEGRAM_USER_IDS`, optional `ALLOWED_TELEGRAM_CHAT_IDS`).
- Keep `.env` mode at `600`.

## Security Notes

- Access gate is user-ID based using `ALLOWED_TELEGRAM_USER_IDS`.
- You can additionally lock chat IDs with `ALLOWED_TELEGRAM_CHAT_IDS`.
- `TELEGRAM_PRIVATE_ONLY=true` blocks group/channel usage (recommended).
- Secrets in chat (API keys/tokens/password-like strings) are redacted before memory/db persistence.
- `.env` is used for runtime secrets/config and is gitignored.

## Group Topics (Per-Topic Sessions)

You can run the bot in a Telegram forum/supergroup with topic isolation:
- each topic gets its own memory/session scope
- `/newsession` and `/memory` operate on the current topic scope

Safe setup:
1. Set `TELEGRAM_PRIVATE_ONLY=false`
2. Set `ALLOWED_TELEGRAM_USER_IDS` to your user id(s)
3. Set `ALLOWED_TELEGRAM_CHAT_IDS` to only your allowed group id(s)
4. Set `TELEGRAM_GROUP_REQUIRE_MENTION=true` to make group handling predictable (`@bot ...` or reply-to-bot only)

This keeps one bot, but isolates context by topic.

### Create A Forum Topic From CLI

Use the helper script:

```bash
./scripts/create-telegram-topic.sh "Facebook Ads"
```

Or via npm:

```bash
npm run tg:topic -- "Facebook Ads"
```

Notes:
- reads `.env` automatically
- auto-selects the first `-100...` id in `ALLOWED_TELEGRAM_CHAT_IDS` unless you pass a `chat_id`
- verifies the chat is forum-enabled before creating the topic

## Auto-start On Mac Reboot

Use Telegram command:
- `/autostart on` to enable boot auto-start (`launchd`)
- `/autostart off` to disable boot auto-start
- `/autostart status` to check current state

When enabled, the bot is installed as a user LaunchAgent and will come back after Mac restart.

## Multi-agent

Multi-agent is enabled for this bot runtime:
- app-server is started with `--enable multi_agent`
- per-thread config includes `features.multi_agent=true`
- developer instructions encourage silent internal delegation for complex tasks
- hidden complexity threshold (`MEMORY_MULTI_AGENT_COMPLEXITY_THRESHOLD`) controls strategy:
  - below threshold: prefer single-agent
  - above threshold: strongly push multi-agent delegation

## Tests

```bash
npm test
```

Included tests:
- remember timezone
- recall decision from two weeks ago
- avoid irrelevant old recall
- contradiction detection in heartbeat

## Key Files

- `src/index.js`
- `src/telegram.js`
- `src/skills.js`
- `src/memory.js`
- `src/db.js`
- `src/codex-client.js`
- `.env.example`
