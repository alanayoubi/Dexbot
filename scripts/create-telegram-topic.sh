#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

TOPIC_NAME="${1:-}"
CHAT_ID="${2:-}"

if [[ -z "$TOPIC_NAME" ]]; then
  echo "Usage: $0 \"Topic Name\" [chat_id]"
  exit 1
fi

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
  echo "Error: TELEGRAM_BOT_TOKEN is not set. Check $ENV_FILE."
  exit 1
fi

if [[ -z "$CHAT_ID" ]]; then
  IFS=',' read -r -a CHAT_IDS <<< "${ALLOWED_TELEGRAM_CHAT_IDS:-}"
  for id in "${CHAT_IDS[@]}"; do
    id_trimmed="$(echo "$id" | tr -d '[:space:]')"
    if [[ "$id_trimmed" == -100* ]]; then
      CHAT_ID="$id_trimmed"
      break
    fi
  done
fi

if [[ -z "$CHAT_ID" ]]; then
  echo "Error: chat_id is missing. Pass it as arg 2 or set ALLOWED_TELEGRAM_CHAT_IDS with a group id."
  exit 1
fi

BASE_URL="https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}"

get_chat_resp="$(curl -sS "${BASE_URL}/getChat?chat_id=${CHAT_ID}")"
if command -v jq >/dev/null 2>&1; then
  chat_ok="$(printf '%s' "$get_chat_resp" | jq -r '.ok')"
  is_forum="$(printf '%s' "$get_chat_resp" | jq -r '.result.is_forum // false')"
  title="$(printf '%s' "$get_chat_resp" | jq -r '.result.title // "unknown"')"
else
  chat_ok="$(printf '%s' "$get_chat_resp" | grep -o '"ok":[^,]*' | head -n1 | cut -d: -f2 | tr -d '" ')"
  is_forum="$(printf '%s' "$get_chat_resp" | grep -o '"is_forum":[^,}]*' | head -n1 | cut -d: -f2 | tr -d '" ')"
  title="unknown"
fi

if [[ "$chat_ok" != "true" ]]; then
  echo "Error: getChat failed."
  echo "$get_chat_resp"
  exit 1
fi

if [[ "$is_forum" != "true" ]]; then
  echo "Error: target chat is not a forum-enabled supergroup."
  echo "$get_chat_resp"
  exit 1
fi

create_resp="$(curl -sS -X POST "${BASE_URL}/createForumTopic" \
  -d "chat_id=${CHAT_ID}" \
  --data-urlencode "name=${TOPIC_NAME}")"

if command -v jq >/dev/null 2>&1; then
  ok="$(printf '%s' "$create_resp" | jq -r '.ok')"
  thread_id="$(printf '%s' "$create_resp" | jq -r '.result.message_thread_id // empty')"
  name="$(printf '%s' "$create_resp" | jq -r '.result.name // empty')"
else
  ok="$(printf '%s' "$create_resp" | grep -o '"ok":[^,]*' | head -n1 | cut -d: -f2 | tr -d '" ')"
  thread_id="$(printf '%s' "$create_resp" | grep -o '"message_thread_id":[0-9]*' | head -n1 | cut -d: -f2)"
  name="$TOPIC_NAME"
fi

if [[ "$ok" != "true" ]]; then
  echo "Error: createForumTopic failed."
  echo "$create_resp"
  exit 1
fi

echo "Created topic in '${title}': '${name}' (thread_id=${thread_id}, chat_id=${CHAT_ID})"
echo "$create_resp"
