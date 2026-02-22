#!/usr/bin/env bash
set -euo pipefail

REPO="${1:-dexbot-codex-telegram}"
VISIBILITY="${2:-public}" # public|private

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI (gh) is not installed."
  echo "Install: brew install gh"
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "You are not logged in to GitHub CLI."
  echo "Run: gh auth login --web"
  exit 1
fi

if [[ "$VISIBILITY" != "public" && "$VISIBILITY" != "private" ]]; then
  echo "Visibility must be 'public' or 'private'."
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree is not clean. Commit changes before publishing."
  exit 1
fi

if git remote get-url origin >/dev/null 2>&1; then
  echo "Remote 'origin' already exists. Pushing..."
  git push -u origin main
  echo "Done."
  exit 0
fi

echo "Creating GitHub repo: $REPO ($VISIBILITY)"
gh repo create "$REPO" "--$VISIBILITY" --source=. --remote=origin --push
echo "Done."
