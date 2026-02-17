#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRANCH="${1:-master}"
MESSAGE="${2:-update}"
PAT="${GITHUB_PAT:-}"
OWNER="${GITHUB_OWNER:-lotohov}"
REPO="${GITHUB_REPO:-banja_overlord_bot}"

if [[ -z "$PAT" ]]; then
  echo "GITHUB_PAT is not set"
  echo "Usage: GITHUB_PAT=... ./publish_with_pat.sh [branch] [message]"
  exit 1
fi

cd "$REPO_DIR"

git add -A

if git diff --cached --quiet; then
  echo "No staged changes. Nothing to commit."
else
  git commit -m "$MESSAGE"
fi

git push "https://${PAT}@github.com/${OWNER}/${REPO}.git" "$BRANCH"

echo "Done: pushed to ${OWNER}/${REPO} branch ${BRANCH}"
