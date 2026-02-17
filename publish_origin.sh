#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRANCH="${1:-master}"
MESSAGE="${2:-update}"

cd "$REPO_DIR"

git add -A

if git diff --cached --quiet; then
  echo "No staged changes. Nothing to commit."
else
  git commit -m "$MESSAGE"
fi

git push origin "$BRANCH"

echo "Done: pushed to origin/$BRANCH"
