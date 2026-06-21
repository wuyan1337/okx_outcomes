#!/bin/bash
cd /opt/okx-outcomes-bot

while true; do
  inotifywait -r -e modify,create,delete,move     --exclude '(\.git|__pycache__|\.pyc$|/data/)'     /opt/okx-outcomes-bot/ 2>/dev/null

  sleep 5

  git add -A
  CHANGES=$(git diff --cached --stat)
  if [ -n "$CHANGES" ]; then
    SUMMARY=$(git diff --cached --name-only | head -5 | tr '\n' ', ')
    git commit -m "auto: ${SUMMARY%,}"
    git push origin main 2>/dev/null
  fi
done
