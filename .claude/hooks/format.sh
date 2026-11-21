#!/usr/bin/env bash
# PostToolUse hook for Edit|Write: format the file Claude just touched.
# Never blocks (always exits 0); formatting problems surface in `make lint` instead.

INPUT="$(cat)"
FILE="$(printf '%s' "$INPUT" | python3 -c 'import sys, json
try:
    print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))
except Exception:
    print("")')"

[ -z "$FILE" ] && exit 0
[ -f "$FILE" ] || exit 0

case "$FILE" in
  *.py)
    if command -v ruff >/dev/null 2>&1; then
      ruff format --quiet "$FILE" >/dev/null 2>&1
      ruff check --fix --quiet "$FILE" >/dev/null 2>&1
    fi ;;
  *.tf)
    command -v terraform >/dev/null 2>&1 && terraform fmt "$FILE" >/dev/null 2>&1 ;;
esac

exit 0
