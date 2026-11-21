#!/usr/bin/env bash
# PreToolUse hook for Bash: blocks commands Claude Code must never run in this repo.
# Exit code 2 = block; the message on stderr is shown to Claude so it can adjust.
# Invoked as: bash "$CLAUDE_PROJECT_DIR"/.claude/hooks/guard-bash.sh  (no exec bit needed)

INPUT="$(cat)"
CMD="$(printf '%s' "$INPUT" | python3 -c 'import sys, json
try:
    print(json.load(sys.stdin).get("tool_input", {}).get("command", ""))
except Exception:
    print("")')"

block() {
  echo "Blocked by .claude/hooks/guard-bash.sh: $1" >&2
  exit 2
}

[ -z "$CMD" ] && exit 0

# ANTHROPIC_API_KEY (but not AML_ANTHROPIC_API_KEY): touching it would switch Claude Code's own billing.
KEY_RE='(^|[^A-Za-z0-9_])ANTHROPIC_API_KEY'
if [[ "$CMD" =~ $KEY_RE ]]; then
  block "never set or read ANTHROPIC_API_KEY (it changes Claude Code's own billing). The app uses AML_ANTHROPIC_API_KEY from .env."
fi

case "$CMD" in
  *-auto-approve*)
    block "no -auto-approve. Cloud changes go through 'make cloud-*' targets that the user approves." ;;
  *"terraform apply"*|*"terraform destroy"*)
    block "run Terraform only via 'make cloud-up' / 'make cloud-down' (user-approved), never directly." ;;
  *"aws "*" delete"*|*"aws "*" rm "*|*"aws "*"terminate-instances"*|*"aws "*"delete-"*)
    block "destructive AWS command. Ask the user to run it, or use a reviewed make cloud-* target." ;;
  *"kubectl delete namespace"*|*"kubectl delete ns "*)
    block "namespace deletion is not allowed from Claude Code." ;;
  *"rm -rf /"*|*"rm -rf ~"*|*'rm -rf $HOME'*|*"rm -rf .."*)
    block "dangerous rm target." ;;
  *".aws/credentials"*|*"kaggle.json"*)
    block "secrets files are off-limits." ;;
esac

# Reading the real .env (but not .env.example) with common pagers/printers.
ENV_RE='(cat|less|more|head|tail|bat|grep)[^|;&]*[[:space:]](\./)?\.env([[:space:]]|$|[;|&])'
if [[ "$CMD" =~ $ENV_RE ]]; then
  block "secrets files are off-limits (.env)."
fi

exit 0
