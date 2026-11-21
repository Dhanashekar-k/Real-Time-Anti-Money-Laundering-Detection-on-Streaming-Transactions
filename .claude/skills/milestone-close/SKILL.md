---
name: milestone-close
description: Close a stream-aml milestone. Runs its Definition-of-Done checks, saves measured outputs to results/, regenerates docs/results.md, runs the reviewer subagent, drafts decision entries and a commit message. Use when the user types /milestone-close Mxx.
argument-hint: "[M00-M11]"
disable-model-invocation: true
---

Close milestone $ARGUMENTS.

1. Read the file in `manual/milestones/` whose name starts with "$ARGUMENTS" and extract every Definition-of-Done item.
2. For each DoD item, run the command that proves it (never a cloud-* target unless the user explicitly asked in this session). Capture outputs into `results/$ARGUMENTS/` as JSON (include git SHA, dataset, config, UTC timestamp). If a DoD item has no command, say so.
3. Run `make test` (and `make itest` if the stack is up). Report failures verbatim (first error + file:line), do not hide them.
4. Run `make results` to regenerate `docs/results.md` from `results/**/*.json`. Never type a number by hand. Missing measurements stay "TBD (not measured)".
5. Ask the reviewer subagent to review `git diff main...HEAD`. Summarize its blocker/major findings.
6. Draft entries for `docs/decisions.md` for any design choice made during this milestone (context, decision, alternatives, consequences, how to revisit; ≤ 12 lines each). Show them; write them only after the user agrees.
7. Output a checklist: each DoD item → PASS / FAIL / NOT RUN with the evidence path. Then a proposed conventional commit message. Do not commit or push unless the user says so.
8. Remind the user to run `/quiz $ARGUMENTS`.
