---
name: milestone-start
description: Start a milestone from the stream-aml build manual. Reads the milestone file, decisions and prior results, then produces an implementation plan for the user to approve. Use when the user types /milestone-start Mxx.
argument-hint: "[M00-M11]"
disable-model-invocation: true
---

Start milestone $ARGUMENTS of the stream-aml manual.

1. Read, in this order:
   - `CLAUDE.md`
   - the file in `manual/milestones/` whose name starts with "$ARGUMENTS"
   - `docs/decisions.md`
   - `features/spec.yaml` (if the milestone touches features, Flink, Spark or the scorer)
   - the `results/` files of earlier milestones that this one depends on (list which ones you read)
2. Check the current repo state: `git status`, `git log --oneline -15`, and which make targets exist.
3. Produce a plan with these sections and nothing else:
   - **Goal and Definition of Done**: copied from the milestone file, plus the exact commands that will prove each DoD item.
   - **Inputs check**: each input the milestone needs, and whether it exists now (yes/no + path).
   - **Steps**: numbered, each with: files to create/change, who writes it (`CC` = you, `HUMAN` = the user per CLAUDE.md rule 4), and the test that proves it.
   - **Tests to write first**: names and what each asserts.
   - **Human-owned parts**: what you will scaffold with `TODO(human)` and the brief you will give the user for each.
   - **Risks**: the three riskiest assumptions, and the cheapest early check for each.
   - **Timebox**: the milestone's timebox and fallback trigger from the manual; propose a checkpoint at 50% of the timebox.
4. Stop and wait for the user to approve or edit the plan. Do not write code in this turn.
