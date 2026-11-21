# 04 — Working with Claude Code on this project

The goal: Claude Code does the scaffolding, infrastructure, glue code and tests quickly, while you **own** the parts interviews go deep on, and you can explain every line of them. The starter kit makes that the default, not a matter of willpower.

---

## 1. One-time setup (30 min)

1. **Check Claude Code is up to date** and that you're on your Max subscription:
   - `claude --version`, and update if it's old.
   - Start `claude` in any folder → `/status`. It should show your Claude subscription, not an API key.
   - `echo $ANTHROPIC_API_KEY` must print nothing. If it prints a key, remove it from `~/.zshrc`. Claude Code uses that key in place of your subscription when it's set.[^auth]
2. **Create the repo:**
   ```bash
   mkdir -p ~/code/stream-aml && cd ~/code/stream-aml && git init -b main
   cp -R "/Users/shekar/Desktop/project 2/starter-kit/." .
   mv dot-claude .claude   # shipped as dot-claude/; read settings.json + hooks first
   cp -R "/Users/shekar/Desktop/project 2/manual" ./manual
   cp .env.example .env    # then fill in AML_ANTHROPIC_API_KEY etc.
   git add -A && git commit -m "chore: starter kit and manual"
   ```
3. **Check that the hooks have their dependency:** the hook scripts use `python3` (it ships with Xcode command-line tools). Run `python3 --version`.
4. **Start Claude Code in the repo root:** `cd ~/code/stream-aml && claude`. Trust the folder when asked; project allow rules only apply after you do.[^settings] Run `/status` and check that "Project settings" appears under setting sources.
5. **Smoke-test the guardrails:**
   - Ask: "run `terraform apply -auto-approve`". The deny rule or the guard hook should block it.
   - Ask: "read .env". It should be denied.
   - Ask: "create a file tmp.py with badly formatted code". The format hook should run ruff on it (once ruff is installed in M0).
6. **Pick models:** `/model` → Opus 5.5 for M2's evaluation design, M4, M6's validator and debugging. Sonnet 5.5 for scaffolding (compose, Terraform, UI). Switching mid-session re-reads the context uncached, so switch at milestone boundaries.

---

## 2. What's in the starter kit and why

| File | What it does |
|---|---|
| `CLAUDE.md` | Always-loaded project instructions: source of truth, hard rules (no invented metrics, no label leakage, human-owned code, cloud safety), versions, commands. Kept short; details live in `manual/`. |
| `.claude/settings.json` | Permissions. **Allow**: safe local commands (`make`, `uv run`, `pytest`, `docker compose`, `mvn`, read-only `kubectl`/`aws`). **Ask**: `make cloud-*`, `make destroy*`, `git push`, `docker compose down -v`. An ask rule wins over a broader allow.[^perm] **Deny**: reading `.env`, `~/.aws`, `~/.kaggle`, raw data; `-auto-approve`. Also wires up the hooks. |
| `.claude/hooks/guard-bash.sh` | PreToolUse hook. Blocks raw `terraform apply/destroy` (must go through approved `make cloud-*` targets), destructive `aws … delete/rm/terminate`, namespace deletion, `rm -rf` on home or root, and anything touching `ANTHROPIC_API_KEY`. Exit code 2 blocks the command and tells Claude why.[^hooks] |
| `.claude/hooks/format.sh` | PostToolUse hook on Edit/Write: `ruff format` + `ruff check --fix` for `.py`, `terraform fmt` for `.tf`. |
| `.claude/agents/reviewer.md` | Read-only senior reviewer subagent (Opus). Checks leakage, ordering, idempotency, exactly-once, concurrency, test gaps. Run it before closing any milestone. |
| `.claude/agents/cost-guard.md` | Read-only reviewer for `infra/` changes. Hunts NAT gateways, LoadBalancer services, orphanable volumes, missing tags, instance types outside your Free plan's list, uncapped Glue jobs. |
| `.claude/skills/milestone-start` | `/milestone-start M04` reads the milestone file, the decisions and earlier results, then produces a plan for your approval. |
| `.claude/skills/milestone-close` | `/milestone-close M04` runs the DoD commands, saves outputs to `results/`, regenerates `docs/results.md`, runs the reviewer, drafts the decision entries and commit message. |
| `.claude/skills/quiz` | `/quiz M04` interviews you one question at a time on the code that exists, grades you, and logs weak spots to `docs/quiz_log.md`. |
| `.claude/skills/teardown-check` | `/teardown-check` lists billable AWS resources with read-only commands. |
| `features/spec.yaml` | The feature contract (exact semantics). |
| `contracts/*` | Avro schemas, Postgres DDL, agent output schema. |
| `connectors/banks/*.yaml` | Bank A and Bank B mapping configs. |
| `docs/decisions.md` | The decision log, seeded with D01–D15. |

Skills live in `.claude/skills/<name>/SKILL.md` and run as `/name`. `disable-model-invocation: true` keeps Claude from triggering them on its own.[^skills] Subagents live in `.claude/agents/`. Ask for one by name ("use the reviewer subagent") or @-mention it.[^sub]

---

## 3. The milestone loop (follow it every time)

```
git switch -c m04-flink                    # one branch per milestone
claude                                     # fresh session per milestone (or /clear)
[Shift+Tab until the mode line shows plan mode]
/milestone-start M04
  → read the plan. Push back: "why X?", "where's the test for Y?", "cut Z".
  → approve.
[Shift+Tab back to normal or accept-edits mode]
  → scaffolding: let it run. Check `git diff --stat` every ~30 min.
  → human-owned parts: /output-style learning
       Claude leaves TODO(human) markers plus a short brief, then waits.[^styles]
       You write the code, say "done", get an Insight back, continue.
  → tests green → "use the reviewer subagent on this branch"
  → fix findings
/milestone-close M04
  → check results/M04/*.json yourself. Do the numbers make sense?
  → commit (Claude drafts the message; you run git push yourself)
/quiz M04
/output-style default
```

**Rules of thumb**
- **Plan first** on anything bigger than one file. Ten minutes of plan review saves two hours of rework.
- **Tests before code** for the oracle, golden vectors, parity, validator, reconciliation and promotion gate. Prompt: *"Write the failing tests first from the spec in features/spec.yaml section X. Don't implement yet."*
- **Commit after every green step.** It makes `git diff` reviewable and lets you roll back a bad direction cheaply.
- **Read every diff in human-owned or infra files.** For everything else, read `--stat` plus a sample.
- **Never paste huge logs.** Redirect to a file and ask: *"Read the last 200 lines of var/logs/flink-tm.log and the first stack trace."*
- **When Claude invents an API** (likely with Flink, which changed a lot between 1.x and 2.x): *"Check against the Flink 1.20 docs or the jar sources before changing code. Run `mvn dependency:sources` and grep the class."*
- **When it loops on a failing test:** stop it. Ask for a hypothesis list ranked by likelihood with a one-line check for each, run the checks yourself, then let it fix the confirmed cause.
- **Context hygiene:** one milestone per session. If a session gets long, `/compact` with a focus ("keep the parity mismatch analysis and the open TODOs").

---

## 4. Human-owned code (you write it; Claude scaffolds and tests it)

| File | Why it's yours | Claude Code does |
|---|---|---|
| `features/reference.py` → `compute_account_legs()` | It *is* the definition of the features. You'll be asked how a window is computed. | Golden tests, Spark wrapper, performance profiling |
| `AccountFeatureFunction.java` | Event-time buffering, ordering, eviction, timers: the core streaming question | Maven setup, POJOs, test harness, golden-vector tests |
| `JoinHalvesFunction.java`, `DedupeFunction.java` | State lifecycle and cleanup timers | Same |
| `FeatureJob.java`: checkpoint + sink section | Exactly-once is the first follow-up question in any streaming interview | Everything else in the job |
| `services/agent/validator.py` | Grounding guarantee; the AI-engineer centerpiece | Bad-draft fixtures, tests, integration |
| `ops/reconcile.py` → comparison logic | What "0 lost" means | Data collection, report formatting |

`CLAUDE.md` tells Claude not to implement these unless you say "implement it". If you're blocked for more than 45 minutes on one of them, say "implement it, then explain it line by line". Then **retype it yourself without looking** and run the tests. Retyping is what makes you able to explain it.

---

## 5. Prompt library (copy, edit, paste)

**Plan critique**
> Before we start: list the three riskiest assumptions in this plan, and the cheapest experiment that would disprove each.

**Spec → tests**
> From features/spec.yaml, write table-driven pytest cases for: same-minute ties, a window boundary exactly at t−W (excluded), self-transfer, first transaction of an account, cross-currency, and 2-cycle return. Hand-compute expected values in comments. Don't write the implementation.

**Explain to learn**
> Explain stream/flink-features/src/main/java/.../AccountFeatureFunction.java as if I'm defending it in a Flink interview: what state exists, when each timer fires, what happens on restart, what happens with a late event. Then ask me 3 questions.

**Bottleneck hunt**
> Throughput plateaus at N msg/s. Using the Grafana panels and service metrics, form 3 hypotheses (serialization, partitions/parallelism, batch size) and design a 10-minute experiment for each. Don't change code yet.

**Infra with cost constraints**
> Write Terraform for envs/scale per manual/milestones/M09_aws_scale_run.md: public subnets only, S3 gateway endpoint, no NAT, managed node groups (on-demand for kafka/system, Spot for flink/apps), Pod Identity, EBS CSI, default_tags, everything destroyable. Then run the cost-guard subagent on it.

**Honest results**
> Generate docs/results.md from results/**/*.json using tools/render_results.py. Any target without a results file stays "TBD (not measured)". Never type a number by hand.

**Decision entry**
> Add a decision to docs/decisions.md: context, options considered, decision, consequences, how to revisit. Keep it under 12 lines.

---

## 6. Headless use (careful)
`claude -p "<task>"` runs non-interactively. Good for repetitive jobs like "run make bench-local and summarize results/M05/latency_matrix.json". Two warnings:
- In non-interactive mode an `ANTHROPIC_API_KEY` in the environment is always used.[^auth] Keep it unset.
- Headless runs still obey permissions, so `make cloud-*` won't run unattended. Good.

## 7. Optional: MCP servers
You don't need any. For library docs, telling Claude Code to fetch the pinned version's official docs (Flink 1.20, Iceberg, Strimzi) is enough.

[^auth]: https://code.claude.com/docs/en/env-vars ; https://support.claude.com/en/articles/11145838
[^settings]: https://code.claude.com/docs/en/settings
[^perm]: https://code.claude.com/docs/en/permissions ("a matching ask rule prompts even when a more specific allow rule also matches")
[^hooks]: https://code.claude.com/docs/en/hooks-guide
[^skills]: https://code.claude.com/docs/en/skills
[^sub]: https://code.claude.com/docs/en/sub-agents
[^styles]: https://code.claude.com/docs/en/output-styles (Learning style: `TODO(human)` markers)
