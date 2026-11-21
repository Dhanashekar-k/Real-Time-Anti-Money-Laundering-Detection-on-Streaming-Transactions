# Project 2 — Real-Time AML Detection: build manual

This folder expands `Project2_Realtime_AML_Detection.md` into a step-by-step manual. Your original plan is unchanged. Where this manual differs from it, `01_plan_review_and_changes.md` says why.

## Read in this order
1. `07_phases_and_cloud_plan.md` (15 min): **start here.** The build in 7 phases (P0–P6), what each one delivers, and which AWS service each one uses on your no-card Free plan.
2. `01_plan_review_and_changes.md` (20 min): the problems in the plan and the decisions that fix them. **Read before building anything.**
3. `05_schedule_and_cut_list.md` (10 min): the calendar, timeboxes and fallback triggers.
4. `03_aws_free_tier_and_costs.md` (20 min): do **Step 0 and Step 1 today**. They tell you whether your Free plan allows what Phase 5 needs.
5. `04_claude_code_playbook.md` (20 min): setup, the milestone loop, which code is yours.
6. `02_architecture_and_stack.md`: the reference you'll come back to (topics, tables, ports, repo layout).
7. `milestones/M00…M11`: one file per milestone. Claude Code reads these through `/milestone-start Mxx`.
8. `06_interview_and_resume.md`: role mapping, bullet templates, answers to rehearse.

## What's in `../starter-kit/` (copy into the repo root)
`CLAUDE.md` · `dot-claude/` → rename to `.claude/` after reviewing it (settings, hooks, 2 subagents, 4 skills) · `features/spec.yaml` · `contracts/` (3 Avro schemas, Postgres DDL, agent output schema) · `connectors/banks/` (Bank A, Bank B mappings) · `docs/decisions.md` (D01–D15 seeded) · `.gitignore` · `.env.example`.

## The 12 changes from your plan, in one screen
1. Exactly-once and p99 ≤ 2 s conflict. Build both modes and **measure** the trade-off.
2. Receiver-side ordering: legs + per-account event-time buffering + join by `tx_id`.
3. Windows 1h/24h/72h (HI-Small is only 10 days), plus a 24 h training burn-in.
4. Agent evidence comes from Iceberg, **cut off at alert time**, with no label column.
5. Neo4j dropped (no new claim). Redis dropped (no reader). Feature vector + SHAP stored on the alert.
6. Reconciliation reports lost, duplicates produced and duplicates suppressed, not just "0 dups".
7. Exact total order `(event_ts, tx_id)` and integer USD cents → parity can be **exact**.
8. Agent's primary metric: pattern accuracy against IBM pattern files, plus a 3-model cost/quality table.
9. MinIO is gone. Local storage is the filesystem; S3 only in the cloud. Official images only.
10. Flink 1.20 LTS (Java 17) for maturity.
11. `ANTHROPIC_API_KEY` must never be in your shell (Claude Code would bill it). The app uses `AML_ANTHROPIC_API_KEY`.
12. A walking skeleton for Flink before the full feature set; hard timeboxes everywhere.

## Your AWS account (new sign-up, Free plan, no card)
- Phases 0–4 run **entirely on your laptop**. AWS is used only in Phase 5, plus an optional small Glue rehearsal in Phase 1.
- Spark in the cloud runs on **AWS Glue 5.1**, not EMR: EMR needs the Paid plan. EKS nodes use only the instance types your plan allows (`m7i-flex.large`, `c7i-flex.large`; nothing `xlarge` or bigger).
- The plan ends after 6 months or when the credits run out, whichever comes first. Then AWS asks you to upgrade with a card. Do Phase 5 well inside that window.
- [Unclear] Whether Spot and a 24-vCPU EC2 quota are available on your plan. Check in Phase 0; the fallback is a 6-node cluster.

## Fast-track summary
About **174 focused hours** with the v2 additions (humans in the loop, drift, hallucination metrics, tenancy), about 4.5 weeks at 40 h/week. Expected cost: about $20–30 of AWS credits ([Guessing]) and about $20 of Claude API, both inside your credits if you follow the teardown rules.

## Today (≈ 2 h)
- [ ] Read 07, 01 and 05
- [ ] AWS Step 0 (credits left, end date, allowed services and instance types) and Step 1 (MFA, budgets, alerts)
- [ ] `echo $ANTHROPIC_API_KEY` prints nothing; `claude` → `/status` shows your subscription
- [ ] Create the repo, copy the starter kit + manual, first commit (04 §1)
- [ ] Message 4–6 people about a 30-minute review session in week 4
- [ ] Start `kaggle datasets download … -f HI-Small_Trans.csv` (M01 step 1)

Tomorrow: `/milestone-start M00` (Phase 0).
