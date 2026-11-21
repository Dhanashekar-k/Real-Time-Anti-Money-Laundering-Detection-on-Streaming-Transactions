# 07 — The build in 7 phases, and which cloud service each one uses

The 12 milestones (M00–M11) group into **7 phases**. Each phase ends with something that works and can be measured, so you can stop after any phase and still have a demo.

**Phases 0–4 run entirely on your laptop.** AWS appears only in Phase 0 (guardrails, about $0) and Phase 5 (the big-data and Kubernetes runs). Your account type forces that split, and it's also the cheapest design.

This file includes the revisions we agreed on: the human-in-the-loop loops, hallucination and drift numbers, tenant isolation, and the agent cascade. Each is marked **(v2)**. The matching steps are in the milestone files under "Revision v2 additions".

---

## Your AWS account: what it allows

You signed up through AWS's new project-based flow without a credit card. Facts (checked Nov 2026):

| Fact | Consequence for this project |
|---|---|
| Most new customers sign up with no payment method and get up to $200 in credits as part of the Free Tier.[^new] Accounts start with $100, and you can earn up to $100 more by completing activities.[^upg] | **Do the credit activities in Phase 0.** They add up to $100, roughly 4× what this project needs. |
| On this sign-up, the account is in the Free Tier. Once the credits run out or the account is 6 months old, you must upgrade to the Paid plan.[^upg] Upgrading means adding a payment method, and you can then set a monthly spend limit starting at $20.[^blog] | **Phase 5 has to happen inside your 6-month window.** Check "days remaining" on your Projects page. |
| EKS, EC2, Glue, Athena, S3, ECR and Bedrock are on the Free Tier list. EMR is Paid-plan only. EMR Serverless is not listed at all, which means it needs "advanced features".[^svc] | **Spark on 180M rows runs as an AWS Glue job, not on EMR.** Glue is the managed Spark service you're allowed. |
| On accounts created on or after July 15, 2025, the free-tier EC2 instance types are t3.micro, t3.small, t4g.micro, t4g.small, c7i-flex.large and m7i-flex.large.[^ec2] A July 2025 test of the Free plan found xlarge and larger instances were not available.[^cm] | **The EKS cluster is built from many 2-vCPU nodes** (m7i-flex.large: 8 GiB; c7i-flex.large: 4 GiB). That lowers the realistic throughput target (Phase 5). |
| IAM Identity Center is on the unsupported list unless you activate advanced features.[^svc] | Use the CLI sign-in your new account gives you. Don't plan on Identity Center. |
| Free-plan accounts can't receive other promotional credits.[^promo] | No AWS Activate or other credit stacking while on the Free plan. |

[Unclear] Two things the docs don't settle, so check them in Phase 0: whether **Spot** instances are allowed on your plan, and your **EC2 vCPU quota**. If Spot is refused, use on-demand flex instances; at these sizes the difference is cents per hour. If the quota is below 24 vCPUs, shrink the Phase 5 cluster.

---

## Phase map

| Phase | Milestones | What exists at the end | Runs on | AWS services | AWS credits (est.) | Hours (fast track, [Guessing]) |
|---|---|---|---|---|---|---|
| **0 · Foundation and guardrails** | M00 | Local stack up, repo + CI, Claude Code guardrails, AWS budgets, credit activities done | Laptop + AWS console | Budgets, Cost Explorer, S3 (Terraform state) | ~$0 | 6 |
| **1 · Batch lakehouse and model** | M01, M02 (+ v2 drift baseline) | HI-Small/Medium in Iceberg, features from the oracle, XGBoost beats rules, published comparison | Laptop (optional cents-level Glue rehearsal) | none required; optional S3 + Glue + Athena rehearsal | ~$0–1 | 27 |
| **2 · Streaming feature pipeline** | M03, M04 | Two banks streaming, config connectors, Flink features matching Spark, exactly-once restart test | Laptop | none | $0 | 38 |
| **3 · Scoring, alerting, tenancy** | M05 (+ v2 isolation and roles) | Every transaction scored, alerts → cases exactly once, latency matrix, banks isolated from each other | Laptop | none | $0 | 14 |
| **4 · AI and humans in the loop** | M06, M07 (+ v2 loops, cascade, aliasing, hallucination numbers) | Claude drafts cases, analyst UI, four human-in-the-loop loops, retraining behind gates | Laptop + **Claude API** (your Anthropic credits) | none (Bedrock optional) | $0 AWS · ~$20 Claude | 48 |
| **5 · Reliability and cloud scale** | M08 (+ v2 drift monitor), M09 | 20+ fault runs reconciled; drift experiment; **180M rows on Glue**; **EKS replay**; Cost Explorer receipt | Laptop + **AWS** | S3, Glue (Spark jobs + Data Catalog), Athena, EC2 (one helper box), ECR, EKS on free-tier node types, EBS gp3, Budgets | ~$20–30 | 27 |
| **6 · FDE validation and packaging** | M10, M11 | Bank C onboarded by config (timed), triage study, README with results table, video, resume bullets | Laptop | none | $0 | 14 |
| | | | | | **≈ $25–30 of $100–200** | **≈ 174** |

At 40 h/week that's about 4.5 weeks of focused work, or longer while Project 1 shares your time. My earlier advice stands: take Project 1 to a demoable state first. Phase 0 of this project is the exception. Do it now, because the credit activities and quota checks take calendar time.

---

## Phase 0 · Foundation and guardrails (M00)

**Goal:** one command starts everything locally, and nothing in AWS can surprise you.

**What gets built**
- Repo from the starter kit (`CLAUDE.md`, `.claude/` renamed from `dot-claude/`, contracts, spec, decisions)
- Docker Compose stack with profiles: Redpanda (Kafka + schema registry + console), Flink 1.20 JobManager/TaskManager, Postgres 17, MLflow 3, Airflow 3 (later), Prometheus/Grafana (later)
- Makefile, uv workspace, GitHub Actions CI (ruff, pytest, `mvn verify`)
- Terraform bootstrap: S3 state bucket (`use_lockfile`), budgets at $5 / $20 / $50

**AWS steps (console, about 45 min)**
1. Projects page: note your credit balance and **days remaining** on the Free plan. Write both into decision D16.
2. Complete the starter activities that earn credits (listed in the console's credits area); there's up to $100 extra on offer.[^upg]
3. Run `aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true` to confirm the allowed instance types for your account.[^ec2] Then check your EC2 vCPU quota under Service Quotas ("Running On-Demand Standard instances"), if your plan exposes it.
4. Create budgets ($5 early warning, $20, $50). AWS Budgets is on the Free Tier list.[^svc]

**Numbers produced:** none yet. You record the account facts.

**Exit gate:** `make up` is healthy, `make test` passes, budgets exist, and the credit balance and days remaining are written down.

---

## Phase 1 · Batch lakehouse and model (M01, M02)

**Goal:** turn raw CSVs into a correct, leakage-proof training table and a model that beats rules.

**What gets built**
- `csv_to_iceberg` (Spark 3.5 local): CSV → `raw_transactions` (Iceberg, day partitions)
- FX table, money helper (`Decimal`, half-even), patterns table, temporal 60/20/20 split
- `features/reference.py` oracle (you write it), plus golden vectors that Flink must match later
- `features.py` (Spark, `applyInPandas` calling the oracle) → `features_tx`
- Rules baseline, XGBoost training (MLflow), threshold tuning, test-set evaluation (once)
- **(v2) Drift baseline:** save per-feature histograms and the score distribution of the training window to `results/M02/<ds>/baseline_dist.json`. Phase 5's drift monitor compares against it.

**How the data is processed**
1. Read the CSV with an explicit schema (the header has a duplicate `Account` column).
2. Stable sort by (timestamp, file row) → `tx_id` = sequence number → total order `(event_ts, tx_id)`.
3. Amounts → minor units → USD cents, with exact `Decimal` arithmetic.
4. Each transaction splits into an OUT leg (sender) and an IN leg (receiver). Per account, the oracle computes window features (1h / 24h / 72h) as of just before each transaction.
5. Halves are joined into one vector of about 80 features.
6. Train on the training split with a 24 h burn-in, tune the threshold on validation, report on test once.

**Numbers produced:** row and laundering counts · split boundaries · leakage tests passing · **T2** F1 vs rules · **T3** next to published · per-pattern recall · baseline distributions (v2).

**Cloud:** none required. Optional, at the end of the phase: rehearse the Phase 5 Glue job on HI-Small (S3 + Glue Data Catalog + one small Glue Spark job + an Athena count). That costs cents and removes most of Phase 5's config risk. At $0.44 per DPU-hour, a 15-minute job on 6 DPUs costs $0.66.[^glue]

**Exit gate:** golden and leakage tests pass, the champion model is in MLflow, and `metrics.json` exists for HI-Small and HI-Medium.

---

## Phase 2 · Streaming feature pipeline (M03, M04)

**Goal:** the same features as Phase 1, computed live from two banks' streams, exactly once.

**What gets built**
- Replayer: reads Iceberg in `tx_id` order, paces by event time (e.g. 1 day per minute), and renders Bank A as CSV and Bank B as JSON. No labels are ever sent.
- Config-driven connectors: one YAML file per bank, validation, a dead-letter topic, and the `onboard-check` tool
- `tx.canonical` (Avro, schema registry, BACKWARD compatibility)
- Flink job (Java 17): dedupe by `tx_id` → split legs → per-account event-time buffering and features → join halves → `tx.features`. Exactly-once or at-least-once is a switch.

**How the data is processed**
1. Each bank-format record is mapped to canonical form; malformed rows go to `tx.dlq` with a reason.
2. Flink assigns event time and watermarks, buffers each account's legs until the watermark passes, then processes them in `(event_ts, tx_id)` order. That makes receiver-side features deterministic.
3. Feature vectors leave through a transactional Kafka sink, committed when each checkpoint completes.

**Numbers produced:** ingest rate per connector · DLQ counts · **T6** parity with Spark (target 100%) · restart test (exactly once, 5/5) · Flink-only latency for EOS vs ALO · late-event count.

**Cloud:** none.

**Exit gate:** parity ≥ 99.9% with zero late events and zero orphan halves; restart test passes.

---

## Phase 3 · Scoring, alerting, tenancy (M05 + v2)

**Goal:** every transaction gets a score with a model version, and alerts become cases exactly once, each bank seeing only its own.

**What gets built**
- Scorer: XGBoost `@champion`, batch inference, SHAP top-5 for candidates, alias hot-swap, refuses a `spec_version` mismatch
- Alert service: per-bank thresholds. The alert insert, case upsert, draft job and audit row commit in **one** Postgres transaction, then the Kafka offset is committed (the outbox pattern).
- **(v2) Tenant isolation:** Postgres row-level security on `bank_id`. The API sets `app.bank_id` and `app.role` per request. Roles are analyst and supervisor, with an `approvals` table for maker-checker. The DDL is in `contracts/postgres_schema.sql`, section v2.

**How the data is processed**
1. Score each batch, then attach top contributions to candidates.
2. Above the bank's threshold: insert with `ON CONFLICT DO NOTHING`; a replay becomes a counted no-op.
3. `latency_ms` = alert time − replay emit time is stored on every alert.

**Numbers produced:** **T5** latency matrix (EOS 1 s / 5 s / 10 s vs ALO × replay speed) · duplicates produced vs suppressed · hot-swap proof · (v2) an isolation test where a Bank A analyst reading Bank B data gets 0 rows.

**Cloud:** none.

**Exit gate:** replaying the same day twice creates 0 new cases; the latency matrix is filled in; the isolation test passes.

---

## Phase 4 · AI and humans in the loop (M06, M07 + v2)

**Goal:** an LLM that explains, humans who decide, and a system that learns from both, measured.

**What gets built**
- Evidence builder: a 2-hop graph cut off at the alert time, from Iceberg via DuckDB, never with labels
- **(v2) Account aliasing:** real account IDs are swapped for `ACCT-n` before any prompt and mapped back after, so no identifiers reach the LLM provider
- Claude client: structured output; **(v2) cascade** of Haiku 5.5 first, Sonnet 5.5 only when the validator fails. **(v2) Bounded tool use:** at most 6 read-only tool calls (`get_account_transactions`, `get_account_profile`, `expand_neighborhood`), with the alert-time cutoff enforced inside each tool. It's compared against the single-call baseline.
- Validator (you write it): IDs, accounts, amounts, numbers in free text, the alert-time cutoff; one retry, then "needs review"
- Review UI (React + TS) and FastAPI: queue, case graph, cited draft, SHAP panel, decision form, audit tab, study mode
- **Loop 1** (exists): analyst decisions → training set with a 2-day label delay → Airflow `train_nightly` → **promotion gate** → `@champion`
- **(v2) Loop 2:** draft edits and rejections with reason codes → `draft_feedback` table → added to the agent eval set → any prompt or model change must pass that eval **and** get a supervisor sign-off
- **(v2) Loop 3:** a random sample of transactions just below the threshold, plus every needs-review draft, goes to a QA queue; the reviews estimate the miss rate
- **(v2) Loop 4:** an operating-point tool showing alerts/day vs recall per bank, built from the validation split plus QA results. A threshold change is proposed by an analyst and approved by a supervisor (maker-checker) before the alert service reloads it.

**How the data is processed**
1. Case job → evidence JSON → aliasing → LLM → validator → stored draft → UI.
2. Decision → `decisions` + `audit_log` (one transaction) → nightly export to Iceberg → training set → gate → registry.
3. Edits → `draft_feedback` → eval set → prompt gate.

**Numbers produced (all from `eval/agent_eval.py` and scripts)**
- Hallucination rate **before** the validator: ungrounded IDs, amounts and accounts, per model
- Ungrounded numbers in free text
- **False-pattern rate on clean cases** (the draft claims a pattern when the alert was a false positive)
- Pattern accuracy on laundering cases, with a confusion matrix
- Needs-review rate · first-pass vs after-retry pass rate
- Prompt-injection test pass rate
- $ per 1,000 cases and p95 latency per model, with and without the cascade
- Single-call vs tool-using agent: accuracy, tokens, latency
- Prequential F1 by replay day · gate decisions logged · QA-sample miss-rate estimate

**Cloud:** no AWS. The LLM runs on your Claude API credits (about $20 total; prices in `03_…`). Optional: a Bedrock provider switch for the "data stays in the customer's AWS account" story. Bedrock is on the Free Tier list,[^svc] but it spends AWS credits, so keep it to a handful of calls.

**Exit gate:** the full loop runs on screen (alert → draft → decision → retrain → gate → new version serving, or a logged rejection), and `agent_eval.json` holds every number above.

---

## Phase 5 · Reliability and cloud scale (M08 + v2, M09)

**Goal:** prove it doesn't lose data under failure, detect drift, and run at real volume on AWS for a known cost.

**5a · Reliability and drift (laptop)**
- Prometheus + Grafana dashboards as code; a consumer-lag exporter
- Fault injection: kill the Flink TaskManager, restart the broker, kill the scorer, alert service or connectors; 20+ runs, each with a fault timeline
- Reconcile against a golden run: lost = 0, extra = 0, score mismatches = 0; duplicates produced and suppressed reported
- **(v2) Drift monitor:** daily PSI per feature and for the score distribution against the Phase 1 baseline, plus alert volume vs budget. Experiment: from replay day D, shift one bank's currency mix or amount distribution (an *induced* shift). Report detection delay, how much precision dropped, and the recovery after retrain + gate.

**5b · Big data on AWS: Spark over 180M rows (Glue)**
1. One **EC2** helper (c7i-flex.large, about 100 GB gp3) downloads HI-Large from Kaggle straight into **S3**; then terminate it.
2. Run **AWS Glue 5.1** Spark jobs. Glue 5.1 runs Spark 3.5.6 and Python 3.11,[^g51] which matches your local Spark 3.5.
   - `csv_to_iceberg` writes Iceberg tables on S3, registered in the **Glue Data Catalog**.
   - `features` runs the same oracle.
3. Validate the row and laundering counts with **Athena**.
4. Glue bills $0.44 per DPU-hour,[^glue] and one DPU is 4 vCPUs and 16 GB.[^dpu] Estimate: 10 workers (G.1X = 1 DPU each) × about 2 h for both jobs ≈ $9 [Guessing]; read the real figure from each run's DPU-seconds.
5. Option: Glue 6.0 is 30% cheaper per DPU-hour but runs Spark 4.1.1,[^g60] which would no longer match your local Spark 3.5. Keep 5.1 unless cost becomes a problem.

**5c · Streaming at scale on AWS (EKS)**
- **ECR** holds the images. Build them for **linux/amd64**, because the allowed larger instance types (the flex instances) are x86. Use `docker buildx` or GitHub Actions.
- **EKS** cluster with managed node groups using only allowed instance types:
  - kafka: 3 × m7i-flex.large (Strimzi, KRaft)
  - flink: 4 × m7i-flex.large (Flink Kubernetes Operator)
  - apps: 3 × c7i-flex.large
  - system: 2 × m7i-flex.large (operators, Postgres, schema registry, kube-prometheus-stack)
  - 24 vCPUs total, gp3 volumes, public subnets with no NAT, an S3 gateway endpoint, ClusterIP services only
- **Rate ladder:** step from 5k up to 40k tx/s and find the highest rate where consumer lag stays flat. On 2-vCPU nodes, [Guessing] expect 15–30k tx/s, below the original 50k. Report the measured number and the bottleneck you fixed; that's a better story than hitting 50k on big machines.
- **Volume:** if sustained ≥ 30k tx/s, replay all 180M rows (under 2 h). Otherwise replay a 20-day slice (about 37M rows) and say so.
- One fault run at scale (delete a broker pod and a TaskManager pod) and reconcile it.
- Cost: about $1.3/h while up [Guessing]. Session A (smoke test) about 2.5 h, Session B (scale) about 5 h, so roughly $10.
- `make cloud-down` → `make teardown-check` → Cost Explorer the next day.

**Numbers produced:** **T7** · drift delay, drop and recovery (v2) · **T1** Glue runtime and $ · **T4** sustained tx/s · **T5** p99 at that rate · **T11** total AWS $.

**Exit gate:** every number above has a results file; teardown check comes back CLEAN; Cost Explorer screenshot saved.

**If the Free plan blocks something** (instance type, quota, credits): shrink the cluster first (6 nodes). If that still fails, upgrade to the Paid plan with a **$30 spend limit**. Paid plans can cap monthly spend from $20.[^blog] Record the choice as a decision.

---

## Phase 6 · FDE validation and packaging (M10, M11)

**Goal:** prove the FDE claims and make everything findable in 60 seconds.

**What gets built**
- Bank C, a deliberately awkward format, onboarded by YAML only. The run is timed and counts code changes. Optionally, an LLM proposes the mapping and a human approves it (another maker-checker).
- Triage study (n stated): baseline vs assisted, time per case, decision accuracy, draft acceptance
- README with a generated results table, architecture diagram, limitations; a 3-minute video; resume bullets filled from `results/` only

**Numbers produced:** **T8**, **T9**, **T10**, plus the final table of everything.

**Cloud:** none. The scale run already happened and the cluster is destroyed. The receipts live in `results/`.

**Exit gate:** `make demo` works from a clean clone in under 30 minutes; the repo is public.

---

## Cloud services by phase

| AWS service | Phase | Used for | On your Free plan? | Est. credits |
|---|---|---|---|---|
| Budgets, Cost Explorer | 0, 5 | Alerts and the final receipt | Budgets: yes[^svc] | $0 |
| S3 | 0, 1 (opt.), 5 | Terraform state, landing zone, Iceberg warehouse, checkpoints | Yes[^svc] | < $1 |
| Glue (Spark jobs + Data Catalog) | 1 (opt. rehearsal), 5 | Spark over 180M rows; Iceberg catalog | Yes[^svc] | ~$13 |
| Athena | 1 (opt.), 5 | Validate counts with SQL | Yes[^svc] | cents |
| EC2 (one helper) | 5 | Kaggle → S3 download | Yes, allowed types only[^ec2] | < $1 |
| ECR | 5 | Container images | Yes[^svc] | cents |
| EKS (+ EC2 nodes, EBS gp3) | 5 | Strimzi Kafka, Flink operator, apps, monitoring | Yes; nodes limited to allowed types[^svc][^ec2] | ~$10 |
| Bedrock | 4 (optional) | Alternate Claude provider inside AWS | Yes[^svc] | cents |
| EMR / EMR Serverless | — | **Not used**: EMR is Paid-plan only and EMR Serverless needs advanced features[^svc] | No | — |
| MSK | — | Not used (Strimzi instead); Paid-plan only[^svc] | No | — |

## Where to start
1. **Now:** Phase 0's AWS steps (credits, days remaining, allowed instance types, budgets). These take minutes but block Phase 5 if skipped.
2. **Then:** Project 1 to its demoable checkpoint.
3. **Then:** Phases 1 → 6 in order. Phases 1–4 need no AWS at all, so the 6-month clock only matters for Phase 5. Check days remaining before you start Phase 4, and plan Phase 5 inside the window.

[^new]: https://aws.amazon.com/about-aws/whats-new/2026/09/New-AWS-Builder-Experience
[^upg]: https://docs.aws.amazon.com/accounts/latest/reference/upgrade-account.html
[^blog]: https://aws.amazon.com/jp/blogs/aws/aws-reimagines-the-getting-started-experience/
[^svc]: https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html
[^ec2]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-free-tier-usage.html
[^cm]: https://dev.classmethod.jp/en/articles/try-new-aws-free-tier-2025/
[^promo]: https://aws.amazon.com/cn/free/faqs/
[^glue]: https://aws.amazon.com/glue/pricing
[^dpu]: https://aws.amazon.com/tw/glue/pricing/
[^g51]: https://aws.amazon.com/about-aws/whats-new/2025/11/aws-glue-5-1
[^g60]: https://aws.amazon.com/blogs/big-data/introducing-aws-glue-6-0-for-apache-spark/
