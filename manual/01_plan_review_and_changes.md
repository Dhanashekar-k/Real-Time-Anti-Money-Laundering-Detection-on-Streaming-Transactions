# 01 — Review of your plan, and what changes

Your plan is mostly sound. The time-based split, gradient-boosted trees ahead of a GNN, one feature spec with two implementations, "the agent explains, it never decides", and "only measured numbers" are all the right calls; keep them. What follows is what will hurt you if you build the plan as written. Most important items first.

Labels: **[Certain]** directly supported by evidence or logic · **[Likely]** strong inference · **[Unclear]** not enough information · **[Guessing]** filling a gap.

---

## A. Problems in the design itself

### A1. T5 (p99 ≤ 2 s) and T7 (exactly-once) conflict as written — [Certain]
Flink's Kafka sink in `EXACTLY_ONCE` mode writes inside Kafka transactions, and those commit only when a checkpoint completes. A downstream consumer with `isolation.level=read_committed` (which it must use, or it reads aborted records and you get duplicates) sees nothing until the commit. So the latency floor for anything after Flink is roughly the **checkpoint interval**, plus checkpoint duration. With a 10 s interval your p99 can't be under 2 s. Push the interval down to 1 s and, at HI-Large scale, checkpoints may take longer than the interval and cause backpressure.

**Change:** build both modes behind a flag and measure them:
- `EOS`: Kafka transactional sink, checkpoint interval ∈ {1 s, 5 s, 10 s}.
- `ALO`: at-least-once sink (output visible immediately), with duplicates removed downstream by the idempotent alert service (unique key on `tx_id`).

Report a latency × guarantee matrix. "I measured what exactly-once cost in latency and chose X for these reasons" is a much stronger interview story than either target alone.

### A2. Keying Kafka by sender does not order an account's *incoming* transfers — [Certain]
Your design says keying by account gives each account's events "in order, which is what keyed state in Flink needs". That holds only for the sender side. Account R's incoming transfers come from many senders, so they sit in many partitions, and after Flink's `keyBy(receiver)` they arrive in no guaranteed order. Receiver-side features (fan-in, pass-through, hold time) would then depend on arrival order. Arrival order isn't reproducible, so the parity test (T6) fails at random.

**Change** (details in M04):
1. Explode each transaction into two **legs**: `OUT` keyed by sender, `IN` keyed by receiver.
2. `keyBy(account)` → a `KeyedProcessFunction` that **buffers legs in state and processes them in `(event_ts, tx_id)` order once the watermark passes**. This is the standard event-time sort-per-key pattern.
3. `keyBy(tx_id)` → join the sender half and the receiver half back into one feature vector.

This is the hardest code in the project. It's also exactly what a streaming interviewer will dig into.

### A3. HI-Small spans 10 days, so your 7-day windows are never "warm" in training — [Certain]
The IBM paper lists HI-Small as 10 days, HI-Medium as 16 and HI-Large as 97.[^altman] With a temporal 60/20/20 split, the HI-Small training period is about the first 6 days. A 7-day window is cut short by the start of the data for **every** training row, but it is (nearly) full in the test period. That's a distribution shift you would build in yourself.

**Change:** default windows `1h, 24h, 72h`. Keep `7d` as an optional window, used only on HI-Large. Exclude the first 24 h of data from training rows (burn-in). Run the ablation (with and without 72 h) and report it.

### A4. The agent's evidence source is undefined — [Certain]
"Pull the 2-hop neighborhood … and load it into Neo4j": pull it from where? Nothing in the stream stores transactions in a form you can query by account and time. Two more rules are missing:
- Evidence must stop at the alert's time (`tx_id ≤ as_of`). Otherwise the agent sees the *rest* of the laundering pattern, which happens after the alert. That is label leakage into the LLM, and it inflates "pattern accuracy".
- Evidence must never include `is_laundering`.

**Change:** the evidence builder queries the Iceberg `raw_transactions` table through DuckDB/PyIceberg, filtered by account, `event_ts ∈ [as_of − 72h, as_of]` and `tx_id ≤ as_of_tx_id`. Its output is stored as JSON on the case.

**I disagree with keeping Neo4j** because it adds a stateful service, a sink and a query language without adding a claim to your resume (Neo4j is already on it from Niyanta and GraphRAG). Here's what I would do instead: keep the evidence subgraph as JSON in Postgres and render it in the UI with Cytoscape.js. The risk in your approach is 6–10 hours on infrastructure that no interviewer will ask about, in a project whose risk is already time. (If you want it anyway, it's a one-day add-on after M11.)

### A5. Redis "online account profile" has no reader — [Certain]
In your diagram nothing reads Redis: the scorer gets complete vectors from `tx.features`. A component with no consumer is a design smell an interviewer will spot.

**Change:** cut Redis. Store the exact feature vector and the model's top SHAP contributions on the alert row. That's also better for audit: the case shows what the model saw *at decision time*, not the account's current state. (Add Redis back only if you build a synchronous `/score` API. That's a valid stretch: "online feature store for pre-authorization scoring".)

### A6. "0 duplicate alerts" is trivially true with a unique constraint — [Certain]
If `alerts.tx_id` is a primary key, duplicates can't exist in the table, so T7 measures nothing. A sharp interviewer will say exactly that.

**Change:** report three numbers per fault run:
- `lost` = expected alerts − actual alerts (the real test)
- `duplicates_produced` = transactions with more than one record in `tx.scores` (what at-least-once actually did)
- `duplicates_suppressed` = idempotent inserts that hit `ON CONFLICT`

The claim becomes "effectively-once at the case table, with N upstream duplicates absorbed". That's honest, and it can be checked.

### A7. Same-minute ties and money rounding will break parity unless the spec defines them — [Certain]
IBM timestamps have minute resolution, so many transactions share a timestamp. And amounts include Bitcoin with many decimals. If "before this transaction" and "convert to USD" aren't defined exactly, Spark and Flink will disagree on a fraction of rows, and you'll spend days chasing float noise.

**Change** (already in `features/spec.yaml`):
- Total order is `(event_ts_ms, tx_id)`, where `tx_id` = `seq` from a stable sort in M1.
- Money is int64 USD cents, computed with `Decimal` half-even rounding through the same Python function in batch and stream.
- Sums are exact integers, so parity on counts and sums can be **exact**, not "within tolerance".

### A8. The agent eval would grade itself — [Likely]
"≥ 70% of drafts accepted with no or minor edits" with you as the only reviewer is self-grading: you wrote the prompt and you know the patterns.

**Change:** measure "minor edit" objectively (normalized edit distance ≤ 0.10). Report acceptance separately for you and for the M10 reviewers. The *objective* agent metric is **pattern-type accuracy against the IBM pattern files**. Lead with that.

---

## B. Problems with the targets

### B1. T3: expect to land below the published tree-model numbers — [Likely]
Published minority-class F1 on HI-Small: GFP+LightGBM **62.86 ± 0.25** and GFP+XGBoost **63.23 ± 0.17**. On HI-Medium the same two are 59.48 and 65.70.[^mega] Multi-GNN variants reach the high 60s (Multi-PNA+EU about 68).[^egressy] The GFP graph features were hand-crafted to match the simulator's laundering patterns, as the Multi-GNN authors point out.[^egressy] Your window features are simpler, so [Guessing] expect F1 somewhere in 30–55 on HI-Small.

That's fine **if you frame it right**: this is a streaming *system* whose features must be computable per event inside a latency budget. Put your number next to theirs with citations and explain the gap. Optional M2 add-on: a few GFP-style features (fan degree, 2-cycle return, already in the spec) and the ablation that shows how much they add.

Also match their split: a **60/20/20 temporal split by transaction order**, where validation and test transactions can use earlier history.[^egressy] Your features already do that.

### B2. T9 with 3–5 reviewers is a usability check, not a statistic — [Certain]
Report medians per reviewer and pooled, with n stated. No p-values. Call it "directional evidence". Recruit the reviewers in **week 1**. This is the scheduling risk most likely to slip.

### B3. T11 ≤ $30 is realistic, but "free tier" isn't the reason — [Likely]
EKS bills its control plane per hour. On accounts created under the July 2025 free-tier rules, EMR (including EMR Serverless) needs the **Paid plan**; EKS, EC2, S3, Glue and Athena are available on the Free plan.[^awsplans] What you actually pay depends on which account type you have and how many credits you hold. That's [Unclear] until you check. See `03_aws_free_tier_and_costs.md`, step 0.

**Update (resolved):** your account is a new sign-up on the Free plan with no card. So cloud Spark runs on AWS Glue 5.1, EKS uses only the allowed flex instance types, and Phase 5 must finish inside the plan's 6-month window. See `07_phases_and_cloud_plan.md` and decisions D16, D22–D24.

### B4. Claude API credits are not your constraint — [Certain]
Claude Haiku 5.5 costs $0.10 per million input tokens and $0.50 per million output tokens (prompts up to 100k tokens). Sonnet 5.5 is $2 / $10, and Opus 5.5 is $4 / $20. The Batch API halves all of these.[^pricing] A case draft with about 6k input and 700 output tokens costs about **$0.001 on Haiku**, so 1,000 drafts cost about $1. Use the slack for a **three-model comparison** (accuracy, grounding, cost and latency per case). That table is AI-engineer evidence.

### B5. The 170–200 h estimate is a guess, and M4 is where it breaks — [Guessing]
You write Python daily, and the Flink job is Java. Debugging event-time buffering, timers and Kafka transactions for the first time usually takes longer than planned. The schedule (`05_schedule_and_cut_list.md`) therefore:
- builds a Flink "walking skeleton" (two trivial features, exactly-once sink, restart test) **before** the full feature set;
- sets a hard timebox with a fallback trigger on every milestone;
- treats Neo4j, Redis, GNN, `short_cycle_flag` (3-hop) and the HI-Large model as Won't-do unless you're ahead.

---

## C. Environment facts that changed since your plan was written

| Item | What's true now | Consequence |
|---|---|---|
| MinIO | Its free Docker Hub images were removed, and the community edition is archived and unmaintained [^minio] | Local dev uses the **local filesystem** for the Iceberg warehouse and Flink checkpoints (S3 only in the cloud). Optional: SeaweedFS if you want S3 behavior locally. |
| Bitnami images | [Likely] most of the free catalog was withdrawn in 2025 | Use official images only (`postgres`, `redpandadata/redpanda`, `flink`, `ghcr.io/mlflow/mlflow`, `apache/airflow`, `prom/prometheus`, `grafana/grafana`). |
| Flink | 1.20 is the LTS line (1.20.5, June 2026); 2.1–2.3 are current. The Kafka connector for 1.20 is `3.4.0-1.20`; for 2.x it's `4.0.0-2.0` [^flink] | Use **Flink 1.20 LTS**: mature connector, most examples online, and Claude Code's training data matches. Note the choice in `docs/decisions.md`. |
| Kafka on EKS | Strimzi 1.3.0 supports Kafka 4.2.2 (KRaft only) [^strimzi] | KRaft cluster, `KafkaNodePool` resources, `v1` CRDs. |
| Claude Code billing | If `ANTHROPIC_API_KEY` is set in your shell, Claude Code uses that key instead of your subscription [^ccauth] | The app's key is named `AML_ANTHROPIC_API_KEY`, and a hook blocks any command that touches `ANTHROPIC_API_KEY`. |

---

## D. What I'd keep exactly as you wrote it
- Two simulated banks with different formats and config-only onboarding (FDE core).
- XGBoost as the main model, with the GNN as a stretch.
- The time-based split, and the rule that the test set is touched once per model family.
- Destroy cloud resources after every session; check Cost Explorer the next day.
- Write the hard parts yourself, then get quizzed on them.

## E. Decisions recorded (also seeded into `starter-kit/docs/decisions.md`)

| ID | Decision | Why |
|---|---|---|
| D01 | Flink 1.20 LTS, Java 17, `flink-connector-kafka 3.4.0-1.20` | Maturity and example density beat novelty when time is the risk |
| D02 | Legs + per-key event-time sort + join by `tx_id` | Deterministic receiver-side features (A2) |
| D03 | Both EOS and ALO modes; latency matrix decides the default | A1 |
| D04 | Windows 1h/24h/72h, 24 h training burn-in; 7d only on HI-Large | A3 |
| D05 | Money = int64 USD cents via Decimal half-even, one shared function | A7: exact parity |
| D06 | Total order `(event_ts_ms, tx_id)`; `tx_id` = M1 `seq` | A7 |
| D07 | Evidence from Iceberg via DuckDB, cut off at `as_of`, no label column; Neo4j dropped | A4 |
| D08 | No Redis; the feature vector and SHAP top-k are stored on the alert | A5 |
| D09 | Reconciliation reports lost, duplicates produced and duplicates suppressed | A6 |
| D10 | Local object storage = filesystem; S3 only in cloud | C (MinIO) |
| D11 | Agent = deterministic evidence + one structured-output call + validator + ≤1 retry | Cost, determinism, faithfulness |
| D12 | Primary agent metric = pattern accuracy against IBM pattern files | A8 |
| D13 | Postgres `SKIP LOCKED` job table as the transactional outbox for drafts | No lost draft jobs; no extra broker |
| D14 | Evidence for the scale run: agent disabled except a 100-case sample | Cost and time; agent quality is measured locally |
| D15 | Flink dedupe by `tx_id`, cleaned up by event-time timers | Connectors are at-least-once; a duplicate would corrupt counts |

---

[^altman]: Altman et al., "Realistic Synthetic Financial Transactions for Anti-Money Laundering Models", NeurIPS 2023 Datasets & Benchmarks, Table 4. https://papers.neurips.cc/paper_files/paper/2023/file/5f38404edff6f3f642d6fa5892479c42-Supplemental-Datasets_and_Benchmarks.pdf
[^mega]: Table 16 in the MEGA paper (arXiv 2412.00241), which reproduces the GFP baselines of Blanuša et al. 2024. https://arxiv.org/pdf/2412.00241. Copy the exact numbers from the original tables before putting them in your README.
[^egressy]: Egressy et al., "Provably Powerful Graph Neural Networks for Directed Multigraphs", AAAI 2024 (arXiv 2306.11586), §7.2 and Appendix E.1. https://arxiv.org/pdf/2306.11586
[^awsplans]: AWS, "Supported AWS services for Sign up for AWS (new)". https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html
[^pricing]: Anthropic pricing. https://platform.claude.com/docs/en/about-claude/pricing
[^minio]: e.g. https://madewithlove.com/blog/thanks-for-all-the-buckets/ and https://www.chainguard.dev/unchained/secure-and-free-minio-chainguard-containers
[^flink]: https://flink.apache.org/blog/ ; AWS Managed Flink connector table: https://docs.aws.amazon.com/managed-flink/latest/java/flink-2-2.html
[^strimzi]: Kafka Monthly Digest, Oct 2026. https://developers.redhat.com/blog/2026/11/02/kafka-monthly-digest-october-2026
[^ccauth]: https://support.claude.com/en/articles/11145838 and https://code.claude.com/docs/en/env-vars
