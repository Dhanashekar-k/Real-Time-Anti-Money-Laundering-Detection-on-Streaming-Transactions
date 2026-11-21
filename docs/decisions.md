# Decision log

Format per entry: **ID — title** · Status · Context · Decision · Alternatives · Consequences · Revisit when.
Reasoning for D01–D15 is in `manual/01_plan_review_and_changes.md`. D16 and D22–D24 come from your Free-plan account (`manual/03_…`, `manual/07_…`). D25–D30 are the v2 additions (milestone sections "Revision v2 additions").

## D01 — Flink 1.20 LTS, Java 17
Accepted. Flink 2.x is current, but 1.20 is LTS with the mature `flink-connector-kafka 3.4.0-1.20` and far more examples (lower risk when time is the constraint). Alternative: 2.2/2.3 with connector `4.0.0-2.0`. Revisit after M11 if a 2.x feature is needed.

## D02 — Legs + per-account event-time sort + join by tx_id
Accepted. Keying Kafka by sender doesn't order an account's incoming transfers. Each tx splits into OUT/IN legs; each account buffers legs until the watermark passes and processes them in (event_ts, tx_id) order; halves join by tx_id. Cost: latency ≥ OOO bound / speedup; state for pending legs.

## D03 — Both EOS and ALO modes; measured latency matrix picks the default
Accepted. Kafka transactional output becomes visible only at checkpoint completion, so the EOS latency floor ≈ checkpoint interval. ALO + idempotent alerts gives effectively-once at the case table with lower latency.

## D04 — Windows 1h/24h/72h; 24h training burn-in; 7d only on HI-Large
Accepted. HI-Small spans 10 days; 7d windows would be truncated for all training rows (train/test shift).

## D05 — Money as int64 USD cents via Decimal ROUND_HALF_EVEN, one implementation
Accepted. Exact sums make Spark↔Flink parity exact; floats would create noise mismatches.

## D06 — Total order (event_ts_ms, tx_id); tx_id = M1 seq
Accepted. Minute-resolution timestamps produce ties; a deterministic tie-break is required for parity and for "before this transaction".

## D07 — Evidence from Iceberg (DuckDB/PyIceberg), cut off at alert time, no label/split columns; Neo4j dropped
Accepted. Prevents label leakage into the LLM; Neo4j added no new resume claim and an extra stateful service.

## D08 — No Redis online store; feature vector + SHAP top-k stored on the alert
Accepted. Redis had no reader. Storing what the model saw at decision time is better for audit.

## D09 — Reconciliation reports lost, extra, duplicates produced, duplicates suppressed, score mismatches
Accepted. "0 duplicate alerts" alone is vacuous under a unique constraint.

## D10 — Local object storage = filesystem; S3 only in cloud
Accepted. MinIO community images were withdrawn. Optional: SeaweedFS if S3 semantics are needed locally.

## D11 — Agent = deterministic evidence + one structured-output call + validator + ≤1 semantic retry
Accepted. Predictable cost, comparable across models, every claim checkable. Tool-using variant is an optional comparison.

## D12 — Primary agent metric: pattern-type accuracy against IBM pattern files
Accepted. Objective; human acceptance rate is secondary and reported with who reviewed.

## D13 — Postgres case_jobs table (FOR UPDATE SKIP LOCKED) as transactional outbox
Accepted. Case and job are committed together; no second broker needed.

## D14 — Agent disabled during the EKS scale run except a 100-case sample
Accepted. Agent quality is measured locally; the scale run measures throughput/latency/recovery.

## D15 — Flink dedupe by tx_id with event-time cleanup timers (48h)
Accepted. Connectors are at-least-once; a duplicate would double-count features. Processing-time TTL is wrong under accelerated replay.

## D16 — AWS account type and credits
Accepted (Nov 2026), details TODO at M0. New project-based sign-up on the **Free plan, no payment method**. $100 sign-up credits plus up to $100 more for completing activities. The plan lasts 6 months or until the credits run out; after that AWS requires an upgrade with a payment method. Allowed EC2 types: t3.micro/small, t4g.micro/small, c7i-flex.large, m7i-flex.large. EMR and MSK need the Paid plan. Consequences: D23, D24; fallback D22.
TODO at M0: credit balance, end date, which activities are done, whether Spot works, the EC2 vCPU quota (`aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A`).

## D17 — Split boundaries
TODO at M1: t1, t2, row and laundering counts per split for hi_small and hi_medium.

## D18 — Watermark strategy and out-of-orderness bound
TODO at M04 (§4).

## D19 — Default checkpoint interval
TODO at M04.

## D20 — Default delivery mode (EOS or ALO) for T5
TODO at M05 (§4), from the latency matrix.

## D21 — Label-delay simulation (L = 2 days)
Proposed (M07 §3). Ground truth is visible only for days ≤ D − L; newer labels come only from analyst decisions; `label_source` recorded. Revisit if the loop shows no effect at L = 2.

## D22 — Upgrade fallback
Proposed. If the Free plan blocks Phase 5 (instance type, quota, Glue workers, credits), first shrink (6 EKS nodes, 5 Glue workers). If that still fails, upgrade to the Paid plan with a **$30 monthly spend limit**. Record the date and reason here. Alternative: stop at a smaller scale and say so in the README.

## D23 — Cloud Spark runs on AWS Glue 5.1, not EMR
Accepted. EMR needs the Paid plan; Glue is allowed. Glue 5.1 runs Spark 3.5.6 and Python 3.11, matching local Spark 3.5, so the same job code runs in both places. G.1X workers (1 DPU = 4 vCPU, 16 GB), 10 to start, timeout 120 min, no retries, no interactive sessions. Alternative: Glue 6.0 (30% cheaper, but Spark 4.1 ≠ local). Revisit if cost forces it.

## D24 — EKS on Free-plan instance types; throughput target is measured, not assumed
Accepted. Nodes are m7i-flex.large / c7i-flex.large only (24 vCPU in 12 nodes; 6-node fallback). Images are built for linux/amd64. The original 50k tx/s target is replaced by "the highest rate with flat consumer lag" from a 5k–40k ladder, reported with the bottleneck that limited it. Full HI-Large replay only at ≥ 30k tx/s; otherwise a 20-day slice, stated.

## D25 — Four human-in-the-loop loops with maker-checker
Accepted (v2). (1) decisions → label-delayed retrain → automatic gate; (2) draft feedback → regression eval set → prompt gate + per-bank supervisor sign-off; (3) hash-sampled QA of the band below the threshold + all needs-review drafts → miss-rate estimate; (4) operating-point proposals → supervisor approval → applied thresholds. No one approves their own change (database constraint). TODO at M07: final prompt-gate thresholds and QA sampling rate.

## D26 — Account aliasing before any LLM call
Accepted (v2). Account IDs and bank codes are replaced with ACCT-n / BANK-k per case; the map stays in Postgres. Amounts, timestamps and tx_ids are kept. Test: no real ID appears in any request body.

## D27 — Haiku → Sonnet cascade; tool-use agent is eval-only
Accepted (v2). D11 still holds (one structured call, ≤ 1 retry); the retry goes to Sonnet. A bounded tool-use agent (≤ 6 read-only calls, cutoff enforced in each tool) is compared on the eval set and served only if it wins on accuracy without a worse hallucination rate. Revisit after the M06 eval.

## D28 — Tenant isolation with Postgres row-level security
Accepted (v2). Policies on `bank_id` (directly or through `cases`); the API runs as `aml_api` with per-transaction `set_config`; services run as `aml_service` (BYPASSRLS). Fails closed when no tenant is set. Verified by `contracts/rls_isolation_test.sql`. Alternative: schema per bank (stronger, heavier to operate). Revisit if a bank requires physical separation.

## D29 — Drift: PSI against a per-model-version baseline; no automatic retrain
Accepted (v2). Baseline saved with each model version (M02 v2). Thresholds 0.1 watch / 0.25 act, calibrated by a false-alarm check on unshifted replay. A drift alert notifies; retraining still goes through labels, the label delay and the gate. TODO at M08: measured false-alarm level and the induced-shift results.

## D30 — Hallucination measured before the validator, with intervals
Accepted (v2). Ungrounded-entity, ungrounded-number and false-pattern rates are computed on raw first-attempt output with Wilson 95% intervals; the after-validator rate is reported but is 0 by construction. Deterministic checks only, no LLM judge; unsupported qualitative claims are counted from analyst reason codes. Limitation stated in the README.

## D-versions — Pinned versions
TODO at M0: exact versions of every component in CLAUDE.md's table.
