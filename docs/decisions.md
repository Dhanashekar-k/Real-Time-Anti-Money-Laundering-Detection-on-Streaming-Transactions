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

## D31 — Project naming
Accepted (M0). GitHub repo: `Real-Time-Anti-Money-Laundering-Detection-on-Streaming-Transactions`. README and display title: "Real-Time Money-Laundering Detection on Streaming Transactions" (an AML transaction-monitoring system). Codename and AWS resource prefix: `stream-aml` (compose project name, Terraform state bucket `stream-aml-tfstate-<account-id>`, tag `project=stream-aml`). Numbered D31, not D17: D17 is already "Split boundaries" (TODO at M1) and is referenced by the manual. Consequence: the repo name is long; the codename is what appears in code and in AWS.

## D32 — M0 local stack and tooling: where it differs from the manual
Accepted (M0). Each item was found by running the stack; none changes the architecture.
- **Host ports:** Redpanda Console on **8082** (8080 was held by another local project) and MLflow on **5001** (5000 is macOS AirPlay Receiver). The M00 DoD text says 8080 and 5000. Both are overridable with `CONSOLE_PORT` and `MLFLOW_PORT`. All published ports bind to 127.0.0.1.
- **MLflow image:** the official `ghcr.io/mlflow/mlflow:v3.18.0` has no Postgres driver and no curl, so a two-line Dockerfile adds `psycopg2-binary`. The backend URL names the driver (`postgresql+psycopg2://`) because 3.18 resolved a bare `postgresql://` to psycopg v3. One worker. It still uses about 1.8 GiB idle (measured with `docker stats`). The image is rebuilt only when its Dockerfile changes: every build got a new image ID even with cached layers, and compose recreates a container whenever the ID changes.
- **Redpanda:** the manual's `--smp 2 --memory 2G --overprovisioned`, without `--mode dev-container` (which also skips fsync and turns on topic auto-creation). `auto_create_topics_enabled=false`, so a producer typo fails instead of creating a stray topic.
- **Postgres init:** `aml_api` / `aml_service` get LOGIN and their passwords from the container environment through `\getenv` in a `.sql` file, not a shell script: Docker Desktop's file sharing makes a mounted script look executable and the entrypoint then fails to run it. mlflow, airflow and the Iceberg catalog use the superuser in local dev; the API never does (rule 11).
- **Flink 1.20.5:** `state.checkpoints.dir` (1.20.5 also has `execution.checkpointing.dir`). The TaskManager has a random RPC port, so its healthcheck asks the JobManager whether it registered (exact match on `"<ip>:`).
- **Healthchecks:** Postgres checks over TCP (`-h 127.0.0.1`): on a fresh volume the entrypoint's temporary init server listens only on the unix socket, so a socket check would report healthy too early (inferred from the entrypoint, not reproduced). Redpanda Console checks `/admin/health` with `wget`.
- **Tests that touch shared state:** the topic drift test runs the whole script against a scratch topic (`AML_TOPICS_SPEC`), never a real one; the smoke test clears stale files under its table directory; a test compares the live `aml` database's RLS flags, policies and grants with a database built from the contract, because init scripts run only on an empty volume.
- **Makefile:** targets that later milestones add are stubs that print the milestone and **exit 1** (the manual says `@echo`): a stub that exits 0 would let `make parity` or `make test-rls` look green when nothing ran. `test-rls` is implemented now on top of the RLS integration tests. `make topics` exits non-zero on drift (partitions or retention differ) instead of changing a topic silently.
- **CI:** the Java build is its own workflow with a native `paths:` filter (no third-party path-filter action). Actions are pinned to commit SHAs; uv is pinned to the version behind `uv.lock`.
- **Maven 3.10.1** (Homebrew) instead of the 3.9 pin in CLAUDE.md. Not yet exercised: the Flink module arrives in M4. Revisit then.
- **Python groups:** `jsonschema` added to `dev`; `shap` is its own optional group (it pulls numba and llvmlite); `manual/` is excluded from ruff.

## D33 — Terraform bootstrap: state bucket and budgets
Accepted (M0). **Written and validated (`fmt`, `validate`), not planned or applied**: the `stream-aml` AWS profile did not exist yet. The user applies it with `make cloud-bootstrap`.
- **State:** this root's own state is local (it creates the bucket). The bucket `stream-aml-tfstate-<account-id>` is versioned, SSE-S3, public access blocked, TLS 1.2+ only, noncurrent versions expire after 90 days, `prevent_destroy`. No DynamoDB: locking is S3-native `use_lockfile` (Terraform 1.11+; checked in the 1.11 changelog).
- **Partial backend config:** other roots commit `key`, `region`, `encrypt`, `use_lockfile` and receive the bucket at `terraform init -backend-config="bucket=…"`. The bucket name contains the account ID, which should not sit in committed files (`cost-guard` finding).
- **Safety:** the provider has `allowed_account_ids` (this machine has other AWS profiles); account ID and alert email are required variables in a gitignored `terraform.tfvars`. Tags `project`, `owner`, `ttl=permanent` (other roots use `session`).
- **Budgets:** $5, $20, $50 monthly, email at 80% and 100% of actual cost, with credits and refunds excluded so sign-up credits do not hide spend. They warn; the Free plan's credit limit is the hard stop.
- **`make cloud-bootstrap`:** no auto-approve flag; it refuses unless stdin is a terminal (stops a plain `yes | make …`) and clears `TF_CLI_ARGS*` (stops an environment variable adding the flag). These are guards against accidents, not a security boundary: a pseudo-terminal fed by a pipe would still pass. The real gate for Claude Code is the `ask` rule for `make cloud-*`.
- **Static rule-5 tests** (`tests/unit/test_infra_rules.py`, text matching, not a Terraform parser): NAT gateways (resource and the VPC module flags), load balancers (`type: LoadBalancer` in any quoting, `--set service.type=`, `aws_lb/alb/elb`), Strimzi `deleteClaim`, Glue timeout and zero retries, S3 backends with `use_lockfile` and no DynamoDB, every non-bootstrap root having an S3 backend, `default_tags`, no auto-approve flag. Each pattern has a positive control. They catch the usual ways to break a rule; `cost-guard` still reviews every `infra/` change.
- **Not verified (no plan or apply was possible):** how Free-plan credits appear in Budgets (so whether excluding credits yields the gross figure), whether the signed-in identity may call the Budgets API, the budget refresh cadence, and whether `limit_amount = "5"` plans without a spurious diff. Check the first `plan` and Cost Explorer after the first day.
- **Known gaps, not addressed (the user's call):** monthly budgets never show cumulative credit use; budget data lags, so a forgotten cluster can overspend before the $5 alert. Options: a DAILY budget, an ANNUAL budget near the credit balance. Mitigation until then: `make cloud-down` then `make teardown-check`.
- **Review:** the `cost-guard` subagent run on 2026-11-21 found no billable-resource risk (its report was not archived). Its findings on the committed account ID, the approval bypasses, TLS minimum, delete markers, state-file loss and teardown were fixed or documented in `infra/terraform/bootstrap/README.md`.

## D34 — guard-bash.sh blocks volume-deleting commands
Accepted (M0). The reviewer found that the `ask` rule for compose `down -v` in `.claude/settings.json` does not match when an option comes before the subcommand (`docker compose -f <file> down -v`), so the broad `Bash(docker compose *)` allow rule let it through. `make -C <dir> nuke-local` had the same gap.
- **Decision:** `.claude/hooks/guard-bash.sh` blocks (exit 2), by regex on the raw command: compose `down` with `-v` / `--volumes`, compose `rm` with `-v` / `--volumes`, `make nuke-*`, `docker volume rm|remove|prune`, and `docker system prune --volumes`. Plain `docker system prune` stays allowed. The existing `settings.json` ask rules are unchanged, and no extra settings patterns were added.
- **Why a hook, not settings patterns:** a hook block is documented to take precedence over allow rules, and matching the raw text also covers wrappers (`bash -c`, `env`, `cd x &&`) that command-prefix rules are documented not to match. Settings patterns that constrain arguments are documented as fragile.
- **Limits:** it also blocks a command that only mentions one of these strings (an echo, a commit message). A block cannot be approved per use: the user runs the command. The docs do not say whether hooks or permission rules apply to the user's own `!` commands, so this is not a guard on what the user types. Other deletions (`docker rm -v`, `rm -rf var/`) are not covered.
- **Verified:** 61 cases against the real script under bash 3.2 (28 blocked, 25 allowed, plus the 8 existing-guard regressions), and the real hook blocked the compose `down -v` form on an empty scratch project. `make test`, `make itest` and `make test-rls` pass: hooks apply to Claude's Bash tool calls, not to subprocesses a test starts.

## D35 — AWS region is us-east-2 (Ohio), not us-east-1
Accepted (M0, the user's choice on 2026-11-21; the `stream-aml` CLI profile was created with region us-east-2). The manual said us-east-1 for everything.
- **Changed:** the Terraform bootstrap `region` default, its README, `manual/03` Step 1 (lines 2 and 5), `manual/milestones/M00` step 7, and the `teardown-check` skill (it looks for leftovers in the project region, so it must follow it).
- **Not changed:** `AWS_REGION` in `.env.example` and the user's `.env` still say us-east-1. The user updates them.
- **Not verified yet:** that every Free-plan instance type the plan relies on (`t3.micro/small`, `t4g.micro/small`, `c7i-flex.large`, `m7i-flex.large`) is offered in us-east-2 and in the chosen availability zones, that Glue 5.1 and EKS are available there, and the EC2 vCPU quota for this region (the quota is per region, so D16's figure must be read for us-east-2). Check with `aws ec2 describe-instance-types --region us-east-2` before M9. IAM and Budgets are global, so the bootstrap is unaffected.
- **Consequence:** the state bucket will be created in us-east-2. Any later move of region means a new bucket and a state migration.

## D-versions — Pinned versions
Observed on 2026-11-21 during M0 (arm64 Mac, Docker Desktop engine 29.4.1). Python packages are exact in `uv.lock`; image tags are in `infra/compose/docker-compose.yml`. "TBD" = not installed or not checked yet, with the milestone that will.

| Component | Version |
|---|---|
| Python | 3.11.15 (uv-managed; `.python-version` 3.11) |
| Java / Maven | OpenJDK 17.0.21 / Maven 3.10.1 (see D32) |
| Flink | image `flink:1.20.5-java17`. `flink-connector-kafka` 3.4.0-1.20 and the K8s operator: TBD (M4, M9) |
| Spark | PySpark 3.5.9 locally. Glue 5.1 = Spark 3.5.6 per D23 (not run yet). `iceberg-spark-runtime-3.5_2.12` 1.12.0, `org.postgresql:postgresql` 42.7.14 |
| Iceberg clients | PyIceberg 0.12.0, DuckDB 2.0.1 (Python and CLI) |
| Kafka | Redpanda `v26.3.1` (rpk 26.3.1), Console `v3.15.0`. Strimzi + Kafka 4.2.x on EKS: TBD (M9) |
| Postgres | `postgres:17.12` |
| ML | xgboost 3.2.0, lightgbm 4.7.0, scikit-learn 1.9.1, MLflow 3.18.0 (server image `v3.18.0` + psycopg2-binary 2.9.13), pandas 3.1.1, numpy 2.4.6, pyarrow 26.0.1 |
| Orchestration | Airflow `apache/airflow:3.4.0` (`standalone`, dev only) |
| Observability | `prom/prometheus:v3.16.0`, `grafana/grafana:13.3.1` |
| LLM | `anthropic` SDK 2.12.0. Model ids as listed in CLAUDE.md (not called yet, M6) |
| Services | fastapi 0.165.0, pydantic 2.14.4, psycopg 3.3.9, confluent-kafka 2.16.2, fastavro 1.13.1 |
| Web | node v26.0.0 / npm 11.12.1 installed. Vite, React, TanStack Query, Cytoscape: TBD (M7) |
| Tooling | uv 0.10.8, ruff 0.17.6, mypy 2.4.1, pytest 9.2.0, Terraform 1.17.2, AWS provider 6.73.0 (`.terraform.lock.hcl`, darwin_arm64 and linux_amd64) |
| GitHub Actions | checkout 7.0.1, setup-uv 10.4.0, setup-java 6.3.0, setup-terraform 4.0.1 (pinned by SHA) |

All images were checked for a `linux/arm64` manifest. They also publish `linux/amd64`, but cloud builds (rule 10) are not done yet.
