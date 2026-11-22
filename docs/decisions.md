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
Accepted (Nov 2026); M0 facts filled in on 2026-11-21. New project-based sign-up on the **Free plan, no payment method**. $100 sign-up credits plus up to $100 more for completing activities. The plan lasts 6 months or until the credits run out; after that AWS requires an upgrade with a payment method. EMR and MSK need the Paid plan. Consequences: D23, D24; fallback D22.
- **Shared account (decided by the user):** stream-aml and edgarqa are the **same AWS account** (ends …2039; the `stream-aml` and `edgarqa` CLI profiles resolve to it, checked with a read-only identity call). Region **us-east-2** (D35). Credits, quotas and billing are shared with edgarqa. The account already has two account-wide budgets that this repo does not manage: `edgarqa-monthly-cost` ($80/month) and `project 1` ($5/month) (read with `aws budgets describe-budgets`).
- **Credits:** about $190 across both projects, as reported by the user from the console (not measured by this repo). **Days remaining: 170 as of 2026-11-21 (the window closes about mid-May 2027), as reported by the user, not measured by this repo.**
- **EC2 types allowed on this plan:** `c7i-flex.large`, `m7i-flex.large`, `t3.micro`, `t3.small`, `t4g.micro`, `t4g.small`. All six are returned by `describe-instance-types --filters Name=free-tier-eligible,Values=true` in us-east-2. That call also lists `t8i.micro/small` and `t9g.micro/small`, which are not in the project's list and are not adopted.
- **Quotas in us-east-2 (read-only Service Quotas calls, 2026-11-21):** Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances: **16 vCPUs, shared with edgarqa** (`L-1216C47A`). All Standard Spot Instance Requests: **32 vCPUs** (`L-34B43A08`). The Spot quota is not evidence that Spot launches work on this plan: not tested.
- **Billing access:** this account cannot list cost-allocation tags (`aws ce list-cost-allocation-tags` returns "Linked account doesn't have access to cost allocation tags"), so activating the `project` tag may need an Organization management account. See D33.

## D17 — Split boundaries
Measured in M1 on HI-Small (`results/M01/hi_small/split.json`) and HI-Medium (`results/M01/hi_medium/split.json`), 2026-11-22. Rule: `t1`, `t2` are the `event_ts` at index floor(0.6N) and floor(0.8N) in `(event_ts_ms, tx_id)` order; train `< t1`, val `[t1, t2)`, test `>= t2`. A timestamp never straddles two sets.

| | HI-Small | HI-Medium |
|---|---|---|
| N | 5,078,345 | 31,898,238 |
| boundary indexes | 3,047,007 and 4,062,676 | 19,138,942 and 25,518,590 |
| `t1` | 2022-09-06T13:36:00+00:00 (1662471360000) | 2022-09-09T20:43:00+00:00 (1662756180000) |
| `t2` | 2022-09-08T16:12:00+00:00 (1662653520000) | 2022-09-14T05:46:00+00:00 (1663134360000) |
| train | 3,046,861 rows, 2,297 laundering (0.075%) | 19,137,646 rows, 15,536 laundering (0.081%) |
| val | 1,015,602 rows, 1,082 laundering (0.107%) | 6,380,410 rows, 9,052 laundering (0.142%) |
| test | 1,015,882 rows, 1,798 laundering (0.177%) | 6,380,182 rows, 10,642 laundering (0.167%) |
| rows that differ from a pure index split | 359 | 1,830 |

- **The laundering rate by split is distorted by a sparse tail** (`results/M01/hi_small/eda.json`, `sparse_days`; see D36). All days: train 0.075%, val 0.107%, test 0.177%. The last 8 days (2022-09-11 to 2022-09-18) hold 1,108 rows, 655 of them laundering (59.1%), all in test. Without those days: train 0.075%, val 0.107%, test 0.113% (1,143 laundering in 1,014,774 rows). So most of the apparent rise from val to test comes from the tail, and the remaining rise is small. **Decided (user): M02 reports test metrics both ways**, with the tail and without it, and labels the tail a generator artifact, not a temporal trend (D36). A threshold chosen on val is on a base rate close to the tail-free test rate, not the all-days one.
- **HI-Medium has the same tail, longer** (`results/M01/hi_medium/eda.json`, `sparse_days`; see D36). The last 12 of its 28 days (2022-09-17 to 2022-09-28) hold 6,987 rows, 4,084 of them laundering (58.5%), all in test: 38.4% of the test split's 10,642 laundering rows in 0.11% of its rows. Without those days: train 0.081%, val 0.142%, test 0.103% (6,558 laundering in 6,373,195 rows). Here the tail-free test rate is below val's, unlike HI-Small, so all-days test (0.167%) overstates it by 62%. Same decision as HI-Small: M02 reports test metrics both ways and labels the tail a generator artifact, not a temporal trend (D36).
- Training excludes rows with `event_ts < data_start + 24h` (D04), so the effective train set is smaller than shown.
- HI-Large: TBD (not measured).

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
Accepted. Nodes are m7i-flex.large / c7i-flex.large only. **Updated 2026-11-21: Phase 5 / M9 starts from the 6-node fallback (6 × 2 vCPU = 12 vCPUs), not 12 nodes / 24 vCPUs.** The On-Demand Standard vCPU quota in us-east-2 is 16 and shared with edgarqa (D16), so 24 vCPUs does not fit. Even 12 vCPUs leaves only 4 for everything else in the account, including the Kaggle helper box (2 vCPUs) and anything edgarqa runs, so check what edgarqa has running before a session. Option, not requested yet: ask for a quota increase to 24 (may need review on the Free plan); until it is granted, every ladder result must state the node count it ran on. `manual/03` and `manual/milestones/M09` still describe the 12-node plan. Images are built for linux/amd64. The original 50k tx/s target is replaced by "the highest rate with flat consumer lag" from a 5k–40k ladder, reported with the bottleneck that limited it. Full HI-Large replay only at ≥ 30k tx/s; otherwise a 20-day slice, stated.

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
- **Budgets:** $5, $20, $50 monthly, email at 80% and 100% of actual cost, with credits and refunds excluded so sign-up credits do not hide spend. They warn; the Free plan's credit limit is the hard stop. **Shared account (D16): they are filtered to the cost-allocation tag `project=stream-aml`** (`user:project$stream-aml`, the format in the provider docs; switch `filter_budgets_by_tag`). Open risk: the `project` tag must be an active cost-allocation tag, it appears in Billing only after tagged resources exist (up to 24 h), and this account was refused when listing cost-allocation tags, so activation may be impossible from here. A filtered budget with an inactive tag stays at $0 and never warns. **Gate before Phase 5:** the tag shows as Active and a tagged, billed resource moves the budget's actual spend; otherwise set `filter_budgets_by_tag = false` or rely on the existing account-wide budgets.
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
- **Verified 2026-11-21 (read-only calls):** the six Free-plan instance types are free-tier-eligible in us-east-2; the On-Demand vCPU quota there is 16 and the Spot quota 32 (D16), which forced D24's 6-node start.
- **Not verified yet:** availability of those types in the chosen availability zones, and that Glue 5.1 and EKS are available in us-east-2. Check before M9. IAM and Budgets are global, so the bootstrap is unaffected.
- **Consequence:** the state bucket will be created in us-east-2. Any later move of region means a new bucket and a state migration.

## D36 — M1 choices: FX freeze, exact ids, money, patterns
Accepted (M1, 2026-11-22). Where this differs from the manual, the manual's wording is in brackets.
- **FX table frozen (`features/fx_rates.yaml`, `fx_version: 1`), approved by the user after reviewing the variance table.** Measured on HI-Small (`results/M01/hi_small/fx_variance.json`, 72,170 cross-currency rows, 201 of 210 possible pairs): the generator uses one constant rate per currency with USD as the hub. Fiat: the USD>X amount-weighted rate matches a round constant (EUR 0.8534, JPY 105.4, MXN 21.1431, ...) within 5e-9, and the per-day rate spread is at most 2.7e-6 over 11 to 18 days for the USD>X fiat pairs, the pairs the table is built from, so no per-day table is needed (the fiat-to-fiat pairs have much smaller samples and larger per-day spreads in `fx_variance.json`; none of them is used). Basis in the file: "recovered round constant". BTC: no clean constant; 8.41607e-5 BTC per USD from the paid>=100 subset (only 25 rows), "last digit uncertain": the subset has 25 rows and a standard error of about 6.9e-06 (relative, cv/sqrt(n)), the independent BTC>USD pair (13 rows) differs from the constant by 1.1e-05, and the 6-digit rounding of the constant is only -3.7e-07. BTC USD cents are therefore exact relative to this table, not to any real or measured rate; a relative error of that size is far below what a model would notice but is not "immaterial to cents" for the largest BTC payments. The table stores USD per unit, the reciprocal of the constant to 15 significant digits (half-even), as quoted strings. `make fx-freeze DS=hi_small` regenerates it byte for byte; a test checks that and the reciprocal rule. The Spark integer arithmetic stays below 1e36 with these digits (decimal(38,0)).
- **FX checked on HI-Medium: the table stays at `fx_version: 1` (user decision).** `make fx DS=hi_medium` (31,898,238 rows, 485,144 cross-currency rows, 208 pairs, `results/M01/hi_medium/fx_variance.json`), then `make fx-check DS=hi_medium`, which compares the measurement with the frozen table, never writes it, and is the only way HI-Medium is compared (`make fx-freeze` would overwrite the table HI-Small was built with). All 13 fiat constants are identical to the frozen ones; the per-day spread of the USD>X fiat rates is at most 6.6e-6. BTC: the paid>=100 subset (312 rows) gives 8.41611e-5 against the frozen 8.41607e-5, a relative difference of 5.0e-6 = 0.72 standard errors of the difference (combined 7.0e-6), inside the 3-standard-error band, so the table is kept (user decision: it is not re-pooled and HI-Small is not rebuilt). Read that result with care: the combined error is dominated by HI-Small's 25-row error (6.9e-6; HI-Medium's own is 1.4e-6), so the check cannot see a BTC shift smaller than about 2e-5. Measured against HI-Medium alone the frozen constant is 3.7 of its standard errors away: HI-Medium points to about 8.41611e-5, and BTC USD cents in both tables carry a possible systematic offset of the order of 5e-6 relative. The errors are cv/sqrt(n) of the per-row rates, an approximation for the amount-weighted ratio that is compared. The all-amounts BTC rate (8.41606e-5) is 6.1e-6 from the 6-digit subset constant, above the 1e-6 limit that `fx-freeze` enforces on itself; the likely cause is rounding of small BTC payments (per-row BTC rates run from 5.0e-5 to 1.0e-4, cv 0.13 over all 7,347 rows against 2.4e-5 on the 312-row subset), but that is a hypothesis: `fx_variance.json` does not split rates by payment size, and on HI-Small the all-amounts rate lies 5.8e-7 above its subset rate, the other way round. The all-amounts rate and the subset rate differ by 6.4e-6 within HI-Medium, 4.7 subset standard errors. For that reason `fx-check` judges each currency on its own (fiat: identical constant; BTC: within 3 combined standard errors) and keeps the 1e-6 limit on the freeze path only. The USD cents in the lake are exact relative to this table, not to real-world rates.
- **`src_row` column added** to `raw_transactions` (0-based data-row index, header excluded): the tie-break behind `tx_id` can be audited. `tx_id` = position after sorting by `(event_ts_ms, src_row)`.
- **Exact row index on the JVM** [manual: `rdd.zipWithIndex`]: persist the frame DISK_ONLY, sum per-partition counts in partition order, add `monotonically_increasing_id - (partition_id << 33)`. Same result as `zipWithIndex`, no Python round trip. Tested on a 16-row file read in several partitions.
- **Header ignored, columns positional.** The file has a duplicate `Account` header, and amounts must stay strings until typed, so the CSV is read as text lines and split on commas. The header line is dropped after the index and ingest raises if line 0 looks like data. Raw columns carry a `raw_` prefix: an output alias equal to an input column name made dependent expressions silently NULL in Spark 3.5.9; `check_no_nulls` also refuses any NULL in a derived column.
- **Money:** int64 minor units and int64 USD cents; a value outside int64 is an error (Python `ValueError`, Spark NULL which ingest refuses), never wrapped. Spark does the conversion in integer arithmetic on decimal(38,0) and gives the same integers as `features/money.py` on 1M random rows, with a synthetic table and with the frozen one (`test_money_spark_parity.py`); a negative amount is NULL in Spark and an error in Python.
- **`owner_bank_id`** is `A` if the last hex digit of `md5(sender_bank)` is even, else `B` (same in Python and Spark; no new dependency).
- **Split:** `t1`, `t2` are the `event_ts` at index floor(0.6N) and floor(0.8N) in `(event_ts_ms, tx_id)` order, in integer arithmetic. train `< t1`, val `[t1, t2)`, test `>= t2`. A timestamp is never split across two sets, so counts can differ slightly from an exact 60/20/20 index split; `split.json` reports the difference. The numbers go in D17 after the real run.
- **Patterns:** the k-th pattern line with a given 9-field key takes the k-th transaction (by `tx_id`) with that key. A line with several candidates is flagged `ambiguous`; an unmatched line keeps `tx_id` NULL. Coverage is reported as assigned-only and as an upper bound.
- **Max transactions per account (measured, HI-Small, `results/M01/hi_small/eda.json`): 169,756**, on account `070:100428660`; the next three are 103,671, 20,647 and 18,771. Against that the per-account percentiles are p50 4, p90 50, p99 115, p99.9 180 (nearest rank). The most entries one account holds inside the 72 h state-retention window is **60,403** (same account; an upper bound, legs with equal timestamps are all counted). So: a few hot keys, not a smooth tail. M2's `applyInPandas` must handle a group of about 169,756 rows, and M4's Flink per-key state and checkpoint size are set by those accounts, not by the typical one. **HI-Medium** (`results/M01/hi_medium/eda.json`, 2,077,023 accounts): **1,083,821**, on the same account `070:100428660`; the next three are 683,233, 210,234 and 133,638. Percentiles p50 6, p90 80, p99 187, p99.9 297. The most entries one account holds inside the 72 h window is **250,096** (same account). That is 6.4 times HI-Small's maximum on 6.3 times the rows, so the hot key grows with the data: M2's `applyInPandas` group is about 1.08M rows on HI-Medium, and M4's per-key state and checkpoint size are set by this account. HI-Large: TBD (not measured).
- **Sparse tail (decided by the user, 2026-11-22).** By the rule in `eda.py` (a trailing run of days under 0.1 x the reference daily rows, here 482,751, the median of the busier half of the days), the last 8 days (2022-09-11 to 2022-09-18) hold 1,108 rows with 655 laundering rows (59.1%, against 0.102% overall). All of them fall in the test split (`test`: 1,108 rows). That is 36% of the test split's 1,798 laundering rows in 0.11% of its rows (percentages computed from `split.json` and `eda.json`). Metrics on the test split will be dominated by these days.

  **Decision (user):** keep the raw lake and the split exactly as they are (D17 unchanged, nothing dropped, no new `fx_version` or rebuild), and in M02 **report test metrics both ways: all test rows, and test without the tail** (the trailing days the `sparse_days` rule flags, read from `eda.json`, not a hand-picked date). Both numbers appear together wherever a test metric is quoted. The tail is labelled **a generator artifact, not a temporal trend**: no result may describe it as drift, seasonality or a late-period rise in laundering. What the measurements support for that label is that both datasets show the same shape (a trailing run of days holding 1,108 rows (HI-Small) and 6,987 rows (HI-Medium) in total, 59.1% and 58.5% of them laundering, all in test); the generator mechanism itself is not measured here. The evaluation period is not shortened.

  **HI-Medium, same rule** (reference 1,930,514.5 rows/day, threshold 193,051.45): the trailing run is 12 days, 2022-09-17 to 2022-09-28 (28 days in all; day 16 has 3,021,866 rows, day 17 has 2,020, day 28 has 7). They hold 6,987 rows with 4,084 laundering (58.5%, against 0.110% overall), all in test, which starts on 2022-09-14; that is 38.4% of the test split's 10,642 laundering rows in 0.11% of its rows. Test metrics on HI-Medium would be dominated by these days, as on HI-Small, so the same decision applies: report both ways (all test rows, and test without the 12-day tail: 6,558 laundering in 6,373,195 rows, 0.103%), tail labelled a generator artifact, not a temporal trend.
- **Every laundering row is same-currency.** 72,170 rows have different paid and received currency, and 0 of them are laundering (HI-Medium: 485,144 rows, also 0). A cross-currency flag is therefore a perfect negative signal in this dataset; it will look strong in validation and is an artifact of the generator, not evidence about real laundering. Note it before it appears in a feature-importance plot.
- **Self-transfers:** 591,212 rows (11.64% of all), 11 of them laundering; 481,056 are `REINVESTMENT`. HI-Medium: 2,561,860 rows (8.03%), 72 of them laundering; 1,945,611 are `REINVESTMENT`. Decide in M2 how counterparty features (`s_new_counterparty_72h` and friends) treat a counterparty that is the account itself.
- **JPY precision:** 153,129 paid and 154,178 received JPY amounts in the file carry decimals although the spec's JPY exponent is 0, so they are rounded to whole yen (half-even); 1,064 paid and 1,065 received JPY amounts round to 0. HI-Medium: 846,105 paid and 850,029 received JPY amounts carry decimals; 3,504 paid and 3,491 received round to 0. No other currency loses precision. Not changed: the spec exponent stays 0.
- **Vocabularies:** the currency and payment-format names in the file are exactly the 15 and 7 in `spec.yaml` (nothing unmapped, nothing unused); HI-Medium too.
- **HI-Medium run (2026-11-22), same standard as HI-Small:** `make iceberg|split|money-check|eda DS=hi_medium` run with `AML_SPARK_DRIVER_MEMORY=10g AML_SPARK_SHUFFLE=96` (the command used; only the driver memory is recorded in `ingest.json`), first attempt, no fallback. 31,898,238 rows (= `wc -l` 31,898,239 minus the header; source SHA-256 equals `data/checksums.txt`), 35,230 laundering, `fx_version` 1; split recomputed from the table with 0 stored mismatches; `make money-check DS=hi_medium` recomputed every row with `features/money.py`: 31,898,238 rows, mismatches 0; patterns: 2,756 attempts, 22,743 lines, 0 ambiguous, 0 unmatched, coverage 64.56% (22,743 of 35,230 laundering rows are in a pattern, 12,487 are not). Everything big ran on the SSD: warehouse `$AML_DATA_ROOT/warehouse/aml_hi_medium`, `spark.local.dir` `$AML_DATA_ROOT/spill`, and, because the Mac disk was 95% full, `TMPDIR` and `JAVA_TOOL_OPTIONS=-Djava.io.tmpdir=...` pointed at `$AML_DATA_ROOT/tmp` (the JVM's default tmpdir and `$TMPDIR` are on the Mac disk). Needed code: `make fx-check`, a NULL-sum guard in the FX measurement, account percentiles from a histogram (a scratch re-run on HI-Small reproduced its committed `eda.json` and `docs/eda_hi_small.md` apart from provenance; that re-run is not committed), DuckDB capped and spilling to the SSD, and `test_real_tables.py` for both datasets.
- **Real-data checks (2026-11-22):** the table was re-read with DuckDB (`tests/integration/test_real_tables.py`, formerly `test_real_hi_small.py`): row count equals `wc -l` minus the header, `tx_id` contiguous, order strictly increasing, no NULLs. `make money-check DS=hi_small` recomputed every row with `features/money.py` from the source text and the frozen table: 5,078,345 rows, mismatches 0 (minor units paid/received and USD cents paid/received; `results/M01/hi_small/money_parity.json`). "Within 1% of 5M" in the manual is a rounded figure: the exact source count is the check.

  **Row-count tolerance waived (decided by the user, 2026-11-22).** The M01 DoD item "within 1% of the published sizes (5M / 32M)" is replaced by an exact match to `wc -l` minus the header (`test_real_tables.py`). The paper's table gives only the rounded "5M" and "32M"; HI-Small's 5,078,345 rows are 1.57% above 5M, so it would fail the manual's test, and HI-Medium's 31,898,238 are 0.32% below 32M. **Laundering count:** the manual notes the paper lists "3.6K" laundering transactions for HI-Small but a rate of 1 in 981. Measured: 5,177 laundering rows in 5,078,345, 1 in 980.9 (`results/M01/hi_small/ingest.json`; `one_laundering_in` in `results/M01/hi_small/eda.json`), which matches the 1 in 981 and not the 3.6K. The 3.6K is not used; our count is the one recorded (HI-Medium: 35,230).
- **Known limitation: the DuckDB re-checks read Parquet files by glob, not the current Iceberg snapshot (decided by the user, 2026-11-22).** `money_check.py` and `tests/integration/test_real_tables.py` read `<table>/data/*/[0-9]*.parquet`. That equals the table only while every data file belongs to the one snapshot, which holds for the M01 tables: each was written by one run (`raw_transactions`: create the empty table, then one append; `patterns`: one `createOrReplace`), and the row counts are asserted against `wc -l`, so a stray file from an aborted write would make those assertions fail, not pass. It stops being true as soon as a table gets a second snapshot (an overwrite, compaction, `expire_snapshots` or a delete leaves files the current snapshot no longer references). **Must be fixed before any table gets a second snapshot (Phase 5, Glue):** list the files from the current snapshot (PyIceberg `scan().plan_files()`, or `iceberg_scan` in DuckDB) instead of globbing, and make `money_check` assert that the rows it compared equal the table's row count. Not done in M01.

## D37 — Big data lives on the external SSD (`AML_DATA_ROOT`)
Accepted (M1, the user's choice on 2026-11-22): the Mac is 95% full, so raw data, the Iceberg warehouse and Spark's spill space go to an external SSD.
- **Paths:** `AML_DATA_ROOT=/Volumes/Shekar_ssd/project_2`; raw files in `$AML_DATA_ROOT/raw`, the Iceberg warehouse in `$AML_DATA_ROOT/warehouse` (replaces `var/warehouse`), `spark.local.dir` in `$AML_DATA_ROOT/spill` (replaces `var/spark-tmp`). The Makefile exports the default (`?=`, so an environment value wins); `batch/spark/common.py` has the same default for a bare `pytest`. `data/checksums.txt` stays in the repo.
- **Fail fast:** `make fx | iceberg | split | eda | money-check` stop with `SSD not mounted at <path>` before starting anything; so does `common.get_spark()` and the batch CLIs. `make up` does not check the SSD: the compose stack starts without it.
- **Not changed (yet):** the compose Flink mount still points at `var/warehouse`. It only matters when Flink reads Iceberg (M3/M4); then the warehouse must be mounted at the same absolute path (Iceberg stores absolute paths), which means adding `/Volumes` to Docker's file sharing. `tests/integration/test_lake_smoke.py` keeps its own `var/warehouse`, so it runs without the SSD. Every other batch test writes to the SSD warehouse and fails with the message above if it is missing.
- **Consequences / risks:** the volume is exFAT. The JDBC catalog keeps its metadata pointer in Postgres, so Iceberg needs no atomic rename on the file system (a Hadoop catalog would). exFAT has no symlinks or hard links, which neither Spark's local mode nor Iceberg uses. Files written from macOS may get `._*` AppleDouble companions. The warehouse is only on this SSD: unplugging it while a job runs fails the job; the catalog rows in Postgres would then point at files that are not there until it is back.
- **Data on disk (2026-11-22):** the Kaggle download was one `archive.zip` holding all 18 files (HI/LI x Small/Medium/Large), not separate HI-Small zips. `HI-Small_*` and `HI-Medium_*` (transactions and patterns) are extracted into `$AML_DATA_ROOT/raw`, hashed in `data/checksums.txt` and ingested; the zip is kept (it also holds the Large files, which M01 does not use). SHA-256 and line counts are in `data/checksums.txt`.

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
