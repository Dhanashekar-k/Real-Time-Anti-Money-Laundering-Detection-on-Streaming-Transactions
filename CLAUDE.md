# stream-aml — instructions for Claude Code

Real-time anti-money-laundering detection on IBM's synthetic AML transactions. A portfolio project: every claim must be backed by a reproducible measurement.

Pipeline: replayer → two simulated banks (different formats) → config-driven connectors → Kafka (`tx.canonical`) → Flink/Java event-time features → XGBoost scorer → alert service (Postgres, idempotent, outbox) → Claude case-draft worker with a validator → React review UI → nightly retrain (Airflow, MLflow, promotion gate). v2 adds per-bank isolation (Postgres row-level security), four human-in-the-loop loops with maker-checker approvals, a drift monitor and measured hallucination rates. Batch side: Spark + Iceberg computes the same features for training.

## Source of truth: read before acting
- `manual/milestones/Mxx_*.md`: the plan for the milestone in progress. Read it before planning or coding.
- `manual/01_plan_review_and_changes.md`: why the design is what it is.
- `features/spec.yaml`: feature definitions and **exact** semantics (ordering, windows, nulls, money). Spark (`features/reference.py`) and Flink both implement it; `tests/golden/` holds the vectors both must reproduce.
- `contracts/`: Avro schemas, Postgres DDL, agent output schema. Change only when asked; bump versions; keep Avro BACKWARD-compatible.
- `docs/decisions.md`: decision log. Add an entry whenever a design choice is made or changed.

## Hard rules
1. **Never invent, estimate or round a metric into existence.** Numbers in `docs/results.md`, the README or chat summaries come only from files in `results/` written by scripts. If something isn't measured, write "TBD (not measured)".
2. **No label leakage.** `is_laundering`, pattern data and `split` never enter Kafka topics, feature lists, evidence queries or prompts. Tests in `tests/leakage/` must pass before any milestone closes.
3. **Tests first** for: golden feature vectors, parity, connector mappings, agent validator, reconciliation, promotion gate.
4. **Human-owned code.** Files or blocks marked `OWNER: human` (`features/reference.py` window logic, `AccountFeatureFunction`, `JoinHalvesFunction`, `DedupeFunction`, the checkpoint/sink block in `FeatureJob`, `services/agent/validator.py`, `ops/reconcile.py:compare`). Scaffold signatures, tests and `TODO(human)` markers. **Don't write the bodies unless the user says "implement it".** If asked to implement, explain the code line by line afterwards.
5. **Cloud safety.** All AWS-touching make targets are named `cloud-*`, and the user approves each one. Never run raw `terraform apply/destroy`, `helm install/uninstall`, `kubectl apply/delete` or destructive `aws` commands yourself, and never use `-auto-approve`. Terraform: S3 backend with `use_lockfile`, `default_tags` on every resource, **no NAT gateways**, **no `Service type: LoadBalancer`**, Strimzi `deleteClaim: true`, Glue jobs with a timeout and 0 retries. Run the `cost-guard` subagent on any `infra/` change.
6. **Secrets.** Never read `.env`, `~/.aws`, `~/.kaggle`. The app's Claude key is `AML_ANTHROPIC_API_KEY`. Never set, export or reference `ANTHROPIC_API_KEY` (it would switch Claude Code's own billing).
7. **Big data stays out of context.** Don't open files in `data/`. Inspect with `head`, `wc -l`, `duckdb -c "…"` or small Spark samples.
8. **Pinned versions.** If an API might differ from memory (Flink especially), check the docs for the pinned version, or the jar sources (`mvn dependency:sources`), before writing code. List what you checked.
9. **Small, reviewable steps.** Plan first for anything spanning more than one file. Commit after each green step with a conventional message. Never push.
10. **The AWS account is on the Free plan (no card).** EC2 and EKS node types are limited to `t3.micro`, `t3.small`, `t4g.micro`, `t4g.small`, `c7i-flex.large`, `m7i-flex.large`. EMR and MSK are not available; Spark in the cloud runs on AWS Glue 5.1. Build images for `linux/amd64`. Never propose other instance types or services without saying they need the Paid plan (decision D22).
11. **Tenant isolation (v2).** The API touches Postgres only as `aml_api` and only through `services/common/db.py:tenant_tx`, which sets `app.bank_id`, `app.role` and `app.user_id` with bound parameters. Never build those settings with string formatting, never connect the API as `aml_service` or `postgres`, and keep `make test-rls` green.

## Versions (pin the latest patch at M0; record in docs/decisions.md D-versions)
| Component | Version |
|---|---|
| Python | 3.11 (the same as AWS Glue 5.1, so Spark job code runs unchanged locally and on Glue) |
| Java | 17 (Maven 3.9) |
| Flink | 1.20.x LTS · `flink-connector-kafka` 3.4.0-1.20 · Flink K8s Operator latest supporting 1.20 |
| Spark | 3.5.x locally · AWS Glue 5.1 in the cloud (Spark 3.5.6) · `iceberg-spark-runtime-3.5_2.12` (latest 1.x for 3.5) |
| Kafka | Redpanda (local) · Strimzi + Kafka 4.2.x KRaft (EKS) |
| Postgres | 17 |
| ML | XGBoost (latest), LightGBM (optional), MLflow 3.x (aliases `champion`, `challenger`) |
| Orchestration | Airflow 3.x (`airflow standalone`, dev only) |
| LLM | Anthropic Python SDK; models `claude-haiku-5-5` (default), `claude-sonnet-5-5`, `claude-opus-5-5`; structured outputs via `output_config.format` |
| Web | Vite + React + TypeScript, TanStack Query, Cytoscape.js |

## Data and naming conventions
- Datasets: `hi_small`, `hi_medium`, `hi_large`. Iceberg namespace `aml_<ds>`; catalog name `lake`.
- `tx_id` = `seq` from M1 (stable sort by timestamp, then source row). Total order is `(event_ts_ms, tx_id)`.
- Big data (raw CSVs, Iceberg warehouse, Spark spill) lives under `$AML_DATA_ROOT` (the external SSD, D37); targets that touch it fail fast if it is not mounted. `data/checksums.txt` stays in the repo.
- Accounts: `<bank_id>:<account_id>`. Time: `event_ts_ms` (epoch millis, UTC). Money: int64 minor units + int64 USD cents, via `features/money.py` (Decimal, ROUND_HALF_EVEN). No floats for money.
- Topics: `bank_a.raw`, `bank_b.raw`, `bank_c.raw`, `tx.canonical`, `tx.dlq`, `tx.late`, `tx.features`, `tx.scores`.
- Results: `results/Mxx/<ds>/*.json`. Every script that measures something writes JSON there, including the git SHA, dataset, config and timestamp.

## Commands (keep this list current as milestones add targets)
```
make up | up-all | down | ps | logs S=<svc> | rebuild   # local stack (compose profiles); rebuild = local mlflow image
make fmt | lint | tf-check | test | itest | test-rls    # quality; itest and test-rls need `make up`; tf-check = terraform fmt + validate
make topics                                          # create Kafka topics (idempotent; exits 1 on drift); needs `make up`
make help                                            # list the implemented targets. Targets for later milestones are stubs that exit 1
make fx DS= | fx-freeze DS= | iceberg DS=hi_small | eda DS= | split DS=   # M1 (fx = measure FX variance, fx-freeze = write fx_rates.yaml after review)
make golden | features DS= | rules DS= | train DS= | eval DS= FINAL=1   # M2
make replay DS= SPEEDUP=|RATE=|ASAP=1 [FROM= TO= BANK= DRY=1] | connectors | connector-local BANK= | register-schemas | onboard-check BANK= SAMPLE= | ingest-bench DS=   # M3 (connectors = compose apps profile; run make up and make topics first)
make flink-build | flink-deploy MODE=eos|alo CKPT=5s | flink-stop | parity DS= | restart-test   # M4
make apps | latency-matrix | seed-thresholds   # M5 (test-rls already exists, see above)
make agent | agent-eval                              # M6 (v2: cascade, tool-use, injection, hallucination blocks)
make api | ui | retrain AS_OF= | airflow             # M7
make chaos RUNS=20 | reconcile RUN= | drift-experiment   # M8
make results | demo                                  # M11
make teardown-check                                  # read-only AWS leftovers check
make cloud-bootstrap | cloud-up | cloud-images | cloud-deploy | cloud-glue-run JOB= DS= | cloud-replay | cloud-down   # user-approved
```

## Repo map
`batch/spark/` Spark jobs · `stream/replayer|connectors|flink-features/` streaming · `ml/` training, thresholds, gate · `eval/` evaluation · `services/scorer|alerts|agent|api/` · `web/` UI · `airflow/dags/` · `ops/` chaos + reconcile · `infra/compose|terraform|helm/` · `tests/unit|golden|leakage|parity|integration/` · `results/` measured outputs · `docs/` decisions, generated results, runbooks · `manual/` the build manual.

## Style
Python: ruff (format + lint), type hints on public functions, pytest markers `unit` (default), `integration`, `slow`. Java: package `com.streamaml.features`; pure logic in plain classes (`FeatureMath`) with JUnit tests; Flink operators thin. TypeScript: strict mode. Logs as JSON lines with `tx_id` when relevant.
