# M00 — Foundation: one-command local stack, guardrails, CI

**Timebox:** 6 h fast / 8 h full · **Trigger:** compose not healthy after 8 h → drop Airflow + Grafana until later.
**Role signal:** SWE hygiene (reproducible env, CI, IaC state discipline), cloud cost discipline.

## Inputs
- `starter-kit/` copied into the repo root, plus `manual/` (see `04_claude_code_playbook.md` §1)
- AWS Step 0–1 done (`03_aws_free_tier_and_costs.md`)
- Tools on your Mac:
  ```bash
  brew install uv git gh jq awscli kubectl helm k9s maven openjdk@17 node duckdb
  brew tap hashicorp/tap && brew install hashicorp/tap/terraform
  brew install redpanda-data/tap/redpanda      # provides `rpk`
  uv tool install kaggle && uv tool install ruff
  ```
  Docker: Docker Desktop or OrbStack. Give it **≥ 10 GB RAM** and 4+ CPUs. If your Mac has 16 GB total, rely on compose profiles and run only what each milestone needs. [Unclear] I don't know your Mac's RAM, so check it before choosing.

## Outputs
| Output | Path |
|---|---|
| uv workspace + deps | `pyproject.toml`, `uv.lock` |
| Compose stack with profiles | `infra/compose/docker-compose.yml` |
| Makefile with the targets from CLAUDE.md | `Makefile` |
| Topic bootstrap | `make topics` (uses `rpk`) |
| Postgres init (DBs: `aml`, `mlflow`, `airflow`, `iceberg_catalog`) | `infra/compose/postgres-init/*.sql` + `contracts/postgres_schema.sql` applied to `aml` |
| CI | `.github/workflows/ci.yml` |
| Terraform bootstrap (state bucket + budgets) | `infra/terraform/bootstrap/` |
| Empty dirs with READMEs | `batch/ stream/ ml/ services/ eval/ ops/ load/ tests/ results/ docs/` |

## Steps

1. **[CC] Python workspace.** `pyproject.toml` (Python 3.11) with dependency groups:
   - `core`: pyyaml, pydantic, numpy, pandas, pyarrow
   - `kafka`: confluent-kafka, fastavro
   - `lake`: pyiceberg[sql-postgres,pyarrow], duckdb
   - `spark`: pyspark==3.5.*
   - `ml`: xgboost, lightgbm, scikit-learn, mlflow, shap (optional)
   - `svc`: fastapi, uvicorn, psycopg[binary,pool], prometheus-client, anthropic, rapidfuzz
   - `dev`: pytest, pytest-xdist, ruff, mypy, hypothesis
2. **[CC] Compose** (`infra/compose/docker-compose.yml`), official images only, with profiles:
   - `core`: `redpanda` (ports 19092/18081/19644; `--smp 2 --memory 2G --overprovisioned` for a laptop), `redpanda-console` (8080), `postgres:17` (5432, init scripts).
   - `stream`: `flink-jobmanager` + `flink-taskmanager` (`flink:1.20-java17`, TM 4 slots, `taskmanager.memory.process.size: 4g`). Volume `var/flink-checkpoints` mounted at `/flink-checkpoints`, and the warehouse mounted **at the same absolute path as on the host** so file paths in Iceberg metadata resolve inside containers too.
   - `ml`: `ghcr.io/mlflow/mlflow` (5000), backend `postgresql://…/mlflow`, artifacts in `var/mlflow`.
   - `orchestration`: `apache/airflow:3.x` running `airflow standalone` (8085) on its own Postgres DB.
   - `obs`: `prom/prometheus` (9090), `grafana/grafana` (3000) with provisioning folders.
   - `apps`: added in M3–M7 (replayer, connectors, scorer, alerts, agent-worker, api).
3. **[CC] Makefile.** `up` (= core, stream, ml), `up-all`, `down`, `ps`, `logs S=<svc>`, `nuke-local` (removes volumes; it's on the ask list), `fmt`, `lint`, `test`, `itest`, `topics`, `bootstrap` (uv sync + mvn dependency:go-offline later). Every AWS-touching target is named `cloud-*`.
4. **[CC] Topics.** `make topics` creates the topics in `02_architecture_and_stack.md` §3 (local partition counts), using `rpk topic create` with retention flags.
5. **[CC] Iceberg catalog smoke test.** A 20-line PySpark script creates `lake.smoke.t1` in the JDBC catalog (Postgres `iceberg_catalog` DB, warehouse `file://$PWD/var/warehouse`), inserts 3 rows and reads them back. Then PyIceberg reads the same table, and DuckDB reads the Parquet files. This proves that the three engines see the same table.
6. **[CC] CI.** GitHub Actions on push/PR: `uv sync`, `ruff check`, `pytest -m "not integration"`, `mvn -B -q verify` (path-filtered to `stream/flink-features/**`), and later `npm ci && npm run build` for `web/`.
7. **[CC, you review] Terraform bootstrap** (`infra/terraform/bootstrap/`): an S3 bucket for Terraform state (versioning on, public access blocked), budgets ($5/$20/$50 with email alerts), and an outputs file printing the backend config. Every other Terraform root uses `backend "s3" { bucket=…, key=…, region="us-east-1", use_lockfile=true }`. Run it once yourself: `make cloud-bootstrap` (ask rule → you confirm).
8. **[YOU] Review `CLAUDE.md`.** Read it end to end, adjust anything you disagree with, and commit it.

## Claude Code prompt (after `/milestone-start M00` produces a plan, or use directly)
> Implement M00 from manual/milestones/M00_foundation.md. Use official images only (no Bitnami, no MinIO). Iceberg uses a JDBC catalog on the compose Postgres with a filesystem warehouse at $PWD/var/warehouse, mounted at the same absolute path in containers. Create the Makefile targets listed in CLAUDE.md that apply to M00; leave the later ones as `@echo "added in Mxx"` stubs. Write the Iceberg three-engine smoke test as tests/integration/test_lake_smoke.py. Stop after `make up` and `make test` succeed and show me `docker compose ps`.

## Tests
- `tests/integration/test_lake_smoke.py`: Spark writes, PyIceberg and DuckDB read the same 3 rows.
- `tests/integration/test_topics.py`: every expected topic exists with the expected partition count.
- `tests/unit/test_contracts.py`: every `.avsc` parses with fastavro; `postgres_schema.sql` applies cleanly to a fresh database (integration); `agent_output.schema.json` is valid JSON Schema.

## Definition of Done
- [ ] `make up && make ps` shows all core/stream/ml services healthy
- [ ] `make test` green; CI green on GitHub
- [ ] `rpk cluster info -X brokers=localhost:19092` works; `make topics` idempotent (second run makes no changes)
- [ ] Flink UI at :8081, Redpanda Console at :8080, MLflow at :5000
- [ ] `aws budgets describe-budgets --account-id <id> --profile stream-aml` lists 3 budgets
- [ ] `docs/decisions.md` has D16 (your account type, credits, expiry) and the pinned versions

## Gotchas
- **Port 8081:** Flink's UI and a default schema registry both want 8081. That's why the registry is on 18081.
- **Iceberg file paths:** metadata stores absolute paths. If Spark on the host writes `file:///Users/shekar/...`, containers need the same path mounted, or reads fail with "file not found".
- **Java:** Spark 3.5 and Flink 1.20 both run on JDK 17. Set `JAVA_HOME` to the brew `openjdk@17`.
- **Apple Silicon:** all the chosen images are multi-arch ([Likely]). If one isn't, pin `platform: linux/arm64` or find an alternative rather than running under emulation.
