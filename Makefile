# stream-aml. Run `make help`. Targets for later milestones are stubs that exit 1 (so nothing can
# look green while doing nothing). Every AWS-touching target is named cloud-* and needs approval.

AML_ROOT := $(CURDIR)
export AML_ROOT

COMPOSE      := docker compose -f infra/compose/docker-compose.yml
PROFILES_UP  := --profile core --profile stream --profile ml
PROFILES_ALL := $(PROFILES_UP) --profile orchestration --profile obs

# Stub for a target a later milestone adds: fail loudly instead of passing silently.
stub = @echo "make $@: not implemented yet (added in $(1))" >&2; exit 1

.DEFAULT_GOAL := help

.PHONY: help up up-all rebuild down ps logs nuke-local dirs bootstrap fmt lint test itest test-rls topics \
	iceberg eda split golden features rules train eval replay connectors onboard-check \
	flink-build flink-deploy flink-stop parity restart-test apps latency-matrix seed-thresholds \
	agent agent-eval api ui retrain airflow chaos reconcile drift-experiment results demo \
	teardown-check cloud-bootstrap cloud-up cloud-images cloud-deploy cloud-glue-run cloud-replay \
	cloud-down

help: ## list targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ------------------------------------------------------------------ local stack (M00)
dirs:
	@mkdir -p var/warehouse var/flink-checkpoints var/mlflow

# Every build of the local MLflow image gets a new image ID even with all layers cached, and
# compose recreates the container when the ID changes. So `up` never builds; the image is built
# only when its Dockerfile is newer than the stamp (or by `make rebuild`).
MLFLOW_STAMP := var/.built-mlflow

$(MLFLOW_STAMP): infra/compose/mlflow/Dockerfile | dirs
	$(COMPOSE) --profile core --profile ml build mlflow
	@touch $@

rebuild: ## force a rebuild of the local images (mlflow)
	@rm -f $(MLFLOW_STAMP)
	@$(MAKE) --no-print-directory $(MLFLOW_STAMP)

up: dirs $(MLFLOW_STAMP) ## start core + stream + ml and wait until healthy
	$(COMPOSE) $(PROFILES_UP) up -d --wait

up-all: dirs $(MLFLOW_STAMP) ## start everything, including Airflow, Prometheus and Grafana
	$(COMPOSE) $(PROFILES_ALL) up -d --wait

down: ## stop all services (keeps volumes)
	$(COMPOSE) $(PROFILES_ALL) down --remove-orphans

ps: ## service status and health
	@$(COMPOSE) $(PROFILES_ALL) ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'

logs: ## follow one service: make logs S=redpanda
	@test -n "$(S)" || { echo "usage: make logs S=<service>" >&2; exit 2; }
	$(COMPOSE) $(PROFILES_ALL) logs -f --tail=200 $(S)

nuke-local: ## DELETE all compose volumes (Postgres, Redpanda data). Asks first.
	$(COMPOSE) $(PROFILES_ALL) down -v --remove-orphans

bootstrap: ## uv sync (+ maven offline deps once the Flink module exists)
	uv sync
	@if [ -f stream/flink-features/pom.xml ]; then mvn -B -q -f stream/flink-features/pom.xml dependency:go-offline; fi

# ---------------------------------------------------------------------- quality
fmt: ## format code
	uv run ruff format .
	uv run ruff check --fix .

lint: ## ruff check + format check
	uv run ruff check .
	uv run ruff format --check .

test: ## unit and golden/leakage tests (no services needed)
	uv run pytest -m "not integration"

itest: ## integration tests (need `make up`)
	uv run pytest -m integration

test-rls: ## tenant-isolation tests on a throwaway database (need `make up`)
	uv run pytest -m integration tests/integration/test_contracts_db.py -k "rls or schema"

topics: ## create Kafka topics (idempotent)
	$(call stub,M00 step 5)

# --------------------------------------------------------------------- later milestones
iceberg eda split: ; $(call stub,M1)
golden features rules train eval: ; $(call stub,M2)
replay connectors onboard-check: ; $(call stub,M3)
flink-build flink-deploy flink-stop parity restart-test: ; $(call stub,M4)
apps latency-matrix seed-thresholds: ; $(call stub,M5)
agent agent-eval: ; $(call stub,M6)
api ui retrain airflow: ; $(call stub,M7)
chaos reconcile drift-experiment: ; $(call stub,M8)
results demo: ; $(call stub,M11)
teardown-check: ; $(call stub,M9)

# ------------------------------------------------- cloud-* (AWS; each one needs your approval)
cloud-bootstrap: ; $(call stub,M00 step 8)
cloud-up cloud-images cloud-deploy cloud-glue-run cloud-replay cloud-down: ; $(call stub,M9)
