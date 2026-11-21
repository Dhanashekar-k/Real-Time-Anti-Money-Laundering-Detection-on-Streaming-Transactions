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

.PHONY: help up up-all rebuild down ps logs nuke-local dirs bootstrap fmt lint tf-check test itest test-rls topics \
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

nuke-local: ## DELETE all compose volumes (Postgres, Redpanda data); you type "nuke" to confirm
	@[ -t 0 ] || { echo "nuke-local needs an interactive terminal to confirm (stdin is not a TTY)" >&2; exit 2; }
	@printf 'This deletes the Postgres and Redpanda volumes. Type "nuke" to continue: '; read -r answer; [ "$$answer" = nuke ] || { echo "aborted" >&2; exit 1; }
	$(COMPOSE) $(PROFILES_ALL) down -v --remove-orphans

bootstrap: ## uv sync (+ maven offline deps once the Flink module exists)
	uv sync
	@if [ -f stream/flink-features/pom.xml ]; then mvn -B -q -f stream/flink-features/pom.xml dependency:go-offline; fi

# ---------------------------------------------------------------------- quality
fmt: ## format code (ruff, and terraform fmt when terraform is installed)
	uv run ruff format .
	uv run ruff check --fix .
	@if command -v terraform >/dev/null; then terraform fmt -recursive infra/terraform; fi

lint: ## ruff check + format check
	uv run ruff check .
	uv run ruff format --check .

tf-check: ## terraform fmt -check + validate for every root (no AWS access needed)
	terraform fmt -check -recursive infra/terraform
	@for d in $$(find infra/terraform -name '*.tf' -not -path '*/.terraform/*' -exec dirname {} \; | sort -u); do \
		echo "validate $$d"; \
		terraform -chdir=$$d init -backend=false -input=false >/dev/null && terraform -chdir=$$d validate || exit 1; \
	done

test: ## unit and golden/leakage tests (no services needed)
	uv run pytest -m "not integration"

itest: ## integration tests (need `make up`)
	uv run pytest -m integration

test-rls: ## tenant-isolation tests on a throwaway database (need `make up`)
	uv run pytest -m integration tests/integration/test_contracts_db.py -k "rls or schema"

topics: ## create Kafka topics (idempotent; fails on drift; needs `make up`)
	@bash infra/compose/topics.sh

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
TF_BOOTSTRAP := infra/terraform/bootstrap

# Never -auto-approve (CLAUDE.md rule 5): terraform shows the plan and waits for you to type yes.
#   - stdin must be a terminal, which stops a plain `yes | make cloud-bootstrap`
#   - TF_CLI_ARGS* are scrubbed, so an environment variable cannot add the auto-approve flag
# These are guards against accidents, not a security boundary: a pseudo-terminal fed by a pipe
# (`yes | script -q /dev/null make ...`) would still get through. The ask rule for make cloud-*
# in .claude/settings.json is the real gate for Claude Code.
cloud-bootstrap: ## [AWS] create the state bucket + budgets; asks you to type yes
	@test -f $(TF_BOOTSTRAP)/terraform.tfvars || { echo "create $(TF_BOOTSTRAP)/terraform.tfvars from terraform.tfvars.example first" >&2; exit 2; }
	@[ -t 0 ] || { echo "cloud-bootstrap needs an interactive terminal to type 'yes' (stdin is not a TTY): run it in your own terminal" >&2; exit 2; }
	env -u TF_CLI_ARGS -u TF_CLI_ARGS_init terraform -chdir=$(TF_BOOTSTRAP) init -input=false
	env -u TF_CLI_ARGS -u TF_CLI_ARGS_apply terraform -chdir=$(TF_BOOTSTRAP) apply

cloud-up cloud-images cloud-deploy cloud-glue-run cloud-replay cloud-down: ; $(call stub,M9)
