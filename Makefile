.DEFAULT_GOAL := help
PY ?= python3

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

install: ## Install in editable mode with dev tools
	$(PY) -m pip install -e ".[dev]"

up: ## Start the full stack (Postgres, Redis, RabbitMQ, API)
	docker compose up --build -d

down: ## Stop the stack and delete volumes
	docker compose down -v

logs: ## Tail API logs
	docker compose logs -f api

dev: ## Run the API locally with auto-reload (needs Postgres from `make infra`)
	uvicorn foundry.api.app:app_factory --factory --reload --port 8000

infra: ## Start only the backing services
	docker compose up -d postgres redis rabbitmq

scale: ## Scale the worker fleet, e.g. `make scale N=8`
	docker compose up -d --scale worker=$(or $(N),6)

local-up: ## Run api + relay + reaper + 3 workers as local processes (no Docker for the apps)
	./scripts/local_stack.sh up

local-down: ## Stop the local processes
	./scripts/local_stack.sh down

limits-demo: ## Tour of rate limits, quotas, concurrency limits and metrics
	$(PY) scripts/demo_part3.py

traffic: ## 60 s of mixed traffic to light up the Grafana dashboard
	$(PY) scripts/demo_part3.py --traffic 60

dashboard: ## Regenerate the Grafana dashboard JSON from ops/grafana/build_dashboard.py
	$(PY) ops/grafana/build_dashboard.py

crash-demo: ## Kill a worker mid-flight and watch jobs recover (stack must be running)
	$(PY) scripts/demo_part2.py

test: ## Run unit + integration tests (needs FOUNDRY_TEST_DATABASE_URL or local Postgres)
	$(PY) -m pytest

lint: ## Lint and format-check
	ruff check src tests scripts ops && ruff format --check src tests scripts ops

fmt: ## Auto-format
	ruff check --fix src tests scripts ops && ruff format src tests scripts ops

demo: ## Smoke-test the running API end to end
	./scripts/demo_part1.sh
