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

loadtest: ## Open-loop load test against the running stack (RATE=50 DURATION=30)
	$(PY) -m foundry.cli loadtest --rate $${RATE:-50} --duration $${DURATION:-30} --output .run/loadtest.json

chaos: ## Load test + fault injection + invariant audit (SCENARIO=kitchen-sink; spawns its own fleet)
	$(PY) -m foundry.cli chaos $${SCENARIO:-worker-crash} --duration 40 --output .run/chaos.json

audit: ## Verify every invariant from the database alone
	$(PY) -m foundry.cli audit

test-chaos: ## Slow end-to-end fault-injection tests
	$(PY) -m pytest -m chaos tests/chaos

test: ## Run unit + integration tests (needs FOUNDRY_TEST_DATABASE_URL or local Postgres)
	$(PY) -m pytest

k8s-validate: ## Render kustomize overlays and validate against the Kubernetes schema
	python3 scripts/validate_k8s.py

k8s-dev: ## Deploy to the current kube context (kind/minikube) with throwaway deps
	kubectl apply -k deploy/k8s/overlays/dev

lint: ## Lint and format-check
	ruff check src tests scripts ops && ruff format --check src tests scripts ops

fmt: ## Auto-format
	ruff check --fix src tests scripts ops && ruff format src tests scripts ops

demo: ## Smoke-test the running API end to end
	./scripts/demo_part1.sh
