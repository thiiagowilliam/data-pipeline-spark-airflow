.PHONY: up down restart logs test lint simulate clean help

# ──────────────────────────────────────────────
# Data Pipeline — Makefile
# ──────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ── Docker ────────────────────────────────────

up: ## Start all services
	docker compose up --build -d

down: ## Stop all services
	docker compose down

restart: down up ## Restart all services

logs: ## Show logs (use: make logs s=airflow-scheduler)
	@if [ -z "$(s)" ]; then \
		docker compose logs -f --tail=100; \
	else \
		docker compose logs -f --tail=100 $(s); \
	fi

# ── Development ───────────────────────────────

test: ## Run tests with pytest
	pytest tests/ -v

lint: ## Run linters (pre-commit)
	pre-commit run --all-files

check-dag: ## Verify DAG parses without errors
	python3 -c "import ast; ast.parse(open('airflow/dags/bronze_pipeline.py').read()); print('✅ DAG OK')"

check-all: ## Syntax check all Python files
	@find airflow/dags/scripts/spark_jobs -name "*.py" -exec sh -c \
		'python3 -c "import ast; ast.parse(open(\"{}\").read()); print(\"✅ {}\")"' \;

# ── Simulator ─────────────────────────────────

simulate: ## Run data simulator
	cd simulator && python3 data_simulator.py

# ── Cleanup ───────────────────────────────────

clean: ## Remove all containers, volumes, and cached data
	docker compose down -v
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
