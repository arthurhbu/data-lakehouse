# =============================================================================
# Data Lakehouse — Makefile
# Atalhos para operações comuns do projeto
# =============================================================================

COMPOSE = docker compose
ENV_FILE = .env

# ---------------------------------------------------------------------------
# Infraestrutura (Docker Compose profiles)
# ---------------------------------------------------------------------------

.PHONY: up down restart status logs

up:  ## Sobe toda a stack (core + cdc + catalog)
	$(COMPOSE) --env-file $(ENV_FILE) --profile core --profile cdc --profile catalog up -d 

up-core:  ## Sobe apenas Postgres, Kafka e MinIO
	$(COMPOSE) --env-file $(ENV_FILE) --profile core up -d

up-cdc:  ## Sobe Kafka Connect + Debezium (inclui core)
	$(COMPOSE) --env-file $(ENV_FILE) --profile core --profile cdc up -d

up-catalog:  ## Sobe Iceberg REST Catalog (inclui core)
	$(COMPOSE) --env-file $(ENV_FILE) --profile core --profile catalog up -d

up-transform:  ## FASE 3 — ainda não implementado
	@echo "Profile transform ainda não implementado (FASE 3)."

up-orchestration:  ## FASE 4 — ainda não implementado
	@echo "Profile orchestration ainda não implementado (FASE 4)."

up-serving:  ## FASE 6 — ainda não implementado
	@echo "FastAPI e BI ainda não implementados (FASE 6). Use make up-trino para consultas."

up-trino:  ## Sobe Trino Query Engine
	$(COMPOSE) --env-file $(ENV_FILE) --profile core --profile catalog --profile trino up -d

up-ai:  ## FASE 6 — ainda não implementado
	@echo "Profile ai ainda não implementado (FASE 6)."

down:  ## Para e remove todos os containers
	$(COMPOSE) --env-file $(ENV_FILE) --profile core --profile cdc --profile catalog --profile trino down

restart: down up  ## Reinicia toda a stack

status:  ## Mostra status dos containers
	$(COMPOSE) ps

logs:  ## Mostra logs de todos os containers (follow)
	$(COMPOSE) logs -f

# ---------------------------------------------------------------------------
# Pipeline de dados
# ---------------------------------------------------------------------------

.PHONY: register-connectors bronze silver gold

register-connectors:  ## Registra/atualiza Debezium e S3 Sink no Kafka Connect
	python -m scripts.register_connectors

bronze: register-connectors  ## Bronze é materializada continuamente pelo S3 Sink

silver:  ## Executa apply Silver (Bronze → Iceberg MERGE INTO) para todas as tabelas
	python -m ingestion.silver.apply_silver --table partners
	python -m ingestion.silver.apply_silver --table accounts
	python -m ingestion.silver.apply_silver --table transactions
	python -m ingestion.silver.apply_silver --table payment_events
	python -m ingestion.silver.apply_silver --table ledger_entries

gold:  ## Executa transformações Gold via dbt
	cd transform/dbt_project && dbt run --select marts

reconcile:  ## Roda script de reconciliação ponta-a-ponta (Postgres vs Bronze vs Silver)
	python -m scripts.reconciliation.check_pipeline

migrate-silver-contract:  ## Dry-run da migração reversível da Silver legada
	python -m scripts.migrate_silver_contract

migrate-silver-contract-apply:  ## Preserva Silver legada e libera reconstrução v2
	python -m scripts.migrate_silver_contract --apply

# ---------------------------------------------------------------------------
# Manutenção Iceberg (Zeladoria)
# ---------------------------------------------------------------------------

.PHONY: maintenance compact expire-snapshots remove-orphans

maintenance: expire-snapshots compact remove-orphans  ## Executa toda a zeladoria

compact:  ## Compaction dos data files
	python scripts/maintenance/compact.py

expire-snapshots:  ## Remove snapshots antigos (> 48h)
	python scripts/maintenance/expire_snapshots.py

remove-orphans:  ## Remove arquivos órfãos sem snapshot
	python scripts/maintenance/remove_orphans.py

# ---------------------------------------------------------------------------
# Qualidade e testes
# ---------------------------------------------------------------------------

.PHONY: test dbt-test dbt-docs

test:  ## Roda todos os testes (dbt + Python)
	cd transform/dbt_project && dbt test

dbt-test:  ## Roda apenas testes dbt
	cd transform/dbt_project && dbt test

dbt-docs:  ## Gera e serve documentação dbt
	cd transform/dbt_project && dbt docs generate && dbt docs serve

# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

.PHONY: generate-data psql clean help

generate-data:  ## Gera dados simulados no Postgres via generator.py
	python ingestion/generator.py

generate-stream:  ## Gera dados de forma contínua para simular CDC
	python ingestion/generator.py --continuous --delay 1.5

psql:  ## Abre shell psql no Postgres do container
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-lakehouse} -d $${POSTGRES_DB:-datalakehouse}

clean:  ## Remove volumes Docker (CUIDADO: apaga dados!)
	$(COMPOSE) --profile core --profile cdc --profile catalog down -v

help:  ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
