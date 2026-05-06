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

up-cdc:  ## Sobe Kafka Connect + Debezium
	$(COMPOSE) --env-file $(ENV_FILE) --profile cdc up -d

up-catalog:  ## Sobe Iceberg REST Catalog
	$(COMPOSE) --env-file $(ENV_FILE) --profile catalog up -d

up-transform:  ## Sobe container DuckDB + dbt
	$(COMPOSE) --env-file $(ENV_FILE) --profile transform up -d

up-orchestration:  ## Sobe Airflow (webserver + scheduler + worker)
	$(COMPOSE) --env-file $(ENV_FILE) --profile orchestration up -d

up-serving:  ## Sobe FastAPI + BI
	$(COMPOSE) --env-file $(ENV_FILE) --profile serving up -d

up-ai:  ## Sobe Ollama + RAG service
	$(COMPOSE) --env-file $(ENV_FILE) --profile ai up -d

down:  ## Para e remove todos os containers
	$(COMPOSE) --env-file $(ENV_FILE) --profile core --profile cdc --profile catalog --profile transform --profile orchestration --profile serving --profile ai down

restart: down up  ## Reinicia toda a stack

status:  ## Mostra status dos containers
	$(COMPOSE) ps

logs:  ## Mostra logs de todos os containers (follow)
	$(COMPOSE) logs -f

# ---------------------------------------------------------------------------
# Pipeline de dados
# ---------------------------------------------------------------------------

.PHONY: bronze silver gold

bronze:  ## Executa job de ingestão Bronze (Kafka → MinIO/Parquet)
	python ingestion/bronze/persist_bronze.py

silver:  ## Executa apply Silver (Bronze → Iceberg MERGE INTO)
	python ingestion/bronze/apply_silver.py

gold:  ## Executa transformações Gold via dbt
	cd transform/dbt_project && dbt run --select marts

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

psql:  ## Abre shell psql no Postgres do container
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-lakehouse} -d $${POSTGRES_DB:-globalnexus}

clean:  ## Remove volumes Docker (CUIDADO: apaga dados!)
	$(COMPOSE) --profile core --profile cdc --profile catalog down -v

help:  ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
