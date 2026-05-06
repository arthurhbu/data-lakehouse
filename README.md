# Data Lakehouse Projeto Arthur

Data Lakehouse completo, 100% Docker, open-source. Plataforma centralizada para ingestão (CDC), transformação (Medallion), camada semântica, BI e IA — tudo local com migração zero-code para nuvem.

**Domínio de referência:**  Clearing House internacional (pagamentos cross-border, câmbio, double-entry ledger).

## Stack

| Camada | Tecnologia |
|---|---|
| Fonte OLTP | PostgreSQL |
| CDC | Debezium + Kafka Connect |
| Broker | Apache Kafka (KRaft) |
| Storage | MinIO (S3-compatible) |
| Table Format | Apache Iceberg v2 |
| Catálogo | Iceberg REST Catalog |
| Processamento | DuckDB + MotherDuck |
| Transformação | dbt-core + MetricFlow |
| Orquestração | Apache Airflow |
| API | FastAPI |
| BI | Superset / Metabase |
| IA/RAG | LangChain + Ollama |

## Estrutura do Repositório

```
data-lakehouse/
├── docker/                  # Dockerfiles customizados
├── docker-compose.yml       # Compose mestre com profiles
├── infra/
│   ├── postgres/            # DDL + postgresql.conf
│   ├── kafka/connectors/    # JSON configs Debezium
│   ├── minio/policies/      # Bucket policies
│   └── catalog/             # Config REST Catalog
├── ingestion/
│   ├── generator.py         # Simulador de dados
│   ├── bronze/              # Jobs persistência Bronze
│   └── cdc/                 # Configs e validações CDC
├── transform/
│   └── dbt_project/         # Projeto dbt (staging/marts/semantic)
├── orchestration/
│   └── airflow/             # DAGs, plugins, config
├── serving/
│   ├── api/                 # FastAPI
│   └── ai/                  # RAG pipeline
├── quality/
│   ├── contracts/           # Data contracts YAML
│   ├── slos/                # SLO definitions
│   └── runbooks/            # Runbooks de incidentes
├── scripts/
│   ├── maintenance/         # Compaction, snapshot expiry
│   └── reconciliation/      # Reconciliação batch
├── docs/                    # ADRs e documentação técnica
├── .env.example
├── Makefile
├── requirements.txt
└── README.md
```

## Quick Start

```bash
# 1. Clone e configure
cp .env.example .env        # Ajuste senhas e tokens

# 2. Suba a infraestrutura core (Postgres + Kafka + MinIO)
make up-core

# 3. Verifique o status
make status

# 4. Gere dados de teste
make generate-data

# 5. Veja todos os comandos disponíveis
make help
```

## Docker Compose Profiles

O projeto usa profiles para subir apenas o que você precisa:

| Profile | Serviços | Comando |
|---|---|---|
| `core` | Postgres, Kafka (KRaft), MinIO | `make up-core` |
| `cdc` | Kafka Connect + Debezium | `make up-cdc` |
| `catalog` | Iceberg REST Catalog | `make up-catalog` |
| `transform` | DuckDB + dbt | `make up-transform` |
| `orchestration` | Airflow | `make up-orchestration` |
| `serving` | FastAPI + BI | `make up-serving` |
| `ai` | Ollama + RAG | `make up-ai` |

## Fases do Projeto

- **FASE 0** — Repositório + Docker + DDL *(atual)*
- **FASE 1** — CDC Debezium + Bronze
- **FASE 2** — Silver Iceberg + MERGE + Validações
- **FASE 3** — Gold dbt + MetricFlow + Testes
- **FASE 4** — Airflow + Observabilidade + Zeladoria
- **FASE 5** — LGPD + Reconciliação + Contracts
- **FASE 6** — API + BI + RAG

## Princípios

1. **"Nenhum dado" > "dado errado"** — se SLO quebrar, bloquear entrega
2. **Idempotência sempre** — mesmo input N vezes = mesmo resultado
3. **Auditabilidade** — qualquer número rastreável até a origem
4. **Soma Zero** — `SUM(débitos) + SUM(créditos) = 0`
5. **Zero-code para nuvem** — MinIO → S3, DuckDB → MotherDuck
