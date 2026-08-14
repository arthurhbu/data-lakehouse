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
│   ├── cdc_contract.py      # Normalização/validação do envelope Debezium
│   ├── bronze/              # Bronze persistida pelo Kafka Connect S3 Sink
│   ├── silver/              # Apply idempotente em tabelas Iceberg
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

Para retomar o desenvolvimento em uma nova sessao, consulte [docs/CONTINUITY.md](docs/CONTINUITY.md).

## Quick Start

```bash
# 1. Clone e configure
cp .env.example .env        # Ajuste senhas e tokens

# 2. Suba infraestrutura, CDC e catálogo
make up

# 3. Registre/atualize Debezium e S3 Sink
make register-connectors

# 4. Verifique o status
make status

# 5. Gere dados de teste e aplique a Silver
make generate-data
make silver
make reconcile

# 6. Veja todos os comandos disponíveis
make help
```

## Docker Compose Profiles

O projeto usa profiles para subir apenas o que você precisa:

| Profile | Serviços | Comando |
|---|---|---|
| `core` | Postgres, Kafka (KRaft), MinIO | `make up-core` |
| `cdc` | Kafka Connect + Debezium | `make up-cdc` |
| `catalog` | Iceberg REST Catalog | `make up-catalog` |
| `trino` | Trino conectado ao catálogo Iceberg | `make up-trino` |

Os profiles `transform`, `orchestration`, `serving` e `ai` ainda pertencem às
fases futuras. Os respectivos comandos informam esse estado sem tentar subir
serviços inexistentes.

## Fases do Projeto

- **FASE 0** — Repositório + Docker + DDL *(implementada)*
- **FASE 1** — CDC Debezium + Bronze *(implementada e validada E2E)*
- **FASE 2** — Silver Iceberg + apply idempotente *(fase atual)*
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

## Contrato Bronze → Silver

- A Bronze é append-only e preserva o evento Debezium original.
- São aceitos envelopes com os campos CDC na raiz ou dentro de `payload`.
- `op`, `source.lsn` e a chave primária são obrigatórios; lotes inválidos falham.
- O maior LSN por chave define o estado do micro-batch.
- Operações `c`, `r` e `u` substituem atomicamente somente as chaves afetadas.
- Operações `d` viram tombstones (`_cdc_deleted=true`) na Silver; consumidores
  filtram essas linhas e o LSN da exclusão impede ressurreição por replay antigo.
- Valores monetários usam `DECIMAL`, nunca ponto flutuante.

Tabelas Silver criadas antes da adoção dos contratos decimais precisam de uma
migração explícita. O pipeline falha ao detectar um schema antigo, evitando uma
conversão silenciosa.

```bash
# Visualiza a migração sem alterar o catálogo
make migrate-silver-contract

# Renomeia tabelas incompatíveis para silver_legacy e preserva os dados
make migrate-silver-contract-apply
```

Schema Registry/Avro está deliberadamente adiado para a Fase 5. A migração deve
usar tópicos versionados; formatos serializados diferentes nunca devem ser
misturados nos tópicos `cdc.public.*` atuais.

## Testes locais

```bash
python -m pytest -q
```

Os testes cobrem envelopes CDC wrapped/unwrapped, deduplicação por LSN,
fail-fast de contrato, precisão decimal, idempotência, tombstones e replay fora
de ordem no PyIceberg.
