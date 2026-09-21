# Data Lakehouse Projeto Arthur

Projeto pessoal e educacional de Data Lakehouse open-source. A plataforma é construída em etapas para praticar ingestão CDC, arquitetura Medallion, modelagem analítica, qualidade e operação sem esconder os conceitos atrás da implementação.

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
| Transformação | dbt-core (MetricFlow planejado) |
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

O dbt da camada Gold já roda localmente pelo ambiente Python, sem um serviço
Docker dedicado. Os profiles `transform`, `orchestration`, `serving` e `ai`
continuam reservados para evoluções futuras.

## Fases do Projeto

- **FASE 0** — Repositório + Docker + DDL *(implementada)*
- **FASE 1** — CDC Debezium + Bronze *(implementada e validada E2E)*
- **FASE 2** — Silver Iceberg + apply idempotente *(MVP concluído)*
- **FASE 3** — Gold dbt + testes *(MVP concluído)*
- **FASE 4** — Airflow + operabilidade *(próxima fase)*
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

## Gold educacional concluída

A Silver foi aceita como um MVP adequado ao projeto pessoal e a Gold foi
concluída sobre essa fundação. Em 2026-09-21, o fluxo foi revalidado com 60
transações, 60 eventos de pagamento, 120 lançamentos, 30 contas e 10 parceiros
reconciliados entre Postgres, Bronze e Silver.

A entrega contém:

1. Explicar o grão e a regra de negócio de cada dimensão e fato.
2. Executar os cinco modelos staging, as três dimensões, `fct_daily_volume` e
   `fct_partner_net_position` (posição líquida por parceiro e moeda) com
   `dbt build`.
3. Demonstrar testes de chave, relacionamento, valores aceitos, reconciliação
   do volume diário e soma zero do ledger.
4. Gerar a documentação e lineage local com `dbt docs generate`.
5. Consultar os marts e conferir manualmente alguns resultados contra a Silver.

Schema evolution automatizada, DLQ, observabilidade, manutenção Iceberg,
MetricFlow e freshness contínua permanecem no backlog. São conceitos valiosos,
mas não bloquearam esta entrega. O `dbt build` final executou 10 modelos e 33
testes sem falhas; uma análise de negócio, a documentação e o catálogo dbt
também foram gerados.

## Roadmap profissional após a Gold

Os itens adiados continuam na lista, agora priorizados pelo aprendizado esperado
para consolidação como engenheiro de dados pleno:

1. **Airflow obrigatório:** orquestrar Silver → dbt build → reconciliação,
   praticando dependências, retries, timeout, data interval, reexecução e
   backfill. O S3 Sink continua responsável pela Bronze em streaming; Airflow
   não substitui o CDC.
2. **Incrementalidade e checkpoint:** evitar reler toda a Bronze, registrar o
   high watermark processado e provar replay seguro.
3. **Operabilidade:** registrar duração, linhas lidas/escritas, último LSN e
   resultado da reconciliação; provocar uma falha e recuperar pelo Airflow.
4. **DLQ e replay:** isolar um evento inválido sem perder os válidos e depois
   reprocessá-lo após a correção.
5. **Schema evolution controlada:** testar uma adição compatível e rejeitar uma
   alteração incompatível de tipo.
6. **Manutenção Iceberg:** inspecionar snapshots e arquivos pequenos antes de
   implementar expiração e compactação.

MetricFlow, Schema Registry, observabilidade com uma stack dedicada, API, BI e
IA permanecem visíveis, mas com prioridade baixa até existir uma necessidade
concreta.

## Testes locais

```bash
.venv/bin/python -m pytest -q
make gold
```

Os testes cobrem envelopes CDC wrapped/unwrapped, deduplicação por LSN,
fail-fast de contrato, precisão decimal, idempotência, tombstones e replay fora
de ordem no PyIceberg.

`make gold` executa `dbt build`: constrói a Gold na ordem definida pelos `ref()`
e roda os testes associados. `make dbt-docs` gera e serve a documentação local.
