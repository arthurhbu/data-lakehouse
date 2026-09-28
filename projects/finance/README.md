# Finance — CDC e ledger

Domínio financeiro existente do Elysium. O fluxo é Postgres → Debezium/Kafka →
MinIO Bronze → Iceberg Silver → dbt Gold, com DAGs do Airflow e reconciliação.

- `ingestion/`: gerador, contrato CDC, manifesto Bronze e apply Silver.
- `infra/`: DDL do Postgres e configurações dos conectores deste domínio.
- `orchestration/`: DAGs financeiras carregadas pelo Airflow compartilhado.
- `scripts/`: registro de conectores, reconciliação e ferramentas de operação.
- `transform/dbt_project/`: modelos e testes da Gold financeira.
- `tests/`: testes Python deste domínio.

Execute os comandos `make` a partir da raiz do repositório. O histórico de
validações e os limites atuais estão em [docs/CONTINUITY.md](docs/CONTINUITY.md).
