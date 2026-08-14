# Handoff — continuidade da Silver

## Estado do projeto

O projeto esta na Fase 2: CDC do Postgres para Kafka/MinIO e materializacao da camada Silver em Iceberg. A Bronze e imutavel e armazena eventos CDC; a Silver representa o estado atual por chave primaria.

Na ultima validacao ponta a ponta, Postgres, Kafka, Kafka Connect, MinIO e o catalogo Iceberg estavam em execucao. Os conectores Debezium e S3 Sink estavam ativos, e a reconciliacao confirmou:

- transactions: 125 registros
- payment_events: 125 registros
- ledger_entries: 250 registros
- accounts: 30 registros
- partners: 10 registros
- soma do ledger igual a zero no Postgres e na Silver

## Implementacao entregue

- `ingestion/cdc_contract.py` normaliza envelopes Debezium e exige operacao, LSN e chave primaria.
- `ingestion/silver/apply_silver.py` materializa o ultimo evento por chave, usando LSN; replays e eventos atrasados sao ignorados.
- Deletes viram tombstones (`_cdc_deleted = true`) para impedir que eventos antigos recriem registros excluidos.
- Valores financeiros usam `DECIMAL`; timestamps usam timezone; as tabelas Silver possuem `_cdc_lsn` e `_cdc_deleted`.
- `scripts/migrate_silver_contract.py` preserva tabelas incompatíveis em `silver_legacy` antes da reconstrucao.
- `scripts/register_connectors.py` registra/atualiza conectores com segredos resolvidos por variaveis de ambiente.
- `scripts/reconciliation/check_pipeline.py` compara Postgres, Bronze normalizado e Silver ativa com a mesma semantica CDC.

## Como validar e operar

```powershell
make up-cdc
make up-catalog
make register-connectors
make silver
make reconcile
.\.venv\Scripts\python.exe -m pytest -q
```

Os testes esperados sao `10 passed`. Para verificar a configuracao Docker sem iniciar servicos, use `docker compose config`.

## Proximas etapas para concluir a Silver

1. Implementar evolucao de schema controlada, com politica para campos novos, removidos e alteracoes de tipo.
2. Criar DLQ para eventos invalidos, com causa, payload original e metadados de origem; o pipeline deve continuar processando eventos validos.
3. Definir rotinas de manutencao Iceberg: compactacao de arquivos pequenos, expiracao de snapshots e monitoramento.
4. Adicionar observabilidade operacional: lag dos conectores, idade do ultimo LSN, volume de tombstones e alertas de reconciliacao.
5. Somente apos esses controles, iniciar transformacoes Gold/dbt sobre a Silver estabilizada.

## Decisao arquitetural pendente

Schema Registry/Avro foi deliberadamente adiado. Os topicos atuais usam JSON simples; misturar JsonSchemaConverter em topicos existentes causou erro de framing (`Unknown magic byte`). Quando essa etapa for retomada, criar topicos versionados e migrar consumidores de forma gradual; nao alterar a serializacao dos topicos `cdc.public.*` atuais em uso.

## Commits que estabelecem este ponto

- `5e8696d feat(silver): harden CDC materialization semantics`
- `90b7486 chore(platform): automate connector registration and profiles`

Leia este arquivo no inicio de uma nova sessao antes de alterar a camada Silver.
