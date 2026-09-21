# Handoff — Gold concluída e continuidade no Airflow

## Estado atual

As fases CDC/Bronze, Silver MVP e Gold MVP estão concluídas. A próxima fase é
**Airflow e operabilidade**. O método de trabalho continua sendo: explicar o
conceito, implementar um incremento pequeno, provocar/observar seu comportamento
e interpretar a evidência.

## Evidência atual de 2026-09-21

- Debezium e S3 Sink estavam `RUNNING`.
- Uma carga controlada adicionou 10 transações, 10 eventos e 20 lançamentos.
- Após a rotação de 60 segundos do S3 Sink, a Silver aplicou exatamente esse
  delta e ignorou 50/50/100 eventos antigos como replay.
- A reconciliação confirmou Postgres = Bronze = Silver: 60 `transactions`, 60
  `payment_events`, 120 `ledger_entries`, 30 `accounts` e 10 `partners`.
- A soma do ledger foi `0.0000` no Postgres e na Silver.
- `dbt build`: 10 modelos e 33 testes, todos aprovados (`PASS=43`).
- `dbt docs generate` criou o catálogo e o lineage.

## Gold entregue

- Cinco modelos staging filtram tombstones.
- Dimensões: `dim_partners`, `dim_accounts` e `dim_currencies`.
- `fct_daily_volume`: grão data de criação + moeda original.
- `fct_partner_net_position`: grão parceiro + moeda do lançamento; apresenta
  débitos e créditos positivos e `net_position = créditos - débitos`.
- O fato de posição representa apenas movimentos observados, não saldo bancário
  com saldo inicial.
- Testes cobrem chaves, relacionamentos, domínios, reconciliação, grão único e
  soma zero por transação.
- A verificação manual confirmou que a posição consolidada fecha em zero para
  cada moeda.
- A análise `top_partner_net_position_by_currency` demonstra o consumo da Gold
  com ranking dos três maiores parceiros por moeda.

## Simplificações conscientes do MVP

- Os modelos staging e marts compartilham atualmente o namespace Iceberg
  `gold`. Uma separação física (`staging`/`gold`) pode ser adotada quando houver
  consumidores externos ou controle de acesso por camada.
- Os modelos dbt usam materialização `table` e são reconstruídos integralmente.
  O volume local torna essa escolha adequada; incrementalidade entra somente
  quando custo ou tempo de execução justificarem o estado adicional.

## Próximo ciclo: Airflow mínimo e explicável

1. Definir a arquitetura local mínima do Airflow e o papel de cada componente.
2. Subir scheduler, UI, metadata database e um executor adequado ao projeto.
3. Criar uma DAG `lakehouse_pipeline`:
   `apply_silver` → `dbt build` → `reconciliation`.
4. Configurar retries, timeout, data interval e concorrência.
5. Provocar uma falha, observar os estados e reexecutar somente a task necessária.
6. Executar um backfill e comprovar que a pipeline continua idempotente.

A Bronze não será transformada em tarefa batch: Debezium/Kafka/S3 Sink continuam
responsáveis pelo CDC contínuo; Airflow orquestrará os consumidores batch.

## Roadmap preservado após o Airflow

**Alta prioridade:**

1. Checkpoint/high watermark da Silver.
2. Metadados operacionais: duração, linhas, último LSN e reconciliação.
3. DLQ com correção e replay.
4. Schema evolution compatível e rejeição de breaking change.
5. Manutenção Iceberg orientada por snapshots e small files.

**Prioridade baixa, sem apagar:** MetricFlow, Schema Registry, contracts
enforced, freshness contínua, stack dedicada de observabilidade, métricas de
chargeback/settlement, API, BI e IA.

## Comandos para reproduzir a validação atual

```bash
make up
.venv/bin/python -m scripts.register_connectors
.venv/bin/python ingestion/generator.py --transactions 10
make silver
make reconcile
cd transform/dbt_project
../../.venv/bin/dbt build
../../.venv/bin/dbt docs generate
```

Leia este arquivo no início da próxima sessão. O próximo trabalho é entender e
montar a infraestrutura mínima do Airflow, não adicionar novos marts.
