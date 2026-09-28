# Handoff — Gold concluída e Airflow manual validado

## Estado atual

As fases CDC/Bronze, Silver MVP e Gold MVP estão concluídas. A fase de
**Airflow e operabilidade** começou com infraestrutura e DAG manual validadas.
O método de trabalho continua sendo: explicar o
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

## Fase 4: Airflow concluído e validado

1. O profile `orchestration` usa Postgres de metadados, API/UI, Dag Processor,
   Scheduler e `LocalExecutor` com paralelismo global 2, sem Redis/Celery.
2. `lakehouse_pipeline` roda a cada 15 minutos: captura manifesto incremental,
   encerra cedo quando vazio, executa as cinco Silver em paralelo, valida as
   chaves afetadas, roda `dbt build` e confirma checkpoint atômico.
3. `lakehouse_pipeline_checkpoint` guarda `lsn` e offsets por tópico/partição.
   O arquivo físico é escolhido por offset; o LSN continua protegendo a versão
   da entidade. A falha induzida provou que Gold e checkpoint ficam bloqueados.
4. `lakehouse_daily_reconciliation` roda às 06:00 UTC e mantém a reconciliação
   completa Postgres = Bronze = Silver sem reler toda a Bronze a cada 15 minutos.
5. O callback registra falhas e envia JSON para `AIRFLOW_ALERT_WEBHOOK_URL` quando
   configurada. `make validate-airflow` verifica imports, grafo e schedule.
6. **Runtime em 2026-09-23:** a DagRun agendada das 14:15 UTC processou os nove
   objetos, passou por todas as tasks e gravou offsets 0/50/100; a DagRun manual
   seguinte detectou manifesto vazio e pulou Silver/Gold. A reconciliação diária
   também terminou em `success`.
7. Backfill temporal fiel permanece fora do MVP porque Silver e Gold materializam
   estado atual, não snapshots reconstruídos por `data_interval`.

A Bronze não será transformada em tarefa batch: Debezium/Kafka/S3 Sink continuam
responsáveis pelo CDC contínuo; Airflow orquestrará os consumidores batch.
O laboratório Flink streaming virá após a primeira fase operacional do Airflow,
com tabelas próprias e reconciliação no mesmo corte de LSN.

**Limite consciente:** o checkpoint de arquivo usa o offset inicial presente no
nome imutável produzido pelo S3 Sink. Caso o particionamento ou padrão de nomes do
conector mude, o parser e a migração do checkpoint devem mudar explicitamente.

## Roadmap preservado após o Airflow

**Alta prioridade:**

1. DLQ com correção e replay.
2. Schema evolution compatível e rejeição de breaking change.
3. Manutenção Iceberg orientada por snapshots e small files.

**Prioridade baixa, sem apagar:** MetricFlow, Schema Registry, contracts
enforced, freshness contínua, stack dedicada de observabilidade, métricas de
chargeback/settlement, API, BI e IA.

## Comandos para reproduzir a validação atual

```bash
make up
.venv/bin/python -m projects.finance.scripts.register_connectors
.venv/bin/python projects/finance/ingestion/generator.py --transactions 10
make silver
make reconcile
make validate-airflow
cd projects/finance/transform/dbt_project
../../../../.venv/bin/dbt build
../../../../.venv/bin/dbt docs generate
```

Leia este arquivo no início da próxima sessão. A fase Airflow está encerrada; o
próximo laboratório planejado é streaming com Flink, sem remover os itens de
DLQ, schema evolution e manutenção Iceberg do roadmap.
