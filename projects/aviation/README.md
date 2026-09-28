# Aviation Intelligence Lakehouse

Segundo domínio do Elysium, ainda na fase de estudo e definição das fontes. A
documentação de referência está no Obsidian, em `1 - Projetos/Data Lakehouse
Elysium/5 - Projeto G — Aviation Intelligence Lakehouse/`.

O código próprio de aviação ficará neste diretório. Kafka, MinIO, Docker Compose
e outros serviços reutilizáveis permanecem na raiz. Este domínio terá seus
próprios tópicos, caminhos de dados, contratos, testes e comandos; o pipeline
financeiro não será usado como implementação implícita de aviação.

Antes de expandir o coletor ou criar tabelas definitivas, validar amostras reais
das fontes, grãos, identificadores, cobertura, limites de API e critérios de
reconciliação conforme o roadmap do Projeto G.

## Primeiro coletor: OpenSky → MinIO

O coletor em `collector/` consulta o endpoint de state vectors e guarda o corpo
JSON original no bucket `bronze`, sob
`aviation/opensky/dt=AAAA-MM-DD/hour=HH/snapshot_id=<uuid>.json`. O nome do bucket
não é repetido dentro da chave. Metadados do objeto incluem o horário da coleta,
`snapshot_id`, `request_id`, bbox, quantidade de vetores e SHA-256. Uma resposta
sem aeronaves também é um snapshot válido. Há um modo de execução única e um modo
periódico local; o coletor ainda não publica no Kafka. O envio manual está
descrito abaixo; manifesto de exportação e carga no Databricks ainda não existem.

1. Se já existe `.env` na raiz, acrescente as variáveis OpenSky e `AVIATION_BBOX_*`
   de `.env.example` sem sobrescrever o arquivo. Caso contrário, copie o exemplo
   para `.env`. Configure seu API client da OpenSky e confira o bbox **provisório**
   para a região GRU + CGH. Não publique o `.env`.
2. Suba apenas o MinIO e o inicializador de buckets; o bucket `bronze` já faz
   parte do Compose. Aguarde o `minio-init` terminar sem erro:

   ```bash
   docker compose --env-file .env --profile core up -d minio minio-init
   ```
3. Para fazer **uma coleta manual**, na raiz do repositório execute:

   ```bash
   .venv/bin/python -m projects.aviation.collector
   ```

O programa imprime um JSON com `bucket`, `object_key`, `snapshot_id`, horários,
quantidade de vetores, tamanho, SHA-256 e créditos restantes informados pela API.
Falhas retornam código de saída 1 e não imprimem tokens nem senhas. Para conferir
o objeto, abra o MinIO Console em `http://localhost:9001` e procure a chave
informada no bucket `bronze`.

## Publicação manual MinIO → Kafka

O `collector/kafka_sender.py` lê **um snapshot pela chave exata**, valida o
checksum e os metadados gravados pelo coletor, converte cada state vector em
um evento com campos nomeados e publica uma mensagem por aeronave no tópico
`aviation.opensky.state_vectors.v1`. A chave Kafka é `icao24`. O evento
contém `source_bucket`, `source_object_key`, `snapshot_id` e um `event_id`
determinístico calculado a partir do vetor original.

Na raiz do repositório, com `.env` configurado e `KAFKA_BROKER=localhost:9094`:

```bash
docker compose --env-file .env --profile core up -d kafka minio-init
.venv/bin/python -m projects.aviation.collector.kafka_sender \
  --object-key 'aviation/opensky/dt=AAAA-MM-DD/hour=HH/snapshot_id=<uuid>.json' \
  --dry-run
```

Substitua a chave de exemplo por um `object_key` real impresso pelo coletor.
O `--dry-run` valida o arquivo e mostra um evento, sem enviar ao Kafka. Para
publicar, execute o mesmo comando **sem** `--dry-run`. O programa só informa
sucesso após receber as confirmações do produtor; uma falha pode deixar
publicação parcial, exigindo replay do mesmo arquivo.

Esta etapa é manual: não há busca automática de novos snapshots nem controle
persistente de arquivos já publicados. O produtor usa idempotência para suas
tentativas internas, mas republicar o arquivo em outra execução pode duplicar
mensagens. O consumidor deverá usar `event_id` para lidar com replay.

## Coleta periódica em segundo plano

Na raiz do repositório, configure `AVIATION_POLL_INTERVAL_SECONDS=60` no `.env`
e inicie somente o serviço de aviação e suas dependências:

```bash
docker compose --env-file .env --profile core --profile aviation up -d --build aviation-collector
docker compose --env-file .env --profile core --profile aviation logs -f aviation-collector
```

O container consulta uma vez ao iniciar e espera ao menos 60 segundos entre
ciclos. O intervalo mínimo aceito é 30 segundos. Com o bbox atual (menos de
25 graus quadrados), uma consulta custa 1 crédito segundo a OpenSky; a cada
60 segundos são até 1.440 consultas/dia, abaixo da cota padrão de 4.000,
sem contar eventuais retries. Cada ciclo registra uma linha JSON nos logs.
O token OAuth é reutilizado até perto de expirar. Em `429`, o serviço espera o
prazo informado pela API antes de tentar novamente; sem prazo válido, espera
uma hora. O modo periódico não é iniciado por `make up-core` ou `make up`.

Para parar sem remover os objetos já coletados:

```bash
docker compose --env-file .env --profile core --profile aviation stop aviation-collector
```

Não execute simultaneamente o modo manual e o serviço periódico com o mesmo
API client: as duas instâncias consumiriam a mesma cota. As credenciais são
passadas ao container como variáveis de ambiente, apropriado para este
laboratório local, mas não como modelo de gestão de segredos em produção.

O bbox é um recorte de coleta de espaço aéreo, **não** uma atribuição de voo a um
aeroporto. Uma execução manual real e sua leitura de volta foram validadas em
2026-09-25; a operação periódica ainda precisa ser observada após a ativação.
Os testes locais, sem chamadas externas, rodam com:

```bash
.venv/bin/python -m pytest -q projects/aviation/tests/test_opensky_collector.py
```
