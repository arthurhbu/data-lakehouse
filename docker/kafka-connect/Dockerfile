FROM confluentinc/cp-kafka-connect-base:7.6.1 AS plugin-builder
# Usa o confluent-hub para baixar o S3 Sink e suas dependências
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.15

FROM debezium/connect:2.7.3.Final
# Copia o plugin baixado pelo confluent-hub para dentro da imagem do Debezium
COPY --from=plugin-builder /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3 /kafka/connect/confluentinc-kafka-connect-s3