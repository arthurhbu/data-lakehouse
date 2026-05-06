#!/bin/sh
set -e

mc alias set lakehouse http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

mc mb --ignore-existing lakehouse/bronze
mc mb --ignore-existing lakehouse/silver
mc mb --ignore-existing lakehouse/gold
mc mb --ignore-existing lakehouse/warehouse

echo "Buckets criados com sucesso: bronze, silver, gold, warehouse"
