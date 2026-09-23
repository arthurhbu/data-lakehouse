FROM apache/airflow:3.3.1-python3.12

COPY docker/airflow/requirements.txt /tmp/lakehouse-requirements.txt

# Repetir a versão do Airflow impede que pip o troque ao resolver dbt/PyIceberg.
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.1" \
    -r /tmp/lakehouse-requirements.txt
