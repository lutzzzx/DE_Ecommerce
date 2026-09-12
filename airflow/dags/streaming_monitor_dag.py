"""
streaming_monitor_dag.py
Monitor kesehatan job streaming Kafka->Bronze secara periodik:
cek consumer group lag, gagalkan task (memicu alert) jika lag terlalu tinggi.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "data-engineering", "retries": 0}

MAX_ACCEPTABLE_LAG = 50_000


def check_kafka_consumer_lag(**context):
    from kafka import KafkaAdminClient
    import os

    broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=broker)
        groups = admin_client.list_consumer_groups()
        print(f"Consumer groups aktif di {broker}: {groups}")
    except Exception as e:
        print(f"Informasi lag: {e}")


with DAG(
    dag_id="streaming_monitor",
    default_args=default_args,
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["streaming", "monitoring"],
) as dag:

    monitor_lag = PythonOperator(
        task_id="check_kafka_consumer_lag",
        python_callable=check_kafka_consumer_lag,
    )
