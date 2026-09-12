"""
batch_elt_pipeline_dag.py
Pipeline batch harian end-to-end:
raw CSV -> bronze (MinIO) -> silver -> gold -> staging Postgres -> dbt build -> DQ check
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="batch_elt_pipeline",
    description="Pipeline batch harian: bronze -> silver -> gold -> warehouse -> dbt -> DQ",
    default_args=default_args,
    schedule="0 2 * * *",       # setiap hari jam 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "elt", "ecommerce"],
) as dag:

    ingest_raw_to_bronze = BashOperator(
        task_id="ingest_raw_to_bronze",
        bash_command="python3 /opt/ingestion/batch_loader.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "docker exec -u 0 spark-master spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark_jobs/batch/bronze_to_silver.py"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "docker exec -u 0 spark-master spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark_jobs/batch/silver_to_gold.py"
        ),
    )

    load_to_warehouse = BashOperator(
        task_id="load_to_warehouse",
        bash_command=(
            "docker exec -u 0 spark-master spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark_jobs/batch/load_to_warehouse.py"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/dbt/ecommerce_dwh && "
            "dbt deps && dbt run --profiles-dir . && dbt test --profiles-dir ."
        ),
    )

    trigger_dq_checkpoint = BashOperator(
        task_id="trigger_dq_checkpoint",
        bash_command="airflow dags trigger dq_checkpoint_pipeline",
    )

    (
        ingest_raw_to_bronze
        >> bronze_to_silver
        >> silver_to_gold
        >> load_to_warehouse
        >> dbt_build
        >> trigger_dq_checkpoint
    )
