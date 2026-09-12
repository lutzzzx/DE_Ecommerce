"""
dq_checkpoint_dag.py
Jalankan Great Expectations checkpoint untuk bronze & silver layer,
lalu catat hasilnya sebagai audit log di Postgres warehouse.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_ge_checkpoint(checkpoint_name: str, layer: str, **context):
    import great_expectations as gx
    import psycopg2
    import os
    import json

    ge_context = gx.get_context(context_root_dir="/opt/great_expectations")
    result = ge_context.run_checkpoint(checkpoint_name=checkpoint_name)

    success = bool(result["success"])
    stats = result["run_results"]
    first_key = next(iter(stats))
    validation_result = stats[first_key]["validation_result"]
    evaluated = validation_result["statistics"]["evaluated_expectations"]
    successful = validation_result["statistics"]["successful_expectations"]

    conn = psycopg2.connect(
        host=os.getenv("DWH_HOST", "postgres-dwh"),
        dbname=os.getenv("DWH_DB", "warehouse"),
        user=os.getenv("DWH_USER", "dwh_user"),
        password=os.getenv("DWH_PASSWORD", "dwh_pass123"),
    )
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dq_logs.checkpoint_runs
            (suite_name, layer, success, evaluated_expectations, successful_expectations, details_json)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (checkpoint_name, layer, success, evaluated, successful, json.dumps({"summary": str(result.get('run_id'))})),
    )
    conn.commit()
    cur.close()
    conn.close()

    if not success:
        raise ValueError(
            f"Data quality checkpoint '{checkpoint_name}' GAGAL "
            f"({successful}/{evaluated} expectation lolos). Cek Great Expectations Data Docs."
        )


with DAG(
    dag_id="dq_checkpoint_pipeline",
    default_args=default_args,
    schedule=None,   # dipicu via trigger dari pipeline utama
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-quality"],
) as dag:

    check_bronze_orders = PythonOperator(
        task_id="check_bronze_orders",
        python_callable=run_ge_checkpoint,
        op_kwargs={"checkpoint_name": "bronze_orders_checkpoint", "layer": "bronze"},
    )

    check_silver_orders = PythonOperator(
        task_id="check_silver_orders",
        python_callable=run_ge_checkpoint,
        op_kwargs={"checkpoint_name": "silver_orders_checkpoint", "layer": "silver"},
    )

    check_bronze_orders >> check_silver_orders
