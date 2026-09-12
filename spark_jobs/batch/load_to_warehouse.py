"""
load_to_warehouse.py
Load tabel-tabel silver (granular) ke schema staging PostgreSQL via JDBC,
sebagai landing zone sebelum dbt membangun star schema.
"""
import os
import sys

sys.path.append("/opt/spark_jobs")
from common.spark_session import get_spark_session

BUCKET = os.getenv("MINIO_BUCKET_DATALAKE", "datalake")
SILVER = f"s3a://{BUCKET}/silver"

DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh")
DWH_DB = os.getenv("DWH_DB", "warehouse")
DWH_USER = os.getenv("DWH_USER", "dwh_user")
DWH_PASSWORD = os.getenv("DWH_PASSWORD", "dwh_pass123")
JDBC_URL = f"jdbc:postgresql://{DWH_HOST}:5432/{DWH_DB}"

TABLES = ["orders", "order_items", "customers", "products", "payments", "events"]


def main():
    spark = get_spark_session("load_to_warehouse")
    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        try:
            df = spark.read.parquet(f"{SILVER}/{table}")
        except Exception as e:
            print(f"[WARN] Skip {table}, belum ada di silver: {e}")
            continue

        (
            df.drop("_ingested_at")
            .write.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", f"staging.{table}")
            .option("user", DWH_USER)
            .option("password", DWH_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
        print(f"[staging] {table}: {df.count()} baris dimuat ke Postgres")

    spark.stop()


if __name__ == "__main__":
    main()
