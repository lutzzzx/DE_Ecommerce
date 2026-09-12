"""
bronze_to_silver.py
Bersihkan & konformasi data dari bronze -> silver untuk seluruh tabel Olist
dan tabel events streaming. Idempotent: overwrite per tabel/partisi.
"""
import os
import sys

sys.path.append("/opt/spark_jobs")
from common.spark_session import get_spark_session
from pyspark.sql import functions as F

BUCKET = os.getenv("MINIO_BUCKET_DATALAKE", "datalake")
BRONZE = f"s3a://{BUCKET}/bronze"
SILVER = f"s3a://{BUCKET}/silver"


def clean_orders(spark):
    df = spark.read.option("header", True).csv(f"{BRONZE}/olist/orders/*/*.csv")
    df = (
        df.dropDuplicates(["order_id"])
        .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
        .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (df.write.mode("overwrite").parquet(f"{SILVER}/orders"))
    return df.count()


def clean_order_items(spark):
    df = spark.read.option("header", True).csv(f"{BRONZE}/olist/order_items/*/*.csv")
    df = (
        df.dropDuplicates(["order_id", "order_item_id"])
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .filter((F.col("price") >= 0) & (F.col("freight_value") >= 0))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").parquet(f"{SILVER}/order_items")
    return df.count()


def clean_customers(spark):
    df = spark.read.option("header", True).csv(f"{BRONZE}/olist/customers/*/*.csv")
    df = (
        df.dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").parquet(f"{SILVER}/customers")
    return df.count()


def clean_products(spark):
    df = spark.read.option("header", True).csv(f"{BRONZE}/olist/products/*/*.csv")
    translation = spark.read.option("header", True).csv(
        f"{BRONZE}/olist/product_category_name_translation/*/*.csv"
    )
    df = (
        df.dropDuplicates(["product_id"])
        .join(translation, on="product_category_name", how="left")
        .withColumn(
            "product_category_name_english",
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").parquet(f"{SILVER}/products")
    return df.count()


def clean_payments(spark):
    df = spark.read.option("header", True).csv(f"{BRONZE}/olist/order_payments/*/*.csv")
    df = (
        df.dropDuplicates(["order_id", "payment_sequential"])
        .withColumn("payment_value", F.col("payment_value").cast("double"))
        .filter(F.col("payment_value") >= 0)
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").parquet(f"{SILVER}/payments")
    return df.count()


def clean_events(spark):
    """Bersihkan event streaming yang sudah mendarat di bronze/events."""
    df = spark.read.parquet(f"{BRONZE}/events")
    df = (
        df.dropDuplicates(["event_id"])
        .filter(
            F.col("event_type").isin(
                "page_view", "add_to_cart", "remove_from_cart", "checkout_started", "purchase"
            )
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (
        df.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{SILVER}/events")
    )
    return df.count()


def main():
    spark = get_spark_session("bronze_to_silver_batch")
    spark.sparkContext.setLogLevel("WARN")

    results = {
        "orders": clean_orders(spark),
        "order_items": clean_order_items(spark),
        "customers": clean_customers(spark),
        "products": clean_products(spark),
        "payments": clean_payments(spark),
    }
    try:
        results["events"] = clean_events(spark)
    except Exception as e:
        print(f"[WARN] Skip cleaning events: {e}")

    for table, cnt in results.items():
        print(f"[silver] {table}: {cnt} baris")

    spark.stop()


if __name__ == "__main__":
    main()
