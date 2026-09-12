"""
silver_to_gold.py
Hasilkan tabel agregat siap pakai (gold layer) dari silver:
- revenue harian
- top produk per kategori
- ringkasan aktivitas streaming per jam (near real-time feed ke warehouse)
"""
import os
import sys

sys.path.append("/opt/spark_jobs")
from common.spark_session import get_spark_session
from pyspark.sql import functions as F

BUCKET = os.getenv("MINIO_BUCKET_DATALAKE", "datalake")
SILVER = f"s3a://{BUCKET}/silver"
GOLD = f"s3a://{BUCKET}/gold"


def daily_revenue(spark):
    orders = spark.read.parquet(f"{SILVER}/orders")
    items = spark.read.parquet(f"{SILVER}/order_items")

    df = (
        orders.join(items, "order_id")
        .withColumn("order_date", F.to_date("order_purchase_timestamp"))
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("price").alias("gross_revenue"),
            F.sum("freight_value").alias("total_freight"),
        )
        .orderBy("order_date")
    )
    df.write.mode("overwrite").parquet(f"{GOLD}/daily_revenue")
    return df.count()


def top_products_by_category(spark):
    items = spark.read.parquet(f"{SILVER}/order_items")
    products = spark.read.parquet(f"{SILVER}/products")

    df = (
        items.join(products, "product_id")
        .groupBy("product_category_name_english")
        .agg(
            F.count("order_id").alias("total_items_sold"),
            F.sum("price").alias("total_revenue"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    df.write.mode("overwrite").parquet(f"{GOLD}/top_products_by_category")
    return df.count()


def hourly_event_summary(spark):
    try:
        events = spark.read.parquet(f"{SILVER}/events")
    except Exception:
        print("[WARN] Belum ada data events di silver, skip hourly_event_summary.")
        return 0

    df = (
        events.withColumn("event_hour", F.date_trunc("hour", "event_timestamp"))
        .groupBy("event_hour", "event_type")
        .agg(
            F.countDistinct("session_id").alias("unique_sessions"),
            F.count("event_id").alias("event_count"),
        )
        .orderBy("event_hour")
    )
    df.write.mode("overwrite").parquet(f"{GOLD}/hourly_event_summary")
    return df.count()


def main():
    spark = get_spark_session("silver_to_gold_batch")
    spark.sparkContext.setLogLevel("WARN")

    print(f"[gold] daily_revenue: {daily_revenue(spark)} baris")
    print(f"[gold] top_products_by_category: {top_products_by_category(spark)} baris")
    print(f"[gold] hourly_event_summary: {hourly_event_summary(spark)} baris")

    spark.stop()


if __name__ == "__main__":
    main()
