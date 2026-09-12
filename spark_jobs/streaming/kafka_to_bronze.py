"""
kafka_to_bronze.py
Consume topik Kafka 'ecommerce.events' secara streaming, tulis ke
MinIO bronze zone sebagai Parquet, partisi per tanggal event.
"""
import os
import sys

sys.path.append("/opt/spark_jobs")
from common.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "ecommerce.events")
BUCKET = os.getenv("MINIO_BUCKET_DATALAKE", "datalake")
OUTPUT_PATH = f"s3a://{BUCKET}/bronze/events"
CHECKPOINT_PATH = f"s3a://{BUCKET}/_checkpoints/kafka_to_bronze"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("session_id", StringType()),
    StructField("price", DoubleType()),
    StructField("device", StringType()),
])


def main():
    spark = get_spark_session("kafka_to_bronze_streaming")
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .withColumn("data", F.from_json("json_str", EVENT_SCHEMA))
        .select("data.*", "kafka_ts")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    query = (
        parsed.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
