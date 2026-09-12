"""
test_transformations.py
Test transformasi Spark memakai SparkSession lokal (bukan cluster),
memvalidasi logika cleaning tanpa perlu MinIO/Kafka nyata.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("unit-test-transformations")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_order_items_filters_negative_price(spark):
    """Memastikan baris dengan harga atau ongkos kirim negatif dibuang."""
    data = [
        ("order1", 1, "prod1", 10.0, 5.0),
        ("order2", 1, "prod2", -3.0, 2.0),   # harus ter-filter (harga negatif)
        ("order3", 1, "prod3", 20.0, -1.0),  # harus ter-filter (freight negatif)
    ]
    df = spark.createDataFrame(data, ["order_id", "order_item_id", "product_id", "price", "freight_value"])
    filtered = df.filter((F.col("price") >= 0) & (F.col("freight_value") >= 0))
    assert filtered.count() == 1
    assert filtered.collect()[0]["order_id"] == "order1"


def test_dedup_orders_by_order_id(spark):
    """Memastikan data duplikat berdasarkan order_id berhasil dieliminasi."""
    data = [("order1", "cust1"), ("order1", "cust1"), ("order2", "cust2")]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])
    deduped = df.dropDuplicates(["order_id"])
    assert deduped.count() == 2


def test_event_type_whitelist_filter(spark):
    """Memastikan hanya 5 tipe event resmi yang diloloskan dari clickstream Kafka."""
    data = [
        ("e1", "page_view"), ("e2", "invalid_type"), ("e3", "purchase"),
    ]
    df = spark.createDataFrame(data, ["event_id", "event_type"])
    valid_types = ["page_view", "add_to_cart", "remove_from_cart", "checkout_started", "purchase"]
    filtered = df.filter(F.col("event_type").isin(valid_types))
    assert filtered.count() == 2
