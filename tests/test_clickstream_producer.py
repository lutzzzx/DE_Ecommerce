"""
test_clickstream_producer.py
Unit test untuk pembangkitan event sintetis: pastikan skema & tipe data benar,
dan distribusi event_type mengikuti bobot yang diharapkan (secara statistik).
"""
import sys
from collections import Counter

sys.path.append("ingestion")
import clickstream_producer as cp  # noqa: E402


def test_build_event_schema():
    """Pastikan semua kolom mandatory clickstream event terbentuk dengan tipe data benar."""
    event = cp.build_event(["cust-1", "cust-2"], ["prod-1", "prod-2"], "sess-1")
    required_keys = {
        "event_id", "event_type", "event_timestamp", "customer_id",
        "product_id", "session_id", "price", "device",
    }
    assert required_keys.issubset(event.keys())
    assert event["customer_id"] in ["cust-1", "cust-2"]
    assert event["product_id"] in ["prod-1", "prod-2"]
    assert isinstance(event["price"], float)
    assert event["device"] in cp.DEVICES


def test_weighted_event_type_distribution():
    """Uji statistik: page_view (bobot 0.55) harus paling sering muncul dalam 5000 sampel."""
    samples = [cp.weighted_event_type() for _ in range(5000)]
    counts = Counter(samples)
    most_common_type, _ = counts.most_common(1)[0]
    assert most_common_type == "page_view"
    assert set(counts.keys()).issubset({t for t, _ in cp.EVENT_TYPES_WEIGHTED})
