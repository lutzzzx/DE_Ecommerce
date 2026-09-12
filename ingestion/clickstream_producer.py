"""
clickstream_producer.py
Simulasikan event clickstream e-commerce (page_view, add_to_cart, purchase, dst)
dan kirim ke topik Kafka. customer_id & product_id diambil dari data Olist
supaya event streaming tetap konsisten dengan data batch.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "ecommerce.events")
RAW_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw/olist"))
EVENT_TYPES_WEIGHTED = [
    ("page_view", 0.55),
    ("add_to_cart", 0.20),
    ("remove_from_cart", 0.05),
    ("checkout_started", 0.12),
    ("purchase", 0.08),
]
DEVICES = ["mobile", "desktop", "tablet"]


def load_reference_ids() -> tuple[list[str], list[str]]:
    customers = pd.read_csv(RAW_DIR / "olist_customers_dataset.csv", usecols=["customer_id"])
    products = pd.read_csv(RAW_DIR / "olist_products_dataset.csv", usecols=["product_id"])
    return customers["customer_id"].tolist(), products["product_id"].tolist()


def weighted_event_type() -> str:
    types, weights = zip(*EVENT_TYPES_WEIGHTED)
    return random.choices(types, weights=weights, k=1)[0]


def build_event(customer_ids: list[str], product_ids: list[str], session_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": weighted_event_type(),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": random.choice(customer_ids),
        "product_id": random.choice(product_ids),
        "session_id": session_id,
        "price": round(random.uniform(15.0, 899.0), 2),
        "device": random.choice(DEVICES),
    }


def main():
    customer_ids, product_ids = load_reference_ids()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )

    print(f"Mengirim event ke topic '{TOPIC}' di broker {KAFKA_BROKER}. Ctrl+C untuk berhenti.")
    session_pool = [f"sess-{uuid.uuid4().hex[:8]}" for _ in range(50)]
    sent = 0
    try:
        while True:
            session_id = random.choice(session_pool)
            event = build_event(customer_ids, product_ids, session_id)
            producer.send(TOPIC, value=event, key=event["customer_id"].encode("utf-8"))
            sent += 1
            if sent % 100 == 0:
                producer.flush()
                print(f"[{datetime.now().isoformat()}] {sent} event terkirim...")
            time.sleep(random.uniform(0.05, 0.4))
    except KeyboardInterrupt:
        print(f"\nBerhenti. Total event terkirim: {sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
