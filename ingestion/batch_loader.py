"""
batch_loader.py
Load seluruh CSV Olist dari data/raw/olist/ ke MinIO bronze zone.
Menerapkan prinsip "raw, immutable ingestion": tidak ada transformasi,
hanya penyalinan 1:1 plus metadata ingestion (load_ts, source_file).
"""
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("batch_loader")

minio_env = os.getenv("MINIO_ENDPOINT_HOST") or os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ENDPOINT = minio_env.replace("http://", "").replace("https://", "").rstrip("/")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET = os.getenv("MINIO_BUCKET_DATALAKE", "datalake")

raw_env = os.getenv("RAW_DATA_DIR")
if raw_env:
    RAW_DIR = Path(raw_env)
elif Path("/opt/data/raw/olist").exists():
    RAW_DIR = Path("/opt/data/raw/olist")
else:
    RAW_DIR = Path("data/raw/olist")

EXPECTED_FILES = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def validate_raw_files() -> list[Path]:
    missing = [f for f in EXPECTED_FILES if not (RAW_DIR / f).exists()]
    if missing:
        logger.error("File berikut tidak ditemukan di %s: %s", RAW_DIR, missing)
        sys.exit(1)
    files = [RAW_DIR / f for f in EXPECTED_FILES]
    for f in files:
        if f.stat().st_size == 0:
            logger.error("File %s kosong (0 byte).", f)
            sys.exit(1)
    return files


def upload_file_to_bronze(client: Minio, filepath: Path, load_ts: str) -> None:
    table_name = filepath.stem.replace("olist_", "").replace("_dataset", "")
    object_name = f"bronze/olist/{table_name}/load_dt={load_ts}/{filepath.name}"
    try:
        client.fput_object(BUCKET, object_name, str(filepath))
        logger.info("Uploaded %s -> s3://%s/%s", filepath.name, BUCKET, object_name)
    except S3Error as e:
        logger.error("Gagal upload %s: %s", filepath.name, e)
        raise


def quick_row_count_check(files: list[Path]) -> None:
    for f in files:
        df = pd.read_csv(f, nrows=5)
        if df.empty:
            logger.error("File %s tidak punya baris data.", f)
            sys.exit(1)
        logger.info("Sanity check OK: %s (%d kolom terbaca)", f.name, len(df.columns))


def main():
    load_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info("=== Mulai batch load Olist dataset (load_dt=%s) ===", load_ts)

    files = validate_raw_files()
    quick_row_count_check(files)

    client = get_minio_client()
    if not client.bucket_exists(BUCKET):
        logger.error("Bucket %s belum ada. Jalankan minio-init dulu.", BUCKET)
        sys.exit(1)

    for f in files:
        upload_file_to_bronze(client, f, load_ts)

    logger.info("=== Batch load selesai: %d file diunggah ke bronze ===", len(files))


if __name__ == "__main__":
    main()
