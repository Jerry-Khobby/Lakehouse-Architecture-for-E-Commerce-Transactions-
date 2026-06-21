"""
ingest_may_2025.py — May 2025 batch entry point.

Uploads products_may_2025.csv, orders_may_2025.csv, and order_items_may_2025.csv
to the raw/ S3 zone. EventBridge detects each upload and the aggregation Lambda
fires a single Step Functions execution once all three files are present.

Run this AFTER the April 2025 pipeline has completed successfully so that
the Delta tables already exist and the May data merges on top of them.

Prerequisites:
    pip install boto3
    terraform apply must have completed successfully.
    The caller's AWS credentials need only s3:PutObject on raw/.
    The three May 2025 CSVs must exist under Data/ (run scripts/generate_may_2025_data.py).

Usage (from the project root):
    python ingestion/ingest_may_2025.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline import run_ingestion

BATCH = "may_2025"

DATASETS = {
    "products": {"file": "products.csv", "key": "raw/products_may_2025.csv"},
    "orders": {"file": "orders_may_2025.csv", "key": "raw/orders_may_2025.csv"},
    "order_items": {"file": "order_items_may_2025.csv", "key": "raw/order_items_may_2025.csv"},
}


def main() -> None:
    run_ingestion(BATCH, DATASETS)


if __name__ == "__main__":
    main()
