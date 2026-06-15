"""
ingest_may_2025.py — May 2025 batch entry point.

Uploads products.csv, orders_may_2025.csv, and order_items_may_2025.csv
to the raw/ S3 zone, then starts a single Step Functions execution that
runs the three Glue jobs in dependency order (products → orders → order_items).

Run this AFTER the April 2025 pipeline has completed successfully so that
the Delta tables already exist and the May data merges on top of them.

Prerequisites:
    pip install boto3
    terraform apply must have completed successfully.
    The caller's AWS credentials need s3:PutObject on raw/ and states:StartExecution.
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
    "products": {"file": "products.csv", "key": "raw/products.csv"},
    "orders": {"file": "orders_may_2025.csv", "key": "raw/orders_may_2025.csv"},
    "order_items": {"file": "order_items_may_2025.csv", "key": "raw/order_items_may_2025.csv"},
}


def main() -> None:
    run_ingestion(BATCH, DATASETS)


if __name__ == "__main__":
    main()
