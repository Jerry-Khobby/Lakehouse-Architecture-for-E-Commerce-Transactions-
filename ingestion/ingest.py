"""
ingest.py — April 2025 batch entry point.

Uploads products_apr_2025.csv, orders_apr_2025.csv, and order_items_apr_2025.csv
to the raw/ S3 zone. EventBridge detects each upload and the aggregation Lambda
fires a single Step Functions execution once all three files are present.

Prerequisites:
    pip install boto3 openpyxl
    terraform apply must have completed successfully.
    The caller's AWS credentials need only s3:PutObject on raw/.

Usage (from the project root):
    python ingestion/ingest.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline import run_ingestion  # noqa: F401

BATCH = "apr_2025"

DATASETS = {
    "products": {"file": "products.csv", "key": "raw/products_apr_2025.csv"},
    "orders": {"file": "orders_apr_2025.xlsx", "key": "raw/orders_apr_2025.csv"},
    "order_items": {"file": "order_items_apr_2025.xlsx", "key": "raw/order_items_apr_2025.csv"},
}


def main() -> None:
    run_ingestion(BATCH, DATASETS)


if __name__ == "__main__":
    main()
