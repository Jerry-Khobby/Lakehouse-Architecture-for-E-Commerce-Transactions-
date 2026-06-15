"""
ingest.py — April 2025 batch entry point.

Uploads products.csv, orders_apr_2025.xlsx, and order_items_apr_2025.xlsx
to the raw/ S3 zone, then starts a single Step Functions execution that
runs the three Glue jobs in dependency order (products → orders → order_items).

Prerequisites:
    pip install boto3 openpyxl
    terraform apply must have completed successfully.
    The caller's AWS credentials need s3:PutObject on raw/ and states:StartExecution.

Usage (from the project root):
    python ingestion/ingest.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline import build_execution_name, run_ingestion  # noqa: F401
from pipeline import start_etl_batch as _start_etl_batch

BATCH = "apr_2025"

DATASETS = {
    "products": {"file": "products.csv", "key": "raw/products.csv"},
    "orders": {"file": "orders_apr_2025.xlsx", "key": "raw/orders_apr_2025.csv"},
    "order_items": {"file": "order_items_apr_2025.xlsx", "key": "raw/order_items_apr_2025.csv"},
}


def start_etl_batch(sfn_client, state_machine_arn: str, bucket: str, files: dict) -> str:
    """Four-argument shim that bakes the April batch label in.

    test_ingest.py imports this name and calls it without a batch argument,
    so the pipeline's five-argument version is wrapped here.
    """
    return _start_etl_batch(sfn_client, state_machine_arn, bucket, BATCH, files)


def main() -> None:
    run_ingestion(BATCH, DATASETS)


if __name__ == "__main__":
    main()
