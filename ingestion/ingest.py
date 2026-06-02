"""
ingest.py — Uploads local datasets to S3 to trigger the lakehouse ETL pipeline.

Converts Excel source files (.xlsx) to CSV in memory, then uploads all three
datasets to the data bucket's raw/ prefix.  EventBridge detects each upload
and fires a separate Step Functions execution for that dataset.

Prerequisites:
    pip install boto3 openpyxl
    terraform apply must have completed successfully.

Usage (from the project root):
    python ingestion/ingest.py
"""

import csv
import io
import subprocess
import sys
from pathlib import Path

import boto3
import openpyxl

PROJECT_ROOT  = Path(__file__).parent.parent
DATA_DIR      = PROJECT_ROOT / "Data"
TERRAFORM_DIR = PROJECT_ROOT / "terraform"

DATASETS = [
    {"file": "products.csv",              "key": "raw/products.csv"},
    {"file": "orders_apr_2025.xlsx",      "key": "raw/orders_apr_2025.csv"},
    {"file": "order_items_apr_2025.xlsx", "key": "raw/order_items_apr_2025.csv"},
]


def fetch_bucket_name() -> str:
    result = subprocess.run(
        ["terraform", "output", "-raw", "data_bucket_name"],
        cwd=TERRAFORM_DIR,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def xlsx_to_csv_bytes(path: Path) -> bytes:
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheet    = workbook.active
    buffer   = io.StringIO()
    writer   = csv.writer(buffer)
    for row in sheet.iter_rows(values_only=True):
        writer.writerow(row)
    workbook.close()
    return buffer.getvalue().encode("utf-8")


def load_dataset(filename: str) -> bytes:
    path = DATA_DIR / filename
    if filename.endswith(".xlsx"):
        return xlsx_to_csv_bytes(path)
    return path.read_bytes()


def upload_dataset(s3_client, bucket: str, filename: str, s3_key: str) -> None:
    payload = load_dataset(filename)
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=payload)
    print(f"  uploaded  s3://{bucket}/{s3_key}  ({len(payload) / 1024:.1f} KB)")


def main() -> None:
    print("Reading data bucket name from Terraform output ...")
    try:
        bucket = fetch_bucket_name()
    except subprocess.CalledProcessError as error:
        print(f"ERROR: terraform output failed.\n{error.stderr}")
        sys.exit(1)

    print(f"Target bucket : {bucket}")
    print(f"Datasets      : {len(DATASETS)}\n")

    s3_client = boto3.client("s3")

    for dataset in DATASETS:
        try:
            upload_dataset(s3_client, bucket, dataset["file"], dataset["key"])
        except Exception as error:
            print(f"  FAILED    {dataset['key']}: {error}")
            sys.exit(1)

    print(f"\nAll {len(DATASETS)} files uploaded.")
    print("EventBridge will trigger a Step Functions execution per file shortly.")


if __name__ == "__main__":
    main()
