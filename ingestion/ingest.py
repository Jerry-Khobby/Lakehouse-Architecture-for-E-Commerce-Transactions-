"""
ingest.py — Lands a data batch in S3 and starts the lakehouse ETL pipeline.

The three datasets (products, orders, order_items) are ONE relational batch:
order_items references both products and orders. So this script uploads all
three CSVs to the raw/ zone and then starts a SINGLE Step Functions execution
with a structured input describing the batch:

    {
      "bucket": "<data-bucket>",
      "batch":  "apr_2025",
      "files": {
        "products":    "raw/products.csv",
        "orders":      "raw/orders_apr_2025.csv",
        "order_items": "raw/order_items_apr_2025.csv"
      }
    }

The state machine then runs the three Glue jobs in dependency order
(products → orders → order_items) within that one execution. There is no
per-file EventBridge trigger — that would fire three independent executions
and race the referential-integrity checks.

Prerequisites:
    pip install boto3 openpyxl
    terraform apply must have completed successfully.
    The caller's AWS credentials need the permissions in
    aws_iam_policy.ingestion (s3:PutObject on raw/ + states:StartExecution).

Usage (from the project root):
    python ingestion/ingest.py
"""

import csv
import io
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import openpyxl
from botocore.exceptions import ClientError

# Path(__file__).resolve() makes the path absolute before traversing so the
# script works correctly regardless of which directory it is run from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "Data"
TERRAFORM_DIR = PROJECT_ROOT / "terraform"

# A short label identifying this load. Becomes part of the Step Functions
# execution name and the success/failure alerts.
BATCH = "apr_2025"

# Keyed by dataset so the files map handed to Step Functions uses the same
# names the state machine reads ($.files.products, $.files.orders, ...).
DATASETS = {
    "products": {"file": "products.csv", "key": "raw/products.csv"},
    "orders": {"file": "orders_apr_2025.xlsx", "key": "raw/orders_apr_2025.csv"},
    "order_items": {"file": "order_items_apr_2025.xlsx", "key": "raw/order_items_apr_2025.csv"},
}

# Step Functions execution names allow [0-9A-Za-z_-] and must be ≤ 80 chars.
EXECUTION_NAME_MAX_LENGTH = 80


def fetch_terraform_output(name: str) -> str:
    """Read a single raw value from `terraform output`. Exits with a clear
    message if Terraform is unavailable or the output is undefined."""
    try:
        result = subprocess.run(
            ["terraform", "output", "-raw", name],
            cwd=TERRAFORM_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except FileNotFoundError:
        print("ERROR: 'terraform' is not on your PATH. Install it or add it to PATH.")
        sys.exit(1)
    except subprocess.CalledProcessError as error:
        print(f"ERROR: 'terraform output -raw {name}' failed.\n{error.stderr}")
        sys.exit(1)


def xlsx_to_csv_bytes(path: Path) -> bytes:
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheet = workbook.active
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in sheet.iter_rows(values_only=True):
        writer.writerow(["" if cell is None else cell for cell in row])
    workbook.close()
    return buffer.getvalue().encode("utf-8")


def load_dataset(filename: str) -> bytes:
    path = DATA_DIR / filename
    if filename.endswith(".xlsx"):
        return xlsx_to_csv_bytes(path)
    return path.read_bytes()


def upload_dataset(s3_client, bucket: str, filename: str, s3_key: str) -> None:
    payload = load_dataset(filename)
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=payload, ContentType="text/csv")
    print(f"  uploaded  s3://{bucket}/{s3_key}  ({len(payload) / 1024:.1f} KB)")


def build_execution_name(batch: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    sanitized = re.sub(r"[^0-9A-Za-z_-]", "-", f"{batch}-{timestamp}")
    return sanitized[:EXECUTION_NAME_MAX_LENGTH]


def start_etl_batch(sfn_client, state_machine_arn: str, bucket: str, files: dict) -> str:
    """Start one Step Functions execution for the whole batch. Returns the
    execution ARN, or exits with a clear message on failure."""
    execution_input = {"bucket": bucket, "batch": BATCH, "files": files}
    execution_name = build_execution_name(BATCH)
    try:
        response = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(execution_input),
        )
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code == "ExecutionAlreadyExists":
            print(f"  An execution named '{execution_name}' already exists — wait a moment and retry.")
        else:
            print(f"  FAILED to start the ETL batch: {error}")
        sys.exit(1)
    return response["executionArn"]


def main() -> None:
    print("Reading Terraform outputs ...")
    bucket = fetch_terraform_output("data_bucket_name")
    state_machine_arn = fetch_terraform_output("sfn_state_machine_arn")

    print(f"Target bucket : {bucket}")
    print(f"State machine : {state_machine_arn}")
    print(f"Batch         : {BATCH}")
    print(f"Datasets      : {len(DATASETS)}\n")

    s3_client = boto3.client("s3")
    files = {}
    for dataset, spec in DATASETS.items():
        try:
            upload_dataset(s3_client, bucket, spec["file"], spec["key"])
            files[dataset] = spec["key"]
        except (ClientError, OSError) as error:
            print(f"  FAILED    {spec['key']}: {error}")
            sys.exit(1)

    print(f"\nAll {len(DATASETS)} files uploaded. Starting the ETL batch ...")
    sfn_client = boto3.client("stepfunctions")
    execution_arn = start_etl_batch(sfn_client, state_machine_arn, bucket, files)

    print(f"Started execution:\n  {execution_arn}")
    print("Track progress with:")
    print(f"  aws stepfunctions describe-execution --execution-arn {execution_arn}")


if __name__ == "__main__":
    main()
