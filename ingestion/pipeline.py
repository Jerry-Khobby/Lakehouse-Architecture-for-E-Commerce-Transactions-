"""
pipeline.py — Shared ingestion utilities used by every batch entry point.

Each entry point (ingest.py, ingest_may_2025.py, …) defines its own BATCH
label and DATASETS map, then delegates file uploads to run_ingestion().

Trigger flow (automatic):
  run_ingestion() uploads all three CSV files to S3 raw/.
  S3 fires Object Created events → EventBridge → aggregation Lambda.
  Once all three files for the batch are present, the Lambda starts a single
  Step Functions execution with the complete files map.

start_etl_batch() is retained for manual or emergency triggering only.
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "Data"
TERRAFORM_DIR = PROJECT_ROOT / "terraform"

EXECUTION_NAME_MAX_LENGTH = 80


def fetch_terraform_output(name: str) -> str:
    """Read a single raw value from `terraform output`. Exits on failure."""
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


def start_etl_batch(sfn_client, state_machine_arn: str, bucket: str, batch: str, files: dict) -> str:
    """Start one Step Functions execution for the whole batch. Returns the execution ARN."""
    execution_input = {"bucket": bucket, "batch": batch, "files": files}
    execution_name = build_execution_name(batch)
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


def run_ingestion(batch: str, datasets: dict) -> None:
    """Upload all dataset files to S3 raw/. EventBridge triggers Step Functions automatically."""
    print("Reading Terraform outputs ...")
    bucket = fetch_terraform_output("data_bucket_name")

    print(f"Target bucket : {bucket}")
    print(f"Batch         : {batch}")
    print(f"Datasets      : {len(datasets)}\n")

    s3_client = boto3.client("s3")
    for dataset, spec in datasets.items():
        try:
            upload_dataset(s3_client, bucket, spec["file"], spec["key"])
        except (ClientError, OSError) as error:
            print(f"  FAILED    {spec['key']}: {error}")
            sys.exit(1)

    print(f"\nAll {len(datasets)} files uploaded.")
    print("EventBridge will detect the uploads and fire Step Functions once all three files are present.")
    print("Track progress: AWS Console → Step Functions → ecom-lakehouse-dev-etl-pipeline")
