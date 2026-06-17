# Ingestion Script — `ingest.py` and `pipeline.py`

## Overview

The ingestion layer consists of two scripts in the `ingestion/` directory: `ingest.py` is the entry point that defines the batch being processed and maps dataset names to local file paths, and `pipeline.py` provides the functions that perform the actual upload and Step Functions trigger. Together they upload all three source files to S3, collect the uploaded keys, and start exactly one Step Functions execution with the complete files map. This document covers the full flow from local file to Step Functions execution, the `Path.resolve()` fix for reliable path handling, how Terraform outputs are read to avoid hardcoding infrastructure ARNs, the Excel-to-CSV conversion step, and how `None` cell values from openpyxl are handled before writing.

---

## `ingest.py` — The Entry Point

```python
from pathlib import Path
from ingestion.pipeline import run_ingestion

BATCH    = "apr_2025"
DATA_DIR = Path(__file__).resolve().parent / "data"

DATASETS = {
    "products": {
        "local_path": DATA_DIR / "products.xlsx",
        "s3_key":     f"raw/{BATCH}/products/products.csv",
    },
    "orders": {
        "local_path": DATA_DIR / "orders.xlsx",
        "s3_key":     f"raw/{BATCH}/orders/orders_{BATCH}.csv",
    },
    "order_items": {
        "local_path": DATA_DIR / "order_items.xlsx",
        "s3_key":     f"raw/{BATCH}/order_items/order_items_{BATCH}.csv",
    },
}

if __name__ == "__main__":
    run_ingestion(batch=BATCH, datasets=DATASETS)
```

`ingest.py` is the only file that changes between monthly batch runs — `BATCH` is updated from `"apr_2025"` to `"may_2025"`, and all downstream paths and execution names update automatically via f-string interpolation. No other file in the pipeline needs to change for a new monthly batch.

---

## `Path.resolve()` — Why Relative Paths Fail

```python
DATA_DIR = Path(__file__).resolve().parent / "data"
```

### The Problem with Relative Paths

`"data/products.xlsx"` or `Path("data") / "products.xlsx"` constructs a path relative to the **current working directory** (`os.getcwd()`) — the directory from which the Python interpreter was invoked. The current working directory depends on where the operator runs the script:

```bash
# From the repo root:
python ingestion/ingest.py
# cwd = /repos/lakehouse — "data/products.xlsx" resolves to /repos/lakehouse/data/products.xlsx (wrong)

# From the ingestion/ directory:
python ingest.py
# cwd = /repos/lakehouse/ingestion — "data/products.xlsx" resolves to /repos/lakehouse/ingestion/data/products.xlsx (correct)
```

The same command produces different file paths depending on where it is invoked. A CI/CD pipeline that runs the script from the repo root would fail with `FileNotFoundError`, while local development from `ingestion/` would succeed. This is a fragile, environment-dependent dependency.

### `Path(__file__).resolve().parent`

`__file__` is a Python special variable that always holds the path of the currently executing module — `ingest.py` itself. `.resolve()` expands it to an absolute path with all symlinks resolved. `.parent` steps up one directory. The result is the absolute path to the `ingestion/` directory, regardless of where the script was invoked.

```python
# __file__ = "/repos/lakehouse/ingestion/ingest.py"  (on any invocation)
# Path(__file__).resolve().parent = /repos/lakehouse/ingestion
# DATA_DIR = /repos/lakehouse/ingestion/data
```

`DATA_DIR / "products.xlsx"` is now always `/repos/lakehouse/ingestion/data/products.xlsx`, independent of the working directory. The script can be run from anywhere in the repository without changing behaviour.

---

## Terraform Output — No Hardcoded ARNs

`pipeline.py` reads the Step Functions state machine ARN and the S3 bucket name from `terraform output` rather than hardcoding them:

```python
import subprocess
import json

def _get_terraform_output(key: str) -> str:
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=Path(__file__).resolve().parent.parent / "terraform",
        capture_output=True,
        text=True,
        check=True,
    )
    outputs = json.loads(result.stdout)
    return outputs[key]["value"]
```

Called at the start of `run_ingestion()`:

```python
state_machine_arn = _get_terraform_output("state_machine_arn")
data_bucket       = _get_terraform_output("data_bucket_name")
```

### Why Not Environment Variables or a Config File

**Environment variables:** Require the operator to set them before running the script. A missed `export STATE_MACHINE_ARN=...` produces a `KeyError` with no indication of what to set. The infrastructure ARN changes per environment (`dev`, `staging`, `prod`) and per Terraform workspace — tracking the correct value for each environment is an operational burden.

**Hardcoded constants:** An ARN contains the AWS account ID and region. Hardcoding it makes the script non-portable across accounts and breaks immediately if the Terraform resource is recreated under a different name (which changes the ARN).

**Terraform output:** The Terraform state is the authoritative source of truth for infrastructure ARNs. `terraform output` reads from the state file in the `terraform/` directory, which is always up to date after `terraform apply`. The script can be run immediately after any `terraform apply` without manual ARN updates. If the state machine is recreated, `terraform apply` updates the state file; the next `ingest.py` run picks up the new ARN automatically.

The `cwd` parameter in `subprocess.run()` uses the same `Path(__file__).resolve()` pattern to locate the `terraform/` directory reliably regardless of invocation path.

---

## Excel-to-CSV Conversion

Source data files are delivered as `.xlsx` Excel workbooks. The Glue jobs expect CSV files in `raw/`. The conversion happens in `pipeline.py` before the S3 upload:

```python
import openpyxl
import csv
import io

def _xlsx_to_csv(xlsx_path: Path) -> bytes:
    workbook = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    sheet    = workbook.active

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")

    for row in sheet.iter_rows(values_only=True):
        writer.writerow([_clean_cell(cell) for cell in row])

    workbook.close()
    return buffer.getvalue().encode("utf-8")
```

`openpyxl.load_workbook(read_only=True, data_only=True)`:
- `read_only=True` — streams the worksheet row-by-row rather than loading the entire workbook into memory. For large worksheets (10,000+ rows), this avoids out-of-memory conditions on the ingestion machine.
- `data_only=True` — reads cell values, not formulas. An Excel cell that contains `=SUM(A1:A10)` is read as its computed numeric value, not the formula string. Source data workbooks may contain calculated columns; `data_only=True` ensures the CSV contains the actual data values.

`sheet.iter_rows(values_only=True)` yields each row as a tuple of Python values (int, float, str, datetime, None) rather than openpyxl `Cell` objects.

The `io.StringIO` buffer collects the CSV rows in memory, then `.encode("utf-8")` converts the string to bytes for S3 upload. The Glue jobs read the uploaded CSV with explicit `StringType` schemas — the encoding is irrelevant to Glue's CSV reader as long as it is a standard encoding, and UTF-8 is always correct.

---

## `None` Cell Handling

openpyxl represents empty cells as `None`. Python's `csv.writer` converts `None` to the string `"None"` by default:

```python
>>> import csv, io
>>> buf = io.StringIO()
>>> csv.writer(buf).writerow([1, None, "apple"])
>>> buf.getvalue()
'1,None,apple\n'   # ← "None" as a literal string in the CSV
```

A Glue job reading this CSV with `FAILFAST` mode and `nullable=False` on that column would see the string `"None"` where it expects an integer or meaningful string. If `nullable=True`, the string `"None"` would not become null — it would be the literal string `"None"` — which would then fail referential integrity checks or value range checks.

The `_clean_cell()` function normalises cell values before writing:

```python
def _clean_cell(value) -> str:
    if value is None:
        return ""           # Empty string → CSV empty field → Spark reads as null (nullable) or fails FAILFAST (non-nullable)
    if isinstance(value, float) and value != value:
        return ""           # NaN check: float("nan") != float("nan") is True
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")   # Consistent timestamp format
    return str(value)
```

### `None` → Empty String

An empty field in CSV (`,,` or a trailing `,`) is read by Spark's CSV reader as `null` when the column is `nullable=True`, or raises `FAILFAST` when `nullable=False`. This is the correct behaviour — an empty cell in the source Excel file means the value is absent, not that it is the string `"None"`. Spark's null handling then takes over: the null check validation rules produce the correct `"null_required_field"` rejection rather than a confusing string mismatch.

### NaN Check

openpyxl can return `float("nan")` for Excel cells that contain `#N/A` error values or cells that resulted from failed formula evaluations. `float("nan") != float("nan")` is `True` in Python (IEEE 754 NaN inequality) — this is the standard Python idiom for detecting NaN without importing `math.isnan`. NaN cells are treated as empty (missing value) rather than propagating an invalid float to the CSV.

### `datetime` → ISO 8601 String

openpyxl reads Excel date cells as Python `datetime` objects. A Python `datetime` converted to string via `str()` produces `"2025-04-15 08:30:00"` (space separator). The Glue jobs expect `"2025-04-15T08:30:00"` (T separator, matching `TIMESTAMP_FORMAT`). The explicit `strftime("%Y-%m-%dT%H:%M:%S")` in `_clean_cell()` produces the correct format.

This is the same format fix that the May 2025 all-rejection bug was about. `_clean_cell()` in the ingestion layer and `TIMESTAMP_FORMAT` in the Glue jobs must be kept in sync. If the format ever changes in one place, it must change in both.

---

## `run_ingestion()` — Upload All Three, Then Trigger Once

```python
def run_ingestion(batch: str, datasets: dict) -> None:
    state_machine_arn = _get_terraform_output("state_machine_arn")
    data_bucket       = _get_terraform_output("data_bucket_name")

    s3_client  = boto3.client("s3")
    sfn_client = boto3.client("stepfunctions")

    uploaded_files = {}
    for dataset_name, config in datasets.items():
        csv_bytes = _xlsx_to_csv(config["local_path"])
        s3_key    = config["s3_key"]

        s3_client.put_object(
            Bucket=data_bucket,
            Key=s3_key,
            Body=csv_bytes,
            ContentType="text/csv",
        )
        uploaded_files[dataset_name] = s3_key
        logger.info("Uploaded %s → s3://%s/%s", dataset_name, data_bucket, s3_key)

    start_etl_batch(
        sfn_client=sfn_client,
        state_machine_arn=state_machine_arn,
        bucket=data_bucket,
        batch=batch,
        files=uploaded_files,
    )
```

All three files are uploaded **before** `start_etl_batch()` is called. This is the critical design decision that prevents the EventBridge race condition documented in [AWS_EventBridge.md](AWS_EventBridge.md). The Step Functions execution does not start until the `for` loop completes — until then, all three CSVs are available at their S3 keys. The Glue jobs will find all three files immediately when they read `$.files` from the execution input.

If any `put_object` call raises (network error, permissions error, bucket does not exist), the exception propagates out of `run_ingestion()` before `start_etl_batch()` is called. No Step Functions execution is started. The partial upload (some files in `raw/`, some not) is harmless — the next `run_ingestion()` call re-uploads all three files (overwriting any partial uploads on the same keys) and then triggers the execution.

---

## `start_etl_batch()` — The Execution Trigger

```python
def start_etl_batch(
    sfn_client,
    state_machine_arn: str,
    bucket: str,
    batch: str,
    files: dict,
) -> str:
    execution_name = _build_execution_name(batch)
    execution_input = json.dumps({
        "bucket": bucket,
        "batch":  batch,
        "files":  files,
    })

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=execution_input,
    )
    logger.info("Started execution: %s", response["executionArn"])
    return response["executionArn"]
```

### `_build_execution_name()`

```python
def _build_execution_name(batch: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    raw_name  = f"{batch}-{timestamp}"
    safe_name = re.sub(r"[^0-9A-Za-z_-]", "-", raw_name)
    return safe_name[:80]
```

Step Functions execution names must match `[a-zA-Z0-9_-]` and be at most 80 characters. `batch = "apr_2025"` and `timestamp = "20250430T092211"` produce `"apr_2025-20250430T092211"` — already within the character set and length limit.

The `re.sub()` replaces any non-allowed character with `-`. This handles batch identifiers that contain spaces, dots, or other punctuation. The `[:80]` truncates at 80 characters regardless. The timestamp suffix ensures uniqueness — the same batch identifier (`apr_2025`) can be run again with a different execution name if a re-run is needed after a failure.

Step Functions enforces execution name uniqueness within an account per state machine — if `start_execution()` is called with the same name as an existing execution (running or completed within the last 90 days), it raises `ExecutionAlreadyExists`. The timestamp suffix prevents this.
