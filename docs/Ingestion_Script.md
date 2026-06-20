# Ingestion Script — `ingest.py` and `pipeline.py`

## Overview

The ingestion layer consists of two scripts in the `ingestion/` directory: `ingest.py` is the entry point that defines the batch being processed and maps dataset names to local file paths, and `pipeline.py` provides the functions that perform the actual S3 upload. Together they upload all three source files to `raw/` in S3. From that point, the trigger is fully automatic: each upload fires an S3 Object Created event to EventBridge, which invokes the aggregation Lambda; once the Lambda confirms all three files for the batch have landed, it fires a single Step Functions execution with the complete files map.

The ingestion scripts no longer call `states:StartExecution` directly — that responsibility belongs to the aggregation Lambda. The ingestion principal therefore needs only `s3:PutObject` on `raw/*`.

This document covers the full upload flow, the `Path.resolve()` fix for reliable path handling, how Terraform outputs are read to avoid hardcoding infrastructure ARNs, the Excel-to-CSV conversion step, and how `None` cell values from openpyxl are handled before writing.

---

## `ingest.py` — The Entry Point

```python
BATCH = "apr_2025"

DATASETS = {
    "products":    {"file": "products.csv",              "key": "raw/products_apr_2025.csv"},
    "orders":      {"file": "orders_apr_2025.xlsx",      "key": "raw/orders_apr_2025.csv"},
    "order_items": {"file": "order_items_apr_2025.xlsx", "key": "raw/order_items_apr_2025.csv"},
}

def main() -> None:
    run_ingestion(BATCH, DATASETS)
```

`ingest.py` is the only file that changes between monthly batch runs — `BATCH` and the three `key` values are updated for the new month. No other file in the pipeline needs to change for a new monthly batch.

**Why all three filenames include the batch label:** The aggregation Lambda identifies which dataset and batch each S3 key belongs to by parsing the filename. All three keys must follow the pattern `<dataset>_<batch>.csv` (e.g. `products_apr_2025.csv`) so the Lambda's regex `^(products|orders|order_items)_(.+)$` resolves consistently. In the previous architecture, `products.csv` had no batch label because the Step Functions trigger was explicit — the Lambda's need for automatic batch correlation is what makes consistent naming a hard requirement.

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

`pipeline.py` reads the S3 bucket name from `terraform output` rather than hardcoding it:

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
bucket = fetch_terraform_output("data_bucket_name")
```

The state machine ARN is no longer read here — the aggregation Lambda reads it from its own environment variable (`STATE_MACHINE_ARN`), injected by Terraform at deploy time. The ingestion script only needs the bucket name to target the `put_object` calls.

### Why Not Environment Variables or a Config File

**Environment variables:** Require the operator to set them before running the script. A missed `export DATA_BUCKET=...` produces a `KeyError` with no indication of what to set. The bucket name changes per environment (`dev`, `staging`, `prod`) and per Terraform workspace.

**Hardcoded constants:** A bucket name contains a project prefix and AWS account ID. Hardcoding it makes the script non-portable across accounts and breaks if the bucket is recreated under a different name.

**Terraform output:** The Terraform state is the authoritative source of truth for infrastructure resource names. `terraform output` reads from the state file in the `terraform/` directory, which is always up to date after `terraform apply`. The script can be run immediately after any `terraform apply` without manual updates.

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

## `run_ingestion()` — Upload All Three Files

```python
def run_ingestion(batch: str, datasets: dict) -> None:
    bucket = fetch_terraform_output("data_bucket_name")

    s3_client = boto3.client("s3")
    for dataset, spec in datasets.items():
        try:
            upload_dataset(s3_client, bucket, spec["file"], spec["key"])
        except (ClientError, OSError) as error:
            print(f"  FAILED    {spec['key']}: {error}")
            sys.exit(1)

    print(f"\nAll {len(datasets)} files uploaded.")
    print("EventBridge will detect the uploads and fire Step Functions once all three files are present.")
```

`run_ingestion()` uploads all three files then exits. It does **not** call `states:StartExecution`. The trigger is automatic:

1. Each `s3.put_object` call completes → S3 fires an `Object Created` event to EventBridge.
2. EventBridge routes the event to the aggregation Lambda.
3. The Lambda records the landed file in DynamoDB and checks the count.
4. When the third file lands, the Lambda fires a single `states:StartExecution` with the complete `files` map.

If any `put_object` call raises (network error, permissions error, bucket does not exist), the script prints the error and exits. No Step Functions execution is started. The partial upload is harmless — the DynamoDB record for the batch has a 24-hour TTL, and the next `run_ingestion()` call overwrites the partial S3 keys. If the Lambda was invoked by the partial upload events, it simply recorded fewer than 3 files and waited — no execution was fired.

---

## `start_etl_batch()` — Manual Trigger Utility

`start_etl_batch()` and `build_execution_name()` remain in `pipeline.py` but are no longer called by `run_ingestion()`. They are retained as a manual override for emergency re-triggering — for example, if the aggregation Lambda failed to fire after all three files landed and the DynamoDB item has already expired.

```python
def start_etl_batch(sfn_client, state_machine_arn: str, bucket: str, batch: str, files: dict) -> str:
    execution_input = {"bucket": bucket, "batch": batch, "files": files}
    execution_name  = build_execution_name(batch)
    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(execution_input),
    )
    return response["executionArn"]
```

To use it manually, the caller needs AWS credentials with `states:StartExecution` on the state machine ARN — the standard ingestion policy no longer grants this, so a separate role or elevated credentials are required for manual re-triggering. The `manual_sfn_trigger_command` Terraform output provides the equivalent AWS CLI command for one-off use.
