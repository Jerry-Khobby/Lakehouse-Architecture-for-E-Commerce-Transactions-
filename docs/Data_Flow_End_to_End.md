# Data Flow — End to End

This document traces every hop a piece of data makes from the moment an operator runs `ingest.py` to the moment an analyst reads a row from Athena. Each step is grounded in the actual code, not a high-level summary.

---

## Overview Map

```
Operator workstation
  └─ ingest.py
       ├─ Terraform outputs → bucket name, state machine ARN
       ├─ load_dataset()  → bytes (xlsx→csv conversion if needed)
       ├─ s3.put_object   → raw/<file>.csv   (× 3 files)
       └─ sfn.start_execution → execution input JSON
                                        │
                          ┌─────────────▼─────────────┐
                          │    AWS Step Functions      │
                          │    STANDARD state machine  │
                          └──────────┬────────────────┘
                                     │
              ┌──────────────────────┼───────────────────────┐
              ▼                      ▼                        ▼
       RunProductsJob          RunOrdersJob          RunOrderItemsJob
       (Glue + Spark)          (Glue + Spark)        (Glue + Spark)
              │                      │                        │
       ┌──────▼──────┐        ┌──────▼──────┐         ┌──────▼──────┐
       │  Read CSV   │        │  Read CSV   │         │  Read CSV   │
       │  Validate   │        │  Validate   │         │  Validate   │
       │  MERGE→     │        │  MERGE→     │         │  MERGE→     │
       │  lakehouse  │        │  lakehouse  │         │  lakehouse  │
       │  Archive    │        │  Archive    │         │  Archive    │
       │  Catalog    │        │  Catalog    │         │  Catalog    │
       └─────────────┘        └─────────────┘         └─────────────┘
                                     │
                          ┌──────────▼─────────────┐
                          │   AthenaValidation      │
                          │   COUNT(*) smoke test   │
                          └──────────┬─────────────┘
                                     │
                          ┌──────────▼─────────────┐
                          │   NotifySuccess / SNS   │
                          └──────────┬─────────────┘
                                     │
                               Slack + Email
                                     │
                          ┌──────────▼─────────────┐
                          │   Athena Query (analyst)│
                          └─────────────────────────┘
```

---

## Step 1 — Operator Triggers Ingestion

**File:** `ingestion/ingest.py` or `ingestion/ingest_may_2025.py`  
**Function:** `run_ingestion(batch, datasets)` in `ingestion/pipeline.py`

The operator runs the ingestion script from a local workstation or CI runner:

```
python ingestion/ingest.py
```

`run_ingestion()` begins by reading two values from Terraform state:

```python
bucket = fetch_terraform_output("data_bucket_name")
state_machine_arn = fetch_terraform_output("sfn_state_machine_arn")
```

`fetch_terraform_output()` runs `terraform output -raw <name>` as a subprocess from the `terraform/` directory. If Terraform is not on the PATH or the output key does not exist, the script prints an error and exits with code 1. Nothing is uploaded until both values are confirmed.

---

## Step 2 — File Upload to `raw/`

**Function:** `upload_dataset(s3_client, bucket, filename, s3_key)`

For each of the three datasets, `upload_dataset()` calls `load_dataset(filename)`:

- If the file extension is `.csv`: reads the file bytes directly from disk (`Path.read_bytes()`).
- If the file extension is `.xlsx`: calls `xlsx_to_csv_bytes(path)`, which opens the workbook with `openpyxl` in `read_only=True, data_only=True` mode, iterates every row with `iter_rows(values_only=True)`, writes each row through Python's `csv.writer` to an in-memory `io.StringIO` buffer, then encodes the buffer to UTF-8 bytes. No temporary file is written to disk. Excel datetime cells are returned by openpyxl as Python `datetime` objects; `csv.writer` calls `str()` on them, producing ISO 8601 format compatible with the Glue job's `TIMESTAMP_FORMAT`.

The bytes are then uploaded:

```python
s3_client.put_object(
    Bucket=bucket,
    Key=s3_key,           # e.g. "raw/orders_may_2025.csv"
    Body=payload,
    ContentType="text/csv"
)
```

All three files are uploaded sequentially. If any upload raises `ClientError` or `OSError`, the script prints the error and exits before starting the Step Functions execution — preventing a partial batch from triggering the pipeline.

**What S3 receives:**

| Dataset | S3 key |
|---|---|
| products | `raw/products.csv` |
| orders (April) | `raw/orders_apr_2025.csv` |
| order items (April) | `raw/order_items_apr_2025.csv` |
| orders (May) | `raw/orders_may_2025.csv` |
| order items (May) | `raw/order_items_may_2025.csv` |

---

## Step 3 — Step Functions Execution Starts

**Function:** `start_etl_batch(sfn_client, state_machine_arn, bucket, batch, files)`

After all three uploads succeed, a single Step Functions execution is started:

```python
execution_input = {
    "bucket": bucket,
    "batch": batch,           # e.g. "may_2025"
    "files": {
        "products":    "raw/products.csv",
        "orders":      "raw/orders_may_2025.csv",
        "order_items": "raw/order_items_may_2025.csv"
    }
}
execution_name = f"{batch}-{utc_timestamp}"   # e.g. "may_2025-20260615T134313"

sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    name=execution_name,
    input=json.dumps(execution_input)
)
```

The execution name is unique because it embeds the UTC timestamp to the second. If the same name already exists (a retry within the same second), `start_etl_batch()` catches `ExecutionAlreadyExists` and exits cleanly, prompting the operator to wait a moment and retry.

The operator's script returns the execution ARN and exits. From this point everything runs asynchronously inside Step Functions.

---

## Step 4 — `RunProductsJob` (State 1 of 5)

The state machine starts at `RunProductsJob`. Step Functions calls Glue's `StartJobRun` API synchronously (`.sync` integration pattern), meaning it waits for the job to complete before advancing.

The state passes two arguments to the Glue job extracted from the execution input:
- `--RAW_KEY`: `$.files.products` → `raw/products.csv`
- `--DATA_BUCKET`: `$.bucket` → the bucket name

The Glue job receives these plus all the other `default_arguments` baked into the job definition by Terraform (processed prefix, archived prefix, rejected prefix, SNS ARN, merge keys, partition columns, etc.).

### 4a — Session Initialisation

`build_spark_session(job_name)` in `common.py`:
- Creates `SparkContext`, `GlueContext`, `SparkSession`, and `Job`.
- Pins session timezone to UTC.
- Verifies that `DeltaSparkSessionExtension` is active in the session configuration. If not, raises `RuntimeError` immediately — the rest of the job would fail silently or produce non-ACID writes without this guard.

### 4b — Read

`read_source(spark, args)` reads `s3://bucket/raw/products.csv` using:
```python
spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .option("enforceSchema", "true")
    .schema(PRODUCTS_SCHEMA)
    .load(source_path)
```

`FAILFAST` means any row whose fields cannot be cast to the declared `StructType` raises an `AnalysisException` immediately, before the DataFrame is materialised. This catches truncated files, encoding corruption, and wrong-column-count rows at the boundary of the system, before any validation logic runs.

The schema for products is:
```
product_id     IntegerType  nullable=False
department_id  IntegerType  nullable=False
department     StringType   nullable=False
product_name   StringType   nullable=False
```

### 4c — Validation

`validate(df, args, job_run_id)` runs five sequential checks. Each check filters the working DataFrame, writes the failing rows to `rejected/`, and removes them before the next check. The surviving rows accumulate in `valid_df`.

For products, the checks are:
1. Null `product_id` → rejected as `null_product_id`
2. Null `department_id`, `department`, or `product_name` → rejected as `null_required_field:<col>`
3. `product_id ≤ 0` or `department_id ≤ 0` → rejected as `invalid_id_value`
4. Whitespace-only `department` or `product_name` → rejected as `empty_string_field:<col>`
5. Duplicate `product_id` within this batch → the later row (by `department_id` then `product_name` for stable ordering) is rejected as `intra_batch_duplicate`

Rejected rows are written by `write_rejected()`:
```
s3://bucket/rejected/products/2026-06-15/20260615T134313/
```
Each rejected file is Parquet with the original columns plus `rejection_reason`, `_rejected_at`, `_job_run_id`, and `_source_key`.

### 4d — Delta MERGE

`merge_into_delta(spark, valid_df, args)`:

First, `ensure_delta_table()` checks `DeltaTable.isDeltaTable(spark, path)`. On the very first pipeline run this returns `False`, so it writes an empty DataFrame to seed the `_delta_log/`:
```python
spark.createDataFrame([], PRODUCTS_SCHEMA)
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("department")
    .save(table_path)
```
On every subsequent run `isDeltaTable()` returns `True` and this block is skipped at the cost of a single S3 HEAD request.

Then the MERGE:
```python
delta_table.alias("target").merge(
    valid_df.alias("source"),
    "target.product_id = source.product_id"
)
.whenMatchedUpdateAll(condition=(
    "source.department_id <> target.department_id OR "
    "source.department <> target.department OR "
    "source.product_name <> target.product_name"
))
.whenNotMatchedInsertAll()
.execute()
```

The MATCHED condition means: only update the row if at least one attribute changed. Re-running an identical file produces zero updates and zero new Delta log entries — a true no-op.

Delta writes the commit atomically to `_delta_log/0000000000000000001.json` (or the next sequential number). Athena will not see the new data until this commit file exists. If the Spark executor crashes mid-write, no commit file is produced and the partial Parquet files are invisible to readers.

### 4e — Catalog Update

`update_catalog_table()` runs:
```sql
CREATE TABLE IF NOT EXISTS `ecom_lakehouse_db`.`products`
USING DELTA
LOCATION 's3://bucket/lakehouse-dwh/products/'
```

The `IF NOT EXISTS` clause makes this idempotent. On the first run it creates the catalog entry. On every subsequent run it is a no-op. Schema is always derived from the live Delta transaction log, not from any catalog-stored definition.

### 4f — Archive Source File

`archive_source_file(args)` copies `raw/products.csv` to `archived/products/2026-06-15/products.csv` then deletes the source:
```python
s3.copy_object(
    Bucket=bucket,
    CopySource={"Bucket": bucket, "Key": "raw/products.csv"},
    Key="archived/products/2026-06-15/products.csv",
    ExpectedBucketOwner=account_id
)
s3.delete_object(
    Bucket=bucket,
    Key="raw/products.csv",
    ExpectedBucketOwner=account_id
)
```

`ExpectedBucketOwner` on both calls guards against a class of bucket-confusion attack where an IAM role is tricked into operating on a bucket owned by a different account. This is a defence-in-depth control since the bucket is already private, but it costs nothing.

Archive failure is logged but not re-raised. The Delta MERGE already committed — the pipeline is successful. The file staying in `raw/` is a minor operational inconvenience, not a data integrity problem.

---

## Step 5 — `RunOrdersJob` (State 2 of 5)

Step Functions advances to `RunOrdersJob` only after `RunProductsJob` reports success. The orders job follows the same six-stage structure as products, with these differences in the validation stage:

**Stage Read** — the orders CSV schema reads `order_timestamp` and `date` as `StringType`, not as `TimestampType` or `DateType`. This is intentional: reading them as strings preserves the bad-format values so they can be captured and written to `rejected/` with a descriptive reason. If Spark read them directly as `TimestampType`, a format mismatch would produce a null silently (PERMISSIVE mode) or raise without context (FAILFAST mode) — either way the row is lost to the rejected audit trail.

**Stage Validate** — nine checks for orders:
1. Null `order_id` → `null_order_id`
2. Null `user_id` → `null_user_id`
3. Null `total_amount` → `null_total_amount`
4. Cast `total_amount` to `Decimal(12,2)` — cast null → `invalid_total_amount_format`
5. Negative `total_amount` → `negative_total_amount`
6. Soft flag: `total_amount > 1,000,000` — written to `flagged/orders/` but NOT removed from `valid_df`
7. Cast `order_timestamp` string to Timestamp with `yyyy-MM-dd'T'HH:mm:ss` — cast null → `invalid_timestamp_format`
8. Future timestamp (`> now + 1 hour`) → `future_timestamp`
9. `date` column parsed and compared to timestamp-derived date — mismatch → `date_timestamp_mismatch`
10. Intra-batch duplicate `order_id` → `intra_batch_duplicate` (keep row with latest `order_timestamp`)

**Stage MERGE** — the orders merge uses a timestamp guard:
```python
.whenMatchedUpdateAll(
    condition="source.order_timestamp > target.order_timestamp"
)
.whenNotMatchedInsertAll()
```
A re-delivered older file cannot overwrite a newer order already committed. A row whose `order_id` already exists in the Delta table but whose timestamp is older is simply skipped — no update, no rejection.

---

## Step 6 — `RunOrderItemsJob` (State 3 of 5)

Step Functions advances to `RunOrderItemsJob` only after `RunOrdersJob` reports success. This sequencing is the core correctness guarantee: the referential integrity checks in this job join against the live Delta tables written by the two prior states.

**Additional validation checks specific to order_items:**

After the standard timestamp and date checks, two cross-table referential integrity checks run:

```python
# Check 9: product_id must exist in the products Delta table
products_df = spark.read.format("delta").load(products_table_path)
valid_product_ids = products_df.select("product_id")

orphan_product = valid_df.join(
    valid_product_ids,
    valid_df["product_id"] == valid_product_ids["product_id"],
    "left_anti"   # rows in valid_df that have NO match in products_df
)
# orphan_product rows → rejected as "invalid_product_id"

# Check 10: order_id must exist in the orders Delta table
orders_df = spark.read.format("delta").load(orders_table_path)
valid_order_ids = orders_df.select("order_id")

orphan_order = valid_df.join(
    valid_order_ids,
    valid_df["order_id"] == valid_order_ids["order_id"],
    "left_anti"
)
# orphan_order rows → rejected as "invalid_order_id"
```

Because `products` and `orders` were committed in the two preceding states, these joins read committed Delta snapshots — no partial data, no race conditions.

The composite key deduplication also runs here: within the batch, duplicates on `(id, order_id)` are resolved by keeping the row with the latest `order_timestamp`.

**Stage MERGE** — same timestamp guard pattern as orders, but the merge key is the composite `(id, order_id)`:
```python
.merge(
    valid_df.alias("source"),
    "target.id = source.id AND target.order_id = source.order_id"
)
.whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
.whenNotMatchedInsertAll()
```

---

## Step 7 — `AthenaValidation` (State 4 of 5)

After all three Glue jobs succeed, Step Functions submits an Athena query using the `.sync` integration:

```sql
SELECT 'products'    AS tbl, COUNT(*) AS row_count FROM ecom_lakehouse_db.products
UNION ALL
SELECT 'orders',             COUNT(*)              FROM ecom_lakehouse_db.orders
UNION ALL
SELECT 'order_items',        COUNT(*)              FROM ecom_lakehouse_db.order_items;
```

This query:
1. Forces the Glue Data Catalog to resolve all three table entries against their S3 locations.
2. Forces Athena to read the Delta transaction log for each table to determine the current snapshot.
3. Forces Athena to scan at least one Parquet file per table.

If catalog registration silently failed (a known failure mode when Lake Formation permissions are misconfigured), this query fails with `TABLE_NOT_FOUND` and the execution routes to `NotifyFailure`. The pipeline is not declared successful until data is actually visible to the query engine.

Results are written to `s3://athena-results-bucket/query-results/` and auto-expired after 7 days. The workgroup enforces a 1 GB scan limit — this particular query scans only partition metadata, so the actual bytes scanned is negligible.

---

## Step 8 — `NotifySuccess` / `NotifyFailure` (State 5 of 5)

**On success**, Step Functions publishes directly to SNS using `arn:aws:states:::sns:publish`:
```
Subject: [dev] Lakehouse ETL — SUCCESS
Message: ✅ Lakehouse ETL batch completed successfully.
         Batch: may_2025
         Execution: may_2025-20260615T134313
```

**On failure**, any task's `Catch` block routes to `NotifyFailure` which publishes:
```
Subject: [dev] Lakehouse ETL — FAILURE
Message: ❌ Lakehouse ETL batch FAILED.
         Batch: may_2025
         Execution: may_2025-20260615T134313
         Check CloudWatch logs for details.
```

After `NotifyFailure` publishes, the execution transitions to `PipelineFailed` — a terminal `Fail` state — so the execution history shows `FAILED` status in the console, not `SUCCEEDED`.

**In addition**, during each Glue job, `PipelineMonitor` publishes per-stage events to the same SNS topic via `SnsNotifier`. These produce the real-time Slack messages visible during the pipeline run:
```
⏳ [dev] ecom-lakehouse-dev-orders-etl — STARTED: ValidateStage 'Validate' started
✅ [dev] ecom-lakehouse-dev-orders-etl — SUCCESS: ValidateStage 'Validate' completed in 15.7s.
   read=850 | valid=800 | rejected=50
```

The SNS topic has a Slack Lambda subscriber that converts the SNS payload to a Slack webhook POST.

---

## Step 9 — Analyst Queries Athena

After the pipeline succeeds, an analyst opens the Athena console, selects the `ecom-lakehouse-wg` workgroup and the `ecom_lakehouse_db` database, and runs queries directly against the Delta tables.

**What Athena does internally for each query:**

1. Looks up the table in the Glue Data Catalog to get the S3 location (e.g. `s3://bucket/lakehouse-dwh/orders/`).
2. Lists the `_delta_log/` directory to find the current snapshot version — the latest commit JSON file.
3. Reads the commit file to get the list of Parquet files that belong to the current version (add files vs. remove files).
4. Applies the `WHERE` clause partition pruning — for `WHERE date = '2025-05-15'` it only lists files under `date=2025-05-15/`, skipping all other date partitions.
5. Scans the qualifying Parquet files and returns results.

**Example queries and what they touch:**

```sql
-- Monthly order volume — touches only the orders table, prunes to May partition
SELECT date, COUNT(*) AS orders, ROUND(SUM(total_amount), 2) AS revenue
FROM orders
WHERE date BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY date
ORDER BY date;

-- Top products by order frequency — touches order_items and products, broadcasts the small products dimension
SELECT p.department, p.product_name, COUNT(*) AS item_count
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
WHERE oi.date BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY p.department, p.product_name
ORDER BY item_count DESC
LIMIT 20;

-- Rejection audit — queries the rejected/ prefix directly, not the Delta table
SELECT rejection_reason, COUNT(*) AS count, _source_key
FROM "s3://bucket/rejected/orders/"
GROUP BY rejection_reason, _source_key
ORDER BY count DESC;
```

Athena results land in `s3://athena-results-bucket/query-results/` and are auto-expired after 7 days. The 1 GB per-query scan limit prevents accidental full-table scans from generating unexpected costs.

---

## Complete Timing Reference

| Hop | Approximate Duration |
|---|---|
| `ingest.py` uploads 3 files | 5–30 seconds (depends on file size and network) |
| Step Functions starts execution | < 1 second |
| `RunProductsJob` (Glue cold start + run) | 4–6 minutes |
| `RunOrdersJob` | 3–5 minutes |
| `RunOrderItemsJob` (includes RI joins) | 5–8 minutes |
| `AthenaValidation` | 10–30 seconds |
| `NotifySuccess` (SNS publish) | < 1 second |
| Total pipeline wall-clock | **~15–20 minutes** |

Glue cold start (JVM + Spark context initialisation) accounts for roughly 2–3 minutes of each job's duration. The actual data processing for these dataset sizes is fast — cold start dominates.
