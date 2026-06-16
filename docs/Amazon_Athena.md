# Amazon Athena — Workgroup Configuration and Delta Lake Integration

## Overview

Amazon Athena is the serverless query engine in this architecture. It sits at the Gold layer of the Medallion model — it does not store data, move data, or transform it. It reads the Delta Lake tables directly from S3 via the Glue Data Catalog and returns results to analysts or to the Step Functions `AthenaValidation` smoke-test gate. This document covers the workgroup configuration, what `enforce_workgroup_configuration` actually enforces, how Athena physically reads a Delta table, the Lake Formation permission model required for it to work, and the analytical queries the pipeline supports.

---

## The Workgroup — `ecom-lakehouse-wg`

All Athena queries in this project run under a single named workgroup. The workgroup is the unit of configuration, access control, and cost governance in Athena.

```hcl
resource "aws_athena_workgroup" "main" {
  name        = var.athena_workgroup_name   # "ecom-lakehouse-wg"
  description = "Workgroup for ecom lakehouse queries"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff  # 1073741824 (1 GB)

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
}
```

### `enforce_workgroup_configuration = true`

This is the most operationally important setting in the workgroup. When set to `true`, Athena **ignores** any configuration an API client or query editor submits and applies the workgroup configuration instead.

In practice this means:

| Client attempts to set | With `enforce = false` | With `enforce = true` |
|---|---|---|
| Custom output location (`OutputLocation`) | Accepted — results go to client-specified bucket | Rejected — results always go to `s3://…/query-results/` |
| No encryption on results | Accepted | Overridden — SSE-S3 applied regardless |
| Bytes scanned override | Accepted | Overridden — 1 GB cutoff applies always |

**Why this matters:** Without enforcement, an analyst using the Athena console could accidentally (or deliberately) redirect query results to a personal S3 bucket that has no encryption or expiry policy. They could also bypass the scan limit by supplying their own workgroup settings, causing a full-table scan on a large Delta partition and generating an unexpectedly large bill.

With `enforce_workgroup_configuration = true`, the workgroup becomes the authority. A developer can trust that every query — from the Step Functions pipeline, from the Athena console, from a boto3 script — produces encrypted results in the designated bucket subject to the 1 GB scan cap. No per-client configuration can override this.

### `bytes_scanned_cutoff_per_query = 1073741824` (1 GB)

Athena charges per terabyte of data scanned. This setting cancels any query that would scan more than 1 GB before it completes.

For this e-commerce dataset (hundreds of thousands of rows, a few GB total), the 1 GB limit is well above normal analytical queries. It is a guard against accidental full-table cartesian joins or `SELECT *` without partition pruning. A query that would cost $5 is cancelled before it starts, and the analyst receives an error instead of a bill.

The cutoff fires **before the scan begins** — Athena estimates the bytes to be scanned from the partition metadata in the Delta log and Data Catalog, and cancels the query if the estimate exceeds the limit. It does not wait until 1 GB has been read.

**Athena pricing note:** Parquet column projection means Athena reads only the columns referenced in the `SELECT` and the predicate. A query like `SELECT order_id, total_amount FROM orders WHERE date = '2025-04-15'` does not scan the entire `orders` table — it reads only the `date=2025-04-15/` partition and only the two referenced columns from the Parquet row groups. This makes the 1 GB cutoff generous for well-structured analytical queries but correctly protective against unfocused ones.

### Output Location and Results Expiry

All query results land at:
```
s3://ecom-lakehouse-dev-athena-results-<account>/query-results/
```

Athena writes two files per query execution:
- `<QueryExecutionId>.csv` — the result rows
- `<QueryExecutionId>.csv.metadata` — execution metadata (column names, types, row count)

Both files are encrypted with SSE-S3. The `enforce_workgroup_configuration = true` setting ensures no query result ever lands anywhere other than this prefix.

The Athena results bucket has a lifecycle rule that expires all objects after 7 days:
```hcl
rule {
  id     = "expire-athena-results"
  status = "Enabled"
  filter { prefix = "query-results/" }
  expiration { days = 7 }
}
```

Athena results are transient by design. An analyst who needs a result beyond 7 days downloads it or integrates directly with a BI tool. Retaining stale result files indefinitely accumulates cost and creates a data governance problem (old result snapshots may reflect outdated data, and there is no mechanism to invalidate them when the source Delta tables are updated).

### `publish_cloudwatch_metrics_enabled = true`

Athena publishes per-workgroup metrics to CloudWatch:
- `ProcessedBytes` — bytes scanned per query
- `QueryPlanningTime`, `QueryQueueTime`, `QueryExecutionTime` — latency breakdown
- `EngineExecutionTime` — actual Athena engine time vs. queue wait

These metrics appear in CloudWatch under the `AWS/Athena` namespace with dimension `WorkGroup = ecom-lakehouse-wg`. A CloudWatch alarm on `ProcessedBytes` approaching the cutoff, or on `QueryExecutionTime` exceeding a threshold, provides early warning before the pipeline's `AthenaValidation` state starts failing.

---

## Athena Engine Version 3 — Why It Is Required

```hcl
engine_version {
  selected_engine_version = "Athena engine version 3"
}
```

Athena engine version 3 (Trino 422) includes native Delta Lake table format support. Without it, Athena cannot read the `_delta_log/` transaction log and treats the Delta table directory as an unstructured collection of Parquet files — resulting in reading **all** Parquet files ever written to the table, including those superseded by MERGE operations, not just the files that belong to the current snapshot.

The consequence of running on engine version 2 with a Delta table:
- A MERGE that updated 1,000 rows writes a new Parquet file with the updated rows and marks the old files as removed in `_delta_log/`. Engine version 2 reads both — it sees the original rows and the updated rows, producing duplicates.
- There is no error message. The query silently returns wrong row counts and duplicate records.

Engine version 3 reads `_delta_log/*.json` first to determine the current snapshot — the exact set of Parquet files that belong to the latest committed transaction — and reads only those files. Superseded Parquet files are invisible to the query.

**Important:** Engine version 3 must be selected at workgroup creation time. Downgrading a workgroup from version 3 to version 2 after Delta tables have been queried against it is not advisable — catalog registrations done against version 3 may behave incorrectly under version 2.

---

## How Athena Reads a Delta Table — Step by Step

Understanding this read path explains why catalog registration, Lake Formation permissions, and engine version are all prerequisites for a working Athena query.

### Step 1 — Catalog Resolution

When a query references `ecom_lakehouse_db.orders`, Athena calls the Glue Data Catalog API to retrieve the table's metadata. The catalog entry for `orders` was created by the Glue job's `update_catalog_table()` call:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{database}`.`{table_name}`
    USING DELTA
    LOCATION '{delta_path}'
""")
```

The catalog entry contains:
- `LOCATION`: `s3://ecom-lakehouse-dev-data-<account>/lakehouse-dwh/orders/`
- `InputFormat`: `org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat` (Delta's Glue-compatible format)
- `SerDe`: Parquet SerDe

Athena reads this catalog entry to know where the table lives on S3. Without the catalog entry, `ecom_lakehouse_db.orders` resolves to nothing and the query fails with `TABLE_NOT_FOUND`.

### Step 2 — Delta Log Snapshot Resolution

Athena engine version 3 navigates to:
```
s3://ecom-lakehouse-dev-data-<account>/lakehouse-dwh/orders/_delta_log/
```

It reads the JSON log files in ascending order to reconstruct the current snapshot. Each log file records one atomic transaction — which files were added, which were removed. After reading all log files, Athena has an exact manifest: the set of Parquet file paths that constitute the current state of the table.

For the orders table after two ingestion batches:
```
_delta_log/
├── 00000000000000000000.json  ← empty table seed (ensure_delta_table)
├── 00000000000000000001.json  ← April MERGE — added 850 files, removed 0
└── 00000000000000000002.json  ← May MERGE — added N new/updated files, removed M superseded files
```

Athena reads log entry `00000000000000000002.json` and sees which files were added and which were removed by the May MERGE. It constructs the union: all files added across all transactions minus all files marked as removed. This is the current snapshot.

Log compaction (checkpoints) can accelerate this step — Delta writes a `_last_checkpoint` file pointing to a `.parquet` checkpoint file that contains the full snapshot at a given version. Athena reads the checkpoint instead of replaying every individual JSON transaction. This pipeline does not explicitly trigger checkpoint creation; Delta writes checkpoints automatically every 10 transactions.

### Step 3 — Partition Pruning

With the current file manifest from the Delta log, Athena applies partition pruning. The `orders` table is partitioned by `date`. A query with `WHERE date = '2025-04-15'` causes Athena to filter the manifest to only files under the `date=2025-04-15/` prefix. Files from all other date partitions are excluded from the S3 read entirely — they are never fetched.

This is why partition columns matter for analytical query performance. Without the `date` partition on `orders`, a query for a single day's orders would scan all Parquet files across all months ever ingested. With the partition, it scans only one directory.

### Step 4 — Column Projection and Predicate Pushdown

Athena reads only the Parquet row groups and columns that satisfy the query. Parquet's columnar format stores each column's data contiguously — Athena reads the `order_id` and `total_amount` byte ranges from the Parquet file without reading the `customer_id`, `shipping_address`, `payment_method`, or other columns.

Row group statistics (min/max values stored in Parquet footer metadata) allow Athena to skip entire row groups when the query predicate cannot match any value in that group. For example, `WHERE total_amount > 1000000` uses the `total_amount` row group max statistic to skip any row group where `max(total_amount) <= 1000000`.

### Step 5 — Result Materialisation

Athena writes the result rows to `s3://…/query-results/<QueryExecutionId>.csv` as the query completes. The result is immediately downloadable from the Athena console or retrievable via `athena:GetQueryResults`.

---

## Lake Formation Permissions

The Glue Data Catalog in this project is governed by AWS Lake Formation. Lake Formation adds a permission layer on top of IAM — a principal must satisfy **both** IAM and Lake Formation permissions to access a catalog resource. Having an IAM policy that grants `glue:GetTable` is not sufficient if Lake Formation has not granted `DESCRIBE` on the table.

### Why Lake Formation Is Enabled Here

Lake Formation is enabled because the project uses `aws_lakeformation_data_lake_settings` to register the Terraform caller as a Lake Formation admin. Once LF is enabled for a Glue catalog, all catalog access (by Athena, Glue jobs, and other services) requires explicit LF grants. This is intentional — it enforces column- and row-level access control if needed, and provides a single place to audit who can read which table.

Without the LF grants below, Athena would return `ACCESS_DENIED` even though the IAM policies on the SFN role include `athena:StartQueryExecution`.

### Permissions on the Step Functions Role

The `AthenaValidation` state runs queries under the Step Functions execution role (`sfn_role`). This role needs two tiers of LF permissions:

**Database-level DESCRIBE:**
```hcl
resource "aws_lakeformation_permissions" "sfn_database" {
  principal   = aws_iam_role.sfn_role.arn
  permissions = ["DESCRIBE"]

  database {
    name = var.glue_database_name  # "ecom_lakehouse_db"
  }
}
```

`DESCRIBE` on the database allows Athena to resolve `ecom_lakehouse_db` to its underlying Glue database record and enumerate table names. Without it, `ecom_lakehouse_db.orders` cannot even be looked up — Athena cannot confirm the database exists.

**Table-level SELECT and DESCRIBE (wildcard):**
```hcl
resource "aws_lakeformation_permissions" "sfn_tables" {
  principal   = aws_iam_role.sfn_role.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = var.glue_database_name
    wildcard      = true
  }
}
```

`wildcard = true` applies `SELECT` and `DESCRIBE` to all current and future tables in `ecom_lakehouse_db`.

- `DESCRIBE`: Allows reading the table schema (column names, types, partition keys). Athena needs this to parse the query and plan the execution.
- `SELECT`: Allows Athena to read rows from the table — which translates into reading Parquet files from `lakehouse-dwh/<table>/` on S3.

The wildcard grant is appropriate here because `sfn_role` only runs the `AthenaValidation` smoke-test — it has no write permissions anywhere. Granting SELECT on all tables means adding a new dataset to the pipeline (e.g. `returns`) does not require a Terraform change to the LF grants.

**S3 access is still governed by IAM.** Lake Formation permissions are satisfied by the LF grants above. After LF clears the request, IAM must also permit `s3:GetObject` on `lakehouse-dwh/*`. The `sfn_role` has this via its IAM policy. Both checks must pass independently — LF does not substitute for IAM on S3.

---

## The `AthenaValidation` State — Pipeline Smoke Test

The Step Functions state machine runs a validation Athena query as the final step before sending a success notification:

```
RunOrderItemsJob → AthenaValidation → NotifySuccess
                        │
                    (on error)
                        │
                   NotifyFailure → PipelineFailed
```

The query executed by this state:
```sql
SELECT 'products'    AS tbl, COUNT(*) AS row_count FROM ecom_lakehouse_db.products
UNION ALL
SELECT 'orders',            COUNT(*)               FROM ecom_lakehouse_db.orders
UNION ALL
SELECT 'order_items',       COUNT(*)               FROM ecom_lakehouse_db.order_items;
```

This is a smoke test, not a correctness test. It answers one binary question: **are all three tables reachable and non-empty?** If any of the three Glue jobs silently failed to register its catalog table, or if the Delta log is corrupt, or if Lake Formation permissions are missing, this query fails and the pipeline routes to `NotifyFailure` instead of `NotifySuccess`.

Without this gate, the pipeline could send a `✅ batch completed` SNS notification even when the Delta tables are unreachable by analysts — because the Glue jobs and Step Functions states all succeeded, but the catalog registration step at the end of each job silently produced a malformed entry.

The `AthenaValidation` task state in `step_functions.tf` uses the `arn:aws:states:::athena:startQueryExecution.sync` integration — Step Functions calls `StartQueryExecution` and then polls `GetQueryExecution` until the query reaches `SUCCEEDED`, `FAILED`, or `CANCELLED`. The task fails if Athena returns any terminal error, which is then caught by:

```hcl
Catch = [{
  ErrorEquals = ["States.ALL"]
  Next        = "NotifyFailure"
  ResultPath  = "$.error"
}]
```

The query result (the three-row `tbl / row_count` table) is written to `$.results.athena` via `ResultPath = "$.results.athena"`. This preserves the original input (`$.bucket`, `$.batch`, `$.files`) for `NotifySuccess` to use in its `States.Format` message.

---

## Analytical Queries the Pipeline Supports

These are representative queries an analyst runs against the pipeline's Gold layer. All assume the workgroup `ecom-lakehouse-wg` is selected in the Athena console.

### Monthly Revenue

```sql
SELECT
    SUBSTR(CAST(order_date AS VARCHAR), 1, 7) AS month,
    SUM(total_amount)                          AS revenue,
    COUNT(DISTINCT order_id)                   AS orders
FROM ecom_lakehouse_db.orders
WHERE status = 'completed'
GROUP BY 1
ORDER BY 1;
```

Partition pruning applies if a specific month is filtered with `WHERE date BETWEEN '2025-04-01' AND '2025-04-30'` — Athena reads only the date partitions in that range.

### Top Products by Revenue

```sql
SELECT
    p.name,
    p.department,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    SUM(oi.quantity)                  AS units_sold
FROM ecom_lakehouse_db.order_items oi
JOIN ecom_lakehouse_db.products     p  ON oi.product_id = p.product_id
JOIN ecom_lakehouse_db.orders       o  ON oi.order_id   = o.order_id
WHERE o.status = 'completed'
GROUP BY p.name, p.department
ORDER BY total_revenue DESC
LIMIT 20;
```

This is a three-table join executed entirely in Athena — no ETL step required. Athena reads the Delta snapshots for all three tables, applies partition pruning on `orders` if a date range is supplied, and returns results.

### Data Quality Audit — Rejection Reasons

```sql
SELECT
    rejection_reason,
    _source_key,
    COUNT(*)          AS rejected_rows,
    MIN(_rejected_at) AS first_seen,
    MAX(_rejected_at) AS last_seen
FROM "s3://ecom-lakehouse-dev-data-<account>/rejected/orders/"
GROUP BY rejection_reason, _source_key
ORDER BY rejected_rows DESC;
```

The `rejected/` prefix stores Parquet files with a consistent schema (original columns + four audit columns). Athena can query them directly by specifying the S3 path without a Data Catalog entry — Athena infers the schema from the Parquet metadata.

This query was the diagnostic tool used to confirm that all May 2025 data was rejected as `invalid_timestamp_format` (450 orders × 850 rows = 0 valid rows after the timestamp check failed for every row due to the format mismatch between `TIMESTAMP_FMT` in `constants.py` and `TIMESTAMP_FORMAT` in `orders_job.py`).

### Flagged High-Value Orders

```sql
SELECT *
FROM "s3://ecom-lakehouse-dev-data-<account>/flagged/orders/"
ORDER BY total_amount DESC;
```

Same direct-S3 pattern. Flagged Parquet files include a `flag_reason` column (`high_value_order`) appended by the Glue job before writing to `flagged/`. Analysts use this to review and approve or escalate unusually large orders.

### Idempotency Verification — Row Count Stability

After re-running an ingestion batch (e.g. regenerating and re-ingesting May 2025 after the timestamp fix), this query confirms the MERGE was truly idempotent:

```sql
SELECT COUNT(*) AS total_orders FROM ecom_lakehouse_db.orders;
```

Run this before and after the second ingestion. If the counts are equal, the MERGE upserted existing rows rather than inserting duplicates. If the count grew, there is a MERGE key collision or the idempotency guard failed.

For the MERGE to be idempotent, two conditions must hold:
1. The MERGE condition matches on the correct key (`order_id` for orders, `(id, order_id)` for order_items, `product_id` for products).
2. The timestamp guard `source.order_timestamp > target.order_timestamp` prevents an older re-delivered file from overwriting a more recent committed row. A re-ingested file with identical timestamps performs `whenMatchedUpdate` but the condition evaluates false — no update is written, and no new Delta log entry is produced for that row.

### Partition Inspection

```sql
SHOW PARTITIONS ecom_lakehouse_db.orders;
```

Returns all `date=` partition values registered in the catalog. After the April batch, this lists dates from 2025-04-01 to 2025-04-30. After the May batch, dates from 2025-05-01 to 2025-05-31 are appended. This is a fast way to confirm that a new batch's data reached the catalog without running a full COUNT query.

---

## Common Failure Modes and Diagnostics

### `TABLE_NOT_FOUND: ecom_lakehouse_db.orders`

**Cause:** The Glue job's `update_catalog_table()` call failed silently, or the Glue job failed before reaching the catalog step. The `orders` Delta table exists on S3 but is not registered in the Data Catalog.

**Fix:** Run `spark.sql("CREATE TABLE IF NOT EXISTS ...")` manually via a Glue interactive session, or re-run the full pipeline to allow the Glue job to register the table. The `AthenaValidation` state would have caught this if the pipeline reached it — but if the Glue job itself failed before `update_catalog_table()`, `AthenaValidation` was never reached.

### `ACCESS_DENIED` from Athena

**Cause:** Either IAM or Lake Formation permissions are missing for the querying principal. Most common cause: a new IAM role was used to query Athena without corresponding `aws_lakeformation_permissions` grants being applied.

**Diagnostic:** Check Lake Formation → Data permissions for the role ARN. If the `DESCRIBE` or `SELECT` grant is absent, apply the Terraform `aws_lakeformation_permissions` resource for the new role.

### Query Returns Duplicate Rows

**Cause:** Athena engine version is not version 3. The workgroup was configured with an older engine version that reads all Parquet files rather than the Delta snapshot manifest.

**Fix:** Confirm `selected_engine_version = "Athena engine version 3"` in the workgroup configuration. If the engine was recently upgraded, re-run the query — existing `QueryExecutionId` results are cached and will not automatically reflect the engine change.

### `QUERY_EXECUTION_FAILED: Query exhausted resources`

**Cause:** The `bytes_scanned_cutoff_per_query` was exceeded. The query was attempting to scan more than 1 GB.

**Fix:** Add partition filters (`WHERE date = '2025-05-01'`) or column projections (`SELECT order_id, total_amount` instead of `SELECT *`) to reduce scan volume. If the query legitimately needs more than 1 GB, the cutoff can be raised in the workgroup Terraform variable `var.athena_bytes_scanned_cutoff`.
