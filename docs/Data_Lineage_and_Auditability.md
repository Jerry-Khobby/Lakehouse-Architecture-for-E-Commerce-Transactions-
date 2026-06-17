# Data Lineage and Auditability — Tracing Any Row to Its Source

## Overview

Every row in the pipeline has a complete, queryable audit trail. A row committed to the Silver layer Delta table can be traced back to: the specific Delta table version that committed it, the Glue job run that executed the MERGE, the CloudWatch log stream for that run, and the original S3 object key of the CSV file it came from. A row rejected during validation has the same traceability plus an explicit rejection reason. This document explains each audit layer, the data each layer holds, and the step-by-step trace procedure for investigating any specific row.

---

## Audit Layer 1 — Delta Log Version History

Every MERGE operation appends a new version entry to `_delta_log/`. The version number is monotonically increasing and immutable — it is never overwritten or deleted (until Delta `VACUUM` removes log entries beyond the retention window, default 30 days). The log is the lowest-level audit record for the Silver layer.

### Reading History in PySpark

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://<data-bucket>/lakehouse-dwh/orders/")
history = delta_table.history()
history.select(
    "version",
    "timestamp",
    "userName",
    "operation",
    "operationParameters",
    "operationMetrics",
).show(truncate=False)
```

**Example output:**

```
+-------+-------------------+-----------+---------+----------------------------------------------+---------------------------------------------------+
|version|timestamp          |userName   |operation|operationParameters                           |operationMetrics                                   |
+-------+-------------------+-----------+---------+----------------------------------------------+---------------------------------------------------+
|2      |2025-05-31 09:14:22|glue-role  |MERGE    |{predicate -> (order_id = order_id), ...}     |{numSourceRows -> 850, numTargetRowsInserted -> 850,|
|       |                   |           |         |                                              | numTargetRowsUpdated -> 0, numTargetRowsCopied -> 0}|
|1      |2025-04-30 09:22:11|glue-role  |MERGE    |{predicate -> (order_id = order_id), ...}     |{numSourceRows -> 850, numTargetRowsInserted -> 850,|
|       |                   |           |         |                                              | numTargetRowsUpdated -> 0, numTargetRowsCopied -> 0}|
|0      |2025-04-30 09:21:58|glue-role  |WRITE    |{mode -> Overwrite, partitionBy -> ["date"]}  |{numFiles -> 0, numOutputRows -> 0}                |
+-------+-------------------+-----------+---------+----------------------------------------------+---------------------------------------------------+
```

- **Version 0**: Empty seed DataFrame written by `ensure_delta_table()` — zero rows, schema and partition registered
- **Version 1**: April batch MERGE — 850 rows inserted
- **Version 2**: May batch MERGE — 850 rows inserted (new May orders, no April updates)

The `timestamp` field is UTC wall-clock time of the commit. `userName` is the IAM principal that ran the Glue job — `arn:aws:iam::123456789012:role/ecom-lakehouse-dev-glue-role` (or its session alias). `operationMetrics` contains the row counts documented in [Merge_Upsert_Logic.md](Merge_Upsert_Logic.md).

### Identifying Which Version Contains a Specific Row

```python
# At what version was order 'ord-abc-001' first inserted?
v1 = spark.read.format("delta").option("versionAsOf", 1).load(orders_path)
v0 = spark.read.format("delta").option("versionAsOf", 0).load(orders_path)

in_v1_not_v0 = v1.filter("order_id = 'ord-abc-001'").exceptAll(
    v0.filter("order_id = 'ord-abc-001'")
)
in_v1_not_v0.show()
# If it shows a row: this order was first inserted at version 1 (April batch)
# If empty: the order was not in version 1; check version 2, etc.
```

### Delta Log as a Tamper-Evident Record

Delta log entries are S3 objects. S3 object versioning (enabled on the data bucket) means that even if a log entry JSON file were overwritten (which Delta never does intentionally), the original version is preserved in the S3 version history. An S3 `ListObjectVersions` call on the `_delta_log/` prefix reveals any such modification. This makes the Delta log a tamper-evident audit record — modifications to committed log entries are detectable.

---

## Audit Layer 2 — Rejected Record Audit Columns

Every row that fails validation is written to `rejected/<dataset>/<date>/<run_id>/` as a Parquet file with four pipeline-generated audit columns appended. These columns exist only in the rejected records — they are not part of the Silver layer schema.

### The Four Audit Columns

| Column | Type | Content | Example |
|---|---|---|---|
| `rejection_reason` | STRING | Machine-readable rejection code | `"unparseable_timestamp"` |
| `_rejected_at` | TIMESTAMP | UTC time `write_rejected()` executed | `2025-04-30 09:21:45.123` |
| `_job_run_id` | STRING | Glue job run ID | `"jr_abc123def456ghi789"` |
| `_source_key` | STRING | S3 key of the source CSV in `raw/` | `"raw/apr_2025/orders/orders_apr_2025.csv"` |

### Querying Rejected Records in Athena

Register the rejection prefix as an Athena table (or use a Glue crawler):

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS rejected_orders (
    order_num        BIGINT,
    order_id         VARCHAR,
    user_id          VARCHAR,
    order_timestamp  VARCHAR,    -- stored as string (cast failed for unparseable_timestamp rows)
    total_amount     DECIMAL(12, 2),
    date             VARCHAR,
    rejection_reason VARCHAR,
    _rejected_at     TIMESTAMP,
    _job_run_id      VARCHAR,
    _source_key      VARCHAR
)
PARTITIONED BY (rejection_date VARCHAR)
STORED AS PARQUET
LOCATION 's3://<data-bucket>/rejected/orders/';

MSCK REPAIR TABLE rejected_orders;  -- discover partition directories
```

**Most common rejection reasons this run:**
```sql
SELECT
    rejection_reason,
    COUNT(*)           AS count,
    MIN(_rejected_at)  AS first_seen,
    MAX(_rejected_at)  AS last_seen
FROM rejected_orders
WHERE rejection_date = '2025-04-30'
GROUP BY rejection_reason
ORDER BY count DESC;
```

**Find all rejections from a specific job run:**
```sql
SELECT *
FROM rejected_orders
WHERE _job_run_id = 'jr_abc123def456ghi789'
ORDER BY _rejected_at;
```

**Trace a specific order ID to its rejection:**
```sql
SELECT
    order_id,
    order_timestamp,
    rejection_reason,
    _rejected_at,
    _job_run_id,
    _source_key
FROM rejected_orders
WHERE order_id = 'ord-missing-001';
```

If this query returns a row, the order was rejected. The `rejection_reason` says why, `_source_key` identifies the source file, and `_job_run_id` links to the CloudWatch log that shows the full validation output for that run.

---

## Audit Layer 3 — CloudWatch Log Run IDs

Every Glue job execution produces a log stream in CloudWatch Logs under `/aws-glue/jobs/output`. The stream name is the Glue job run ID — the same ID stored in `_job_run_id` on rejected records.

### Finding the Log Stream for a Specific Run

```bash
# Given a job run ID from a rejected record:
JOB_RUN_ID="jr_abc123def456ghi789"
JOB_NAME="ecom-lakehouse-dev-orders-job"

aws logs get-log-events \
  --log-group-name "/aws-glue/jobs/output" \
  --log-stream-name "${JOB_NAME}/${JOB_RUN_ID}" \
  --output text \
  --query 'events[*].message'
```

### Key Log Lines to Look For

**`log_counts` line** — written at the end of the Validate stage, always present:
```
[orders] total_read=850 | valid=848 | rejected=2 | pass_rate=99.76%
```

**Delta MERGE history line** — written after every MERGE by `history(1).show()`:
```
|1|MERGE|{numTargetRowsInserted -> 848, numTargetRowsUpdated -> 0, numTargetRowsCopied -> 0}|
```

**Stage notification lines** — written by `PipelineMonitor` at entry and exit of each stage:
```
[dev] orders_job — STARTED: Validate
[dev] orders_job — SUCCESS: Validate
[dev] orders_job — STARTED: Delta Merge
[dev] orders_job — SUCCESS: Delta Merge
```

**Archive completion line**:
```
Archived ecom-lakehouse-dev-data-123456789012/raw/apr_2025/orders/orders_apr_2025.csv
       → ecom-lakehouse-dev-data-123456789012/archived/orders/apr_2025/orders_apr_2025.csv
```

### Logs Insights Query for Rejection Summary Across Runs

```
fields @timestamp, @message
| filter @logStream like /ecom-lakehouse-dev-orders-job/
| filter @message like /total_read=/
| parse @message "total_read=* | valid=* | rejected=* | pass_rate=*%" as total, valid, rejected, pass_rate
| sort @timestamp desc
| limit 20
```

This query surfaces the `log_counts` line from the last 20 orders job runs, showing the pass rate trend over time. A sudden drop in `pass_rate` (e.g. 99% → 0%) immediately flags a source format change or pipeline bug.

---

## Audit Layer 4 — Step Functions Execution History

Each Step Functions execution records every state transition with timestamps, inputs, outputs, and error details. The execution ARN links the pipeline-level result back to the individual Glue job runs.

### Reading Execution History

```bash
EXECUTION_ARN="arn:aws:states:eu-west-1:123456789012:execution:ecom-lakehouse-dev-pipeline:apr_2025-20250430T092211"

aws stepfunctions get-execution-history \
  --execution-arn "$EXECUTION_ARN" \
  --query 'events[?type==`TaskStateExited`].{state: stateExitedEventDetails.name, output: stateExitedEventDetails.output}' \
  --output table
```

The `TaskStateExited` events for `ProcessProducts`, `ProcessOrders`, `ProcessOrderItems` each contain the Glue `JobRunId` in their output. This links the Step Functions execution to the specific Glue job runs — and from those run IDs, to the CloudWatch log streams, to the rejected record `_job_run_id` values.

---

## End-to-End Row Trace Procedure

### Scenario: "What happened to order `ord-missing-001`?"

**Step 1 — Check the Silver layer:**

```sql
-- Is the order in the Delta table?
SELECT order_id, order_timestamp, total_amount, date
FROM ecom_lakehouse_dev.orders
WHERE order_id = 'ord-missing-001';
```

If this returns a row: the order was committed successfully. Note the `date` partition and `order_timestamp` to identify which batch it came from.

If empty: proceed to Step 2.

**Step 2 — Check the rejection table:**

```sql
SELECT
    order_id,
    order_timestamp,    -- the raw string value before casting
    total_amount,
    rejection_reason,
    _rejected_at,
    _job_run_id,
    _source_key
FROM rejected_orders
WHERE order_id = 'ord-missing-001';
```

If this returns a row: the order was rejected. `rejection_reason` explains why. `_source_key` is the S3 key of the source file. `_job_run_id` is the Glue run to investigate.

**Step 3 — Read the source file:**

```bash
SOURCE_KEY="raw/apr_2025/orders/orders_apr_2025.csv"
# After archival, the raw/ key is moved to archived/:
SOURCE_KEY="archived/orders/apr_2025/orders_apr_2025.csv"

aws s3 cp "s3://<data-bucket>/${SOURCE_KEY}" - | grep "ord-missing-001"
```

The original row from the CSV is visible. Compare the `order_timestamp` field against `TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"`. If the timestamp uses a space separator instead of `T`, the root cause is immediately visible.

**Step 4 — Inspect the CloudWatch log for the rejection run:**

```bash
JOB_RUN_ID="jr_abc123def456ghi789"

aws logs get-log-events \
  --log-group-name "/aws-glue/jobs/output" \
  --log-stream-name "ecom-lakehouse-dev-orders-job/${JOB_RUN_ID}" \
  --query 'events[*].message' \
  --output text | grep -E "total_read|rejected|unparseable"
```

Expected output for the timestamp bug:
```
[orders] total_read=850 | valid=0 | rejected=850 | pass_rate=0.00%
```

**Step 5 — Confirm the Delta table was unaffected:**

```python
delta_table = DeltaTable.forPath(spark, orders_path)
delta_table.history(3).select("version", "timestamp", "operationMetrics").show(truncate=False)
```

If the MERGE for this run produced `numTargetRowsInserted = 0`, no rows from the failing batch were committed to the Silver layer. The table is unchanged from the previous version.

**Step 6 — Fix and re-run:**

After fixing the source format issue (correcting `TIMESTAMP_FMT` in `constants.py`):

```bash
python ingestion/ingest.py
```

The same source file is re-uploaded (overwriting the same `raw/` key), a new Step Functions execution starts, and the MERGE commits the rows that were previously rejected. Because `ensure_delta_table()` finds the Delta table already initialised and the MERGE key (`order_id`) for the re-run rows does not yet exist in the table, all previously-rejected rows are inserted as new rows via `whenNotMatchedInsertAll`.

---

## Lineage Summary — All Audit Anchors for One Row

| Question | Where to Look | Key Field |
|---|---|---|
| Is the row in the Silver layer? | `ecom_lakehouse_dev.orders` Delta table | `order_id` |
| Which Delta version committed it? | `delta_table.history()` or `FOR VERSION AS OF` time travel | `version`, `timestamp` |
| Which Glue run executed the MERGE? | CloudWatch `/aws-glue/jobs/output` | `_job_run_id` (from rejected table) or Step Functions execution history |
| Why was the row rejected (if absent)? | `rejected_orders` Athena table | `rejection_reason`, `_job_run_id`, `_source_key` |
| What did the source row look like? | S3 object at `_source_key` (in `archived/` after processing) | `_source_key` |
| What was the pipeline state when the row was processed? | CloudWatch log stream for `_job_run_id` | `log_counts`, stage notifications, MERGE metrics |
| Was the entire batch clean? | Step Functions execution history | Execution ARN → state exit events → Glue job run IDs |
