# CloudWatch Monitoring — Log Groups, Metrics, and Debugging Guide

## Overview

Amazon CloudWatch is the observability layer for this pipeline. It collects three distinct types of signals: structured logs from Glue ETL jobs (driver output, stage markers, rejection summaries), execution-level logs from Step Functions (every state transition with full input and output), and Lambda invocation logs from the Slack notifier. This document covers every log group, how data reaches CloudWatch, retention policies, the Glue metrics and insights features, and a practical debugging guide for the most common failure patterns.

---

## Log Groups

### Glue Jobs — `/aws-glue/jobs/ecom-lakehouse-dev`

```hcl
resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws-glue/jobs/${local.name_prefix}"
  retention_in_days = 30
}
```

This log group receives the driver output from all three Glue jobs. Every `logger.info()`, `logger.warning()`, and `logger.exception()` call in `common.py`, `monitor.py`, `orders_job.py`, `products_job.py`, and `order_items_job.py` flows here.

**How Glue logs reach CloudWatch:**

Glue's `--enable-continuous-cloudwatch-log` argument (set in `common_glue_args` in `glue_jobs.tf`) enables real-time streaming of driver stdout to CloudWatch. Without this flag, logs are written to a file on the Glue driver and only made available after the job completes. With it, each log line appears in CloudWatch within seconds of being written — meaning an operator can tail the log group while the job is running and see each stage check in as it happens.

The log stream names follow the Glue job run ID pattern:
```
/aws-glue/jobs/ecom-lakehouse-dev
  └── jr_abc1234567890abcdef0  (one stream per job run)
```

**Spark UI logs** are separate and go to S3 rather than CloudWatch:

```hcl
"--enable-spark-ui"        = "true"
"--spark-event-logs-path"  = "s3://${aws_s3_bucket.logs.id}/spark-ui-logs/"
```

Spark UI events (stage DAGs, executor metrics, shuffle statistics) are large and structured for the Spark History Server UI, not for text scanning. Putting them in S3 keeps the CloudWatch log group clean and avoids CloudWatch Logs ingestion costs for high-volume Spark internal events.

**30-day retention:** Glue job logs are read when debugging a specific failed or unexpected run. After 30 days, a run's logs are irrelevant — the corresponding source file has been archived, the Delta MERGE is committed or rolled back, and any data quality issues have been investigated. Retaining beyond 30 days accumulates significant log volume (each Spark stage emits multiple log lines across hundreds of tasks) with no operational value.

### Step Functions — `/aws/states/ecom-lakehouse-dev-etl-pipeline`

```hcl
resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${local.name_prefix}-etl-pipeline"
  retention_in_days = 30
}
```

This log group receives execution-level event logs from the Step Functions state machine, configured in `step_functions.tf`:

```hcl
logging_configuration {
  log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
  include_execution_data = true
  level                  = "ALL"
}
```

**`level = "ALL"`** logs every event type: `ExecutionStarted`, `StateEntered`, `StateExited`, `TaskStateEntered`, `TaskStateExited`, `TaskScheduled`, `TaskStarted`, `TaskSucceeded`, `TaskFailed`, `ExecutionSucceeded`, `ExecutionFailed`. For a pipeline where the root cause of a failure might be a specific argument passed to a Glue job or a specific input path that resolved incorrectly, seeing the full state machine trace is essential.

**`include_execution_data = true`** embeds the execution state data (the `$` object) in each log event. A `StateEntered` log event with `include_execution_data` looks like:

```json
{
  "type": "TaskStateEntered",
  "id": "2",
  "previousEventId": "1",
  "stateEnteredEventDetails": {
    "name": "RunOrdersJob",
    "input": {
      "bucket": "ecom-lakehouse-dev-data-123456789012",
      "batch": "may_2025",
      "files": {
        "products": "raw/products.csv",
        "orders": "raw/orders_may_2025.csv",
        "order_items": "raw/order_items_may_2025.csv"
      }
    }
  }
}
```

Without `include_execution_data`, the `input` field is absent — you see that `RunOrdersJob` was entered but not what arguments it received. Diagnosing a wrong S3 key or missing file would require cross-referencing the ingestion script's output manually.

**The log resource policy** allows Step Functions to write to this log group:

```hcl
resource "aws_cloudwatch_log_resource_policy" "sfn" {
  policy_document = jsonencode({
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = ["logs:CreateLogStream", "logs:PutLogEvents"]
      Resource  = "${aws_cloudwatch_log_group.sfn.arn}:*"
    }]
  })
}
```

This is a resource-based policy on the log group itself (not an IAM policy on a role). Step Functions uses its own internal service principal to write logs, separate from the `sfn_role` IAM role. Without this resource policy, Step Functions cannot create log streams and `logging_configuration` is silently ineffective — no error is shown, but no logs appear.

### Lambda Slack Notifier — `/aws/lambda/ecom-lakehouse-dev-slack-notifier`

```hcl
resource "aws_cloudwatch_log_group" "slack_notifier" {
  count             = local.slack_enabled
  name              = "/aws/lambda/${aws_lambda_function.slack_notifier[0].function_name}"
  retention_in_days = 14
}
```

Lambda writes its standard invocation lines (`START`, `END`, `REPORT`) and any `print()` output here. The 14-day retention is shorter than Glue and Step Functions because Lambda logs for this function are only relevant during the immediate debugging window for a Slack delivery failure. After two weeks, historical Lambda invocation records serve no purpose.

---

## Glue Metrics and Observability Features

Beyond logs, three Glue arguments emit additional observability signals:

### `--enable-metrics`

```hcl
"--enable-metrics" = "true"
```

Enables Glue's CloudWatch metrics emission. Glue publishes metrics to the `Glue` namespace in CloudWatch under the job name dimension. Key metrics:

| Metric | What it measures | When to watch it |
|---|---|---|
| `glue.driver.jvm.heap.used` | JVM heap usage on the driver | Rising steadily → driver OOM risk |
| `glue.driver.BlockManager.memory.remainingMem_MB` | Free memory for caching/shuffle | Near zero → spill to disk, slowdown |
| `glue.driver.aggregate.numCompletedTasks` | Tasks finished | Stuck counter → task hanging |
| `glue.driver.aggregate.numFailedTasks` | Failed tasks | Non-zero → Spark-level failures |
| `glue.ALL.s3.filesystem.read_bytes` | Bytes read from S3 | Validates expected data volume |
| `glue.ALL.s3.filesystem.write_bytes` | Bytes written to S3 | Validates Delta MERGE output size |

These metrics are visible in CloudWatch under **Metrics → Glue → Job Metrics**. The job name dimension is the Glue job name (`ecom-lakehouse-dev-orders-etl`). A CloudWatch dashboard can display all three job metrics side by side for a pipeline run comparison.

### `--enable-continuous-cloudwatch-log`

Covered above — enables real-time log streaming from the driver. Also enables streaming of driver error output (Python exceptions, Java stack traces) rather than buffering it until job completion.

### `--enable-job-insights`

```hcl
"--enable-job-insights" = "true"
```

Glue Job Insights is an ML-based anomaly detection feature. After a job has run multiple times, Insights learns the expected duration and memory usage profile. If a run is significantly slower or uses significantly more memory than its historical baseline, Insights flags it in the Glue console and optionally publishes a CloudWatch Events notification.

For a monthly pipeline, Insights needs several runs to establish a baseline — it is most useful after 3–5 runs have completed. On first runs, no baseline exists and Insights has nothing to compare against.

---

## What the Logs Look Like During a Normal Run

The Glue driver log stream for `orders_job.py` produces a structured sequence:

```
2026-06-15T13:43:12 [INFO] lakehouse.common — Spark session ready. Delta extensions: io.delta.sql.DeltaSparkSessionExtension
2026-06-15T13:43:12 [INFO] lakehouse.common — Job args parsed | dataset=orders | raw_key=raw/orders_may_2025.csv | environment=dev
2026-06-15T13:43:12 [INFO] ─────────────────────────────────────────────────────────
2026-06-15T13:43:12 [INFO]   [START] Read | job=ecom-lakehouse-dev-orders-etl
2026-06-15T13:43:12 [INFO] ─────────────────────────────────────────────────────────
2026-06-15T13:43:12 [INFO] lakehouse.common — Reading orders CSV from s3://ecom-lakehouse-dev-data-123456789012/raw/orders_may_2025.csv
2026-06-15T13:43:13 [INFO] lakehouse.common — Read 850 raw rows from s3://.../raw/orders_may_2025.csv
2026-06-15T13:43:13 [INFO]   [SUCCESS] Read — 1.2s | rows=850 | job=ecom-lakehouse-dev-orders-etl
2026-06-15T13:43:13 [INFO] ─────────────────────────────────────────────────────────
2026-06-15T13:43:13 [INFO]   [START] Validate | job=ecom-lakehouse-dev-orders-etl
...
2026-06-15T13:43:26 [INFO]   [SUCCESS] Validate — 12.4s | read=850 | valid=849 | rejected=1 | job=ecom-lakehouse-dev-orders-etl
2026-06-15T13:43:26 [INFO] lakehouse.common — total_read=850 | valid=849 | rejected=1 | pass_rate=99.9%
...
2026-06-15T13:44:01 [INFO]   [SUCCESS] Delta Merge — 34.1s | merged=849 | job=ecom-lakehouse-dev-orders-etl
...
2026-06-15T13:44:03 [INFO]   [SUCCESS] Catalog Update — 2.1s | table=ecom_lakehouse_db.orders | job=ecom-lakehouse-dev-orders-etl
...
2026-06-15T13:44:04 [INFO]   [SUCCESS] Archive — 1.0s | job=ecom-lakehouse-dev-orders-etl
2026-06-15T13:44:04 [INFO] ════════════════════════════════════════════════════════
2026-06-15T13:44:04 [INFO]   ecom-lakehouse-dev-orders-etl — all stages complete
2026-06-15T13:44:04 [INFO] ─────────────────────────────────────────────────────────
2026-06-15T13:44:04 [INFO]     Read                                            1.2s
2026-06-15T13:44:04 [INFO]     Validate                                       12.4s
2026-06-15T13:44:04 [INFO]     Delta Merge                                    34.1s
2026-06-15T13:44:04 [INFO]     Catalog Update                                  2.1s
2026-06-15T13:44:04 [INFO]     Archive                                         1.0s
2026-06-15T13:44:04 [INFO] ─────────────────────────────────────────────────────────
2026-06-15T13:44:04 [INFO]     Total                                          50.8s
2026-06-15T13:44:04 [INFO] ════════════════════════════════════════════════════════
```

The `SECTION_LINE` and `SUMMARY_LINE` separators from `monitor.py` make it easy to identify stage boundaries when scanning a long log stream in the CloudWatch console.

---

## Debugging Guide — Common Failure Patterns

### All rows rejected — `valid=0, rejected=<total>`

**Log to look for:**

```
[INFO] total_read=850 | valid=0 | rejected=850 | pass_rate=0.0%
[WARNING] All rows in raw/orders_may_2025.csv were rejected. No Delta merge.
```

**How to diagnose:**

Query the rejected Parquet files in Athena:

```sql
SELECT rejection_reason, COUNT(*) AS count
FROM "s3://ecom-lakehouse-dev-data-<account>/rejected/orders/2026-06-15/<run_id>/"
GROUP BY rejection_reason
ORDER BY count DESC;
```

The `rejection_reason` column names the exact check that failed. The most common cause is `invalid_timestamp_format` — this happened with the May 2025 data when `TIMESTAMP_FMT` in `constants.py` used a space separator while `TIMESTAMP_FORMAT` in `orders_job.py` expected a `T` separator. Every row failed the timestamp cast and was rejected.

**Fix path:** Correct the source data or the format constant, delete the stale CSV from `raw/`, regenerate and re-upload, then re-run the pipeline.

---

### `Delta Lake extensions not loaded`

**Log to look for:**

```
[ERROR] RuntimeError: Delta Lake extensions not loaded.
        Check --conf spark.sql.extensions in Glue job default_arguments.
```

**Cause:** The `build_spark_session()` guard in `common.py` checks `spark.conf.get("spark.sql.extensions", "")` and raises if `DeltaSparkSessionExtension` is not present. This fires when:

1. The `--conf` chain in `glue_jobs.tf` is malformed — a typo in the extension class name, or the chain was split across multiple `--conf` keys (only one is allowed in a Terraform map).
2. `--datalake-formats delta` is absent — the Delta JARs are not on the classpath so the extension cannot load even if `--conf` is correct.
3. The Glue job was triggered manually with custom arguments that overrode `spark.sql.extensions` to an empty string.

**Fix path:** Check the `--conf` value in the Glue job's `Default arguments` in the AWS console. It should be one long string:

```
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore --conf spark.sql.warehouse.dir=s3://<data-bucket>/glue-warehouse/
```

---

### `IllegalArgumentException: Can not create a Path from an empty string`

**Log to look for:**

```
[ERROR] Exception in thread "main" java.lang.IllegalArgumentException:
        Can not create a Path from an empty string
```

**Cause:** The Glue Data Catalog database (`ecom_lakehouse_db`) has no `location_uri` set. When `update_catalog_table()` calls `spark.sql("CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION ...")`, the DeltaCatalog connector tries to resolve the database's base URI and receives an empty string.

**Fix path:** Confirm `aws_glue_catalog_database.lakehouse.location_uri` is set in Terraform and `terraform apply` has been run. Run `aws glue get-database --name ecom_lakehouse_db` and check the `LocationUri` field in the response — it should be `s3://<data-bucket>/<processed-prefix>`.

---

### Step Functions execution failed — checking which state failed

In the Step Functions console, open the execution and select the **Execution event history** tab. Events are listed in order. Look for the first `TaskFailed` event — its `cause` field contains the Glue job error message or Athena error code.

Alternatively, query the CloudWatch log group directly. The execution ID appears in every log event's context:

```
Logs Insights query:
fields @timestamp, type, id, stateEnteredEventDetails.name
| filter type = "TaskFailed"
| sort @timestamp asc
```

This returns only the failure events with the state name, which immediately points to `RunProductsJob`, `RunOrdersJob`, `RunOrderItemsJob`, or `AthenaValidation`.

---

### AthenaValidation failed — `TABLE_NOT_FOUND`

**Step Functions log event:**

```json
{
  "type": "TaskFailed",
  "taskFailedEventDetails": {
    "resourceType": "athena",
    "resource": "startQueryExecution.sync",
    "error": "Athena.AthenaException",
    "cause": "... TABLE_NOT_FOUND: ecom_lakehouse_db.orders ..."
  }
}
```

**Cause:** `update_catalog_table()` in the Glue job did not register the table, or registered it with a broken definition. Common sub-causes:

- The Glue job crashed before reaching the `Catalog Update` stage — look for `[FAILED] Catalog Update` in the Glue driver log.
- Lake Formation `CREATE_TABLE` permission is missing for `glue_role` — look for `AccessDeniedException` in the Glue log during the catalog step.
- The `terraform_data.drop_stale_catalog_tables` block in `main.tf` deleted the table on a recent `terraform apply` and the pipeline has not run since.

**Fix path:** Run `aws glue get-table --database-name ecom_lakehouse_db --name orders` — if it returns `EntityNotFoundException`, the table is missing. Re-run the full pipeline (not just `RunOrdersJob`) or manually register the table with `aws glue create-table`.

---

### Glue job slow — Delta Merge stage taking too long

**Log to look for:**

```
[START] Delta Merge | job=ecom-lakehouse-dev-orders-etl
... (no SUCCESS line appears for 10+ minutes)
```

**What to check:**

1. **CloudWatch Metrics → `glue.driver.BlockManager.memory.remainingMem_MB`**: If near zero, the G.1X worker (16 GB) is running out of memory. The Delta MERGE is shuffling more data than fits in memory and spilling to disk. This dramatically slows the merge. Fix: increase to G.2X workers or reduce the batch size.

2. **CloudWatch Metrics → `glue.driver.aggregate.numCompletedTasks`**: If the counter has stopped incrementing, a Spark task is hung — usually a network partition between the Glue worker and S3. The Step Functions `HeartbeatSeconds = 300` will eventually detect this and trigger the retry chain.

3. **Delta history in the log**: After each MERGE, `orders_job.py` calls `delta_table.history(1).show()`, which appears in CloudWatch. If the history shows `numOutputRows = 0` on a batch that should have written data, the MERGE condition matched everything as "already up to date" — which is correct for a true idempotent re-run but wrong if you expected new rows.

---

### Viewing Logs in the AWS Console

**For a Glue job run:**

1. AWS Console → Glue → Jobs → select the job → Job run monitoring
2. Find the run by start time → Logs column → Driver logs link
3. This opens CloudWatch Logs with the specific log stream filtered

**For a Step Functions execution:**

1. AWS Console → Step Functions → State machines → `ecom-lakehouse-dev-etl-pipeline`
2. Select the execution → Execution event history
3. Expand any `TaskFailed` event for the error cause and context

**For filtering across all job runs with CloudWatch Logs Insights:**

```
# Find all validation failures across all orders job runs
fields @timestamp, @message
| filter @logStream like /jr_/
| filter @message like /\[FAILED\]/
| sort @timestamp desc
| limit 50
```

```
# Find rejection counts for a specific date
fields @timestamp, @message
| filter @message like /total_read=/
| parse @message "total_read=* | valid=* | rejected=*" as total, valid, rejected
| sort @timestamp desc
```

These Logs Insights queries run across the entire `/aws-glue/jobs/ecom-lakehouse-dev` log group and return matches from all three job types and all job run IDs.
