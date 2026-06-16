# AWS Glue Jobs and Crawlers

## Overview

This project uses three AWS Glue ETL jobs — one per dataset — and three Glue crawlers provisioned as a fallback catalog mechanism. All three jobs share the same worker configuration, IAM role, and a common set of Spark arguments. Each job differs only in which dataset it processes, which merge key it uses, and which partition column it applies. This document covers every configuration decision: Glue version, worker type, Delta Lake activation, argument passing, utility packaging, the `max_concurrent_runs` guard, and crawler strategy.

---

## Glue Version and Spark Runtime

All three jobs run on **Glue 4.0**, which ships with **Apache Spark 3.3.2** and **Python 3.10**. Terraform sets this via `glue_version = var.glue_version`, defaulting to `"4.0"`.

```hcl
resource "aws_glue_job" "orders" {
  glue_version = var.glue_version   # "4.0"
  ...
}
```

### Why Glue 4.0 Specifically

Glue 4.0 is the version that ships native Delta Lake support via the `--datalake-formats` argument. Earlier versions (Glue 3.0) required manually bundling the Delta Lake JARs as custom connectors. Glue 4.0 puts the correct `delta-core` and `delta-storage` JARs on the classpath automatically when `--datalake-formats = "delta"` is set.

However, Glue 4.0 has an important nuance: putting the JARs on the classpath is **not enough** to make Delta Lake work. The Spark session extensions and the DeltaCatalog must also be registered explicitly. This is a documented requirement that is easy to miss, and the consequence of missing it is silent — Spark initialises without Delta and every Delta operation fails at runtime, not at startup. The next section explains how this is handled.

---

## Worker Type and Capacity

```hcl
worker_type       = var.glue_worker_type    # "G.1X"
number_of_workers = var.glue_num_workers    # 2
```

### G.1X Workers

A `G.1X` worker provides:
- 4 vCPUs
- 16 GB RAM
- 64 GB disk
- 1 DPU (Data Processing Unit)

With 2 workers, the cluster has:
- 1 driver worker (runs the SparkContext, GlueContext, job coordination)
- 1 executor worker (runs the actual Spark tasks, Parquet reads, MERGE operations)
- Total: 8 vCPUs, 32 GB RAM across the cluster

### Why G.1X With 2 Workers

This dataset is small: 1,000 products, ~850 orders per batch, ~2,500 order items per batch. A single Parquet write per partition per job. The bottleneck is network I/O against S3 and Glue cold-start time (JVM initialisation + Spark context + Delta extension loading), not compute.

Using `G.2X` workers (8 vCPU, 32 GB RAM, 2 DPU) or adding more workers would make cold-start 10% faster while doubling the per-DPU-hour cost. For monthly batch jobs processing data at this scale, `G.1X` with 2 workers is the correct cost-performance point.

If the dataset grew to millions of rows per batch, the right response would be to increase `glue_num_workers` in Terraform without changing any job code, because the Spark logic is inherently distributed.

### Job Timeout and Retries

```hcl
timeout     = var.glue_timeout_minutes   # 60
max_retries = var.glue_max_retries       # 0
```

**Timeout at 60 minutes:** If a Glue job hangs — network partition between worker and S3, OOM kill, Delta log lock not released — the job is force-terminated at 60 minutes. Without a timeout, a stuck job holds a Step Functions task state open indefinitely, consuming DPU-hours and blocking subsequent executions.

**max_retries = 0:** Glue has its own retry mechanism independent of Step Functions. Setting it to 0 disables Glue-level retries. All retry logic is handled by Step Functions, which sees the full execution context, can route to the failure notification state, and logs the reason. If both Glue and Step Functions had retry policies, a failure could produce up to `(Glue retries + 1) × (SFN retries + 1)` job runs — with no coordination between them, producing duplicate partially-committed writes.

### Stall Notification

```hcl
notification_property {
  notify_delay_after = 10   # minutes
}
```

If a Glue job run does not complete within 10 minutes of starting, CloudWatch emits an alert. This is separate from the Step Functions timeout and separate from the SNS pipeline alerts. It catches a job that is running but is stuck at a specific stage (e.g. Delta MERGE stalled waiting for S3 consistency) before the 60-minute hard timeout fires.

---

## `max_concurrent_runs = 1`

```hcl
execution_property {
  max_concurrent_runs = 1
}
```

This is set on all three jobs and is the enforcement point for the single-execution model.

If the Step Functions state machine allows only one execution at a time (it does not — STANDARD state machines allow concurrent executions by default), this setting would be redundant. But if an operator accidentally runs `ingest.py` a second time before the first execution completes, two Step Functions executions would try to start the same Glue jobs simultaneously. Without `max_concurrent_runs = 1`, both would succeed in calling `glue:StartJobRun`, and two jobs would attempt concurrent Delta MERGEs against the same table, triggering Delta's optimistic concurrency conflict.

With `max_concurrent_runs = 1`, the second `StartJobRun` call is rejected by Glue with `ConcurrentRunsExceededException`. Step Functions catches this as `States.TaskFailed`, applies the retry policy, and retries after 30 seconds. By that point the first execution's job run has usually advanced far enough that the second execution's retry attempt succeeds. This is a best-effort guard, not a perfect mutex — the correct operational practice is to wait for `SUCCEEDED` before starting the next batch.

---

## Delta Lake Activation — The Critical Configuration

This is the most technically complex part of the Glue job configuration. Getting this wrong produces jobs that appear to start correctly but fail at the first Delta operation.

### Step 1: Put Delta JARs on the Classpath

```hcl
"--datalake-formats" = "delta"
```

This tells Glue to add the Delta Lake JARs (`delta-core`, `delta-storage`, `delta-hudi-compatibility`) to the Spark classpath. Without this, importing `from delta.tables import DeltaTable` raises `ModuleNotFoundError`.

### Step 2: Register the Spark Extensions and Catalog

```hcl
"--conf" = (
  "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
  "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
  "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore "
  "--conf spark.sql.warehouse.dir=s3://<data-bucket>/glue-warehouse/"
)
```

This is a single Terraform map key that contains four `--conf` tokens chained together. Terraform map keys must be unique, so multiple `--conf` entries cannot be used. The chaining pattern (`value --conf next_key=next_value`) is the AWS-documented approach for Glue; Glue's argument parser splits this into separate `spark-submit --conf` flags.

Each conf flag:

**`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`**
Registers the Delta SQL extension into the SparkSession. This enables Delta-specific SQL syntax (`CREATE TABLE ... USING DELTA`, `DESCRIBE HISTORY`, `VACUUM`) and hooks the Delta writer into Spark's DataFrame API. Without this, `spark.read.format("delta")` and `DeltaTable.forPath()` raise `AnalysisException: Table format 'delta' is not supported`.

**`spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`**
Replaces Spark's default session catalog with Delta's catalog implementation. This is required for `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION '...'` (the Glue Data Catalog registration call in `update_catalog_table()`) to route through the DeltaCatalog connector, which in turn uses the Glue job's IAM role to write to the Glue Data Catalog. Without this, `spark.sql(CREATE TABLE ...)` uses Hive Metastore, which does not exist in the Glue runtime environment, and raises a connection error.

**`spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore`**
Specifies how Delta Lake writes to its transaction log on S3. The default `LogStore` implementation uses HDFS semantics (atomic rename), which S3 does not support — S3 is eventually consistent for LIST operations and has no atomic rename. `S3SingleDriverLogStore` is Delta's S3-specific implementation that uses S3's `put-if-absent` semantics (conditional writes based on ETag matching) to guarantee that only one writer commits a given transaction log entry. Without this, concurrent Delta writes on S3 can produce transaction log corruption.

**`spark.sql.warehouse.dir=s3://<data-bucket>/glue-warehouse/`**
Sets the default SQL warehouse directory. Spark uses this path as the root for managed tables and for certain internal staging operations during `CREATE TABLE`. Without it, Spark's internal Path constructor receives an empty string and raises `IllegalArgumentException: Can not create a Path from an empty string`. This manifested as a real failure during development and was fixed by explicitly pointing the warehouse dir to the data bucket.

### Step 3: Runtime Guard in `build_spark_session()`

Even with the Terraform configuration correct, `build_spark_session()` in `common.py` verifies the extension is active at runtime:

```python
active_extensions = spark.conf.get("spark.sql.extensions", "")
if "DeltaSparkSessionExtension" not in active_extensions:
    raise RuntimeError(
        "Delta Lake extensions not loaded. "
        "Check --conf spark.sql.extensions in Glue job default_arguments."
    )
```

This guard fires within seconds of job startup, before any data is read. It converts a silent configuration failure (which would surface as a confusing `AnalysisException` on the first Delta operation) into an immediate, unambiguous error with an actionable message.

---

## Argument Passing Architecture

Glue jobs receive arguments in two categories: **static defaults** baked in at job definition time, and **dynamic overrides** injected per execution by Step Functions.

### Static Defaults (`common_glue_args` local)

These are set once in Terraform and never change between executions:

```hcl
locals {
  common_glue_args = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://<logs-bucket>/spark-ui-logs/"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "delta"
    "--conf"                             = "..."   # Delta conf chain
    "--TempDir"                          = "s3://<data-bucket>/glue-temp/"
    "--extra-py-files"                   = "s3://<scripts-bucket>/glue_jobs/glue_jobs.zip"
    "--DATA_BUCKET"                      = "<data-bucket-name>"
    "--SCRIPTS_BUCKET"                   = "<scripts-bucket-name>"
    "--ENVIRONMENT"                      = "dev"
    "--DATABASE_NAME"                    = "ecom_lakehouse_db"
    "--SNS_TOPIC_ARN"                    = "<sns-topic-arn>"
    "--FLAGGED_PREFIX"                   = "flagged/"
  }
}
```

**`--enable-metrics`**: Publishes Glue job metrics (executor memory utilisation, shuffle bytes, records read/written) to CloudWatch Metrics. Visible in the Glue console's job run details.

**`--enable-continuous-cloudwatch-log`**: Streams Python `print()` and `logging` output to CloudWatch Logs in near-real-time as the job runs, rather than batching at job completion. Critical for debugging — without it, you cannot see log output until the job finishes.

**`--enable-spark-ui`**: Generates Spark event logs for the Spark History Server. Spark UI logs land at `--spark-event-logs-path` and are viewable via the Glue console's "Spark UI" tab on any completed job run. Shows DAG visualisation, task timing, shuffle statistics, and executor utilisation — essential for diagnosing performance bottlenecks.

**`--enable-job-insights`**: Activates Glue's automatic job insights, which analyse job execution patterns and surface anomalies (unexpected runtime increase, out-of-memory patterns) as CloudWatch recommendations.

**`--enable-glue-datacatalog`**: Enables the Glue job to access the Glue Data Catalog through Spark. Required for the `spark.sql(CREATE TABLE ...)` call in `update_catalog_table()` to route to the Glue catalog rather than a local Hive metastore.

**`--TempDir`**: The S3 path Glue uses for shuffle spill and Delta MERGE staging. Delta's MERGE operation stages intermediate data (the rows that matched and need updating) in a temporary location before committing the final write. The data bucket is the correct location because the Glue role has full read/write/delete there. Using the scripts bucket (read-only for the Glue role) or the logs bucket would cause the MERGE to fail with `AccessDenied`.

**`--extra-py-files`**: Points to `glue_jobs.zip`, the utility package. This makes `from glue_jobs.utils.common import ...` resolvable in the Glue Python runtime. Without this, the import fails because `glue_jobs/` is not on the Python path inside the Glue executor.

### Per-Job Static Overrides

Each job merges the common args with its own dataset-specific values:

```hcl
default_arguments = merge(local.common_glue_args, {
  "--DATASET"          = "orders"
  "--RAW_PREFIX"       = "raw/"
  "--PROCESSED_PREFIX" = "lakehouse-dwh/"
  "--ARCHIVED_PREFIX"  = "archived/"
  "--REJECTED_PREFIX"  = "rejected/"
  "--MERGE_KEYS"       = "order_id"
  "--PARTITION_COLS"   = "date"
})
```

`--MERGE_KEYS` and `--PARTITION_COLS` are passed as comma-separated strings. `parse_args()` in `common.py` splits them:
```python
raw["MERGE_KEYS_LIST"] = [k.strip() for k in raw["MERGE_KEYS"].split(",") if k.strip()]
raw["PARTITION_COLS_LIST"] = [c.strip() for c in raw["PARTITION_COLS"].split(",") if c.strip()]
```

For order_items, `--MERGE_KEYS = "id,order_id"` produces `MERGE_KEYS_LIST = ["id", "order_id"]`.

### Dynamic Overrides per Execution (Step Functions)

Step Functions overrides two arguments at runtime, injected from the execution input:
```json
"Arguments": {
  "--RAW_KEY.$":     "$.files.orders",
  "--DATA_BUCKET.$": "$.bucket"
}
```

The `.$` suffix is the Step Functions JSONPath syntax for reading a value from the execution input at runtime. `--RAW_KEY` changes per execution (April uses `raw/orders_apr_2025.csv`, May uses `raw/orders_may_2025.csv`) while all other arguments remain the same. This is how the same Glue job definition processes different monthly files without any code change.

---

## Utility Packaging — `glue_jobs.zip`

The three job entry-point scripts (`products_job.py`, `orders_job.py`, `order_items_job.py`) all import from `glue_jobs.utils`:
```python
from glue_jobs.utils.common import build_spark_session, parse_args, write_rejected, ...
from glue_jobs.utils.monitor import PipelineMonitor
from glue_jobs.utils.notifier import SnsNotifier
```

In the Glue runtime, Python's module resolution path does not include arbitrary S3 locations. The `--extra-py-files` mechanism works by downloading the specified ZIP and extracting it into a directory that is added to `sys.path` before the job script runs. For the import `from glue_jobs.utils.common import ...` to resolve, the ZIP must contain `glue_jobs/` at its root, not just the contents of `glue_jobs/`.

Terraform packages the ZIP correctly:
```hcl
data "archive_file" "glue_jobs_package" {
  type        = "zip"
  output_path = "${path.module}/../glue_jobs.zip"

  source {
    content  = file("${path.module}/../glue_jobs/__init__.py")
    filename = "glue_jobs/__init__.py"             # ← correct root prefix
  }
  source {
    content  = file("${path.module}/../glue_jobs/utils/common.py")
    filename = "glue_jobs/utils/common.py"         # ← correct relative path
  }
  ...
}
```

Each `source` block explicitly specifies the `filename` path within the ZIP, preserving the `glue_jobs/` directory structure. If the files were added with `filename = "common.py"` (no prefix), the import would fail with `ModuleNotFoundError: No module named 'glue_jobs'`.

The ZIP is re-uploaded by Terraform only when its MD5 changes (tracked via `output_md5`). GitHub Actions also re-packages and re-uploads the ZIP in `deploy.yml` on every push to main, so code changes are deployed without requiring a `terraform apply`.

---

## Glue Crawlers

### What the Crawlers Are

Three crawlers are provisioned — one per dataset — as Delta Lake native crawlers:

```hcl
resource "aws_glue_crawler" "orders" {
  name          = "ecom-lakehouse-dev-crawler-orders"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name

  delta_target {
    delta_tables              = ["s3://<data-bucket>/lakehouse-dwh/orders/"]
    write_manifest            = false
    create_native_delta_table = true
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  schedule = ""   # on-demand only
}
```

**`create_native_delta_table = true`**: Instructs the crawler to create a Glue catalog table of type `DELTA` rather than `HIVE`. Athena engine version 3 reads Delta tables natively; a Hive-type catalog entry pointing at Delta Parquet files would cause Athena to read the raw Parquet without Delta's snapshot isolation, potentially returning partial or stale data.

**`update_behavior = "UPDATE_IN_DATABASE"`**: If the crawler detects a schema change (new column added by a Delta schema evolution operation), it updates the catalog table in place.

**`delete_behavior = "LOG"`**: If the crawler detects that data has disappeared from the S3 path (e.g. a prefix was accidentally renamed), it logs the event rather than dropping the catalog table. This is the safer default — dropping a catalog table breaks all Athena queries against it instantly.

**`schedule = ""`**: Crawlers are on-demand only. The `schedule` block is omitted when the variable is empty, so no cron expression is registered. Crawlers are not invoked automatically — they are available for manual execution from the Glue console.

### Why Crawlers Are Not in the Step Functions Workflow

The Glue jobs call `update_catalog_table()` directly at the end of each successful run:
```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{database}`.`{table_name}`
    USING DELTA
    LOCATION '{table_path}'
""")
```

This is faster and more reliable than invoking a crawler:
- **Speed:** A crawler run takes 2–5 minutes to scan the S3 path and update the catalog. The direct Spark SQL call completes in under 1 second.
- **Precision:** The Spark SQL call targets exactly the table that was just merged. A crawler re-scans the entire Delta prefix and may pick up unrelated changes.
- **No concurrency conflict:** A crawler run occupies a Glue crawler slot. Adding crawler invocations to the Step Functions workflow would add 3 × 2–5 minutes to the total pipeline runtime.

The crawlers exist as a safety net: if the direct catalog registration fails due to a transient Lake Formation permission issue, an operator can manually trigger the crawler from the Glue console to re-register the table without re-running the entire pipeline.

### When to Manually Run a Crawler

Run a crawler manually when:
- An Athena query returns `TABLE_NOT_FOUND` after a pipeline run that the Glue logs show completed the catalog update step.
- The schema of a Delta table changed (column added or renamed) and Athena queries are returning schema mismatch errors.
- A catastrophic failure left the Delta table in a state where `CREATE TABLE IF NOT EXISTS` cannot register it but the underlying Parquet data is intact.

From the Glue console: Crawlers → select the crawler → Run crawler. The crawler completes in ~2–5 minutes and the updated catalog entry is immediately visible in Athena.
