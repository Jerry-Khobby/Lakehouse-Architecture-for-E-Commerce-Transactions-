# Architecture Overview — E-Commerce Lakehouse on AWS

## What This System Does

This project is a production-grade Lakehouse for an e-commerce platform. Three datasets — `products`, `orders`, and `order_items` — are ingested from CSV files, cleaned and validated by Spark on AWS Glue, merged into ACID Delta Lake tables on S3, and exposed for downstream analytics through Amazon Athena. AWS Step Functions orchestrates the entire lifecycle as a single ordered batch execution. Everything is provisioned with Terraform and deployed through GitHub Actions.

---

## Why Lakehouse Instead of a Traditional Data Warehouse

A traditional data warehouse (Redshift, Snowflake, BigQuery) owns both storage and compute. You pay for a running cluster whether or not queries are running, schemas are locked to the engine's proprietary format, and raw data lives separately in S3 before it is loaded in — creating two copies of everything.

This project takes the Lakehouse approach, which decouples storage from compute entirely:

| Property | Traditional DW | This Lakehouse |
|---|---|---|
| Storage format | Proprietary (Redshift blocks, Snowflake micro-partitions) | Open Parquet + Delta transaction log |
| Compute cost | Always-on cluster | Pay-per-use (Glue per DPU-hour, Athena per TB scanned) |
| ACID guarantees | Inside the engine only | Delta Lake provides ACID on plain S3 |
| Raw data | Must be loaded separately | Lives alongside processed data in the same bucket |
| Schema evolution | ALTER TABLE with downtime | Delta Lake handles column adds/renames non-destructively |
| Time travel / audit | Engine-specific, often costly | Delta transaction log (`_delta_log/`) retains all versions |
| Vendor lock-in | High — proprietary query engine | None — Parquet is readable by Spark, Hive, Trino, DuckDB |

The specific trade-off accepted here: query performance for ad-hoc workloads is lower than a highly tuned columnar DW, but this is appropriate for monthly batch analytics where cost predictability and data reliability matter more than sub-second latency.

---

## Services and How They Connect

```
Developer workstation / GitHub Actions
          │
          │  1. ingest.py uploads 3 CSVs to raw/
          │  2. Calls states:StartExecution
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    AWS Step Functions                           │
│  STANDARD state machine — ecom-lakehouse-dev-etl-pipeline       │
│                                                                 │
│  RunProductsJob ──▶ RunOrdersJob ──▶ RunOrderItemsJob           │
│         │                │                   │                  │
│     (Glue job)       (Glue job)          (Glue job)             │
│         │                │                   │                  │
│         └────────────────┴───────────────────┘                  │
│                          │                                      │
│                    AthenaValidation                             │
│                          │                                      │
│              NotifySuccess / NotifyFailure (SNS)                │
└─────────────────────────────────────────────────────────────────┘
          │                                    │
          ▼                                    ▼
   Amazon S3 (data bucket)            Amazon SNS topic
   ├── raw/                           ├── Email alert
   ├── lakehouse-dwh/                 └── Slack webhook (Lambda)
   ├── archived/
   ├── rejected/
   └── flagged/
          │
          ▼
  AWS Glue Data Catalog
  database: ecom_lakehouse_db
  tables: products, orders, order_items
          │
          ▼
  Amazon Athena
  workgroup: ecom-lakehouse-wg
  (queries lakehouse-dwh/ via Data Catalog)
```

---

## Service-by-Service Breakdown

### Amazon S3

Four buckets are provisioned, each with a distinct security and lifecycle posture:

**`ecom-lakehouse-dev-data-<account>`** — the central data bucket. All pipeline data lives here under prefix-separated zones. Versioning is enabled, server-side encryption (AES-256) is enforced, and a bucket policy denies all non-TLS requests. Lifecycle rules tier raw files to Infrequent Access after 30 days, archived files to Glacier after 90 days, and expire rejected records after 60 days.

**`ecom-lakehouse-dev-scripts-<account>`** — stores Glue job Python scripts (`products_job.py`, `orders_job.py`, `order_items_job.py`) and the utility zip (`glue_jobs.zip`). The deploy step in GitHub Actions syncs this bucket on every push to main.

**`ecom-lakehouse-dev-logs-<account>`** — receives S3 access logs from the data bucket. Created first in Terraform because other buckets reference it.

**`ecom-lakehouse-dev-athena-results-<account>`** — Athena query results land here. Results are encrypted and auto-expired after 7 days.

### AWS Glue + PySpark

Three Glue ETL jobs run on Glue version 4.0 (PySpark 3.3.2) with G.1X workers (4 vCPU, 16 GB RAM, 64 GB disk per worker). Each job:

1. Reads its source CSV from `raw/` using an explicit `StructType` schema with `mode=FAILFAST` — corrupt files raise immediately rather than producing silent nulls.
2. Runs a staged validation pipeline, writing rejected rows to `rejected/` with structured `rejection_reason` metadata.
3. Merges valid rows into the Delta table using `DeltaTable.merge()`.
4. Archives the source file from `raw/` to `archived/` using `boto3.copy_object` → `delete_object`.
5. Registers the Delta table in the Glue Data Catalog via Spark SQL `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION`.

Delta Lake is activated by Spark conf arguments injected by Terraform into every job's `default_arguments`:
```
spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
```

`common.py` verifies the extension is loaded at session startup and raises `RuntimeError` immediately if it is not, preventing silent fallback to non-ACID writes.

The session timezone is pinned to UTC (`spark.sql.session.timeZone = UTC`) so timestamp casts and the future-timestamp cutoff comparisons are unambiguous regardless of the EC2 worker's local timezone.

Glue's own job-level retry is set to 0. All retry logic lives in Step Functions, which has full visibility into the failure reason and can route to the notification state correctly.

### Delta Lake

Delta Lake provides three capabilities this architecture depends on:

**ACID transactions.** Each `DeltaTable.merge()` call either commits fully or not at all. If a Glue job fails mid-write, the Delta transaction log (`_delta_log/`) has no record of the partial write and downstream Athena queries never see it.

**MERGE / upsert semantics.** All three datasets use `DeltaTable.alias("target").merge(source, condition)`. Products use a change-detection condition on the match (update only if an attribute actually changed — identical re-runs are true no-ops and produce no new Delta log entry). Orders and order_items use a timestamp guard: `whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")` — a re-delivered older file cannot overwrite a more recent upsert already committed.

**First-run initialisation.** `ensure_delta_table()` in `common.py` checks `DeltaTable.isDeltaTable(spark, path)`. On the very first run it writes an empty DataFrame to seed the transaction log, making the subsequent MERGE behave correctly. This is idempotent — every subsequent run skips the init with no I/O cost.

### AWS Step Functions

The state machine type is `STANDARD` (not EXPRESS), which provides full execution history, audit logging, and exactly-once execution semantics — important for a pipeline where duplicate commits must be prevented.

The execution is strictly linear and ordered:

```
RunProductsJob → RunOrdersJob → RunOrderItemsJob → AthenaValidation → NotifySuccess
     │                │                │                   │
  (Catch)          (Catch)           (Catch)             (Catch)
     └────────────────┴─────────────────┴───────────────────┘
                                │
                         NotifyFailure
                                │
                         PipelineFailed (Fail state)
```

The ordering is a structural guarantee, not a convention. `order_items` holds foreign keys into both `products` (`product_id`) and `orders` (`order_id`). Its referential integrity validation joins against the live Delta tables. If `order_items` ran in parallel with or before the parent jobs, those joins would see an empty or partial Delta table and reject all rows that would otherwise be valid. Running products first, then orders, then order_items makes this dependency a fact of the execution graph.

Every Glue task has:
- `TimeoutSeconds`: guards against runaway Glue jobs holding Step Functions state indefinitely.
- `HeartbeatSeconds: 300`: if the Glue job stops sending heartbeats (worker crash, OOM), Step Functions detects it within 5 minutes and fires the retry/catch.
- `Retry`: two retries with exponential backoff on `Glue.AWSGlueException`, `States.TaskFailed`, and `States.Timeout`.
- `Catch`: all errors (`States.ALL`) route to `NotifyFailure`, which publishes to SNS before transitioning to the terminal `PipelineFailed` state.

The execution input is a structured JSON object passed through every state unchanged:
```json
{
  "bucket": "ecom-lakehouse-dev-data-<account>",
  "batch": "apr_2025",
  "files": {
    "products":    "raw/products.csv",
    "orders":      "raw/orders_apr_2025.csv",
    "order_items": "raw/order_items_apr_2025.csv"
  }
}
```

Each Glue task reads only its own file key from `$.files.<dataset>` and writes its result to a dedicated `$.results.<dataset>` path, so no two states share a `ResultPath` and the original input survives end-to-end.

### AWS Glue Data Catalog

Each Glue job registers its Delta table in the catalog using Spark SQL through the DeltaCatalog connector:
```sql
CREATE TABLE IF NOT EXISTS `ecom_lakehouse_db`.`orders`
USING DELTA
LOCATION 's3://ecom-lakehouse-dev-data-<account>/lakehouse-dwh/orders/'
```

This is a `CREATE TABLE IF NOT EXISTS` — not a DROP + CREATE — so repeated pipeline runs are safe. The schema is read automatically from the Delta transaction log, meaning it stays in sync with the actual data without manual schema management.

Three Glue crawlers are also provisioned (one per dataset) for on-demand schema discovery and as a fallback if direct registration fails. They are configured as `create_native_delta_table = true` Delta targets.

Lake Formation permissions are layered on top of IAM. The Glue job role holds `CREATE_TABLE` and `DESCRIBE` on the database, and `ALL` on all tables. The Step Functions role holds `SELECT` and `DESCRIBE` on all tables, which satisfies Athena's Lake Formation check before it falls through to IAM for S3 access.

### Amazon Athena

Athena is the query engine for downstream analytics. It reads Delta tables directly from `lakehouse-dwh/` via the Data Catalog — no data movement or loading is required.

The workgroup `ecom-lakehouse-wg` enforces:
- **Output location**: all results go to `s3://.../query-results/` and are auto-expired after 7 days.
- **Scan limit**: 1 GB per query, rejecting runaway full-table scans before they become expensive.
- **Encryption**: SSE-S3 on all result files.
- **Athena engine version 3**: required for native Delta Lake table reading.

The Step Functions `AthenaValidation` state runs a smoke-test query after every successful pipeline run:
```sql
SELECT 'products' AS tbl, COUNT(*) AS row_count FROM ecom_lakehouse_db.products
UNION ALL SELECT 'orders', COUNT(*) FROM ecom_lakehouse_db.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM ecom_lakehouse_db.order_items;
```
This confirms all three tables are reachable and contain rows before `NotifySuccess` fires. If Athena cannot resolve the tables (e.g. catalog registration failed), this state fails and routes to `NotifyFailure`.

### Amazon SNS + Slack Lambda

An SNS topic (`ecom-lakehouse-dev-pipeline-alerts`) receives notifications from two places:

- **Step Functions** publishes directly to SNS from the `NotifySuccess` and `NotifyFailure` states using `arn:aws:states:::sns:publish`, so no Lambda is needed for the Step Functions-level alert.
- **Glue jobs** publish per-stage START / SUCCESS / FAILURE events via `PipelineMonitor` and `SnsNotifier` in `utils/monitor.py` and `utils/notifier.py`, giving real-time visibility as each stage completes.

A Slack Lambda subscriber converts SNS messages to Slack webhook payloads for the real-time channel notifications visible in the project's Slack feed.

### Infrastructure as Code — Terraform

All AWS resources are defined in Terraform under `terraform/`. The state is stored remotely (configured in `provider.tf`). Key design decisions encoded in Terraform:

- **Resource naming**: every resource name is prefixed `${project_name}-${environment}` (e.g. `ecom-lakehouse-dev`) and suffixed with the AWS account ID where global uniqueness is required (S3 bucket names).
- **S3 prefix objects**: `aws_s3_object` resources for each prefix (`raw/`, `lakehouse-dwh/`, etc.) ensure the logical folders exist before any Glue job runs.
- **Least-privilege ingestion policy**: `aws_iam_policy.ingestion` grants only `s3:PutObject` on `raw/*` and `states:StartExecution` on the state machine. This is the policy to attach to the developer or CI IAM principal that runs `ingest.py`.
- **Lake Formation admin**: `aws_lakeformation_data_lake_settings` sets the caller's IAM principal as the LF admin so `aws_lakeformation_permissions` resources apply without `AccessDeniedException`.

### CI/CD — GitHub Actions

Two workflows run on the `main` branch:

**`ci.yml`** — runs on push and pull request:
1. `lint`: `black --check` (formatting) + `flake8` (style errors) across `glue_jobs/`, `ingestion/`, `tests/`.
2. `test`: installs Java 11 (Temurin, required by PySpark), installs Python dependencies, runs `pytest tests/` with `--cov=glue_jobs --cov-fail-under=70`. Coverage threshold is enforced — a drop below 70% blocks the merge.
3. `terraform-check`: `terraform fmt -check`, `terraform init -backend=false`, `terraform validate` — catches syntax and provider schema errors without AWS credentials.

**`deploy.yml`** — runs on push to main only, skips silently if AWS secrets are not configured:
1. Packages `glue_jobs/utils/` into `glue_jobs.zip` (preserving the `glue_jobs/` root so imports resolve correctly inside the Glue Python runtime).
2. Uploads all four objects (`products_job.py`, `orders_job.py`, `order_items_job.py`, `glue_jobs.zip`) to the scripts bucket.
3. Exports the current Step Functions ASL definition to the GitHub Actions step summary for audit trail.

---

## Security Design

| Control | Implementation |
|---|---|
| Encryption at rest | AES-256 SSE on all four S3 buckets; `bucket_key_enabled = true` reduces KMS API calls |
| Encryption in transit | TLS-only S3 bucket policy (`aws:SecureTransport = false` → Deny) on all buckets |
| Public access | All four buckets have all public-access-block settings enabled |
| IAM least privilege | Glue role scoped to specific bucket ARNs and Glue catalog ARNs; SFN role scoped to specific job ARNs |
| Lake Formation | Explicit DESCRIBE, SELECT grants on the Glue role and SFN role prevent wildcard IAM from granting unintended catalog access |
| Bucket confusion protection | `archive_source_file()` sets `ExpectedBucketOwner` on both `copy_object` and `delete_object` calls |
| S3 versioning | Enabled on data and scripts buckets; noncurrent versions expire after 30 days |
| Sensitive values | Slack webhook URL is a `sensitive = true` Terraform variable; credentials use `.env` (gitignored) locally and GitHub Secrets in CI |

---

## Observability

| Signal | Source | Destination |
|---|---|---|
| Per-stage timing and row counts | `PipelineMonitor.stage()` context manager | SNS → Slack |
| Full job logs | Glue CloudWatch integration | `/aws-glue/jobs/ecom-lakehouse-dev` log group, 30-day retention |
| State machine execution history | Step Functions STANDARD type + `logging_configuration level=ALL` | `/aws/states/ecom-lakehouse-dev-etl-pipeline` log group |
| X-Ray traces | `tracing_configuration { enabled = true }` on the state machine | X-Ray service map |
| Validation pass rates | `log_counts()` in `common.py` | CloudWatch via Glue job logs |
| Rejection detail | `write_rejected()` writes Parquet to `rejected/<dataset>/<date>/<run_id>/` with `rejection_reason`, `_rejected_at`, `_job_run_id`, `_source_key` columns | Queryable via Athena |
