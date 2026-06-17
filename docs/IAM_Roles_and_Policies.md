# IAM Roles and Policies — Who Can Do What and Why

## Overview

AWS Identity and Access Management (IAM) controls which AWS principal can call which AWS API on which resource. This pipeline has four IAM principals — the Glue job role, the Step Functions execution role, the Lambda execution role, and the ingestion principal — each with a carefully scoped set of permissions. There is no EventBridge role because this project does not use EventBridge for triggering (the ingestion policy covers the trigger mechanism directly). This document explains every role and policy, what each permission statement does, and the reasoning behind each scoping decision.

---

## Principle of Least Privilege

Every role in this pipeline is scoped to the minimum permissions required for its function. No role has `*` on actions or resources unless AWS explicitly requires it (the one exception is the CloudWatch log delivery actions on the Step Functions role, documented below). The practical consequences:

- A compromised Glue job role cannot start or stop Step Functions executions.
- A compromised Step Functions role cannot read raw CSV files or write to the Delta tables directly.
- A compromised ingestion principal cannot read existing data, stop running executions, or access Glue jobs.
- No role has `iam:*` or `sts:AssumeRole` on other roles — lateral movement is not possible through these credentials.

---

## Role 1 — Glue Job Role

```hcl
resource "aws_iam_role" "glue_role" {
  name = "${local.name_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}
```

`Principal = { Service = "glue.amazonaws.com" }` restricts role assumption to the Glue service. A human IAM user or another AWS service cannot assume this role — only Glue job runs can.

### Policy 1 — `AWSGlueServiceRole` (managed)

```hcl
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
```

AWS's managed policy for Glue jobs. It grants:
- `glue:*` on Glue resources (job runs, crawlers, connections, dev endpoints)
- `s3:GetObject` on `aws-glue-*` prefixed buckets (Glue's own bootstrap assets)
- `ec2:*` permissions needed to launch Glue workers in a VPC
- `cloudwatch:PutMetricData` for Glue metric emission
- `logs:*` on `/aws-glue/*` log groups

The managed policy covers the infrastructure-level permissions Glue needs to run workers and manage its own internal state. The inline policies below cover application-level permissions specific to this pipeline.

### Policy 2 — S3 Access (`glue_s3`)

```hcl
Statement = [
  {
    Sid    = "DataBucketReadWrite"
    Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
              "s3:GetObjectVersion", "s3:ListBucket", "s3:GetBucketLocation"]
    Resource = [data_bucket_arn, "${data_bucket_arn}/*"]
  },
  {
    Sid    = "ScriptsBucketRead"
    Action = ["s3:GetObject", "s3:ListBucket"]
    Resource = [scripts_bucket_arn, "${scripts_bucket_arn}/*"]
  },
  {
    Sid    = "LogsBucketWrite"
    Action = ["s3:PutObject"]
    Resource = ["${logs_bucket_arn}/*"]
  }
]
```

**`DataBucketReadWrite`:** The Glue jobs read from `raw/` (`GetObject`), write to `lakehouse-dwh/`, `rejected/`, `flagged/`, and `glue-temp/` (`PutObject`), delete superseded Delta Parquet files during compaction (`DeleteObject`), read S3 object versions during Delta log replay (`GetObjectVersion`), and list prefixes for Delta log discovery (`ListBucket`). `GetBucketLocation` is required by the S3 client to resolve the bucket's AWS region before making any API call.

The ARN covers both `data_bucket_arn` (for `ListBucket`) and `${data_bucket_arn}/*` (for object-level operations). ListBucket is a bucket-level action and requires the bucket ARN without the `/*` suffix — including only `/*` would silently deny `ListBucket`.

**`ScriptsBucketRead`:** Glue reads the job scripts (`products_job.py`, `orders_job.py`, `order_items_job.py`) and the utility zip (`glue_jobs.zip`) from the scripts bucket at job startup. The job role needs `GetObject` to download these files. `ListBucket` is needed for `--extra-py-files` resolution. No `PutObject` — the Glue job role cannot overwrite its own scripts, which prevents a compromised job from injecting malicious code into future runs.

**`LogsBucketWrite`:** Glue writes Spark UI event logs to `s3://<logs-bucket>/spark-ui-logs/` (configured via `--spark-event-logs-path`). `PutObject` only — the job cannot read or delete existing logs.

**Why `--TempDir` uses the data bucket and not the logs bucket:**

The `--TempDir` argument sets the S3 staging area for Glue shuffle spill and Delta MERGE commit staging. It requires `PutObject`, `GetObject`, and `DeleteObject`. The logs bucket only has `PutObject` for the Glue role — not `GetObject` or `DeleteObject`. If `--TempDir` pointed to the logs bucket, Glue would fail on startup:
```
IllegalArgumentException: Can not create a Path from an empty string
```
The data bucket satisfies all three operations, which is why `--TempDir = s3://<data-bucket>/glue-temp/` is used.

### Policy 3 — Glue Data Catalog (`glue_catalog`)

```hcl
Action = [
  "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
  "glue:GetTable",    "glue:GetTables",    "glue:CreateTable",
  "glue:UpdateTable", "glue:DeleteTable",
  "glue:GetPartition", "glue:GetPartitions",
  "glue:CreatePartition", "glue:UpdatePartition", "glue:BatchCreatePartition"
]
Resource = [
  "arn:aws:glue:<region>:<account>:catalog",
  "arn:aws:glue:<region>:<account>:database/ecom_lakehouse_db",
  "arn:aws:glue:<region>:<account>:table/ecom_lakehouse_db/*"
]
```

**`GetDatabase` / `GetDatabases`:** The DeltaCatalog connector reads the database record (including `LocationUri`) before placing the table definition. Called on every `CREATE TABLE IF NOT EXISTS` execution.

**`CreateDatabase`:** Required even if the database already exists, because the DeltaCatalog connector checks for the database and may attempt to create it if the catalog entry is absent during initialization.

**`CreateTable` / `UpdateTable`:** Called by `update_catalog_table()` via `spark.sql("CREATE TABLE IF NOT EXISTS ...")`. `UpdateTable` is needed when the Delta schema evolves (new column added) and the DeltaCatalog connector updates the existing catalog entry.

**`DeleteTable`:** Needed for the one-time cleanup provisioner block (`terraform_data.drop_stale_catalog_tables`) and for any Delta vacuum operation that removes old table versions. Also used if a crawler detects a schema incompatibility and recreates the table.

**`GetPartition` / `GetPartitions` / `CreatePartition` / `UpdatePartition` / `BatchCreatePartition`:** Delta tables are partitioned by `date` (orders, order_items) and `department` (products). When the Glue job writes to a new partition for the first time (e.g. a new date in May 2025), it must register the partition in the catalog. `BatchCreatePartition` allows multiple partition registrations in a single API call, which is more efficient than one `CreatePartition` call per date.

**Resource scoping to `ecom_lakehouse_db`:** The table wildcard (`table/ecom_lakehouse_db/*`) covers all current and future tables in this database only. The Glue job cannot create tables in other Glue databases, which prevents a compromised job from poisoning unrelated data pipelines in the same account.

### Policy 4 — CloudWatch Logs (`glue_cloudwatch`)

```hcl
Action = [
  "logs:CreateLogGroup", "logs:CreateLogStream",
  "logs:PutLogEvents",   "logs:DescribeLogGroups",
  "logs:DescribeLogStreams"
]
Resource = "arn:aws:logs:<region>:<account>:log-group:/aws-glue/*"
```

Scoped to the `/aws-glue/*` log group prefix. The Glue job cannot write to arbitrary CloudWatch log groups — it can only write to the designated Glue log groups. `DescribeLogGroups` and `DescribeLogStreams` are needed by the Glue driver to check whether a log group or stream already exists before attempting to create it.

### Policy 5 — SNS (`glue_sns`)

```hcl
Action   = ["sns:Publish"]
Resource = [pipeline_alerts_topic_arn]
```

Scoped to a single SNS topic ARN. The Glue job's `SnsNotifier` class calls `boto3.client("sns").publish()` for every stage event. The role can only publish to the pipeline alerts topic — it cannot create topics, manage subscriptions, or publish to any other topic.

---

## Role 2 — Step Functions Execution Role

```hcl
resource "aws_iam_role" "sfn_role" {
  assume_role_policy = jsonencode({
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}
```

Only `states.amazonaws.com` can assume this role.

### `StartGlueJobs`

```hcl
Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
Resource = [products_job_arn, orders_job_arn, order_items_job_arn]
```

`StartJobRun` — the primary action. Step Functions calls this to trigger a Glue job run via the `.sync` integration.

`GetJobRun` / `GetJobRuns` — Step Functions polls these internally during the `.sync` wait to check whether the Glue job has reached a terminal state. Without these, the `.sync` integration cannot determine when the job finishes and the execution hangs indefinitely.

`BatchStopJobRun` — called if the Step Functions execution is aborted (e.g. by a `TaskTimeout` during a heartbeat failure). Step Functions needs to stop the running Glue job to prevent it from continuing after the execution terminates.

Resource is scoped to the three specific Glue job ARNs. The Step Functions role cannot start any other Glue job in the account.

### `ManageCrawlers`

```hcl
Action   = ["glue:StartCrawler", "glue:GetCrawler"]
Resource = [products_crawler_arn, orders_crawler_arn, order_items_crawler_arn]
```

The crawlers are provisioned and the SFN role has permission to start them, but they are not in the Step Functions state machine. This permission exists so the state machine can be extended to include crawler states without an IAM change. Currently unused by the pipeline execution flow.

### `AthenaQuery`

```hcl
Action   = ["athena:StartQueryExecution", "athena:StopQueryExecution",
            "athena:GetQueryExecution",   "athena:GetQueryResults"]
Resource = ["arn:aws:athena:<region>:<account>:workgroup/ecom-lakehouse-wg"]
```

`StartQueryExecution` — Step Functions calls this from the `AthenaValidation` state.

`GetQueryExecution` — Step Functions polls this during the `.sync` wait to check whether the Athena query has completed. Without it, the state hangs.

`StopQueryExecution` — called if the Step Functions execution times out while waiting for the Athena query.

`GetQueryResults` — Step Functions reads the query result to populate `$.results.athena` via `ResultPath`.

Scoped to the specific workgroup ARN. The SFN role cannot run queries against any other Athena workgroup.

### `AthenaCatalogRead`

```hcl
# Athena resolves table metadata through the Glue Data Catalog using the
# CALLER's permissions (Step Functions is the caller), so the execution
# role — not just the Glue job role — needs read access to the catalog.
Action   = ["glue:GetDatabase", "glue:GetDatabases",
            "glue:GetTable",    "glue:GetTables",
            "glue:GetPartition","glue:GetPartitions"]
Resource = [catalog_arn, database_arn, "table/ecom_lakehouse_db/*"]
```

This is the most commonly overlooked permission for Athena pipelines. When Step Functions calls `athena:StartQueryExecution` for a query on `ecom_lakehouse_db.orders`, Athena resolves the table metadata by calling the Glue Data Catalog **using the calling principal's credentials** — not its own internal service account.

The calling principal is the Step Functions execution role. If `sfn_role` lacks `glue:GetTable` on `ecom_lakehouse_db.orders`, Athena cannot resolve the table schema and the query fails with:
```
FAILED: SemanticException [Error 10001]: Line 1:14 Table not found ecom_lakehouse_db.orders
```

This error looks like the table does not exist, but the actual cause is an IAM permission gap on the principal running the query. The comment in the Terraform code documents this because it is a non-obvious failure mode.

### `AthenaDataRead`

```hcl
Action   = ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket", "s3:GetBucketLocation"]
Resource = [data_bucket_arn, "${data_bucket_arn}/*"]
```

After resolving the table metadata, Athena reads the actual Parquet files from `lakehouse-dwh/`. It does this under the calling principal's credentials. The `sfn_role` needs `GetObject` on the data bucket. Without this, Athena can resolve the table schema but cannot read the data files and fails with `Access Denied` on the S3 read.

### `AthenaResultsS3`

```hcl
Action   = ["s3:GetBucketLocation", "s3:GetObject", "s3:PutObject", "s3:ListBucket"]
Resource = [athena_results_bucket_arn, "${athena_results_bucket_arn}/*"]
```

Athena writes query results to S3 under the calling principal's credentials. The workgroup enforces the output location (`s3://<athena-results-bucket>/query-results/`). The `sfn_role` needs `PutObject` to write the result CSV. `GetObject` and `ListBucket` are needed because Step Functions reads the result back via `GetQueryResults`, which internally resolves the S3 location.

### `SNSPublish`

```hcl
Action   = ["sns:Publish"]
Resource = [pipeline_alerts_topic_arn]
```

Used by the `NotifySuccess` and `NotifyFailure` states, which publish directly to SNS using the `arn:aws:states:::sns:publish` integration. The Step Functions service calls `sns:Publish` using the execution role's credentials.

### `CloudWatchLogDelivery` — The Required `"*"` Resource

```hcl
# Log-delivery management actions are not resource-scopable in IAM and
# MUST use "*". This is the documented requirement for Step Functions
# logging configuration, not an over-grant we can tighten.
Action = [
  "logs:CreateLogDelivery",   "logs:GetLogDelivery",
  "logs:UpdateLogDelivery",   "logs:DeleteLogDelivery",
  "logs:ListLogDeliveries",   "logs:PutResourcePolicy",
  "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
]
Resource = "*"
```

These `logs:*Delivery` actions manage the logging delivery configuration — the binding between the Step Functions state machine and its CloudWatch log group. AWS IAM does not support resource-level restrictions for these actions. Any attempt to scope them to a specific log group ARN results in `AccessDeniedException` when Step Functions tries to set up logging on the first execution.

This is not a misconfiguration or an oversight. The AWS documentation for Step Functions logging explicitly states that these actions require `"*"` as the resource. The comment in the Terraform file records this rationale so future reviewers do not "fix" it and break logging.

---

## Role 3 — Lambda Execution Role

```hcl
resource "aws_iam_role" "lambda_slack_role" {
  assume_role_policy = jsonencode({
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
```

The Slack notifier Lambda makes no AWS API calls — it only reads an environment variable and makes an outbound HTTPS call to Slack. The only IAM permission it needs is the ability to write its logs to CloudWatch, which `AWSLambdaBasicExecutionRole` provides (`logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`).

No S3, no SNS, no Glue, no Step Functions permissions. This is the minimal possible Lambda role.

---

## The Ingestion Policy — No EventBridge Role

```hcl
resource "aws_iam_policy" "ingestion" {
  Statement = [
    {
      Sid      = "UploadRawFiles"
      Action   = ["s3:PutObject"]
      Resource = ["${data_bucket_arn}/raw/*"]
    },
    {
      Sid      = "StartEtlBatch"
      Action   = ["states:StartExecution"]
      Resource = [state_machine_arn]
    }
  ]
}
```

This policy is attached to the developer or CI principal that runs `ingest.py`. It is not a role — it is a standalone policy that gets attached to whatever IAM identity the operator uses.

**Why no EventBridge role:** In an EventBridge-triggered design, an EventBridge rule would need an IAM role with `states:StartExecution` to invoke Step Functions. This project does not use EventBridge — `ingest.py` calls `states:StartExecution` directly using the operator's credentials. The ingestion policy is what provides that permission.

**`UploadRawFiles`:** Scoped to `raw/*` within the data bucket only. The ingestion principal cannot write to `lakehouse-dwh/`, `rejected/`, `archived/`, or any other prefix. A credential leak cannot corrupt processed data.

**`StartEtlBatch`:** Scoped to the specific state machine ARN. The ingestion principal cannot start any other Step Functions state machine in the account.

**What the ingestion principal cannot do:** `s3:GetObject` (cannot read existing data), `s3:DeleteObject` (cannot delete files), `glue:StartJobRun` (cannot trigger Glue jobs directly), `states:StopExecution` (cannot cancel running pipelines), `states:DescribeExecution` (cannot inspect execution state). Attaching this policy to a CI runner or developer workstation gives it exactly the rights needed to ingest one batch and nothing more.

---

## Permission Interaction Summary

| Action | Glue Role | SFN Role | Lambda Role | Ingestion Policy |
|---|---|---|---|---|
| Read `raw/` CSV | ✅ | ❌ | ❌ | ❌ |
| Write `lakehouse-dwh/` | ✅ | ❌ | ❌ | ❌ |
| Write `rejected/` | ✅ | ❌ | ❌ | ❌ |
| Upload `raw/` file | ❌ | ❌ | ❌ | ✅ |
| Start Step Functions | ❌ | ❌ | ❌ | ✅ |
| Start Glue job | ❌ | ✅ | ❌ | ❌ |
| Run Athena query | ❌ | ✅ | ❌ | ❌ |
| Publish to SNS | ✅ | ✅ | ❌ | ❌ |
| Register catalog table | ✅ | ❌ | ❌ | ❌ |
| Write CloudWatch logs | ✅ | ✅ | ✅ | ❌ |
