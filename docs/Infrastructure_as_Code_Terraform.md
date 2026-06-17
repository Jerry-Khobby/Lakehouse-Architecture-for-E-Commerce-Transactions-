# Infrastructure as Code — Terraform Project Structure

## Overview

All AWS infrastructure for this pipeline is defined in Terraform HCL files under the `terraform/` directory. No resources are created manually via the console. This document covers which `.tf` file owns which resources, how `locals` centralise naming and derived values, variable declarations and their defaults, and how `etag` on `aws_s3_object` resources ensures Glue scripts are re-uploaded whenever their source files change.

---

## File Structure

```
terraform/
├── main.tf             — S3 buckets, encryption, public access blocks, lifecycle, IAM roles/policies,
│                          Lake Formation settings and permissions, Glue Data Catalog database and crawlers,
│                          Athena workgroup, CloudWatch log groups and log resource policy, SNS topic
├── glue_jobs.tf        — aws_glue_job resources, S3 script object uploads (aws_s3_object),
│                          Glue job IAM policy attachments, Glue utils zip object
├── step_functions.tf   — aws_sfn_state_machine, Step Functions IAM role and inline policy
├── lambda.tf           — aws_lambda_function (Slack notifier), Lambda IAM role,
│                          aws_sns_topic_subscription, aws_lambda_permission (SNS invoke)
├── variables.tf        — all var.* declarations with types, defaults, and descriptions
├── outputs.tf          — terraform output values consumed by ingestion/pipeline.py
└── terraform.tfvars    — (not committed) local environment overrides for var values
```

### `main.tf` — The Infrastructure Core

`main.tf` owns all foundational resources. Every other `.tf` file depends on resources declared here. The ordering within `main.tf` follows the dependency graph top-to-bottom:

1. **Data sources** — `aws_caller_identity.current`, `aws_region.current`, `aws_iam_session_context.current` (used for the Lake Formation admin ARN)
2. **Locals block** — derived names and computed values
3. **S3 buckets** — data, scripts, athena_results, logs (in this order; logs bucket is created first because the data bucket references it for access logging)
4. **S3 configuration** — encryption, public access blocks, lifecycle, versioning, server access logging (all as separate resources per bucket)
5. **S3 bucket policies** — TLS-only Deny policy applied to all four buckets via `for_each`
6. **IAM roles and policies** — Glue role, Step Functions role, Lambda role, ingestion policy
7. **Lake Formation** — `aws_lakeformation_data_lake_settings`, `aws_lakeformation_resource`, `aws_lakeformation_permissions` (four grants)
8. **Glue Data Catalog** — `aws_glue_catalog_database` with `location_uri`
9. **Glue crawlers** — one per dataset
10. **Athena workgroup**
11. **CloudWatch log groups** — Glue output, Glue error, Step Functions
12. **CloudWatch Logs resource policy** — allows Step Functions to deliver logs
13. **SNS topic** — single pipeline alert topic
14. **`terraform_data` one-time cleanup block** — drops stale catalog tables on apply

### `glue_jobs.tf` — Job Definitions and Script Uploads

```hcl
resource "aws_s3_object" "glue_utils_zip" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/utils/utils.zip"
  source = "${path.module}/../glue_jobs/utils/utils.zip"
  etag   = filemd5("${path.module}/../glue_jobs/utils/utils.zip")
}

resource "aws_s3_object" "products_job_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/products_job.py"
  source = "${path.module}/../glue_jobs/products_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/products_job.py")
}

resource "aws_glue_job" "products" {
  name     = "ecom-lakehouse-${var.environment}-products-job"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.products_job_script.key}"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "delta"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.glue_utils_zip.key}"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--conf" = join(" ", [
      "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
      "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
      "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
      "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
      "--conf spark.sql.session.timeZone=UTC",
    ])
    "--SNS_TOPIC_ARN"           = aws_sns_topic.pipeline_alerts.arn
    "--GLUE_DATABASE"           = aws_glue_catalog_database.lakehouse.name
    "--DATA_BUCKET"             = aws_s3_bucket.data.id
    "--PROCESSED_DATA_PREFIX"   = var.processed_data_prefix
    "--ENVIRONMENT"             = var.environment
    "--STRICT_REFERENTIAL_INTEGRITY" = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}
```

`glue_jobs.tf` repeats this pattern for all three jobs (`products`, `orders`, `order_items`) and their corresponding script uploads.

### `step_functions.tf` — State Machine

Declares `aws_sfn_state_machine` with the full ASL definition as a `jsonencode()`-rendered Terraform heredoc. The state machine ARN is threaded into the Glue job arguments (for the `--STEP_FUNCTION_EXECUTION_ID` context) via Terraform interpolation. The Step Functions IAM role is also declared here, separate from the Glue role in `main.tf`, because its permission set is distinct.

### `lambda.tf` — Optional Slack Notifier

Uses a `count` gate controlled by `local.slack_enabled`:

```hcl
locals {
  slack_enabled = var.slack_webhook_url != "" ? 1 : 0
}

resource "aws_lambda_function" "slack_notifier" {
  count = local.slack_enabled
  ...
}

resource "aws_sns_topic_subscription" "lambda" {
  count     = local.slack_enabled
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.slack_notifier[0].arn
}
```

When `var.slack_webhook_url` is empty (the default), `local.slack_enabled = 0`, and `count = 0` means no Lambda, no role, no subscription is created. The SNS topic still exists — email subscribers and CloudWatch alarms continue to function. Only the Lambda → Slack forwarding path is absent.

---

## The `locals` Block

```hcl
locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  # Bucket names incorporate account ID to guarantee global uniqueness
  data_bucket_name          = "ecom-lakehouse-${var.environment}-data-${local.account_id}"
  scripts_bucket_name       = "ecom-lakehouse-${var.environment}-scripts-${local.account_id}"
  athena_results_bucket_name = "ecom-lakehouse-${var.environment}-athena-results-${local.account_id}"
  logs_bucket_name          = "ecom-lakehouse-${var.environment}-logs-${local.account_id}"

  # Lake Formation admin: strip the assumed-role session suffix from the caller ARN
  # aws_iam_session_context.current.issuer_arn returns the role ARN without :session-name
  lf_admin_arn = data.aws_iam_session_context.current.issuer_arn

  slack_enabled = var.slack_webhook_url != "" ? 1 : 0
}
```

### Why `locals` Over Repeated Interpolations

Each bucket name incorporates the environment and account ID. Without `locals`, every resource that references a bucket name would repeat `"ecom-lakehouse-${var.environment}-data-${data.aws_caller_identity.current.account_id}"`. That string appears in S3 bucket resource names, IAM policy Resource ARNs, Lake Formation resource registrations, and lifecycle rules — approximately 20 places. A typo in any one produces an inconsistency that Terraform silently accepts (the policy ARN does not match the actual bucket ARN). Centralising the name in `locals` means a single definition that all references read from.

### `local.lf_admin_arn` — The Session Context Strip

AWS IAM session context is the mechanism that resolves `arn:aws:sts::123456789012:assumed-role/OrganizationAccountAccessRole/session-name` (an assumed-role session ARN) to `arn:aws:iam::123456789012:role/OrganizationAccountAccessRole` (the underlying IAM role ARN). Lake Formation admin registration requires the role ARN, not the session ARN. `data.aws_iam_session_context.current.issuer_arn` performs this resolution automatically. Without it, Terraform running in a CI/CD environment where the caller is an assumed-role session would register a session ARN as the LF admin — an ARN that changes per session and does not persist.

---

## Variable Declarations and Defaults

```hcl
variable "environment" {
  type        = string
  default     = "dev"
  description = "Deployment environment: dev, staging, or prod"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "processed_data_prefix" {
  type    = string
  default = "lakehouse-dwh/"
  description = "S3 key prefix for Silver layer Delta tables"
}

variable "raw_data_prefix" {
  type    = string
  default = "raw/"
}

variable "slack_webhook_url" {
  type      = string
  default   = ""
  sensitive = true
  description = "Slack incoming webhook URL. Leave empty to disable Lambda notifier."
}

variable "large_order_threshold" {
  type    = number
  default = 10000
  description = "Orders above this total_amount are soft-flagged to flagged/orders/"
}
```

### `sensitive = true` on `slack_webhook_url`

`sensitive = true` prevents the webhook URL from appearing in `terraform plan` output, `terraform show` output, and CI/CD logs. Without it, any `terraform plan` run in a CI environment would print the webhook URL in plaintext. The value is still stored in Terraform state (which should itself be encrypted and access-controlled via S3 backend with SSE and IAM).

### `validation` Block on `environment`

The `validation` block causes `terraform plan` to fail immediately with a clear error message if `var.environment` is set to anything other than `dev`, `staging`, or `prod`. Without this, an invalid value like `"development"` would be silently accepted and produce bucket names like `"ecom-lakehouse-development-data-123456789012"` — valid S3 names but inconsistent with the naming convention, potentially creating orphaned resources.

---

## `etag` — Content-Aware Script Re-Uploads

Glue job Python scripts are uploaded to S3 as `aws_s3_object` resources. The `source` attribute is a file path:

```hcl
resource "aws_s3_object" "orders_job_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/orders_job.py"
  source = "${path.module}/../glue_jobs/orders_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/orders_job.py")
}
```

### The Problem Without `etag`

Terraform tracks S3 object resources by their `bucket` and `key`. If the `source` file path does not change (it is a constant), Terraform sees no change to the resource between plans — it does not re-read the file contents to check if they differ. A developer modifying `orders_job.py` and running `terraform apply` would see `No changes` for the script upload. The old version of the script remains in S3. The Glue job runs the outdated code silently.

### How `etag` Fixes It

`filemd5("${path.module}/../glue_jobs/orders_job.py")` computes the MD5 hash of the local file at plan time. Terraform stores this hash in its state. On the next `terraform plan`, if the file has changed, `filemd5()` returns a different hash. Terraform sees that `etag` has changed and marks the `aws_s3_object` as needing an update — the file is re-uploaded on `terraform apply`.

The `etag` attribute maps to the S3 ETag header that S3 assigns to every object (which for unencrypted uploads is the MD5 of the content). Terraform's comparison is: `state.etag != filemd5(source)` → plan an update.

### Scope of `etag` Coverage

The same pattern applies to:
- `aws_s3_object.products_job_script` — `products_job.py`
- `aws_s3_object.orders_job_script` — `orders_job.py`
- `aws_s3_object.order_items_job_script` — `order_items_job.py`
- `aws_s3_object.glue_utils_zip` — `utils.zip` (the packaged utility module)

The utils zip requires the developer to re-run `make package` (or the equivalent zip command) before `terraform apply` — Terraform detects the new zip's changed MD5, but it cannot generate the zip itself. A CI/CD pipeline that runs `zip -r utils.zip utils/` before `terraform apply` ensures the zip is always current.

---

## `outputs.tf` — Consumed by Ingestion Scripts

```hcl
output "state_machine_arn" {
  value       = aws_sfn_state_machine.pipeline.arn
  description = "ARN of the Step Functions state machine — used by ingestion/pipeline.py"
}

output "data_bucket_name" {
  value       = aws_s3_bucket.data.id
  description = "Name of the data S3 bucket — used by ingestion/pipeline.py for uploads"
}

output "glue_database_name" {
  value       = aws_glue_catalog_database.lakehouse.name
}

output "sns_topic_arn" {
  value       = aws_sns_topic.pipeline_alerts.arn
}
```

`ingestion/pipeline.py` calls `terraform output -json` to read these values at runtime. See [Ingestion_Script.md](Ingestion_Script.md) for the full implementation. Outputs are the contract between the Terraform-managed infrastructure and the Python ingestion code — changing an output name is a breaking change that requires updating `pipeline.py`.
