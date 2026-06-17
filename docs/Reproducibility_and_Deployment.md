# Reproducibility and Deployment — From Fresh Clone to Running Pipeline

## Overview

A fresh clone of this repository, combined with AWS credentials and the commands in this document, produces a fully operational pipeline. No manual console steps are required for infrastructure creation. Terraform provisions every AWS resource; `ingest.py` uploads source data and triggers the first execution. This document is a complete walkthrough of the end-to-end deployment sequence with explanations for decisions that would otherwise be unclear.

---

## Prerequisites

### Tools

| Tool | Version | Install |
|---|---|---|
| Python | 3.11+ | [python.org](https://python.org) or `pyenv` |
| Terraform | 1.7+ | `brew install terraform` / [tfenv](https://github.com/tfutils/tfenv) |
| AWS CLI | v2 | [aws.amazon.com/cli](https://aws.amazon.com/cli) |
| Java JRE | 11 or 17 | Required by PySpark for local test runs |
| Git | Any | Pre-installed on most systems |

### AWS Account Requirements

- An AWS account with permissions to create: IAM roles/policies, S3 buckets, Glue jobs/crawlers/database, Step Functions state machine, Lambda function, SNS topic, CloudWatch log groups, Lake Formation settings, Athena workgroup
- Lake Formation must not be in the "hybrid access mode" default state — if this is a fresh AWS account, Lake Formation will be in hybrid mode; the Terraform `aws_lakeformation_data_lake_settings` resource will register the deploy caller as the LF admin, transitioning the account to LF-governed mode for resources managed by this project
- The Terraform backend (S3 + DynamoDB for state locking) should be pre-created or the `backend` block in `terraform/main.tf` should be updated to point to an existing state bucket

---

## Step 1 — Clone and Install Python Dependencies

```bash
git clone https://github.com/<org>/lakehouse-architecture.git
cd "lakehouse-architecture"

# Install Glue job and test dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

`requirements.txt` contains the runtime dependencies shared between the Glue jobs and the ingestion scripts: `boto3`, `openpyxl`, `pyspark==3.3.2`. `requirements-dev.txt` adds test dependencies: `pytest`, `moto`, `ruff`, `mypy`.

**Why `pyspark==3.3.2`:** The Glue 4.0 runtime uses PySpark 3.3.2. Installing the exact version locally ensures that local unit tests and type checks run against the same PySpark API surface that the production Glue jobs use.

---

## Step 2 — Configure AWS Credentials

```bash
# Option A: Named profile (recommended for development)
aws configure --profile lakehouse-dev
# Enter: AWS Access Key ID, Secret Access Key, region (eu-west-1), output format (json)

export AWS_PROFILE=lakehouse-dev

# Option B: Environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=eu-west-1
```

Verify access:
```bash
aws sts get-caller-identity
```

Expected output:
```json
{
    "UserId": "AIDAI...",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/deploy-user"
}
```

The caller identity ARN is used by Terraform's `data.aws_iam_session_context.current.issuer_arn` to register the Lake Formation admin. If this is an assumed-role session (`arn:aws:sts::123456789012:assumed-role/...`), `aws_iam_session_context` strips the session suffix to produce the underlying role ARN.

---

## Step 3 — Create `terraform.tfvars`

```bash
cat > terraform/terraform.tfvars <<EOF
environment           = "dev"
aws_region            = "eu-west-1"
processed_data_prefix = "lakehouse-dwh/"
# slack_webhook_url   = "https://hooks.slack.com/services/..."  # uncomment to enable Slack
EOF
```

`terraform.tfvars` is not committed to the repository (it is in `.gitignore`) because it can contain the `slack_webhook_url` secret. Each developer or CI environment creates their own `terraform.tfvars` or uses `TF_VAR_*` environment variables.

---

## Step 4 — Package the Glue Utils Zip

The Glue utility module (`glue_jobs/utils/`) must be packaged as a zip before Terraform can upload it to S3:

```bash
cd glue_jobs
zip -r utils/utils.zip utils/__init__.py utils/common.py utils/monitor.py utils/notifier.py
cd ..
```

Verify:
```bash
unzip -l glue_jobs/utils/utils.zip
```

Expected: four Python files listed. If the zip is stale (created from an older version of the source), Terraform's `filemd5()` will compute the hash of the stale zip and upload it. Always re-create the zip from the current source before `terraform apply`.

---

## Step 5 — Terraform Init

```bash
cd terraform
terraform init
```

`terraform init` downloads the AWS provider plugin (`hashicorp/aws` ~> 5.0) and initialises the state backend. On a fresh clone, the `.terraform/` directory does not exist — `init` creates it.

If the backend configuration in `main.tf` points to an S3 state bucket that does not exist yet:

```hcl
terraform {
  backend "s3" {
    bucket = "ecom-lakehouse-terraform-state"
    key    = "dev/terraform.tfstate"
    region = "eu-west-1"
  }
}
```

The state bucket must exist before `terraform init`. Create it manually once:

```bash
aws s3api create-bucket \
  --bucket ecom-lakehouse-terraform-state \
  --region eu-west-1 \
  --create-bucket-configuration LocationConstraint=eu-west-1

aws s3api put-bucket-versioning \
  --bucket ecom-lakehouse-terraform-state \
  --versioning-configuration Status=Enabled
```

Alternatively, use `terraform init -backend=false` and migrate to the remote backend later.

---

## Step 6 — Terraform Plan

```bash
terraform plan -out=tfplan
```

Review the plan output. On a fresh account, the plan will show approximately 60–80 resources to create. Key things to verify:

- `aws_lakeformation_data_lake_settings.main` — confirms the current caller will become LF admin
- `aws_s3_bucket.data` — bucket name incorporates the account ID (check it matches your account)
- `aws_glue_job.*` — all three jobs appear with the correct S3 script locations
- `aws_sfn_state_machine.pipeline` — Step Functions state machine is present
- `aws_lambda_function.slack_notifier` — should appear only if `slack_webhook_url` is set in `terraform.tfvars`

If the plan shows unexpected existing resources (from a previous partial apply), run `terraform state list` to inspect and `terraform import` if necessary.

---

## Step 7 — Terraform Apply

```bash
terraform apply tfplan
```

Terraform creates all resources in dependency order. This takes approximately 3–5 minutes. Resources are created in parallel where possible; the `depends_on` constraints (Lake Formation settings before permissions, CloudWatch resource policy before Step Functions state machine) enforce sequencing where needed.

**First-apply common issues:**

| Error | Cause | Fix |
|---|---|---|
| `AccessDeniedException: Missing Lake Formation permission(s)` | LF permissions tried before LF settings | `depends_on` is missing or was removed — restore it |
| `IllegalArgumentException: Can not create a Path from an empty string` | `aws_glue_catalog_database.lakehouse` missing `location_uri` | Ensure `location_uri = "s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}"` is set |
| `Error: creating S3 Bucket (name): BucketAlreadyExists` | Another account owns that bucket name | Change the bucket name suffix in `locals` |
| `ConflictException: Resource already exists` | State machine already exists from a previous apply | Either import it or `terraform destroy` the state machine and re-apply |

After a successful apply, the output section shows the Terraform outputs:

```
Outputs:

data_bucket_name    = "ecom-lakehouse-dev-data-123456789012"
glue_database_name  = "ecom_lakehouse_dev"
sns_topic_arn       = "arn:aws:sns:eu-west-1:123456789012:ecom-lakehouse-dev-alerts"
state_machine_arn   = "arn:aws:states:eu-west-1:123456789012:stateMachine:ecom-lakehouse-dev-pipeline"
```

These values are read by `ingestion/pipeline.py` via `terraform output -json` — no manual copying is required.

---

## Step 8 — Subscribe to SNS Alerts

The SNS topic exists after `terraform apply`, but email subscriptions are not managed by Terraform (they require the subscriber to confirm via email, which cannot be automated in Terraform without manual intervention):

```bash
aws sns subscribe \
  --topic-arn "$(terraform output -raw sns_topic_arn)" \
  --protocol email \
  --notification-endpoint your@email.com
```

Check your email for the AWS SNS confirmation message and click the confirmation link. Until the subscription is confirmed, SNS notifications are not delivered to your email.

---

## Step 9 — Run the Ingestion Script

```bash
cd ..   # Back to repo root
python ingestion/ingest.py
```

`ingest.py` calls `run_ingestion()` which:
1. Reads `state_machine_arn` and `data_bucket_name` from `terraform output -json` (runs `terraform` subprocess pointed at `terraform/`)
2. Converts `ingestion/data/products.xlsx`, `orders.xlsx`, `order_items.xlsx` to CSV
3. Uploads all three CSVs to `raw/apr_2025/<dataset>/` on the data bucket
4. Calls `sfn:StartExecution` with a single execution input containing all three S3 keys

Expected terminal output:
```
INFO  Uploaded products → s3://ecom-lakehouse-dev-data-123456789012/raw/apr_2025/products/products.csv
INFO  Uploaded orders → s3://ecom-lakehouse-dev-data-123456789012/raw/apr_2025/orders/orders_apr_2025.csv
INFO  Uploaded order_items → s3://ecom-lakehouse-dev-data-123456789012/raw/apr_2025/order_items/order_items_apr_2025.csv
INFO  Started execution: arn:aws:states:eu-west-1:123456789012:execution:ecom-lakehouse-dev-pipeline:apr_2025-20250430T092211
```

---

## Step 10 — Monitor the Pipeline Execution

### Step Functions Console

Navigate to AWS Step Functions → State machines → `ecom-lakehouse-dev-pipeline` → Executions. The running execution appears. Click it to see the visual workflow with each state highlighted as it completes.

Typical execution timeline for the April batch:
- `ProcessProducts`: ~2 minutes
- `ProcessOrders`: ~2 minutes
- `ProcessOrderItems`: ~3 minutes (referential integrity joins add time)
- `RunCrawlers` (parallel): ~1 minute
- Total: ~8–9 minutes

### CloudWatch Logs

Glue job logs stream continuously to CloudWatch during execution:

```bash
# Products job output log — most recent 50 lines
aws logs tail /aws-glue/jobs/output \
  --filter-pattern "products_job" \
  --since 30m \
  --follow
```

Look for the `log_counts` line at the end of the Validate stage:
```
[products] total_read=49 | valid=49 | rejected=0 | pass_rate=100.00%
```

And the Delta history after the MERGE:
```
+-------+---------+---------------------------------------------------+
|version|operation|operationMetrics                                   |
+-------+---------+---------------------------------------------------+
|1      |MERGE    |{numTargetRowsInserted -> 49, numTargetRowsUpdated -> 0, ...}
```

### SNS Email Notifications

Each Glue job stage sends an SNS notification on start, success, and failure (via `PipelineMonitor`). The Step Functions state machine sends a pipeline-level success or failure notification after the final state. For a clean first run of the April batch, expect 31 email notifications:
- 3 jobs × 5 stages × 2 (start + success) = 30 stage notifications
- 1 pipeline-level `SUCCESS` notification

---

## Step 11 — Verify Delta Tables in Athena

After the pipeline completes, open the Athena console → Query editor → Select database `ecom_lakehouse_dev`.

```sql
-- Verify all three tables are registered
SHOW TABLES IN ecom_lakehouse_dev;

-- Count rows committed to Silver layer
SELECT COUNT(*) FROM ecom_lakehouse_dev.products;      -- expect 49
SELECT COUNT(*) FROM ecom_lakehouse_dev.orders;        -- expect ~850
SELECT COUNT(*) FROM ecom_lakehouse_dev.order_items;   -- expect ~2540

-- Verify Delta metadata is readable
SELECT "$path", COUNT(*) FROM ecom_lakehouse_dev.orders GROUP BY "$path" LIMIT 5;
```

If tables do not appear, check whether the crawlers completed successfully in the `RunCrawlers` Step Functions state and whether `update_catalog_table()` ran in each Glue job's Catalog Update stage.

---

## Re-Running After a Failure

If any pipeline stage fails:

1. Check the SNS failure notification for the error and cause
2. Investigate the CloudWatch log for the specific Glue job stage
3. Fix the issue (source data, code bug, Terraform misconfiguration)
4. Re-run `python ingestion/ingest.py` — the script uploads the same files to the same S3 keys (overwriting) and starts a new Step Functions execution with a new timestamp-suffixed name
5. The MERGE idempotency guarantees that re-running after a partial commit produces no duplicates

For infrastructure changes (Terraform), run `terraform plan && terraform apply` after making `.tf` file changes. Glue job code changes only need `terraform apply` (the `etag` change triggers re-upload automatically).

---

## Deployment Sequence Summary

```
git clone → pip install → aws configure → create terraform.tfvars
    → zip utils → terraform init → terraform plan → terraform apply
    → sns subscribe (email confirm) → python ingestion/ingest.py
    → monitor Step Functions → verify Athena tables
```

Total time from fresh clone to first successful pipeline execution:
- Terraform apply: ~5 minutes
- Glue execution: ~9 minutes
- Total: ~15 minutes
