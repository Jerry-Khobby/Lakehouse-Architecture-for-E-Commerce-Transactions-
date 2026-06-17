# Terraform Resource Dependencies — `depends_on` Chains and Ordering

## Overview

Terraform builds an implicit dependency graph from resource references — if resource B uses `resource.A.id`, Terraform knows to create A before B. But some dependencies are not expressed through attribute references: the relationship is a sequencing constraint, not a data flow. These must be declared explicitly with `depends_on`. This document covers every explicit `depends_on` in the pipeline, why it is needed, and what happens if it is removed.

---

## Lake Formation: Settings Before Permissions

```hcl
resource "aws_lakeformation_data_lake_settings" "main" {
  admins = [local.lf_admin_arn]
}

resource "aws_lakeformation_permissions" "glue_role_database" {
  principal   = aws_iam_role.glue.arn
  permissions = ["CREATE_TABLE", "DESCRIBE"]

  database {
    name = aws_glue_catalog_database.lakehouse.name
  }

  depends_on = [aws_lakeformation_data_lake_settings.main]
}

resource "aws_lakeformation_permissions" "glue_role_tables" {
  principal   = aws_iam_role.glue.arn
  permissions = ["SELECT", "INSERT", "ALTER", "DESCRIBE"]

  table {
    database_name = aws_glue_catalog_database.lakehouse.name
    wildcard      = true
  }

  depends_on = [aws_lakeformation_data_lake_settings.main]
}

# All other aws_lakeformation_permissions resources also depend_on the settings
```

### Why `depends_on` Is Required Here

Terraform sees no attribute reference from `aws_lakeformation_permissions` to `aws_lakeformation_data_lake_settings`. The permissions resource references `aws_iam_role.glue.arn` and `aws_glue_catalog_database.lakehouse.name` — neither of which comes from the settings resource. Without `depends_on`, Terraform may attempt to create both the settings and the permissions resources concurrently.

The Lake Formation API enforces a sequencing rule: the caller must be a registered Lake Formation admin to grant Lake Formation permissions to other principals. `aws_lakeformation_data_lake_settings` registers `local.lf_admin_arn` as the LF admin. If `aws_lakeformation_permissions` runs before this registration completes, the Lake Formation API returns:

```
AccessDeniedException: Insufficient Lake Formation permission(s) on
arn:aws:iam::123456789012:role/ecom-lakehouse-dev-glue-role:
Required Lake Formation permissions missing on the database.
```

This error is misleading — it looks like a permissions problem on the Glue role, but the real issue is that the Terraform caller is not yet a registered LF admin at the moment the grant is attempted.

`depends_on = [aws_lakeformation_data_lake_settings.main]` ensures the settings resource (and therefore the admin registration) is fully applied before any permissions resource is processed.

### Why All Four Permission Grants Need It

```hcl
# Grant 1: Glue role — database-level CREATE_TABLE, DESCRIBE
aws_lakeformation_permissions.glue_role_database

# Grant 2: Glue role — table-level SELECT, INSERT, ALTER, DESCRIBE (wildcard)
aws_lakeformation_permissions.glue_role_tables

# Grant 3: Step Functions role — SELECT on all tables (for AthenaValidation)
aws_lakeformation_permissions.sfn_role_tables

# Grant 4: Athena workgroup principal — SELECT on all tables
aws_lakeformation_permissions.athena_principal_tables
```

All four grants depend on the settings resource because all four are LF permission grants that require the caller to be an LF admin. None of them have an implicit attribute reference to the settings resource. Each must declare `depends_on` independently.

---

## Step Functions: State Machine After CloudWatch Log Resource Policy

```hcl
resource "aws_cloudwatch_log_resource_policy" "sfn_logging" {
  policy_name = "ecom-lakehouse-${var.environment}-sfn-log-delivery"

  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = ["logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
                   "logs:DeleteLogDelivery", "logs:ListLogDeliveries",
                   "logs:PutLogEvents", "logs:PutResourcePolicy",
                   "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"]
      Resource  = "*"
    }]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "ecom-lakehouse-${var.environment}-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({ ... })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  depends_on = [aws_cloudwatch_log_resource_policy.sfn_logging]
}
```

### Why `depends_on` Is Required Here

`aws_sfn_state_machine.pipeline` references `aws_cloudwatch_log_group.step_functions.arn` (in `logging_configuration.log_destination`). This creates an implicit dependency on the log group — Terraform will not create the state machine before the log group exists. But there is no attribute reference from the state machine to the resource policy, `aws_cloudwatch_log_resource_policy.sfn_logging`.

When Step Functions creates a state machine with `logging_configuration.level = "ALL"`, it immediately attempts to validate that it can write to the specified log group. This validation calls CloudWatch Logs API operations (`logs:DescribeLogGroups`, `logs:PutResourcePolicy`). These operations are governed by the CloudWatch Logs resource policy.

If the state machine is created before the resource policy is attached:
- The state machine is created successfully (AWS does not block creation on log delivery permission)
- The first execution's log delivery fails with `AccessDeniedException`
- CloudWatch Logs does not receive execution events
- The CloudWatch log group appears empty after pipeline runs
- Debugging is impossible because there are no step-level events

`depends_on = [aws_cloudwatch_log_resource_policy.sfn_logging]` ensures the resource policy — which grants `states.amazonaws.com` the necessary CloudWatch permissions — is in place before the state machine is created and attempts log delivery.

### The `"*"` Resource in the Log Resource Policy

```hcl
Action   = ["logs:CreateLogDelivery", ..., "logs:PutLogEvents", ...]
Resource = "*"
```

`Resource = "*"` is required here, not a security weakness. The specific CloudWatch permissions that Step Functions needs — `logs:CreateLogDelivery`, `logs:GetLogDelivery`, `logs:UpdateLogDelivery`, `logs:DeleteLogDelivery`, `logs:ListLogDeliveries` — are **log delivery management actions** that operate on the log delivery subsystem, not on individual log groups or streams. The CloudWatch Logs API does not support resource-level ARN scoping for these actions. Attempting to scope them to a specific log group ARN would cause Step Functions to fail with `ResourceNotFoundException` because the action does not operate on a log group ARN.

`logs:PutLogEvents`, `logs:DescribeLogGroups`, `logs:DescribeResourcePolicies`, and `logs:PutResourcePolicy` can in principle be scoped, but the resource policy is a service-level trust document — its purpose is to tell CloudWatch Logs "trust the Step Functions service principal." Scoping it to a single log group ARN would require updating it every time the log group name changes. The `"*"` scope on this resource policy is consistent with the AWS documentation recommendation for Step Functions log delivery.

---

## Glue Jobs: After Script Upload and Utils Zip

Glue jobs reference their script S3 location via Terraform interpolation:

```hcl
command {
  script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.products_job_script.key}"
}

default_arguments = {
  "--extra-py-files" = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.glue_utils_zip.key}"
}
```

The attribute reference `aws_s3_object.products_job_script.key` creates an **implicit** dependency: Terraform will not create the Glue job before the S3 object exists. No explicit `depends_on` is needed for the script upload. The same applies to `aws_s3_object.glue_utils_zip`.

However, the S3 objects depend implicitly on the scripts bucket:

```
aws_s3_bucket.scripts
        ↓ (implicit: bucket = aws_s3_bucket.scripts.id)
aws_s3_object.products_job_script
        ↓ (implicit: script_location uses aws_s3_object.products_job_script.key)
aws_glue_job.products
```

The full implicit chain means all three resources are created in the correct order without any explicit `depends_on`.

---

## Glue Catalog Database: After Lake Formation Resource Registration

```hcl
resource "aws_lakeformation_resource" "data_bucket" {
  arn = aws_s3_bucket.data.arn
}

resource "aws_glue_catalog_database" "lakehouse" {
  name         = "ecom_lakehouse_${var.environment}"
  location_uri = "s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}"

  depends_on = [aws_lakeformation_resource.data_bucket]
}
```

`aws_lakeformation_resource` registers the S3 data bucket with Lake Formation, making Lake Formation the governing authority for access to objects in that bucket. `aws_glue_catalog_database` creates the Glue catalog database with `location_uri` pointing into that S3 bucket.

If the catalog database is created before the LF resource registration, the database creation may succeed, but the first Glue job that attempts to create a table in the database via `spark.sql("CREATE TABLE IF NOT EXISTS ...")` will fail because Lake Formation has not yet taken ownership of the `location_uri` S3 path. The LF intercept on the `glue:CreateTable` call checks whether the target location is under a registered LF resource; if not, it may bypass LF permission checks or fail depending on the account's `create_database_default_permission` setting.

`depends_on = [aws_lakeformation_resource.data_bucket]` ensures LF has registered the bucket before the catalog database is created with that bucket's path.

---

## One-Time Catalog Cleanup: After Crawlers

```hcl
resource "terraform_data" "drop_stale_catalog_tables" {
  triggers_replace = [aws_glue_catalog_database.lakehouse.id]

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = <<-EOT
      foreach ($table in @("products", "orders", "order_items")) {
        aws glue delete-table --database-name ${aws_glue_catalog_database.lakehouse.name} --name $table 2>&1 | Out-Null; exit 0
      }
    EOT
  }

  depends_on = [
    aws_glue_crawler.products,
    aws_glue_crawler.orders,
    aws_glue_crawler.order_items,
  ]
}
```

The `depends_on` on the three crawlers prevents the cleanup provisioner from running before the crawlers are registered. If the cleanup ran before the crawlers existed, the `aws glue delete-table` commands would fail (no table to delete) — which is harmless due to `exit 0`, but the `depends_on` makes the intent explicit: this cleanup is associated with the crawler-managed table lifecycle, not with the database alone.

**This block must be removed after the first successful pipeline run.** It runs on every `terraform apply` as long as it is present. On subsequent applies after tables are registered, it drops the tables immediately after Terraform creates the crawlers — before the pipeline runs and recreates them. Leaving it in causes a delete-on-every-apply loop. See [Glue_Data_Catalog.md](Glue_Data_Catalog.md) for the full context.

---

## Dependency Graph Summary

```
aws_s3_bucket.data
        │ (implicit: arn reference)
        ▼
aws_lakeformation_resource.data_bucket
        │ depends_on ←──────────────────────────────────────────┐
        ▼                                                        │
aws_glue_catalog_database.lakehouse                             │
        │ (implicit: name reference)                            │
        ▼                                                        │
aws_lakeformation_data_lake_settings.main                       │
        │ depends_on ←─────────────────────────────────────┐   │
        ▼                                                   │   │
aws_lakeformation_permissions.*  (all four grants)         │   │
                                                           │   │
aws_cloudwatch_log_resource_policy.sfn_logging             │   │
        │ depends_on                                       │   │
        ▼                                                   │   │
aws_sfn_state_machine.pipeline                             │   │
                                                           │   │
aws_s3_bucket.scripts ──(implicit)──► aws_s3_object.*     │   │
        │                                    │             │   │
        │                              (implicit)          │   │
        ▼                                    ▼             │   │
                                    aws_glue_job.*         │   │
                                                           │   │
aws_glue_crawler.* ──────────────depends_on───────────────┘   │
        │                                                        │
        └──────────────── terraform_data.drop_stale depends_on ─┘
```
