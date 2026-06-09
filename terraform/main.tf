# ── Data sources ──────────────────────────────────────────────────────────────
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  name_prefix = "${var.project_name}-${var.environment}"

  # Consistent bucket names derived from project + account to guarantee global uniqueness
  data_bucket_name    = "${local.name_prefix}-data-${local.account_id}"
  scripts_bucket_name = "${local.name_prefix}-scripts-${local.account_id}"
  logs_bucket_name    = "${local.name_prefix}-logs-${local.account_id}"
  athena_bucket_name  = "${local.name_prefix}-athena-results-${local.account_id}"
}

# ─────────────────────────────────────────────────────────────────────────────
# S3 BUCKETS
# ─────────────────────────────────────────────────────────────────────────────

# -- Access logs bucket (created first; other buckets reference it) ------------
resource "aws_s3_bucket" "logs" {
  bucket        = local.logs_bucket_name
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_ownership_controls" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule { object_ownership = "BucketOwnerPreferred" }
}

resource "aws_s3_bucket_acl" "logs" {
  depends_on = [aws_s3_bucket_ownership_controls.logs]
  bucket     = aws_s3_bucket.logs.id
  acl        = "log-delivery-write"
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    id     = "expire-old-logs"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = var.log_retention_days }
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -- Main data bucket (raw / processed / archived / rejected) -----------------
resource "aws_s3_bucket" "data" {
  bucket        = local.data_bucket_name
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "data" {
  bucket        = aws_s3_bucket.data.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3-access-logs/data-bucket/"
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  # Raw files: move to Infrequent Access after 30 days once archived
  rule {
    id     = "raw-ia-transition"
    status = "Enabled"
    filter { prefix = var.raw_data_prefix }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  # Archived files: IA after 30 days, Glacier after 90
  rule {
    id     = "archived-tiering"
    status = "Enabled"
    filter { prefix = var.archived_data_prefix }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  # Noncurrent versions: expire after configured days
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter { prefix = "" }
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiry_days
    }
  }

  # Rejected records: expire after 60 days
  rule {
    id     = "expire-rejected"
    status = "Enabled"
    filter { prefix = var.rejected_data_prefix }
    expiration { days = 60 }
  }

  # Soft-flagged records: expire after 90 days (kept longer for analyst review)
  rule {
    id     = "expire-flagged"
    status = "Enabled"
    filter { prefix = var.flagged_data_prefix }
    expiration { days = 90 }
  }
}

# Logical "folders" — Terraform objects ensure prefixes exist before Glue runs
resource "aws_s3_object" "raw_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.raw_data_prefix
  content = ""
}

resource "aws_s3_object" "processed_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.processed_data_prefix
  content = ""
}

resource "aws_s3_object" "archived_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.archived_data_prefix
  content = ""
}

resource "aws_s3_object" "rejected_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.rejected_data_prefix
  content = ""
}

resource "aws_s3_object" "flagged_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.flagged_data_prefix
  content = ""
}

# -- Glue scripts bucket -------------------------------------------------------
resource "aws_s3_bucket" "scripts" {
  bucket        = local.scripts_bucket_name
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -- Athena results bucket -----------------------------------------------------
resource "aws_s3_bucket" "athena_results" {
  bucket        = local.athena_bucket_name
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 7 }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# S3 BUCKET POLICIES — deny any request not made over TLS
# A baseline control: reject s3 actions when aws:SecureTransport is false so
# data can never be read or written in plaintext over the wire. The condition
# is a Deny, not a public grant, so it coexists with the public-access blocks.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  tls_only_buckets = {
    data           = { id = aws_s3_bucket.data.id, arn = aws_s3_bucket.data.arn }
    scripts        = { id = aws_s3_bucket.scripts.id, arn = aws_s3_bucket.scripts.arn }
    athena_results = { id = aws_s3_bucket.athena_results.id, arn = aws_s3_bucket.athena_results.arn }
    logs           = { id = aws_s3_bucket.logs.id, arn = aws_s3_bucket.logs.arn }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = local.tls_only_buckets
  bucket   = each.value.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM
# ─────────────────────────────────────────────────────────────────────────────

# -- Glue service role ---------------------------------------------------------
resource "aws_iam_role" "glue_role" {
  name = "${local.name_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.name_prefix}-glue-s3-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DataBucketReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:GetObjectVersion", "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*"
        ]
      },
      {
        Sid    = "ScriptsBucketRead"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*"
        ]
      },
      {
        Sid      = "LogsBucketWrite"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.logs.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_catalog" {
  name = "${local.name_prefix}-glue-catalog-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "GlueCatalogAccess"
      Effect = "Allow"
      Action = [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:CreateDatabase",
        "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:CreatePartition", "glue:UpdatePartition", "glue:BatchCreatePartition"
      ]
      Resource = [
        "arn:aws:glue:${local.region}:${local.account_id}:catalog",
        "arn:aws:glue:${local.region}:${local.account_id}:database/${var.glue_database_name}",
        "arn:aws:glue:${local.region}:${local.account_id}:table/${var.glue_database_name}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "glue_cloudwatch" {
  name = "${local.name_prefix}-glue-cw-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "CloudWatchLogs"
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup", "logs:CreateLogStream",
        "logs:PutLogEvents", "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ]
      Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"
    }]
  })
}

# -- Step Functions execution role ---------------------------------------------
resource "aws_iam_role" "sfn_role" {
  name = "${local.name_prefix}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_glue" {
  name = "${local.name_prefix}-sfn-glue-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "StartGlueJobs"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun", "glue:GetJobRun",
          "glue:GetJobRuns", "glue:BatchStopJobRun"
        ]
        Resource = [
          aws_glue_job.products.arn,
          aws_glue_job.orders.arn,
          aws_glue_job.order_items.arn
        ]
      },
      {
        Sid    = "ManageCrawlers"
        Effect = "Allow"
        Action = ["glue:StartCrawler", "glue:GetCrawler"]
        Resource = [
          aws_glue_crawler.products.arn,
          aws_glue_crawler.orders.arn,
          aws_glue_crawler.order_items.arn
        ]
      },
      {
        Sid    = "AthenaQuery"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:StopQueryExecution",
          "athena:GetQueryExecution", "athena:GetQueryResults"
        ]
        Resource = ["arn:aws:athena:${local.region}:${local.account_id}:workgroup/${var.athena_workgroup_name}"]
      },
      {
        # Athena resolves table metadata through the Glue Data Catalog using the
        # CALLER's permissions (Step Functions is the caller), so the execution
        # role — not just the Glue job role — needs read access to the catalog.
        Sid    = "AthenaCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:GetPartition", "glue:GetPartitions"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          "arn:aws:glue:${local.region}:${local.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${local.region}:${local.account_id}:table/${var.glue_database_name}/*"
        ]
      },
      {
        # Athena scans the Delta files in the processed zone under the caller's
        # permissions, so the execution role needs read access to the data bucket.
        Sid      = "AthenaDataRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      },
      {
        Sid    = "AthenaResultsS3"
        Effect = "Allow"
        Action = ["s3:GetBucketLocation", "s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.pipeline_alerts.arn]
      },
      {
        # Log-delivery management actions are not resource-scopable in IAM and
        # MUST use "*". This is the documented requirement for Step Functions
        # logging configuration, not an over-grant we can tighten.
        Sid    = "CloudWatchLogDelivery"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# GLUE DATA CATALOG
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "lakehouse" {
  name        = var.glue_database_name
  description = "E-commerce lakehouse Delta Lake tables — ${var.environment}"

  create_table_default_permission {
    permissions = ["SELECT"]
    principal { data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS" }
  }
}

# -- Crawlers (one per dataset so failures are isolated) ----------------------
resource "aws_glue_crawler" "products" {
  name          = "${local.name_prefix}-crawler-products"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  description   = "Crawls products Delta table and updates catalog"

  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}products/"]
    write_manifest            = false
    # Athena engine v3 requires native Delta table format for proper schema resolution under Lake Formation.
    create_native_delta_table = true
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })

  schedule = var.crawler_schedule != "" ? var.crawler_schedule : null
}

resource "aws_glue_crawler" "orders" {
  name          = "${local.name_prefix}-crawler-orders"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  description   = "Crawls orders Delta table and updates catalog"

  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}orders/"]
    write_manifest            = false
    create_native_delta_table = true
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  schedule = var.crawler_schedule != "" ? var.crawler_schedule : null
}

resource "aws_glue_crawler" "order_items" {
  name          = "${local.name_prefix}-crawler-order-items"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  description   = "Crawls order_items Delta table and updates catalog"

  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}order_items/"]
    write_manifest            = false
    create_native_delta_table = true
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  schedule = var.crawler_schedule != "" ? var.crawler_schedule : null
}

# ─────────────────────────────────────────────────────────────────────────────
# ATHENA
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "lakehouse" {
  name        = var.athena_workgroup_name
  description = "Workgroup for e-commerce lakehouse analytics"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

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

  force_destroy = true
}

# ─────────────────────────────────────────────────────────────────────────────
# CLOUDWATCH LOG GROUPS
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws-glue/jobs/${local.name_prefix}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${local.name_prefix}-etl-pipeline"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_resource_policy" "sfn" {
  policy_name = "${local.name_prefix}-sfn-log-policy"

  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = ["logs:CreateLogStream", "logs:PutLogEvents"]
      Resource  = "${aws_cloudwatch_log_group.sfn.arn}:*"
    }]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# SNS ALERT TOPIC (optional — only created if alert_email is set)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name_prefix}-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE TRIGGER
#
# There is intentionally NO EventBridge S3 trigger. The three datasets form one
# relational batch (order_items references products and orders), so they are
# ingested by a SINGLE Step Functions execution started explicitly by
# ingestion/ingest.py after all three files have landed. Per-file S3 events
# would fire three independent executions and race the referential-integrity
# checks — see step_functions.tf for the full rationale.
#
# The least-privilege permission set the ingestion principal needs
# (states:StartExecution on this state machine + s3:PutObject on raw/) is
# defined as aws_iam_policy.ingestion below; attach it to the developer or CI
# principal that runs ingest.py.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_policy" "ingestion" {
  name        = "${local.name_prefix}-ingestion-policy"
  description = "Least-privilege permissions for the principal that runs ingest.py (upload raw files + start the ETL batch)."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "UploadRawFiles"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.data.arn}/${var.raw_data_prefix}*"]
      },
      {
        Sid      = "StartEtlBatch"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.etl_pipeline.arn]
      }
    ]
  })
}