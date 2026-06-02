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
        Sid    = "LogsBucketWrite"
        Effect = "Allow"
        Action = ["s3:PutObject"]
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
          "glue:GetJobRuns", "glue:BatchStopJobRun",
          "glue:StartCrawler", "glue:GetCrawler"
        ]
        Resource = "*"
      },
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ]
        Resource = "*"
      },
      {
        Sid    = "AthenaResultsS3"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = var.alert_email != "" ? [aws_sns_topic.pipeline_alerts[0].arn] : ["*"]
      },
      {
        Sid    = "CloudWatchLogs"
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

# -- EventBridge role (to trigger Step Functions from S3 events) --------------
resource "aws_iam_role" "eventbridge_role" {
  name = "${local.name_prefix}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn" {
  name = "${local.name_prefix}-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.etl_pipeline.arn]
    }]
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
    delta_tables   = ["s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}products/"]
    write_manifest = false
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
    delta_tables   = ["s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}orders/"]
    write_manifest = false
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
    delta_tables   = ["s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}order_items/"]
    write_manifest = false
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
  count = var.alert_email != "" ? 1 : 0
  name  = "${local.name_prefix}-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.pipeline_alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ─────────────────────────────────────────────────────────────────────────────
# EVENTBRIDGE — S3 trigger for Step Functions
# ─────────────────────────────────────────────────────────────────────────────

# Enable EventBridge notifications on the data bucket
resource "aws_s3_bucket_notification" "data_bucket_events" {
  bucket      = aws_s3_bucket.data.id
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "s3_raw_ingest" {
  name        = "${local.name_prefix}-raw-file-arrived"
  description = "Fires when a CSV lands in the raw/ prefix of the data bucket"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.data.id] }
      object = { key = [{ prefix = var.raw_data_prefix }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "trigger_sfn" {
  rule     = aws_cloudwatch_event_rule.s3_raw_ingest.name
  arn      = aws_sfn_state_machine.etl_pipeline.arn
  role_arn = aws_iam_role.eventbridge_role.arn

  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = "{\"bucket\": \"<bucket>\", \"key\": \"<key>\"}"
  }
}