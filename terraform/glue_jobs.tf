# ─────────────────────────────────────────────────────────────────────────────
# GLUE JOBS — one per dataset
# Delta Lake connector JAR is passed as an extra JAR on the classpath.
# All jobs share the same execution role defined in main.tf.
# ─────────────────────────────────────────────────────────────────────────────

# -- Package glue_jobs/ as a zip so Glue can import it via --extra-py-files ----
# The zip must contain glue_jobs/ at the root so `from glue_jobs.utils.common`
# resolves correctly inside the Glue Python runtime.
data "archive_file" "glue_jobs_package" {
  type        = "zip"
  output_path = "${path.module}/../glue_jobs.zip"

  source {
    content  = file("${path.module}/../glue_jobs/__init__.py")
    filename = "glue_jobs/__init__.py"
  }
  source {
    content  = file("${path.module}/../glue_jobs/utils/__init__.py")
    filename = "glue_jobs/utils/__init__.py"
  }
  source {
    content  = file("${path.module}/../glue_jobs/utils/common.py")
    filename = "glue_jobs/utils/common.py"
  }
  source {
    content  = file("${path.module}/../glue_jobs/utils/monitor.py")
    filename = "glue_jobs/utils/monitor.py"
  }
  source {
    content  = file("${path.module}/../glue_jobs/utils/notifier.py")
    filename = "glue_jobs/utils/notifier.py"
  }
}

resource "aws_s3_object" "glue_jobs_package" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/glue_jobs.zip"
  source = data.archive_file.glue_jobs_package.output_path
  etag   = data.archive_file.glue_jobs_package.output_md5
}

# -- Upload scripts to S3 ------------------------------------------------------
# etag ensures Terraform re-uploads a file whenever its local content changes.

resource "aws_s3_object" "products_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/products_job.py"
  source = "${path.module}/../glue_jobs/products_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/products_job.py")
}

resource "aws_s3_object" "orders_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/orders_job.py"
  source = "${path.module}/../glue_jobs/orders_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/orders_job.py")
}

resource "aws_s3_object" "order_items_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/order_items_job.py"
  source = "${path.module}/../glue_jobs/order_items_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/order_items_job.py")
}

resource "aws_s3_object" "common_utils" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/utils/common.py"
  source = "${path.module}/../glue_jobs/utils/common.py"
  etag   = filemd5("${path.module}/../glue_jobs/utils/common.py")
}

locals {
  common_glue_args = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.logs.id}/spark-ui-logs/"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    # --datalake-formats delta instructs Glue 4.0 to automatically activate the
    # Delta Lake connector, register DeltaSparkSessionExtension, and configure
    # the DeltaCatalog. No manual --conf entries are needed or safe here —
    # passing a second --conf value for the same key in a Terraform map is
    # unsupported and causes Spark to receive a malformed extension string.
    "--datalake-formats" = "delta"
    # Spark staging area for shuffle spill and Delta merge commit staging.
    # Must point to a bucket where the Glue role has full read/write/delete.
    # The data bucket satisfies this — scripts and logs buckets do not.
    # Without this, Glue passes an empty string to Hadoop's Path constructor,
    # causing: IllegalArgumentException: Can not create a Path from an empty string
    "--TempDir" = "s3://${aws_s3_bucket.data.id}/glue-temp/"
    # Makes `from glue_jobs.utils.common import ...` resolvable in the Glue runtime
    "--extra-py-files" = "s3://${aws_s3_bucket.scripts.id}/glue_jobs/glue_jobs.zip"
    # Runtime parameters — overridden per execution by Step Functions
    "--DATA_BUCKET"    = aws_s3_bucket.data.id
    "--SCRIPTS_BUCKET" = aws_s3_bucket.scripts.id
    "--ENVIRONMENT"    = var.environment
    "--DATABASE_NAME"  = var.glue_database_name
    "--SNS_TOPIC_ARN"  = aws_sns_topic.pipeline_alerts.arn
    "--FLAGGED_PREFIX" = var.flagged_data_prefix
  }
}

# -- Products job --------------------------------------------------------------
resource "aws_glue_job" "products" {
  name              = "${local.name_prefix}-products-etl"
  role_arn          = aws_iam_role.glue_role.arn
  description       = "Ingests, validates, deduplicates and merges products CSV into Delta Lake"
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/glue_jobs/products_job.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--DATASET"          = "products"
    "--RAW_PREFIX"       = var.raw_data_prefix
    "--PROCESSED_PREFIX" = var.processed_data_prefix
    "--ARCHIVED_PREFIX"  = var.archived_data_prefix
    "--REJECTED_PREFIX"  = var.rejected_data_prefix
    "--MERGE_KEYS"       = "product_id"
    "--PARTITION_COLS"   = "department"
  })

  execution_property {
    max_concurrent_runs = 1
  }

  notification_property {
    notify_delay_after = 10 # minutes — CloudWatch alert if job stalls
  }
}

# -- Orders job ----------------------------------------------------------------
resource "aws_glue_job" "orders" {
  name              = "${local.name_prefix}-orders-etl"
  role_arn          = aws_iam_role.glue_role.arn
  description       = "Ingests, validates, deduplicates and merges orders CSV into Delta Lake"
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/glue_jobs/orders_job.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--DATASET"          = "orders"
    "--RAW_PREFIX"       = var.raw_data_prefix
    "--PROCESSED_PREFIX" = var.processed_data_prefix
    "--ARCHIVED_PREFIX"  = var.archived_data_prefix
    "--REJECTED_PREFIX"  = var.rejected_data_prefix
    "--MERGE_KEYS"       = "order_id"
    "--PARTITION_COLS"   = "date"
  })

  execution_property {
    max_concurrent_runs = 1
  }

  notification_property {
    notify_delay_after = 10
  }
}

# -- Order items job -----------------------------------------------------------
resource "aws_glue_job" "order_items" {
  name              = "${local.name_prefix}-order-items-etl"
  role_arn          = aws_iam_role.glue_role.arn
  description       = "Ingests, validates, deduplicates and merges order_items CSV into Delta Lake"
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/glue_jobs/order_items_job.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--DATASET"          = "order_items"
    "--RAW_PREFIX"       = var.raw_data_prefix
    "--PROCESSED_PREFIX" = var.processed_data_prefix
    "--ARCHIVED_PREFIX"  = var.archived_data_prefix
    "--REJECTED_PREFIX"  = var.rejected_data_prefix
    "--MERGE_KEYS"       = "id,order_id"
    "--PARTITION_COLS"   = "date"
  })

  execution_property {
    max_concurrent_runs = 1
  }

  notification_property {
    notify_delay_after = 10
  }
}