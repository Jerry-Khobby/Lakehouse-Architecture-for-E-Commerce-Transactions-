# ─────────────────────────────────────────────────────────────────────────────
# GLUE JOBS — one per dataset
# Delta Lake connector JAR is passed as an extra JAR on the classpath.
# All jobs share the same execution role defined in main.tf.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  common_glue_args = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.logs.id}/spark-ui-logs/"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--conf"                             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--datalake-formats"                 = "delta"
    # Runtime parameters — overridden per execution by Step Functions
    "--DATA_BUCKET"    = aws_s3_bucket.data.id
    "--SCRIPTS_BUCKET" = aws_s3_bucket.scripts.id
    "--ENVIRONMENT"    = var.environment
    "--DATABASE_NAME"  = var.glue_database_name
  }
}

# -- Products job --------------------------------------------------------------
resource "aws_glue_job" "products" {
  name         = "${local.name_prefix}-products-etl"
  role_arn     = aws_iam_role.glue_role.arn
  description  = "Ingests, validates, deduplicates and merges products CSV into Delta Lake"
  glue_version = var.glue_version
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  max_retries  = var.glue_max_retries
  timeout      = var.glue_timeout_minutes

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
  name         = "${local.name_prefix}-orders-etl"
  role_arn     = aws_iam_role.glue_role.arn
  description  = "Ingests, validates, deduplicates and merges orders CSV into Delta Lake"
  glue_version = var.glue_version
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  max_retries  = var.glue_max_retries
  timeout      = var.glue_timeout_minutes

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
  name         = "${local.name_prefix}-order-items-etl"
  role_arn     = aws_iam_role.glue_role.arn
  description  = "Ingests, validates, deduplicates and merges order_items CSV into Delta Lake"
  glue_version = var.glue_version
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  max_retries  = var.glue_max_retries
  timeout      = var.glue_timeout_minutes

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