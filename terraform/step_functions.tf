# ─────────────────────────────────────────────────────────────────────────────
# STEP FUNCTIONS STATE MACHINE — single-batch ETL orchestration
#
# The three datasets (products, orders, order_items) are ONE logical batch:
# order_items holds foreign keys into BOTH products (product_id) and orders
# (order_id). They must therefore be ingested as an atomic unit, in dependency
# order, by a SINGLE execution — not as three independent file-triggered runs.
#
# Trigger:  ingestion/ingest.py uploads all three CSVs to raw/, then calls
#           StartExecution ONCE with a structured input:
#             {
#               "bucket": "<data-bucket>",
#               "batch":  "apr_2025",
#               "files": {
#                 "products":    "raw/products.csv",
#                 "orders":      "raw/orders_apr_2025.csv",
#                 "order_items": "raw/order_items_apr_2025.csv"
#               }
#             }
#
# Flow (strictly linear — dependency order):
#   RunProductsJob → RunOrdersJob → RunOrderItemsJob
#                  → RunCrawlers (parallel, 3 crawlers)
#                  → AthenaValidation → NotifySuccess
#   any failure   → NotifyFailure → PipelineFailed
#
# Why linear and ordered:
#   products and orders MUST be committed to Delta before order_items runs, so
#   that order_items' referential-integrity joins see the parent rows. A single
#   ordered execution makes this a structural guarantee rather than a race.
#
# Input/output contract:
#   - Each Glue task reads its own key from $.files.<dataset> and writes its
#     result to a DEDICATED side path ($.results.<dataset>). No two tasks share
#     a ResultPath, so nothing is overwritten and the original input survives
#     end-to-end. $.bucket and $.batch are always readable by every state.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  # Generic Glue failure retry — shared by all three job tasks. With a single
  # ordered execution there is no same-job concurrency and no cross-execution
  # crawler race, so the old ConcurrentRunsExceededException retry blocks are
  # gone; this covers transient Glue/infra failures only.
  glue_job_retry = [
    {
      ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed", "States.Timeout"]
      IntervalSeconds = 30
      MaxAttempts     = 2
      BackoffRate     = 2.0
    }
  ]

  glue_job_catch = [
    {
      ErrorEquals = ["States.ALL"]
      Next        = "NotifyFailure"
      ResultPath  = "$.error"
    }
  ]
}

resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${local.name_prefix}-etl-pipeline"
  role_arn = aws_iam_role.sfn_role.arn
  type     = "STANDARD"

  depends_on = [
    aws_iam_role_policy.sfn_glue,
    aws_cloudwatch_log_resource_policy.sfn,
  ]

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }

  definition = jsonencode({
    Comment = "E-commerce Lakehouse single-batch ETL pipeline — ${var.environment}"
    StartAt = "RunProductsJob"

    States = {

      # ── Step 1: Products dimension (no upstream dependency) ───────────────
      RunProductsJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.products.name
          Arguments = {
            "--RAW_KEY.$"     = "$.files.products"
            "--DATA_BUCKET.$" = "$.bucket"
          }
        }
        TimeoutSeconds   = var.sfn_timeout_seconds
        HeartbeatSeconds = 300
        Retry            = local.glue_job_retry
        Catch            = local.glue_job_catch
        ResultPath       = "$.results.products"
        Next             = "RunOrdersJob"
      },

      # ── Step 2: Orders fact (no upstream dependency) ──────────────────────
      RunOrdersJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.orders.name
          Arguments = {
            "--RAW_KEY.$"     = "$.files.orders"
            "--DATA_BUCKET.$" = "$.bucket"
          }
        }
        TimeoutSeconds   = var.sfn_timeout_seconds
        HeartbeatSeconds = 300
        Retry            = local.glue_job_retry
        Catch            = local.glue_job_catch
        ResultPath       = "$.results.orders"
        Next             = "RunOrderItemsJob"
      },

      # ── Step 3: Order items fact (depends on products AND orders) ─────────
      # Runs last so its referential-integrity joins resolve against the
      # products and orders Delta tables committed by the two prior steps.
      RunOrderItemsJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.order_items.name
          Arguments = {
            "--RAW_KEY.$"     = "$.files.order_items"
            "--DATA_BUCKET.$" = "$.bucket"
          }
        }
        TimeoutSeconds   = var.sfn_timeout_seconds
        HeartbeatSeconds = 300
        Retry            = local.glue_job_retry
        Catch            = local.glue_job_catch
        ResultPath       = "$.results.order_items"
        Next             = "RunCrawlers"
      },

      # ── Step 4: Refresh the Data Catalog (all three crawlers in parallel) ──
      # A single execution owns all three crawlers, so there is no longer any
      # cross-execution contention. A short retry covers an out-of-band /
      # scheduled crawler that happens to be mid-run.
      RunCrawlers = {
        Type = "Parallel"
        Branches = [
          {
            StartAt = "CrawlProducts"
            States = {
              CrawlProducts = {
                Type       = "Task"
                Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.products.name }
                Retry = [{
                  ErrorEquals     = ["Glue.CrawlerRunningException"]
                  IntervalSeconds = 60
                  MaxAttempts     = 3
                  BackoffRate     = 2.0
                }]
                End = true
              }
            }
          },
          {
            StartAt = "CrawlOrders"
            States = {
              CrawlOrders = {
                Type       = "Task"
                Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.orders.name }
                Retry = [{
                  ErrorEquals     = ["Glue.CrawlerRunningException"]
                  IntervalSeconds = 60
                  MaxAttempts     = 3
                  BackoffRate     = 2.0
                }]
                End = true
              }
            }
          },
          {
            StartAt = "CrawlOrderItems"
            States = {
              CrawlOrderItems = {
                Type       = "Task"
                Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.order_items.name }
                Retry = [{
                  ErrorEquals     = ["Glue.CrawlerRunningException"]
                  IntervalSeconds = 60
                  MaxAttempts     = 3
                  BackoffRate     = 2.0
                }]
                End = true
              }
            }
          }
        ]
        ResultPath = "$.results.crawlers"
        Next       = "AthenaValidation"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
      },

      # ── Step 5: Athena smoke-test ─────────────────────────────────────────
      # Confirms all three tables are queryable and report row counts.
      # ResultConfiguration is intentionally omitted: the workgroup enforces
      # its own OutputLocation and rejects any client-supplied value.
      AthenaValidation = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          WorkGroup   = var.athena_workgroup_name
          QueryString = "SELECT 'products' AS tbl, COUNT(*) AS row_count FROM ${var.glue_database_name}.products UNION ALL SELECT 'orders', COUNT(*) FROM ${var.glue_database_name}.orders UNION ALL SELECT 'order_items', COUNT(*) FROM ${var.glue_database_name}.order_items;"
        }
        Retry = [{
          ErrorEquals     = ["Athena.AthenaException", "Athena.TooManyRequestsException"]
          IntervalSeconds = 15
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
        ResultPath = "$.results.athena"
        Next       = "NotifySuccess"
      },

      # ── Step 6a: Success notification ─────────────────────────────────────
      # $.batch is part of the original input and is never overwritten, so it
      # is always readable here regardless of which states ran.
      NotifySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          "Message.$" = "States.Format('✅ Lakehouse ETL batch completed successfully.\nBatch: {}\nExecution: {}', $.batch, $$.Execution.Name)"
          Subject     = "[${var.environment}] Lakehouse ETL — SUCCESS"
        }
        End = true
      },

      # ── Step 6b: Failure notification ─────────────────────────────────────
      # Every Task/Parallel writes its result to a dedicated $.results.* path
      # and its error to $.error, so the original input ($.batch, $.bucket,
      # $.files) survives any failure and $.batch is always present here.
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          "Message.$" = "States.Format('❌ Lakehouse ETL batch FAILED.\nBatch: {}\nExecution: {}\nCheck CloudWatch logs for details.', $.batch, $$.Execution.Name)"
          Subject     = "[${var.environment}] Lakehouse ETL — FAILURE"
        }
        Next = "PipelineFailed"
      },

      # ── Terminal failure state ────────────────────────────────────────────
      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One or more ETL stages failed. Check CloudWatch logs."
      }
    }
  })
}
