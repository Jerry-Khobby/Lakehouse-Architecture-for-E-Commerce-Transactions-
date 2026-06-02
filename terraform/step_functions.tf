# ─────────────────────────────────────────────────────────────────────────────
# STEP FUNCTIONS STATE MACHINE
# Full ETL orchestration:
#   Trigger → Products | Orders | OrderItems (parallel)
#          → Crawlers (parallel)
#          → Athena validation
#          → Success / Failure notification
# ─────────────────────────────────────────────────────────────────────────────

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
    Comment = "E-commerce Lakehouse ETL pipeline — ${var.environment}"
    StartAt = "RunETLJobs"

    States = {

      # ── Fan out: run all three Glue jobs in parallel ──────────────────────
      RunETLJobs = {
        Type = "Parallel"
        Branches = [

          # Branch 1 — Products
          {
            StartAt = "RunProductsJob"
            States = {
              RunProductsJob = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = aws_glue_job.products.name
                  Arguments = {
                    "--RAW_KEY.$" = "$.key"
                    "--DATA_BUCKET.$" = "$.bucket"
                  }
                }
                TimeoutSeconds = var.sfn_timeout_seconds
                HeartbeatSeconds = 300
                Retry = [{
                  ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
                  IntervalSeconds = 30
                  MaxAttempts     = 2
                  BackoffRate     = 2.0
                }]
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "ProductsJobFailed"
                  ResultPath  = "$.error"
                }]
                End = true
              }
              ProductsJobFailed = {
                Type  = "Fail"
                Error = "ProductsJobFailed"
                Cause = "Glue products job encountered an error"
              }
            }
          },

          # Branch 2 — Orders
          {
            StartAt = "RunOrdersJob"
            States = {
              RunOrdersJob = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = aws_glue_job.orders.name
                  Arguments = {
                    "--RAW_KEY.$" = "$.key"
                    "--DATA_BUCKET.$" = "$.bucket"
                  }
                }
                TimeoutSeconds   = var.sfn_timeout_seconds
                HeartbeatSeconds = 300
                Retry = [{
                  ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
                  IntervalSeconds = 30
                  MaxAttempts     = 2
                  BackoffRate     = 2.0
                }]
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "OrdersJobFailed"
                  ResultPath  = "$.error"
                }]
                End = true
              }
              OrdersJobFailed = {
                Type  = "Fail"
                Error = "OrdersJobFailed"
                Cause = "Glue orders job encountered an error"
              }
            }
          },

          # Branch 3 — Order Items
          {
            StartAt = "RunOrderItemsJob"
            States = {
              RunOrderItemsJob = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = aws_glue_job.order_items.name
                  Arguments = {
                    "--RAW_KEY.$" = "$.key"
                    "--DATA_BUCKET.$" = "$.bucket"
                  }
                }
                TimeoutSeconds   = var.sfn_timeout_seconds
                HeartbeatSeconds = 300
                Retry = [{
                  ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
                  IntervalSeconds = 30
                  MaxAttempts     = 2
                  BackoffRate     = 2.0
                }]
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "OrderItemsJobFailed"
                  ResultPath  = "$.error"
                }]
                End = true
              }
              OrderItemsJobFailed = {
                Type  = "Fail"
                Error = "OrderItemsJobFailed"
                Cause = "Glue order_items job encountered an error"
              }
            }
          }
        ]

        # ── After all 3 parallel jobs succeed ────────────────────────────────
        Next = "RunCrawlers"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.parallelError"
        }]
      },

      # ── Fan out: update all three catalog tables in parallel ──────────────
      RunCrawlers = {
        Type = "Parallel"
        Branches = [
          {
            StartAt = "CrawlProducts"
            States = {
              CrawlProducts = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.products.name }
                Retry = [{
                  ErrorEquals     = ["Glue.CrawlerRunningException"]
                  IntervalSeconds = 60
                  MaxAttempts     = 3
                  BackoffRate     = 1.5
                }]
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "CrawlProductsFailed"
                  ResultPath  = "$.error"
                }]
                End = true
              }
              CrawlProductsFailed = {
                Type  = "Fail"
                Error = "CrawlProductsFailed"
              }
            }
          },
          {
            StartAt = "CrawlOrders"
            States = {
              CrawlOrders = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.orders.name }
                Retry = [{
                  ErrorEquals     = ["Glue.CrawlerRunningException"]
                  IntervalSeconds = 60
                  MaxAttempts     = 3
                  BackoffRate     = 1.5
                }]
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "CrawlOrdersFailed"
                  ResultPath  = "$.error"
                }]
                End = true
              }
              CrawlOrdersFailed = {
                Type  = "Fail"
                Error = "CrawlOrdersFailed"
              }
            }
          },
          {
            StartAt = "CrawlOrderItems"
            States = {
              CrawlOrderItems = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.order_items.name }
                Retry = [{
                  ErrorEquals     = ["Glue.CrawlerRunningException"]
                  IntervalSeconds = 60
                  MaxAttempts     = 3
                  BackoffRate     = 1.5
                }]
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "CrawlOrderItemsFailed"
                  ResultPath  = "$.error"
                }]
                End = true
              }
              CrawlOrderItemsFailed = {
                Type  = "Fail"
                Error = "CrawlOrderItemsFailed"
              }
            }
          }
        ]
        Next = "AthenaValidation"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.crawlerError"
        }]
      },

      # ── Athena smoke-test query after crawlers complete ───────────────────
      AthenaValidation = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          WorkGroup = var.athena_workgroup_name
          QueryString = "SELECT 'products' AS tbl, COUNT(*) AS row_count FROM ${var.glue_database_name}.products UNION ALL SELECT 'orders', COUNT(*) FROM ${var.glue_database_name}.orders UNION ALL SELECT 'order_items', COUNT(*) FROM ${var.glue_database_name}.order_items;"
          ResultConfiguration = {
            OutputLocation = "s3://${local.athena_bucket_name}/sfn-validation/"
          }
        }
        Retry = [{
          ErrorEquals     = ["Athena.AthenaException"]
          IntervalSeconds = 15
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.athenaError"
        }]
        Next = "NotifySuccess"
      },

      # ── Success notification ──────────────────────────────────────────────
      NotifySuccess = {
        Type = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.alert_email != "" ? "${aws_sns_topic.pipeline_alerts[0].arn}" : "arn:aws:sns:${local.region}:${local.account_id}:dummy"
          Message = "✅ Lakehouse ETL pipeline completed successfully."
          Subject = "[${var.environment}] Lakehouse ETL — SUCCESS"
        }
        End = true
      },

      # ── Failure notification ──────────────────────────────────────────────
      NotifyFailure = {
        Type = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.alert_email != "" ? "${aws_sns_topic.pipeline_alerts[0].arn}" : "arn:aws:sns:${local.region}:${local.account_id}:dummy"
          "Message.$" = "States.Format('❌ Lakehouse ETL pipeline FAILED. Execution: {}', $$.Execution.Name)"
          Subject = "[${var.environment}] Lakehouse ETL — FAILURE"
        }
        Next = "PipelineFailed"
      },

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One or more ETL stages failed. Check CloudWatch logs."
      }
    }
  })
}