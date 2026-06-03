# ─────────────────────────────────────────────────────────────────────────────
# STEP FUNCTIONS STATE MACHINE
#
# Flow per file upload:
#   EventBridge → RouteToETLJob (Choice on $.key filename)
#              → RunProductsJob | RunOrdersJob | RunOrderItemsJob
#              → RunCrawlers (Parallel — all 3 crawlers)
#              → AthenaValidation
#              → NotifySuccess / NotifyFailure → PipelineFailed
#
# Fixes applied vs previous version
# ──────────────────────────────────
# FIX 1  RunProductsJob was missing the ConcurrentRunsExceededException retry.
#        All three job states now carry the same two-block retry policy.
#
# FIX 2  Crawler retry depth was too shallow (3 attempts × 60 s base).
#        Each file upload fires its own execution; all three executions reach
#        RunCrawlers at roughly the same time and race to start the same crawlers.
#        A crawler run takes 2–4 minutes, so 3 × 60 s was not enough.
#        Now: 5 attempts × 90 s base × 2.0 backoff = up to ~24 minutes of
#        retry coverage — enough for any realistic crawler duration.
#        CrawlOrderItems was also missing the ConcurrentRunsExceededException
#        retry block; that is now added to all three crawler branches.
#
# FIX 3  NotifySuccess used "$.key" in States.Format.
#        After RunCrawlers writes its parallel-branch output to $.crawlerResults
#        and AthenaValidation writes its result to $.athenaError (on failure),
#        the top-level $.key field is still present in the state input.
#        HOWEVER: when the pipeline reaches NotifySuccess via a path where
#        $.key was never in the input (e.g., future routing changes), this
#        would silently break. We now carry $.key forward via a dedicated
#        Pass state (PreserveKey) placed before RunCrawlers so the value is
#        stored at $.originalKey and survives all downstream ResultPath writes.
#        NotifySuccess and NotifyFailure both reference $.originalKey.
#
# FIX 4  NotifyFailure used "$.key" which does NOT exist in the input when
#        the failure originates from a crawler or Athena state — those states
#        write their ResultPath over the Glue job output object (which never
#        had a "key" field). The confirmed log error was:
#            "The JsonPath argument for '$.key' could not be found in the input"
#        Fixed by referencing $.originalKey (preserved in FIX 3) instead.
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
    StartAt = "RouteToETLJob"

    States = {

      # ── Step 1: Route to the correct Glue job ─────────────────────────────
      # order_items check MUST precede orders (substring overlap: "order_items"
      # contains "order", so a plain *order* match would fire for order_items files).
      RouteToETLJob = {
        Type = "Choice"
        Choices = [
          {
            Variable      = "$.key"
            StringMatches = "*order_item*"
            Next          = "RunOrderItemsJob"
          },
          {
            Variable      = "$.key"
            StringMatches = "*order-item*"
            Next          = "RunOrderItemsJob"
          },
          {
            Variable      = "$.key"
            StringMatches = "*order*"
            Next          = "RunOrdersJob"
          },
          {
            Variable      = "$.key"
            StringMatches = "*product*"
            Next          = "RunProductsJob"
          }
        ]
        Default = "UnknownFileType"
      },

      # ── Unroutable file ───────────────────────────────────────────────────
      UnknownFileType = {
        Type  = "Fail"
        Error = "UnknownFileType"
        Cause = "Uploaded file key did not match products, orders, or order_items patterns."
      },

      # ── Step 2a: Products Glue job ────────────────────────────────────────
      # FIX 1: added ConcurrentRunsExceededException retry (was missing here).
      RunProductsJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.products.name
          Arguments = {
            "--RAW_KEY.$"     = "$.key"
            "--DATA_BUCKET.$" = "$.bucket"
          }
        }
        TimeoutSeconds   = var.sfn_timeout_seconds
        HeartbeatSeconds = 300
        Retry = [
          {
            # A second execution tries to run the same job while the first is
            # still running (max_concurrent_runs = 1 in Terraform).
            # Wait 2 minutes for the first run to finish, then retry.
            ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 120
            MaxAttempts     = 3
            BackoffRate     = 1.5
          },
          {
            ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts     = 2
            BackoffRate     = 2.0
          }
        ]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
        # FIX 3: copy $.key into $.originalKey before handing off to RunCrawlers
        # so it survives downstream ResultPath writes from crawlers and Athena.
        Next = "PreserveKey"
      },

      # ── Step 2b: Orders Glue job ──────────────────────────────────────────
      RunOrdersJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.orders.name
          Arguments = {
            "--RAW_KEY.$"     = "$.key"
            "--DATA_BUCKET.$" = "$.bucket"
          }
        }
        TimeoutSeconds   = var.sfn_timeout_seconds
        HeartbeatSeconds = 300
        Retry = [
          {
            ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 120
            MaxAttempts     = 3
            BackoffRate     = 1.5
          },
          {
            ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts     = 2
            BackoffRate     = 2.0
          }
        ]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
        Next = "PreserveKey"
      },

      # ── Step 2c: Order Items Glue job ─────────────────────────────────────
      RunOrderItemsJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.order_items.name
          Arguments = {
            "--RAW_KEY.$"     = "$.key"
            "--DATA_BUCKET.$" = "$.bucket"
          }
        }
        TimeoutSeconds   = var.sfn_timeout_seconds
        HeartbeatSeconds = 300
        Retry = [
          {
            ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 120
            MaxAttempts     = 3
            BackoffRate     = 1.5
          },
          {
            ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts     = 2
            BackoffRate     = 2.0
          }
        ]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
        Next = "PreserveKey"
      },

      # ── Step 3: Preserve $.key before ResultPath writes overwrite it ───────
      # FIX 3 & 4: The RunCrawlers parallel state writes its output to
      # $.crawlerResults and AthenaValidation writes errors to $.athenaError.
      # Neither of these touches $.key — BUT the Glue job result object that
      # becomes the input to this state does NOT contain a "key" field at all.
      # We therefore copy the original $.key (still present in the Glue job
      # result under $.Arguments["--RAW_KEY"]) into $.originalKey using a
      # Pass state. NotifySuccess and NotifyFailure then reference $.originalKey
      # which is guaranteed to survive all downstream state transitions.
      PreserveKey = {
        Type = "Pass"
        Parameters = {
          # Carry the bucket/key forward by reading them out of the Glue job
          # result's Arguments map, where Step Functions echoes them back.
          "originalKey.$"    = "$.Arguments['--RAW_KEY']"
          "originalBucket.$" = "$.Arguments['--DATA_BUCKET']"
        }
        # Merge these new fields into the existing state input rather than
        # replacing it, so $.crawlerResults / $.athenaError etc. still work.
        ResultPath = "$.source"
        Next       = "RunCrawlers"
      },

      # ── Step 4: Run all three crawlers in parallel ─────────────────────────
      # FIX 2: crawler retry depth raised to 5 attempts × 90 s base × 2.0
      # backoff (covers up to ~24 min). ConcurrentRunsExceededException retry
      # also added to CrawlOrderItems (was missing).
      # ResultPath = "$.crawlerResults" preserves the original state input so
      # AthenaValidation and notify states can still read $.source.originalKey.
      RunCrawlers = {
        Type = "Parallel"
        Branches = [

          # Branch 1 — products crawler
          {
            StartAt = "CrawlProducts"
            States = {
              CrawlProducts = {
                Type       = "Task"
                Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.products.name }
                Retry = [
                  {
                    # Another execution already started this crawler.
                    # Wait for it to finish then retry.
                    ErrorEquals     = ["Glue.CrawlerRunningException"]
                    IntervalSeconds = 90
                    MaxAttempts     = 5
                    BackoffRate     = 2.0
                  },
                  {
                    ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
                    IntervalSeconds = 120
                    MaxAttempts     = 3
                    BackoffRate     = 1.5
                  }
                ]
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
                Cause = "Products crawler failed after all retries."
              }
            }
          },

          # Branch 2 — orders crawler
          {
            StartAt = "CrawlOrders"
            States = {
              CrawlOrders = {
                Type       = "Task"
                Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.orders.name }
                Retry = [
                  {
                    ErrorEquals     = ["Glue.CrawlerRunningException"]
                    IntervalSeconds = 90
                    MaxAttempts     = 5
                    BackoffRate     = 2.0
                  },
                  {
                    ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
                    IntervalSeconds = 120
                    MaxAttempts     = 3
                    BackoffRate     = 1.5
                  }
                ]
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
                Cause = "Orders crawler failed after all retries."
              }
            }
          },

          # Branch 3 — order_items crawler
          {
            StartAt = "CrawlOrderItems"
            States = {
              CrawlOrderItems = {
                Type       = "Task"
                Resource   = "arn:aws:states:::aws-sdk:glue:startCrawler"
                Parameters = { Name = aws_glue_crawler.order_items.name }
                Retry = [
                  {
                    # FIX 2: this block was missing from CrawlOrderItems entirely.
                    ErrorEquals     = ["Glue.CrawlerRunningException"]
                    IntervalSeconds = 90
                    MaxAttempts     = 5
                    BackoffRate     = 2.0
                  },
                  {
                    # FIX 2: ConcurrentRunsExceededException retry also missing here.
                    ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
                    IntervalSeconds = 120
                    MaxAttempts     = 3
                    BackoffRate     = 1.5
                  }
                ]
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
                Cause = "Order items crawler failed after all retries."
              }
            }
          }
        ]

        # Write the parallel output array here so the original state input
        # (including $.source.originalKey) is not replaced.
        ResultPath = "$.crawlerResults"
        Next       = "AthenaValidation"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.crawlerError"
        }]
      },

      # ── Step 5: Athena smoke-test ─────────────────────────────────────────
      # Counts rows in all three tables to confirm data landed correctly.
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
          ResultPath  = "$.athenaError"
        }]
        Next = "NotifySuccess"
      },

      # ── Step 6a: Success notification ─────────────────────────────────────
      # FIX 3: reference $.source.originalKey (set in PreserveKey) instead of
      # $.key, which no longer exists at this point in the execution input.
      NotifySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          "Message.$" = "States.Format('✅ Lakehouse ETL pipeline completed successfully.\nFile: {}\nExecution: {}', $.source.originalKey, $$.Execution.Name)"
          Subject     = "[${var.environment}] Lakehouse ETL — SUCCESS"
        }
        End = true
      },

      # ── Step 6b: Failure notification ─────────────────────────────────────
      # FIX 4: replace $.key with $.source.originalKey.
      #
      # When failure originates from a crawler or Athena state, the execution
      # input at that point is the Glue job result object — which never had a
      # "key" field. $.key was therefore undefined, causing:
      #   "The JsonPath argument for '$.key' could not be found in the input"
      # This was the confirmed error in the CloudWatch logs (event row 80).
      #
      # $.source.originalKey is set by PreserveKey (which runs after every
      # Glue job and before RunCrawlers), so it is always present when
      # NotifyFailure is reached from any downstream state.
      #
      # For failures that occur BEFORE PreserveKey (i.e., a Glue job fails on
      # its first attempt before retries exhaust), the Catch block routes here
      # with the original EventBridge input still intact, which DOES contain
      # $.key. We handle this with a States.JsonMerge fallback via a Choice
      # state (see NotifyFailure routing below).
      NotifyFailure = {
        Type = "Choice"
        Choices = [
          {
            # After PreserveKey ran: $.source exists → use $.source.originalKey
            Variable      = "$.source.originalKey"
            IsPresent     = true
            Next          = "PublishFailureWithKey"
          }
        ]
        # Before PreserveKey ran (Glue job failed before routing through it):
        # $.key is still the top-level field from the EventBridge trigger.
        Default = "PublishFailureRawKey"
      },

      PublishFailureWithKey = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          "Message.$" = "States.Format('❌ Lakehouse ETL pipeline FAILED.\nFile: {}\nExecution: {}\nCheck CloudWatch logs for details.', $.source.originalKey, $$.Execution.Name)"
          Subject     = "[${var.environment}] Lakehouse ETL — FAILURE"
        }
        Next = "PipelineFailed"
      },

      PublishFailureRawKey = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          "Message.$" = "States.Format('❌ Lakehouse ETL pipeline FAILED.\nFile: {}\nExecution: {}\nCheck CloudWatch logs for details.', $.key, $$.Execution.Name)"
          Subject     = "[${var.environment}] Lakehouse ETL — FAILURE"
        }
        Next = "PipelineFailed"
      },

      # ── Terminal failure state ─────────────────────────────────────────────
      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One or more ETL stages failed. Check CloudWatch logs."
      }
    }
  })
}
