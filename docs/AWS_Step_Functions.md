# AWS Step Functions — State Machine Design

## Overview

The Step Functions state machine is the orchestration backbone of this pipeline. It starts a single ordered execution per ingestion batch, coordinates three Glue jobs in strict dependency order, validates the results with Athena, and routes every outcome — success or any category of failure — to an SNS notification before terminating. This document covers the state machine type, the execution input contract, the ResultPath pattern that preserves input across all states, the retry and failure branching strategy, the intrinsic function calls for notifications, and observability configuration.

---

## State Machine Type — STANDARD vs EXPRESS

```hcl
resource "aws_sfn_state_machine" "etl_pipeline" {
  type = "STANDARD"
  ...
}
```

AWS Step Functions offers two types:

| Property | STANDARD | EXPRESS |
|---|---|---|
| Execution history | Full — every state transition persisted | Not stored by default |
| Audit log | Durable — survives indefinitely in the console | Ephemeral — only in CloudWatch if configured |
| Pricing model | Per state transition | Per execution count + duration |
| Max duration | 1 year | 5 minutes |
| Execution semantics | Exactly-once for activities and Lambda | At-least-once |

**STANDARD is correct for this pipeline for three reasons:**

1. **Audit trail.** Every state transition — which Glue job ran, when it started, what the input was, what the output was, whether it retried — is stored in Step Functions execution history and queryable from the AWS console or CLI. For a data pipeline where data provenance matters, this durable record is essential.

2. **Execution duration.** A full pipeline run takes 15–20 minutes. EXPRESS executions cap at 5 minutes.

3. **Exactly-once semantics.** STANDARD guarantees that a Glue `StartJobRun` call in a `.sync` task is attempted exactly once per retry. EXPRESS provides at-least-once, which could trigger duplicate Glue job runs — a correctness problem for a pipeline performing Delta MERGEs.

---

## The Execution Input Contract

Every execution starts with a structured JSON input that carries all the context each state needs:

```json
{
  "bucket": "ecom-lakehouse-dev-data-123456789",
  "batch":  "may_2025",
  "files": {
    "products":    "raw/products.csv",
    "orders":      "raw/orders_may_2025.csv",
    "order_items": "raw/order_items_may_2025.csv"
  }
}
```

This input is produced by `start_etl_batch()` in `ingestion/pipeline.py` and passed to `sfn_client.start_execution(input=json.dumps(execution_input))`.

**Why this structure instead of hardcoding values in the state machine:**

- `bucket` varies by environment (dev vs staging vs prod account IDs differ). Passing it at runtime means the same state machine definition works across all environments.
- `files` carries the per-execution file keys. The orders key changes every month (`orders_apr_2025.csv`, `orders_may_2025.csv`). The state machine does not need to know what month it is — it receives the key and passes it to Glue.
- `batch` is a human-readable label (`apr_2025`, `may_2025`) used in SNS notification messages and execution names. It makes the execution history human-interpretable without reading the full input.

The execution name is constructed from `batch` plus a UTC timestamp:
```python
sanitized = re.sub(r"[^0-9A-Za-z_-]", "-", f"{batch}-{timestamp}")
# → "may_2025-20260615T134313"
```
The sanitisation replaces any character not allowed in a Step Functions execution name (letters, digits, hyphens, underscores). The 80-character cap prevents the name exceeding the AWS-imposed limit.

---

## The ResultPath Pattern — Preserving Input Across States

This is the most important structural pattern in the state machine. Every task state writes its result to a **dedicated sub-path** under `$.results`, never to the root of the execution state:

```hcl
RunProductsJob = {
  ...
  ResultPath = "$.results.products"
  Next       = "RunOrdersJob"
}

RunOrdersJob = {
  ...
  ResultPath = "$.results.orders"
  Next       = "RunOrderItemsJob"
}

RunOrderItemsJob = {
  ...
  ResultPath = "$.results.order_items"
  Next       = "AthenaValidation"
}

AthenaValidation = {
  ...
  ResultPath = "$.results.athena"
  Next       = "NotifySuccess"
}
```

**Why this matters:**

When a Step Functions task completes, it produces an output object (Glue's `StartJobRun` response contains `JobRunId`, status metadata, etc.). By default (`ResultPath = null` would discard the result, and omitting `ResultPath` would replace the entire state with the task output). Using `ResultPath = "$.results.products"` means:

- The task output is written to `$.results.products`.
- The original keys — `$.bucket`, `$.batch`, `$.files` — remain intact at the root level.
- Every subsequent state can still read `$.bucket` and `$.batch` as if no state had run before.

Without this pattern, `RunProductsJob`'s output (a Glue job run status object) would overwrite the entire execution state. `RunOrdersJob` would then have no `$.files.orders` to read for its `--RAW_KEY` argument, and the execution would fail with a path resolution error.

**The Catch path also uses a dedicated ResultPath:**

```hcl
Catch = [{
  ErrorEquals = ["States.ALL"]
  Next        = "NotifyFailure"
  ResultPath  = "$.error"
}]
```

When any state fails and is caught, the error details (Error code, Cause string) are written to `$.error`. This preserves `$.batch` at the root — which `NotifyFailure` needs to build its SNS message. If `ResultPath` were omitted here, the caught error object would replace the entire state, and `NotifyFailure`'s `States.Format('...Batch: {}...', $.batch)` call would fail because `$.batch` no longer exists.

The execution state at `NotifySuccess` therefore looks like this:

```json
{
  "bucket": "ecom-lakehouse-dev-data-123456789",
  "batch":  "may_2025",
  "files": { ... },
  "results": {
    "products":    { "JobRunId": "jr_abc123", ... },
    "orders":      { "JobRunId": "jr_def456", ... },
    "order_items": { "JobRunId": "jr_ghi789", ... },
    "athena":      { "QueryExecutionId": "qe_xyz", ... }
  }
}
```

All original context and all task outputs coexist without overwriting each other.

---

## Execution Flow — Linear Dependency Graph

The state machine is intentionally linear, not parallel:

```
StartAt: RunProductsJob

RunProductsJob   →(success)→  RunOrdersJob
RunOrdersJob     →(success)→  RunOrderItemsJob
RunOrderItemsJob →(success)→  AthenaValidation
AthenaValidation →(success)→  NotifySuccess → (End: true)

Any state →(error)→  NotifyFailure → PipelineFailed (Fail state)
```

**Why linear and not parallel:**

`order_items` has foreign-key references into both `products` (`product_id`) and `orders` (`order_id`). The `order_items` Glue job validates these by reading the live Delta tables that the prior jobs committed. If `RunProductsJob`, `RunOrdersJob`, and `RunOrderItemsJob` ran in a `Parallel` state, `RunOrderItemsJob` would start before either parent table was committed, its referential-integrity joins would read empty Delta tables, and every `order_items` row would be rejected as an orphan.

The linear graph makes the dependency a structural guarantee of the execution engine, not a convention in application code.

**Why there is no explicit Choice state:**

The user-visible branching in this pipeline — success path vs. failure path — is handled entirely by the `Catch` mechanism on each task, not by a `Choice` state. A `Choice` state evaluates a condition on the execution data and routes accordingly. The branching here is always error-driven: if everything works, control flows through `Next`; if anything fails, `Catch` intercepts and routes to `NotifyFailure`. This is the correct pattern for error-driven branching — `Choice` would be used if the pipeline needed to branch on a data value (e.g. "if the row count is zero, skip the MERGE and go directly to archive"), which is not a requirement in this pipeline.

---

## Glue Task Configuration

Each of the three Glue task states shares the same structure:

```hcl
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
  TimeoutSeconds   = var.sfn_timeout_seconds     # 7200 (2 hours)
  HeartbeatSeconds = 300
  Retry            = local.glue_job_retry
  Catch            = local.glue_job_catch
  ResultPath       = "$.results.orders"
  Next             = "RunOrderItemsJob"
}
```

### `.sync` Integration Pattern

`arn:aws:states:::glue:startJobRun.sync` uses the **optimistic synchronous integration** — Step Functions calls `glue:StartJobRun`, then polls `glue:GetJobRun` internally until the job reaches a terminal state (`SUCCEEDED`, `FAILED`, `STOPPED`, `ERROR`, `TIMEOUT`). The Step Functions task does not complete until the Glue job completes. There is no Lambda or callback token involved.

Without `.sync`, Step Functions would call `StartJobRun` and immediately advance to the next state — the Glue job would run unmonitored in the background and pipeline failures would go undetected.

### Dynamic Arguments via JSONPath

The `.$` suffix on argument keys enables JSONPath resolution at execution time:

```hcl
"--RAW_KEY.$"     = "$.files.orders"
"--DATA_BUCKET.$" = "$.bucket"
```

At runtime, `$.files.orders` resolves to `"raw/orders_may_2025.csv"` from the execution input. Without the `.$` suffix, the value would be treated as a literal string — the Glue job would receive `"$.files.orders"` as its `--RAW_KEY`, not the actual S3 key.

This allows the same Glue job definition to process different monthly files across executions: in April, `$.files.orders` → `raw/orders_apr_2025.csv`; in May, `$.files.orders` → `raw/orders_may_2025.csv`. No Glue job modification required.

### TimeoutSeconds vs HeartbeatSeconds

**`TimeoutSeconds = 7200`** (2 hours): The hard wall-clock limit for the entire Glue job run, from `StartJobRun` until the job reaches a terminal state. If a Glue job hangs at any stage — network partition between Glue worker and S3, a Delta MERGE waiting indefinitely for a lock — Step Functions force-terminates the task after 2 hours and fires the retry/catch logic.

**`HeartbeatSeconds = 300`** (5 minutes): A liveness check interval separate from the overall timeout. While `TimeoutSeconds` waits for the job to finish, `HeartbeatSeconds` fires if Step Functions receives no heartbeat signal from the Glue job within 5 minutes. In practice, the `.sync` integration polls Glue internally — the heartbeat mechanism here catches scenarios where the Glue API becomes unreachable (regional outage, IAM permission revocation mid-run) rather than the job itself stalling. A `HeartbeatTimedOut` error is surfaced as `States.HeartbeatTimeout` and triggers the retry/catch chain.

---

## Retry Logic

All three Glue tasks share the same retry policy:

```hcl
glue_job_retry = [{
  ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed", "States.Timeout"]
  IntervalSeconds = 30
  MaxAttempts     = 2
  BackoffRate     = 2.0
}]
```

**`ErrorEquals`:**
- `Glue.AWSGlueException`: Any error returned by the Glue API (job run failure, worker crash, OOM kill). The Glue job's `raise` propagates back to Step Functions as this error type.
- `States.TaskFailed`: Generic task failure — covers errors in the Step Functions integration layer itself (e.g. API call to Glue failed after its own internal retries).
- `States.Timeout`: Fires when `TimeoutSeconds` is exceeded. Retrying a timed-out job is appropriate for transient infrastructure hangs.

**`MaxAttempts = 2`:** The task is attempted a maximum of 3 times total (1 initial + 2 retries). Each retry starts a new Glue job run. Because the Delta MERGE is idempotent (timestamp guard prevents re-processing of already-committed data), retrying is safe — the second run will not corrupt what the first partially committed.

**`BackoffRate = 2.0` with `IntervalSeconds = 30`:** The wait between attempts follows exponential backoff:
- Retry 1: wait 30 seconds before re-attempt
- Retry 2: wait 60 seconds before re-attempt

Exponential backoff prevents immediate retry storms against a temporarily degraded service (e.g. Glue control plane slowdown) and gives transient conditions time to resolve.

**Why `ConcurrentRunsExceededException` is not in `ErrorEquals`:**

An earlier version of the retry config included this error code to handle concurrent Glue job starts. It was removed because `max_concurrent_runs = 1` on each Glue job definition is the enforcement point for preventing concurrent runs. If that guard is working correctly, `ConcurrentRunsExceededException` never fires. If it does fire (indicating two executions are running simultaneously), retrying would make it worse — the correct response is for the second execution to fail and notify, prompting the operator to investigate why two executions started in parallel.

### AthenaValidation Retry — Different Policy

```hcl
Retry = [{
  ErrorEquals     = ["Athena.AthenaException", "Athena.TooManyRequestsException"]
  IntervalSeconds = 15
  MaxAttempts     = 3
  BackoffRate     = 2.0
}]
```

Athena has different failure modes than Glue:

- `Athena.TooManyRequestsException`: Athena enforces per-workgroup concurrency limits. If multiple queries are running against the workgroup simultaneously (e.g. analysts running ad-hoc queries during the pipeline run), the `AthenaValidation` query may be rate-limited. Retrying with shorter backoff (15s, 30s, 60s) is appropriate because the query itself is trivially fast — the limit is about slot availability, not query duration.

- `MaxAttempts = 3` (one more than the Glue retry): Athena throttling tends to be short-lived. Three attempts with 15-second gaps gives the concurrent queries time to complete without an excessive wait.

The shorter initial `IntervalSeconds = 15` vs Glue's `30` reflects Athena's faster recovery from transient throttling compared to a crashed Glue worker.

---

## Failure Branching — Catch and Terminal State

### The Catch Block

```hcl
glue_job_catch = [{
  ErrorEquals = ["States.ALL"]
  Next        = "NotifyFailure"
  ResultPath  = "$.error"
}]
```

`States.ALL` is a wildcard that catches any error not handled by the `Retry` block. After all retry attempts are exhausted, the error falls through to the Catch. The error details are written to `$.error` (preserving the original input as explained in the ResultPath section), and execution transitions to `NotifyFailure`.

The AthenaValidation state has its own catch:
```hcl
Catch = [{
  ErrorEquals = ["States.ALL"]
  Next        = "NotifyFailure"
  ResultPath  = "$.error"
}]
```

Identical pattern — any unhandled Athena error (including `TABLE_NOT_FOUND` if catalog registration silently failed) routes to `NotifyFailure`.

### NotifyFailure State

```hcl
NotifyFailure = {
  Type     = "Task"
  Resource = "arn:aws:states:::sns:publish"
  Parameters = {
    TopicArn    = aws_sns_topic.pipeline_alerts.arn
    "Message.$" = "States.Format('❌ Lakehouse ETL batch FAILED.\nBatch: {}\nExecution: {}\nCheck CloudWatch logs for details.', $.batch, $$.Execution.Name)"
    Subject     = "[dev] Lakehouse ETL — FAILURE"
  }
  Next = "PipelineFailed"
}
```

**`States.Format`** is a Step Functions intrinsic function that builds a string from a template and arguments. It accesses:
- `$.batch`: from the original execution input, preserved by the ResultPath pattern on every prior state.
- `$$.Execution.Name`: from the execution context object (`$$`). This is the execution name generated by `build_execution_name()` — e.g. `may_2025-20260615T134313`. The `$$` prefix accesses Step Functions metadata (execution ARN, name, start time) rather than the execution state data (`$`).

The `"Message.$"` key with the `.$` suffix means the value is evaluated as a JSONPath/intrinsic expression rather than treated as a literal string. Without `.$`, the SNS message would literally be the string `"States.Format(...)"` rather than the formatted result.

### PipelineFailed — Terminal State

```hcl
PipelineFailed = {
  Type  = "Fail"
  Error = "PipelineFailed"
  Cause = "One or more ETL stages failed. Check CloudWatch logs."
}
```

A `Fail` state is a terminal state that marks the execution as `FAILED` in Step Functions history. Without this explicit `Fail` state, `NotifyFailure` would be an `End: true` state, which would mark the execution as `SUCCEEDED` — because `NotifyFailure` itself (the SNS publish call) succeeded. An operator looking at execution history would see a green checkmark on a failed pipeline.

The `Fail` state ensures that execution history status correctly reflects the pipeline outcome:
- All stages succeeded → execution status = `SUCCEEDED`
- Any stage failed → execution status = `FAILED`

This status is also what external monitoring (CloudWatch alarms on Step Functions execution failure metrics) reads to trigger operational alerts.

### NotifySuccess State

```hcl
NotifySuccess = {
  Type     = "Task"
  Resource = "arn:aws:states:::sns:publish"
  Parameters = {
    TopicArn    = aws_sns_topic.pipeline_alerts.arn
    "Message.$" = "States.Format('✅ Lakehouse ETL batch completed successfully.\nBatch: {}\nExecution: {}', $.batch, $$.Execution.Name)"
    Subject     = "[dev] Lakehouse ETL — SUCCESS"
  }
  End = true
}
```

`End: true` marks this as a terminal success state. No `Fail` state follows — the execution terminates with status `SUCCEEDED`.

The SNS publish here is a direct Step Functions → SNS integration (no Lambda). Step Functions' `arn:aws:states:::sns:publish` resource calls the SNS `Publish` API synchronously. The SNS topic then fans out to its subscribers: the email subscription (if configured via `var.alert_email`) and the Slack Lambda subscriber.

---

## Observability Configuration

### CloudWatch Logging

```hcl
logging_configuration {
  log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
  include_execution_data = true
  level                  = "ALL"
}
```

**`level = "ALL"`**: Logs every event — state transitions, retry attempts, catches, input/output for every state. The alternatives (`ERROR`, `FATAL`, `OFF`) log less. `ALL` is appropriate during development and for a pipeline where debugging a data quality failure requires seeing exactly what input each state received and what output it produced.

**`include_execution_data = true`**: Includes the execution state data (the `$` object) in each log event. This means the full JSON context — `$.batch`, `$.files`, `$.results`, `$.error` — is visible in CloudWatch for every state transition. Without this, log entries show only state names and timestamps, not the data being processed.

Log destination: `/aws/states/ecom-lakehouse-dev-etl-pipeline` with 30-day retention.

**The CloudWatchLogDelivery IAM permissions use `"*"` as the resource:**

```hcl
{
  Sid    = "CloudWatchLogDelivery"
  Action = [
    "logs:CreateLogDelivery",
    "logs:GetLogDelivery",
    "logs:UpdateLogDelivery",
    ...
  ]
  Resource = "*"
}
```

This is not an oversight. The CloudWatch log delivery actions (`logs:CreateLogDelivery`, `logs:GetLogDelivery`, etc.) are not resource-scopable in IAM — the IAM documentation explicitly states they only accept `"*"` as the resource. Any attempt to scope them to a specific log group ARN results in an `AccessDeniedException` when Step Functions tries to set up its log delivery configuration. This is the AWS-documented requirement.

### X-Ray Tracing

```hcl
tracing_configuration {
  enabled = true
}
```

X-Ray traces each Step Functions state transition as a segment. The resulting service map in X-Ray shows the call graph from the execution through each Glue task and SNS notification. For debugging a slow pipeline run, X-Ray identifies which state spent the most time and whether the bottleneck was the Glue job itself or the Step Functions integration layer (polling interval, IAM latency).
