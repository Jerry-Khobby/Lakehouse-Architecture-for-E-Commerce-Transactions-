# Step Functions State Machine — Every State Explained

## Overview

The pipeline's orchestration is a STANDARD-type Step Functions state machine. STANDARD is chosen over EXPRESS because STANDARD provides exactly-once execution semantics, persistent execution history, and maximum execution duration up to one year — none of which EXPRESS provides. The state machine receives a single execution input containing the S3 bucket, batch identifier, and the S3 keys for all three source files, then runs each ETL stage in strict sequence before invoking crawlers in parallel.

---

## Execution Input

Every execution begins with this input structure, produced by `ingest.py` calling `sfn_client.start_execution()`:

```json
{
  "bucket": "ecom-lakehouse-dev-data-123456789012",
  "batch": "apr_2025",
  "files": {
    "products":    "raw/apr_2025/products/products.csv",
    "orders":      "raw/apr_2025/orders/orders_apr_2025.csv",
    "order_items": "raw/apr_2025/order_items/order_items_apr_2025.csv"
  }
}
```

This input is passed to every state that needs it. States that call Glue jobs extract from it using `States.Format` to construct the S3 path argument. States that do not need to transform data simply pass it through via `ResultPath`.

---

## `ResultPath` — The PreserveKey Pattern

By default, a Step Functions Task state replaces the entire execution input with the task's output. A Glue StartJobRun `.sync` integration returns a response object like `{"JobRunId": "jr_abc123", "JobRunState": "SUCCEEDED", ...}`. If this output replaced the input, `$.bucket`, `$.batch`, and `$.files` would be gone — the next state would have no source path information.

The **PreserveKey pattern** prevents this using `ResultPath`:

```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::glue:startJobRun.sync:2",
  "Parameters": { ... },
  "ResultPath": "$.productsJobResult",
  "Next": "ProcessOrders"
}
```

`ResultPath: "$.productsJobResult"` writes the Glue task output into a new key `$.productsJobResult` on the existing input object, rather than replacing the object. After `ProcessProducts` completes, the state is:

```json
{
  "bucket": "ecom-lakehouse-dev-data-123456789012",
  "batch": "apr_2025",
  "files": { "products": "...", "orders": "...", "order_items": "..." },
  "productsJobResult": { "JobRunId": "jr_abc123", "JobRunState": "SUCCEEDED" }
}
```

The original keys are intact. `ProcessOrders` reads `$.files.orders`, `ProcessOrderItems` reads `$.files.order_items`. Each subsequent state appends its own `*JobResult` key without disturbing the others.

---

## State Walkthrough

### 1. `ValidateInput` — Pass State

```json
{
  "Type": "Pass",
  "Comment": "Validates execution input shape is present; no transformation needed.",
  "ResultPath": null,
  "Next": "RouteToETLJob"
}
```

`ResultPath: null` discards any output the Pass state might produce — the input flows through unchanged. This state exists as a named anchor: the execution history shows `ValidateInput` as the first state, making it visually clear in the console that the execution started at input validation before any Glue job ran. It also provides a natural place to attach a Choice state or a Lambda-based validation in future without changing the downstream state sequence.

### 2. `RouteToETLJob` — Choice State

```json
{
  "Type": "Choice",
  "Choices": [
    {
      "Variable": "$.files.order_items",
      "IsPresent": true,
      "Next": "ProcessProducts"
    },
    {
      "Variable": "$.files.orders",
      "IsPresent": true,
      "Next": "ProcessProducts"
    }
  ],
  "Default": "ProcessProductsOnly"
}
```

This Choice state checks whether the execution input contains the expected file keys before committing to the full three-job pipeline. It gates the ETL path on what files are actually present.

**Why `order_items` is checked before `orders`:**

Step Functions Choice states evaluate conditions in declared order and take the first match. The full three-dataset pipeline requires all three files. The condition "all three files are present" could be checked by verifying any one of them (since the ingestion script always uploads all three or fails), but `order_items` is checked first because it is the highest-dependency dataset: if `order_items` is present, by construction `orders` and `products` must also be present (the ingestion script would not have called `start_execution()` with a partial `files` dict). Checking `order_items` first is therefore a single-condition proxy for the "all three present" case.

The `orders` check is a fallback for a partial batch (products + orders only, no order_items), routing to a two-job path. The `Default` handles a products-only batch.

This routing structure means the state machine degrades gracefully if upstream ingestion sends partial files — it does not fail immediately, it runs what it can.

### 3. `ProcessProducts` — Glue Task State

```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::glue:startJobRun.sync:2",
  "Parameters": {
    "JobName": "ecom-lakehouse-products-job",
    "Arguments": {
      "--SOURCE_BUCKET.$":           "$.bucket",
      "--SOURCE_KEY.$":              "$.files.products",
      "--DATA_BUCKET.$":             "$.bucket",
      "--PROCESSED_DATA_PREFIX":     "lakehouse-dwh/",
      "--GLUE_DATABASE":             "ecom_lakehouse",
      "--SNS_TOPIC_ARN":             "arn:aws:sns:...",
      "--ENVIRONMENT":               "dev"
    }
  },
  "HeartbeatSeconds": 300,
  "TimeoutSeconds": 7200,
  "ResultPath": "$.productsJobResult",
  "Retry": [ ... ],
  "Catch": [ ... ],
  "Next": "ProcessOrders"
}
```

**`HeartbeatSeconds: 300` vs `TimeoutSeconds: 7200`:**

`TimeoutSeconds: 7200` is the absolute maximum the state can run — 2 hours. If the Glue job has not completed after 2 hours, Step Functions raises `States.Timeout` and the execution fails. This prevents a stuck Glue job from holding a Step Functions execution open indefinitely.

`HeartbeatSeconds: 300` is a liveness check on the Glue worker. The `.sync:2` Glue integration sends heartbeat events from Glue to Step Functions every few minutes while the job is active. If Step Functions receives no heartbeat for 300 seconds (5 minutes), it raises `States.HeartbeatTimeout`. This catches jobs where the Glue worker itself has crashed or lost connectivity rather than the job taking a long time — a subtle but important difference. A job legitimately running for 90 minutes will not trigger `HeartbeatTimeout` because it keeps sending heartbeats; a dead worker that produced no output after 5 minutes does trigger it.

**`States.Format` for dynamic paths:**

The `"--SOURCE_KEY.$": "$.files.products"` syntax (note the `.$` suffix on the key name) tells Step Functions to resolve the value as a JSONPath reference against the execution state. The value `$.files.products` is not a literal string — it is a path that resolves to `"raw/apr_2025/products/products.csv"` at execution time. Without the `.$` suffix, the literal string `"$.files.products"` would be passed to the Glue job, not the resolved S3 key.

### 4. `ProcessOrders` — Glue Task State

Identical structure to `ProcessProducts` with `JobName: "ecom-lakehouse-orders-job"`, `"--SOURCE_KEY.$": "$.files.orders"`, and `ResultPath: "$.ordersJobResult"`.

`ProcessOrders` runs only after `ProcessProducts` completes successfully. If `ProcessProducts` fails, the Catch block intercepts and routes to `NotifyFailure` — `ProcessOrders` never starts. This is not a fault tolerance measure; it is a dependency requirement. The orders job does not use the products Delta table directly, so there is no technical reason orders must wait for products. The sequential ordering exists for operational cleanliness: a partial pipeline run where orders committed but products did not produces an inconsistent Silver layer state that is harder to debug.

### 5. `ProcessOrderItems` — Glue Task State

Identical structure with `JobName: "ecom-lakehouse-order-items-job"`, `"--SOURCE_KEY.$": "$.files.order_items"`, `ResultPath: "$.orderItemsJobResult"`.

**Why `ProcessOrderItems` is the last ETL state, not concurrent with `ProcessOrders`:**

`order_items_job.py` performs referential integrity checks against both the products Delta table and the orders Delta table via `left_anti` join. Both checks read the current committed state of those tables. If `ProcessOrderItems` ran concurrently with `ProcessOrders`, there is a race:

- `ProcessOrders` starts its MERGE at time T
- `ProcessOrderItems` starts its referential check at time T+2 seconds
- The orders MERGE has not committed yet
- `left_anti` join against the orders Delta table finds no matching `order_id` values
- 100% of order_items are rejected as `"unknown_order_id"`
- `ProcessOrders` commits successfully at time T+45 seconds
- But `ProcessOrderItems` already wrote zero rows with `numTargetRowsInserted = 0`

The sequential `ProcessOrders → ProcessOrderItems` dependency is what guarantees the orders MERGE is fully committed before the `order_items_job` reads the orders Delta table. See [Referential_Integrity.md](Referential_Integrity.md) for full detail on the EventBridge race condition that led to this design.

### 6. `RunCrawlers` — Parallel State

```json
{
  "Type": "Parallel",
  "Branches": [
    {
      "StartAt": "CrawlProducts",
      "States": {
        "CrawlProducts": {
          "Type": "Task",
          "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
          "Parameters": { "Name": "ecom-lakehouse-products-crawler" },
          "Retry": [ { "ErrorEquals": ["Glue.CrawlerRunningException"], ... } ],
          "Catch": [ ... ],
          "End": true
        }
      }
    },
    {
      "StartAt": "CrawlOrders",
      "States": { ... }
    },
    {
      "StartAt": "CrawlOrderItems",
      "States": { ... }
    }
  ],
  "ResultPath": "$.crawlerResults",
  "Next": "NotifySuccess"
}
```

The three crawlers run in parallel branches. Each branch starts its crawler independently and completes when the crawler run finishes (via SDK integration polling). The `Parallel` state completes when all three branches complete — it is a join barrier. The crawler results are collected into `$.crawlerResults` without disturbing the rest of the execution state.

**Why crawlers run in parallel, not sequentially:**

The three crawlers update the Glue Data Catalog entries for three independent tables. There is no dependency between the products catalog update and the orders catalog update. Running them sequentially wastes wall-clock time: three 30-second crawlers sequential = 90 seconds; three in parallel = 30 seconds. The `Parallel` state's join barrier ensures `NotifySuccess` does not fire until all three catalogs have been updated.

Note: `update_catalog_table()` in each Glue job already registers the Delta table via `CREATE TABLE IF NOT EXISTS`. The crawlers are supplementary — they update column statistics and partition metadata that the Spark SQL registration does not populate. The pipeline would function without the crawlers, but Athena query performance benefits from the up-to-date statistics.

### 7. `NotifySuccess` — SNS Task State

```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::sns:publish",
  "Parameters": {
    "TopicArn": "arn:aws:sns:...",
    "Subject": "States.Format('[{}] Pipeline SUCCESS — batch: {}', $.environment, $.batch)",
    "Message": "States.Format('All three datasets processed. Batch: {}. Bucket: {}.', $.batch, $.bucket)"
  },
  "ResultPath": null,
  "Next": "PipelineSucceeded"
}
```

`States.Format` constructs the subject and message strings by interpolating execution state values at runtime. The `ResultPath: null` discards the SNS publish response — there is no downstream state that needs it.

### 8. `PipelineSucceeded` — Succeed State

```json
{
  "Type": "Succeed"
}
```

The terminal success state. Step Functions marks the execution as `SUCCEEDED`. The execution history is preserved for 90 days (STANDARD workflow). The CloudWatch Logs delivery captures the full event log at the `ALL` level — every state transition, every retry, every input and output.

### 9. `NotifyFailure` — SNS Task State

```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::sns:publish",
  "Parameters": {
    "TopicArn": "arn:aws:sns:...",
    "Subject": "States.Format('[{}] Pipeline FAILED — batch: {}', $.environment, $.batch)",
    "Message": "States.Format('Pipeline failed. Batch: {}. Error: {}. Cause: {}.', $.batch, $.failureDetail.Error, $.failureDetail.Cause)"
  },
  "ResultPath": null,
  "Next": "PipelineFailed"
}
```

`$.failureDetail` is populated by the Catch block that routes here (see [Error_Handling_and_Retry.md](Error_Handling_and_Retry.md)). The `Error` field contains the exception class name (e.g. `Glue.JobRunFailed`), and `Cause` contains the full exception message from the Glue job. This gives the SNS subscriber (email or Slack via Lambda) the failure context without requiring them to navigate the Step Functions console.

### 10. `PipelineFailed` — Fail State

```json
{
  "Type": "Fail",
  "Error": "PipelineFailed",
  "Cause": "One or more ETL stages failed. Check CloudWatch logs for details."
}
```

The terminal failure state. Step Functions marks the execution as `FAILED`. The Fail state is required — without it, the execution would end in `SUCCEEDED` status even after `NotifyFailure` fired, because the execution ended normally. `PipelineFailed` ensures the execution status accurately reflects the pipeline outcome. This matters for any monitoring that queries Step Functions execution status rather than SNS alerts.

---

## State Machine Topology

```
ValidateInput (Pass)
      │
      ▼
RouteToETLJob (Choice)
      │
      ├── order_items present → ProcessProducts
      ├── orders present      → ProcessProducts
      └── default             → ProcessProductsOnly
            │
            ▼
      ProcessProducts (Task, Glue .sync)
            │ success
            ▼
      ProcessOrders (Task, Glue .sync)
            │ success
            ▼
      ProcessOrderItems (Task, Glue .sync)
            │ success
            ▼
      RunCrawlers (Parallel)
       ├── CrawlProducts
       ├── CrawlOrders
       └── CrawlOrderItems
            │ all branches complete
            ▼
      NotifySuccess (Task, SNS)
            │
            ▼
      PipelineSucceeded (Succeed)

      [Any Task failure] ──Catch──▶ NotifyFailure (Task, SNS) ──▶ PipelineFailed (Fail)
```
