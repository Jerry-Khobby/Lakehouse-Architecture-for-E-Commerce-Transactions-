# AWS EventBridge — S3 Event Architecture and the Trigger Decision

## Overview

Amazon EventBridge is the AWS event bus that routes events from AWS services — including S3 — to downstream targets such as Step Functions state machines, Lambda functions, and SQS queues. For an S3-triggered pipeline, EventBridge can listen for `Object Created` events on a bucket and automatically start a Step Functions execution whenever a file lands.

This document covers how S3 EventBridge notifications work, how event pattern filters and input transformers are configured, why this architecture does not use an EventBridge trigger, and what the trigger pattern is instead.

---

## How S3 EventBridge Notifications Work

S3 emits events to EventBridge when S3 Event Notifications are enabled on a bucket. Unlike the older S3 event notification mechanism (which routes directly to Lambda, SQS, or SNS), the EventBridge path goes through the default AWS event bus and is filterable by any field in the event payload.

S3 Event Notifications to EventBridge are enabled per-bucket:

```hcl
resource "aws_s3_bucket_notification" "data_bucket" {
  bucket      = aws_s3_bucket.data.id
  eventbridge = true
}
```

Setting `eventbridge = true` tells S3 to forward all object-level events — `Object Created`, `Object Deleted`, `Object Restore Completed`, etc. — to the account's default EventBridge event bus. No other configuration on the bucket is required. The filtering happens at the EventBridge rule layer.

**This block is not present in this project's Terraform.** S3 EventBridge notifications are disabled on the data bucket. The decision is documented at `terraform/main.tf` line 801 and is explained fully below.

---

## The S3 Object Created Event Structure

When a file is uploaded to S3 (via `s3:PutObject`, `s3:CopyObject`, or multipart upload completion), EventBridge receives an event in this structure:

```json
{
  "version": "0",
  "id": "89d37573-7a3c-4d85-b7b4-9b4a81f1ca85",
  "source": "aws.s3",
  "account": "123456789012",
  "time": "2025-04-15T14:30:00Z",
  "region": "eu-west-1",
  "resources": [
    "arn:aws:s3:::ecom-lakehouse-dev-data-123456789012"
  ],
  "detail-type": "Object Created",
  "detail": {
    "version": "0",
    "bucket": {
      "name": "ecom-lakehouse-dev-data-123456789012"
    },
    "object": {
      "key": "raw/orders_apr_2025.csv",
      "size": 987654,
      "etag": "5b4a3e2f1d0c9b8a7e6f5d4c3b2a1e9d",
      "sequencer": "0055AED6DCD90281E5"
    },
    "request-id": "N4N7GDK58YTINXNJ",
    "requester": "arn:aws:iam::123456789012:user/data-engineer",
    "source-ip-address": "203.0.113.1",
    "reason": "PutObject"
  }
}
```

Key fields for filtering and transformation:

| Field | Path | Value in this pipeline |
|---|---|---|
| Event source | `$.source` | `"aws.s3"` |
| Event type | `$.detail-type` | `"Object Created"` |
| Bucket name | `$.detail.bucket.name` | `"ecom-lakehouse-dev-data-<account>"` |
| Object key | `$.detail.object.key` | `"raw/orders_apr_2025.csv"` |
| Upload reason | `$.detail.reason` | `"PutObject"` |

---

## Event Pattern Filter

An EventBridge rule uses an event pattern to select which events it acts on. Without a precise pattern, a rule fires for every S3 event on the bucket — including writes to `rejected/`, `archived/`, `flagged/`, and `lakehouse-dwh/` by the Glue jobs themselves.

A pattern filter scoped to `raw/` uploads only:

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["ecom-lakehouse-dev-data-123456789012"]
    },
    "object": {
      "key": [{
        "prefix": "raw/"
      }]
    }
  }
}
```

**`source: ["aws.s3"]`** — restricts the rule to S3 events only. EventBridge receives events from many AWS services on the default bus; without this filter a rule matching on `detail-type` alone could fire on identically-named events from other services.

**`detail-type: ["Object Created"]`** — restricts to upload events. `"Object Deleted"` and `"Object Restore Completed"` events also arrive from S3; they should not trigger a pipeline run.

**`detail.bucket.name`** — restricts to a specific bucket. Without this, if multiple S3 buckets have EventBridge enabled in the same account, the rule fires for uploads to any of them.

**`detail.object.key: [{"prefix": "raw/"}]`** — restricts to the raw landing zone. This prevents the rule from firing when Glue writes Parquet files to `lakehouse-dwh/`, when rejected records are written to `rejected/`, or when source files are copied to `archived/` — all of which are `PutObject` calls that would otherwise match an unfiltered rule.

Additional filters that could be added:

```json
"object": {
  "key": [{"suffix": ".csv"}]
}
```

Filtering by `.csv` suffix prevents the EventBridge rule from firing for the empty prefix marker objects (`raw/` with `content = ""`) that Terraform creates.

---

## Input Transformer

An EventBridge rule targeting Step Functions needs to convert the S3 event payload into the execution input format that the state machine expects. This is done with an **input transformer**: a mapping from event fields to a custom JSON template.

The Step Functions execution input contract requires:
```json
{
  "bucket": "ecom-lakehouse-dev-data-123456789012",
  "batch": "apr_2025",
  "files": {
    "orders": "raw/orders_apr_2025.csv"
  }
}
```

An input transformer that maps the S3 event fields:

```json
{
  "inputPathsMap": {
    "bucket": "$.detail.bucket.name",
    "key":    "$.detail.object.key",
    "time":   "$.time"
  },
  "inputTemplate": "{\"bucket\": \"<bucket>\", \"batch\": \"inferred\", \"files\": {\"<key>\": \"<key>\"}}"
}
```

The `inputPathsMap` extracts named values from the event using JSONPath. The `inputTemplate` places those values into the execution input string. `<bucket>`, `<key>`, and `<time>` are substituted at runtime with the extracted values.

In Terraform, this is expressed as:

```hcl
resource "aws_cloudwatch_event_target" "sfn_trigger" {
  rule      = aws_cloudwatch_event_rule.s3_raw_upload.name
  target_id = "StartEtlPipeline"
  arn       = aws_sfn_state_machine.etl_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_sfn_role.arn

  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = jsonencode({
      bucket = "<bucket>"
      files  = { "<key>" = "<key>" }
    })
  }
}
```

A separate IAM role grants EventBridge the `states:StartExecution` permission on the specific state machine ARN.

---

## Why This Pipeline Does Not Use EventBridge

The `terraform/main.tf` comment at line 801 states the decision explicitly:

```
# PIPELINE TRIGGER
#
# There is intentionally NO EventBridge S3 trigger. The three datasets form one
# relational batch (order_items references products and orders), so they are
# ingested by a SINGLE Step Functions execution started explicitly by
# ingestion/ingest.py after all three files have landed. Per-file S3 events
# would fire three independent executions and race the referential-integrity
# checks — see step_functions.tf for the full rationale.
```

There are two distinct failure modes that EventBridge triggering introduces.

### Failure Mode 1 — Three Executions for One Batch

The pipeline ingests three files per batch: `products.csv`, `orders_<batch>.csv`, and `order_items_<batch>.csv`. If EventBridge fires on every file landing in `raw/`, three independent Step Functions executions start — one per file. Each execution attempts to run all three Glue jobs (products → orders → order_items).

Two of those three executions will have `--RAW_KEY` pointing to the wrong file for that job. More critically, all three executions attempt to run `RunOrderItemsJob` concurrently against the same Delta table. Delta Lake on S3 uses `S3SingleDriverLogStore` for optimistic concurrency — the first writer to commit wins and subsequent concurrent writes raise `ConcurrentModificationException`. The Step Functions retry logic would retry those jobs, but they are retrying against a race condition, not a transient fault.

### Failure Mode 2 — The Left Anti-Join on an Empty Table

The `order_items` Glue job validates `product_id` and `order_id` referential integrity by performing a left anti-join against the live `products` and `orders` Delta tables:

```python
invalid_product_id = valid.join(
    spark.read.format("delta").load(products_path),
    on="product_id",
    how="left_anti"
)
```

If the `products.csv` upload fires its EventBridge rule and starts an execution, and that execution reaches `RunOrderItemsJob` before the products Glue job has committed its Delta table — which is likely, since EventBridge latency is near-zero and Glue startup takes 2-3 minutes — the `left_anti` join reads an empty `products` Delta table. Every row in `order_items` fails the join: every `product_id` is an "orphan" because no product has been committed yet. All `order_items` rows are written to `rejected/` as `invalid_product_id`, even though they are valid.

This failure is silent from the operator's perspective: the pipeline marks itself as `SUCCEEDED` (all three Glue jobs ran, no exception was raised), the SNS notification says `✅ batch completed`, but 100% of order_items rows are in `rejected/` and the `order_items` Delta table is empty.

### Why the Input Transformer Cannot Fix This

The input transformer maps a single S3 event to a single execution input. For the multi-file execution input that the state machine requires:

```json
{
  "bucket": "...",
  "batch": "apr_2025",
  "files": {
    "products":    "raw/products.csv",
    "orders":      "raw/orders_apr_2025.csv",
    "order_items": "raw/order_items_apr_2025.csv"
  }
}
```

An input transformer can only extract values from the single event that triggered the rule. A single `Object Created` event carries one `$.detail.object.key`. It cannot aggregate across three separate upload events. The transformer cannot produce a `files` object with all three keys from a single S3 event.

---

## What Was Implemented Instead — Explicit Trigger Pattern

The ingestion entry points (`ingest.py`, `ingest_may_2025.py`) implement a **upload-then-trigger** pattern: upload all files first, then start exactly one Step Functions execution with the complete `files` map.

```python
def run_ingestion(batch: str, datasets: dict) -> None:
    bucket = fetch_terraform_output("data_bucket_name")
    state_machine_arn = fetch_terraform_output("sfn_state_machine_arn")

    s3_client = boto3.client("s3")
    files = {}
    for dataset, spec in datasets.items():
        upload_dataset(s3_client, bucket, spec["file"], spec["key"])
        files[dataset] = spec["key"]           # collect all keys first

    sfn_client = boto3.client("stepfunctions")
    start_etl_batch(sfn_client, state_machine_arn, bucket, batch, files)
```

All three files are uploaded sequentially (products → orders → order_items), then `start_etl_batch()` fires a single `sfn:StartExecution` call with the complete files map. The execution input is identical to what an EventBridge transformer would produce — except it contains all three file keys, which is only possible once all three uploads have finished.

```python
def start_etl_batch(sfn_client, state_machine_arn, bucket, batch, files):
    execution_input = {"bucket": bucket, "batch": batch, "files": files}
    execution_name  = build_execution_name(batch)
    response = sfn_client.start_execution(
        stateMachineArn = state_machine_arn,
        name            = execution_name,
        input           = json.dumps(execution_input),
    )
    return response["executionArn"]
```

`ExecutionAlreadyExists` is caught explicitly — if the operator accidentally runs `ingest.py` twice for the same batch within the same minute (the execution name includes a UTC timestamp to the second), the second call fails gracefully rather than starting a duplicate execution.

The least-privilege IAM policy for the ingestion principal (`aws_iam_policy.ingestion`) grants:
- `s3:PutObject` on `${data_bucket_arn}/raw/*` — upload raw files
- `states:StartExecution` on the specific state machine ARN — start the execution

No `s3:GetObject`, no `states:DescribeExecution`, no other permissions. A compromised ingestion credential cannot read existing data or stop a running pipeline.

---

## What a Production EventBridge Integration Would Need

If the pipeline were extended to support fully automated triggering — where dropping three files in S3 starts the pipeline without any operator action — the correct architecture adds an aggregation Lambda between S3 events and Step Functions.

**Proposed flow:**

```
S3 Object Created (raw/)
        │
        ▼
EventBridge Rule
(source: aws.s3, key prefix: raw/)
        │
        ▼
Aggregation Lambda
  - Receives each S3 upload event
  - Stores the landed file keys in DynamoDB (keyed by batch label)
  - Checks if all expected files for this batch have landed
  - If all files present: calls states:StartExecution with the full files map
  - If not all files: exits; waits for the next upload event
        │
        ▼
Step Functions (single execution, full files map)
```

The aggregation Lambda holds the state that the input transformer cannot: which files have landed so far. The DynamoDB item for the batch is set to a TTL (e.g. 24 hours) to clean up stale partial-upload state automatically.

This pattern adds a Lambda, a DynamoDB table, an EventBridge rule, and an IAM role to the architecture, plus operational complexity around the definition of "all expected files" (which must be encoded somewhere — either as a static list or derived from the batch label). For a monthly batch where an operator is present at ingestion time, this complexity is not justified — the explicit trigger in `ingest.py` is simpler, auditable, and correct.

The aggregation Lambda becomes worthwhile when:
- Ingestion is fully automated with no operator involvement
- File arrivals are asynchronous from multiple upstream systems
- The batch size is variable (not always exactly three files)
- SLA requirements demand automatic retry on partial-upload failure without human intervention
