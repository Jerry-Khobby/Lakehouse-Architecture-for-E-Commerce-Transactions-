# Aggregation Lambda — Design, Code, and Pipeline Wiring

## The Problem It Solves

The pipeline state machine requires all three file keys — `products`, `orders`, and `order_items` — in a single execution input before it starts. This is not optional: `order_items` carries foreign keys into both parent tables and its Glue job joins against the live Delta tables written by the two prior states. If Step Functions started the moment each individual file landed, the `order_items` job would run against empty Delta tables and silently reject every row as an invalid foreign key — the pipeline would report SUCCESS with zero data loaded.

S3 fires one event per upload. There is no native AWS mechanism that waits for three separate S3 uploads and then fires a single action. EventBridge's input transformer can reshape an event but cannot aggregate across events. Step Functions cannot wait for S3 uploads without polling. SNS fan-out fires immediately per event with no memory of prior events.

The aggregation Lambda fills this gap. It acts as a stateful accumulator: each invocation records one landed file in DynamoDB, checks whether the full set has arrived, and fires exactly one Step Functions execution the moment the third file lands.

---

## Where It Sits in the Full Flow

```text
Operator workstation
  └─ python ingestion/ingest.py
       └─ s3.put_object × 3  (products, orders, order_items)
                │
                │  S3 fires Object Created events to EventBridge
                │  (one event per upload — three events total)
                ▼
        ┌─────────────────────────────────────────────────────┐
        │              Amazon EventBridge                      │
        │  Rule: raw_csv_upload                               │
        │  Pattern: source=aws.s3, detail-type=Object Created │
        │           bucket=<data-bucket>                       │
        │           key matches raw/*.csv                      │
        └──────────────────────┬──────────────────────────────┘
                               │  lambda:InvokeFunction (× 3)
                               ▼
        ┌─────────────────────────────────────────────────────┐
        │         Aggregation Lambda (invoked 3 times)        │
        │                                                     │
        │  Invocation 1 (e.g. products_apr_2025.csv)          │
        │    → DynamoDB: SET products = "raw/products_..."    │
        │    → 1/3 present → return, wait                     │
        │                                                     │
        │  Invocation 2 (e.g. orders_apr_2025.csv)            │
        │    → DynamoDB: SET orders   = "raw/orders_..."      │
        │    → 2/3 present → return, wait                     │
        │                                                     │
        │  Invocation 3 (e.g. order_items_apr_2025.csv)       │
        │    → DynamoDB: SET order_items = "raw/order_..."    │
        │    → 3/3 present                                    │
        │    → Atomic claim: SET triggered = True             │
        │    → sfn.start_execution(all 3 keys in input)       │
        └──────────────────────┬──────────────────────────────┘
                               │  states:StartExecution (once)
                               ▼
        ┌─────────────────────────────────────────────────────┐
        │            AWS Step Functions                        │
        │  RunProductsJob → RunOrdersJob → RunOrderItemsJob   │
        │  → AthenaValidation → NotifySuccess                 │
        └─────────────────────────────────────────────────────┘
```

---

## EventBridge Wiring

### S3 → EventBridge

The data S3 bucket has EventBridge notifications enabled:

```hcl
resource "aws_s3_bucket_notification" "data_bucket" {
  bucket      = aws_s3_bucket.data.id
  eventbridge = true
}
```

With `eventbridge = true`, every S3 event on the bucket is forwarded to the account's default EventBridge event bus as an `aws.s3` event. Without this, S3 events never reach EventBridge regardless of any rules defined.

### The EventBridge Rule

```hcl
resource "aws_cloudwatch_event_rule" "raw_csv_upload" {
  name        = "${local.name_prefix}-raw-csv-upload"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.data.id] }
      object = {
        key = [{ wildcard = "raw/*.csv" }]
      }
    }
  })
}
```

**`source = ["aws.s3"]`** — only S3 events. EventBridge rules evaluate every event on the bus; this eliminates all non-S3 traffic immediately.

**`detail-type = ["Object Created"]`** — only upload events. `Object Deleted`, `Object Restore`, `Replication` events are excluded.

**`bucket.name = [<data-bucket-id>]`** — scoped to this specific bucket. Events from other S3 buckets in the account do not match.

**`key = [{ wildcard = "raw/*.csv" }]`** — matches any CSV file under the `raw/` prefix. The `wildcard` operator supports `*` (any sequence of characters except `/`) and `?` (single character). This correctly ignores uploads to `lakehouse-dwh/`, `rejected/`, `archived/`, or any other prefix. It also ignores non-CSV files if any appear in `raw/`.

> **Why `wildcard` and not `prefix` + `suffix`?** EventBridge S3 events expose the object key as a single string field (`detail.object.key`). There is no separate `detail.object.suffix` field — that field does not exist in the S3 event schema. A filter referencing a non-existent field never matches, silently dropping all events. Using `wildcard = "raw/*.csv"` applies both conditions on the actual key field in a single expression.

### EventBridge → Lambda

```hcl
resource "aws_cloudwatch_event_target" "aggregation_lambda" {
  rule      = aws_cloudwatch_event_rule.raw_csv_upload.name
  target_id = "AggregationLambda"
  arn       = aws_lambda_function.aggregation.arn
}

resource "aws_lambda_permission" "eventbridge_invoke_aggregation" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aggregation.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.raw_csv_upload.arn
}
```

The `aws_cloudwatch_event_target` tells EventBridge to invoke the Lambda when the rule matches. The `aws_lambda_permission` grants EventBridge the right to call `lambda:InvokeFunction`. Both resources are required — the target without the permission produces `AccessDeniedException`; the permission without the target means the rule has nowhere to send events.

`source_arn` on the permission scopes the grant to the specific rule ARN. No other EventBridge rule in the account can invoke this Lambda, even if it targeted the same function ARN directly.

---

## The Handler — Line by Line

### Module-Level Initialisation

```python
EXPECTED_DATASETS = {"products", "orders", "order_items"}
TABLE_NAME        = os.environ["BATCH_TRACKER_TABLE"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
TTL_HOURS         = int(os.environ.get("TTL_HOURS", "24"))

ddb = boto3.resource("dynamodb")
sfn = boto3.client("stepfunctions")
```

`EXPECTED_DATASETS` is the complete set of dataset names the Lambda waits for. Changing this set is the only code change needed to support a four-dataset batch in the future.

`TABLE_NAME` and `STATE_MACHINE_ARN` are injected by Terraform at deploy time. They are never hardcoded — using environment variables means the same Lambda code works across `dev`, `staging`, and `prod` environments with different Terraform workspaces.

`ddb` and `sfn` are initialised once at module load time, not inside `handler()`. Lambda reuses the execution environment across warm invocations. Initialising these clients once avoids the TCP connection setup overhead on every invocation — a meaningful saving when the Lambda is called three times in quick succession for a batch upload.

---

### `resolve_dataset_and_batch(key)`

```python
def resolve_dataset_and_batch(key):
    filename = key.split("/")[-1].replace(".csv", "")
    match = re.match(r"^(products|orders|order_items)_(.+)$", filename)
    if not match:
        raise ValueError(f"Cannot parse dataset/batch from S3 key: {key}")
    return match.group(1), match.group(2)
```

**What it does:** Parses an S3 key like `raw/orders_apr_2025.csv` into the tuple `("orders", "apr_2025")`.

**Step by step:**

1. `key.split("/")[-1]` — strips the `raw/` prefix, leaving `orders_apr_2025.csv`.
2. `.replace(".csv", "")` — strips the extension, leaving `orders_apr_2025`.
3. `re.match(r"^(products|orders|order_items)_(.+)$", ...)` — the regex anchors at both ends (`^` and `$`). Group 1 captures the dataset name (one of the three known names only). Group 2 captures everything after the first underscore as the batch label (`apr_2025`).

**Why the batch label matters:** The Lambda needs to group the three files for the same batch under one DynamoDB item. All three filenames carry the same batch label (`apr_2025`) — the Lambda extracts it from any of the three keys and uses it as the DynamoDB partition key. Without a consistent batch label across all three filenames, there is no way to group them.

**What happens on failure:** A key that does not match (e.g. `raw/manifest.csv`, `raw/products.csv` with no label) raises `ValueError`. Lambda catches this as an unhandled exception, marks the invocation as failed, and after the built-in retry (two attempts by default), routes the raw event to the SQS dead-letter queue. The failure is visible in CloudWatch Logs and the DLQ queue depth metric.

---

### `handler(event, context)` — Stage 1: Extract and Record

```python
def handler(event, context):
    bucket = event["detail"]["bucket"]["name"]
    key    = event["detail"]["object"]["key"]

    dataset, batch = resolve_dataset_and_batch(key)
    table = ddb.Table(TABLE_NAME)

    response = table.update_item(
        Key={"batch_id": batch},
        UpdateExpression="SET #ds = :key, expires_at = :ttl",
        ExpressionAttributeNames={"#ds": dataset},
        ExpressionAttributeValues={
            ":key": key,
            ":ttl": int(time.time()) + TTL_HOURS * 3600,
        },
        ReturnValues="ALL_NEW",
    )
```

The EventBridge S3 event payload structure is:
```json
{
  "source": "aws.s3",
  "detail-type": "Object Created",
  "detail": {
    "bucket": { "name": "ecom-lakehouse-dev-data-123456789012" },
    "object": { "key": "raw/orders_apr_2025.csv", "size": 45230 }
  }
}
```

`UpdateExpression = "SET #ds = :key"` writes the S3 key into the DynamoDB item under the dataset name as the attribute. `#ds` is an expression attribute name because `orders`, `products`, and `order_items` could conflict with reserved words in DynamoDB's expression syntax — the `#` prefix escapes them safely.

`ReturnValues = "ALL_NEW"` returns the entire item after the update in a single API call. This is critical: the Lambda needs to know which datasets have landed so far, and `ALL_NEW` provides that without requiring a separate `GetItem` call. One round-trip instead of two.

**The DynamoDB item after all three files land:**

```json
{
  "batch_id":    "apr_2025",
  "products":    "raw/products_apr_2025.csv",
  "orders":      "raw/orders_apr_2025.csv",
  "order_items": "raw/order_items_apr_2025.csv",
  "expires_at":  1750000000,
  "triggered":   true
}
```

`expires_at` is a Unix timestamp set to `now + TTL_HOURS * 3600`. DynamoDB's TTL process reads this attribute and deletes the item after the timestamp passes. This automatic cleanup handles the case where only 1 or 2 files of a batch ever arrive — the partial item does not accumulate indefinitely.

---

### `handler()` — Stage 2: Check Completeness

```python
    landed = response["Attributes"]
    landed_datasets = {k for k in landed if k in EXPECTED_DATASETS}

    if landed_datasets < EXPECTED_DATASETS:
        print(f"Batch {batch}: {len(landed_datasets)}/3 files landed. Waiting.")
        return
```

`response["Attributes"]` is the full DynamoDB item after the update — including `batch_id`, `expires_at`, and whichever dataset keys have been written so far.

`{k for k in landed if k in EXPECTED_DATASETS}` — a set comprehension that keeps only keys whose names are in `EXPECTED_DATASETS`. This correctly ignores `batch_id`, `expires_at`, and `triggered` when counting landed files.

`landed_datasets < EXPECTED_DATASETS` — set subset operator. This returns `True` if `landed_datasets` is a proper subset of `EXPECTED_DATASETS`, meaning at least one dataset is still missing. If true, the Lambda prints a progress log and returns cleanly — no Step Functions call, no error.

---

### `handler()` — Stage 3: Atomic Trigger Guard

```python
    try:
        table.update_item(
            Key={"batch_id": batch},
            UpdateExpression="SET triggered = :t",
            ConditionExpression="attribute_not_exists(triggered)",
            ExpressionAttributeValues={":t": True},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f"Batch {batch} already triggered. Skipping duplicate.")
            return
        raise
```

This is the most important step in the entire handler.

**Why it is needed:** EventBridge guarantees at-least-once delivery. For the same S3 upload event, EventBridge may invoke the Lambda more than once — typically within milliseconds of each other but occasionally minutes apart. If two invocations both see all three files present at the same time, both will attempt to call `states:StartExecution`. Running the pipeline twice on the same batch doubles the Glue job costs and creates duplicate Delta MERGE operations.

**How the guard works:** `ConditionExpression = "attribute_not_exists(triggered)"` tells DynamoDB to only perform the write if the `triggered` attribute does not yet exist on the item. This check-and-set is atomic at the DynamoDB level — there is no window between the check and the set where another invocation can slip through.

- **First invocation to reach this point:** The condition succeeds, `triggered = True` is written, execution proceeds to `start_execution`.
- **Any subsequent invocation:** The condition fails because `triggered` now exists. DynamoDB raises `ConditionalCheckFailedException`. The Lambda catches it, prints a log message, and returns cleanly. No duplicate execution starts.

`raise` at the bottom re-raises any `ClientError` that is not `ConditionalCheckFailedException` — for example, a DynamoDB throttle error or network failure. These are genuine errors and should propagate so Lambda retries them.

---

### `handler()` — Stage 4: Start Step Functions

```python
    execution_name = f"{batch}-{context.aws_request_id[:8]}"
    sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps({
            "bucket": bucket,
            "batch":  batch,
            "files": {
                "products":    landed["products"],
                "orders":      landed["orders"],
                "order_items": landed["order_items"],
            },
        }),
    )
    print(f"Batch {batch}: execution '{execution_name}' started.")
```

`execution_name` combines the batch label with the first 8 characters of the Lambda request ID. Step Functions requires execution names to be unique within a state machine — the request ID suffix ensures uniqueness if the same batch is ever re-triggered manually after the `triggered` attribute is cleared.

`input` is the exact JSON structure the state machine's states reference via `$.files.products`, `$.files.orders`, and `$.files.order_items`. Every Glue job receives its raw S3 key via `--RAW_KEY`. The file keys come from `landed`, which is the `ALL_NEW` DynamoDB response — the actual S3 keys the ingestion script uploaded, not reconstructed or guessed values.

---

## DynamoDB Batch Tracker Table

```hcl
resource "aws_dynamodb_table" "batch_tracker" {
  name         = "${local.name_prefix}-batch-tracker"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "batch_id"

  attribute {
    name = "batch_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}
```

**`billing_mode = "PAY_PER_REQUEST"`** — on-demand capacity. The table receives at most a handful of writes per pipeline run (three `UpdateItem` calls per batch). Provisioned capacity with reserved read/write units would be wasted. PAY_PER_REQUEST charges per request with no minimum.

**`hash_key = "batch_id"`** — the batch label (`apr_2025`, `may_2025`) is the partition key. All three files for a batch share the same DynamoDB item — one partition key, up to five attributes (`products`, `orders`, `order_items`, `triggered`, `expires_at`).

**TTL on `expires_at`** — DynamoDB's TTL process runs in the background and deletes items whose `expires_at` value (Unix timestamp in seconds) has passed. Default TTL is 24 hours (`TTL_HOURS = 24`). A partial batch — where only 1 or 2 files arrive and the upload is abandoned — is automatically cleaned up without any manual intervention or Lambda cleanup code.

Note: DynamoDB TTL deletion is not instantaneous. Items may persist for up to 48 hours past their TTL. This is fine for this use case — the Lambda checks the item contents, not its age, and a stale partial item simply lacks the `triggered` attribute so re-uploading the files for the same batch label would re-trigger correctly.

---

## Configuration

| Environment Variable | Terraform source | Purpose |
| --- | --- | --- |
| `BATCH_TRACKER_TABLE` | `aws_dynamodb_table.batch_tracker.name` | DynamoDB table the Lambda reads and writes |
| `STATE_MACHINE_ARN` | `aws_sfn_state_machine.etl_pipeline.arn` | Step Functions state machine to invoke |
| `TTL_HOURS` | `var.batch_tracker_ttl_hours` (default `24`) | Lifetime of a partial-batch DynamoDB item |

All three are injected by Terraform at deploy time:

```hcl
environment {
  variables = {
    BATCH_TRACKER_TABLE = aws_dynamodb_table.batch_tracker.name
    STATE_MACHINE_ARN   = aws_sfn_state_machine.etl_pipeline.arn
    TTL_HOURS           = tostring(var.batch_tracker_ttl_hours)
  }
}
```

---

## IAM — What the Lambda Can and Cannot Do

The role `ecom-lakehouse-dev-lambda-aggregation-role` holds four permissions:

| Permission | Resource | Reason |
| --- | --- | --- |
| `dynamodb:UpdateItem`, `dynamodb:GetItem` | `batch_tracker` table only | Record file arrivals and read item state |
| `states:StartExecution` | ETL state machine only | Fire the pipeline once all files are present |
| `sqs:SendMessage` | Aggregation DLQ only | Lambda's dead-letter config requires this to write failed events |
| CloudWatch Logs (`CreateLogGroup`, `CreateLogStream`, `PutLogEvents`) | Lambda log group (via `AWSLambdaBasicExecutionRole`) | Write execution logs |

The Lambda cannot read S3, write to S3, stop running executions, access other DynamoDB tables, or invoke other Lambdas. If its credentials were somehow compromised, the blast radius is limited to writing to a single DynamoDB table and starting the one state machine.

---

## Dead-Letter Queue

```hcl
resource "aws_sqs_queue" "aggregation_dlq" {
  name                      = "${local.name_prefix}-aggregation-dlq"
  message_retention_seconds = 1209600  # 14 days
}
```

Lambda retries a failed invocation twice (the default for asynchronous invocations). After both retries fail, the original EventBridge event is delivered to the DLQ as an SQS message.

**A message in the DLQ means:** a CSV file landed in S3, the EventBridge rule matched it, but the Lambda could not process it after three attempts. The file is still in `raw/` but has not been registered in DynamoDB.

**Common causes:**
- An S3 key that does not match the `^(products|orders|order_items)_(.+)$` regex — a file uploaded to `raw/` that is not one of the three expected datasets.
- DynamoDB `ProvisionedThroughputExceededException` — unlikely with PAY_PER_REQUEST billing but possible under extreme load.
- `states:StartExecution` throttle — Step Functions has a default limit of 800 new executions per second per region.

**How to respond:** Open the DLQ message in the AWS Console (`SQS → ecom-lakehouse-dev-aggregation-dlq`). The message body is the original EventBridge event JSON. Check the `detail.object.key` to identify which file caused the failure. Cross-check the Lambda CloudWatch Logs (`/aws/lambda/ecom-lakehouse-dev-aggregation`) for the error trace. If the file is valid, re-upload it via `python ingestion/ingest.py` to trigger a fresh Lambda invocation.

---

## Operational Runbook

### How to confirm the Lambda ran after an upload

1. **CloudWatch Logs** — `/aws/lambda/ecom-lakehouse-dev-aggregation`. Each invocation writes at least one log line. You should see three log streams for a three-file upload in quick succession.

2. **DynamoDB** — `ecom-lakehouse-dev-batch-tracker`. Browse items and look for the batch label (`apr_2025`). The item shows which datasets have landed and whether `triggered` is set.

3. **EventBridge** — `CloudWatch → Events → Event buses → default → Monitoring`. The `MatchedEvents` and `InvokedTargets` metrics show whether the rule matched and whether Lambda was invoked.

### How to manually re-trigger if the Lambda missed all three files

If all three files are in S3 but the Lambda never fired (no CloudWatch logs, empty DynamoDB table), re-upload the files:

```powershell
python ingestion/ingest.py
```

This overwrites the same S3 keys, which fires three new S3 Object Created events. EventBridge routes them to the Lambda again. The Lambda writes to DynamoDB as normal. Since the previous item (if any) had no `triggered` attribute, the conditional guard succeeds and Step Functions starts.

### How to force-start Step Functions without re-uploading

Use the emergency CLI command from Terraform outputs:

```powershell
cd terraform
terraform output -raw manual_sfn_trigger_command
```

This prints a ready-to-run `aws stepfunctions start-execution` command with the correct execution input. Paste and run it directly. This bypasses the Lambda entirely and requires credentials with `states:StartExecution` — the standard ingestion policy no longer grants this, so use elevated credentials or the AWS console.
