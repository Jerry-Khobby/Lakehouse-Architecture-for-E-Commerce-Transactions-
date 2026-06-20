# AWS Lambda — Functions in This Pipeline

This pipeline uses two Lambda functions serving distinct purposes:

1. **Aggregation Lambda** (`ecom-lakehouse-dev-aggregation`) — always active. Bridges S3 Object Created events to Step Functions by buffering file arrivals in DynamoDB until all three batch files are present, then firing a single execution.
2. **Slack Notifier Lambda** (`ecom-lakehouse-dev-slack-notifier`) — optional. Converts SNS pipeline alert messages to Slack webhook payloads.

---

## Lambda 1 — Aggregation Trigger

## Purpose

EventBridge fires one event per S3 upload. The state machine requires all three file keys (`products`, `orders`, `order_items`) in a single execution input. The aggregation Lambda bridges this mismatch: it receives each individual S3 event, tracks which files have landed for the batch in DynamoDB, and fires exactly one `states:StartExecution` call once all three are confirmed present.

## The Handler

```python
EXPECTED_DATASETS = {"products", "orders", "order_items"}

def resolve_dataset_and_batch(key):
    filename = key.split("/")[-1].replace(".csv", "")
    match = re.match(r"^(products|orders|order_items)_(.+)$", filename)
    if not match:
        raise ValueError(f"Cannot parse dataset/batch from S3 key: {key}")
    return match.group(1), match.group(2)

def handler(event, context):
    bucket = event["detail"]["bucket"]["name"]
    key    = event["detail"]["object"]["key"]

    dataset, batch = resolve_dataset_and_batch(key)

    response = table.update_item(
        Key={"batch_id": batch},
        UpdateExpression="SET #ds = :key, expires_at = :ttl",
        ExpressionAttributeNames={"#ds": dataset},
        ExpressionAttributeValues={":key": key, ":ttl": int(time.time()) + TTL_HOURS * 3600},
        ReturnValues="ALL_NEW",
    )

    landed = response["Attributes"]
    if {k for k in landed if k in EXPECTED_DATASETS} < EXPECTED_DATASETS:
        return   # waiting for remaining files

    # Atomically claim the trigger slot — prevents duplicate executions
    table.update_item(
        Key={"batch_id": batch},
        UpdateExpression="SET triggered = :t",
        ConditionExpression="attribute_not_exists(triggered)",
        ExpressionAttributeValues={":t": True},
    )

    sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=f"{batch}-{context.aws_request_id[:8]}",
        input=json.dumps({"bucket": bucket, "batch": batch, "files": {
            "products":    landed["products"],
            "orders":      landed["orders"],
            "order_items": landed["order_items"],
        }}),
    )
```

### `resolve_dataset_and_batch()`

Parses a key like `raw/orders_apr_2025.csv` into `("orders", "apr_2025")` using the regex `^(products|orders|order_items)_(.+)$`. All three filenames carry the batch label, making this parse unambiguous for every file. A key that does not match (e.g. a non-pipeline CSV in `raw/`) raises `ValueError`, which Lambda logs and marks as a failed invocation — it routes to the SQS dead-letter queue after retries.

### DynamoDB State

The `batch_tracker` table holds one item per batch. Each invocation adds its dataset key using `UpdateExpression = SET #ds = :key`. The `ReturnValues="ALL_NEW"` response gives the full item after the update, so the Lambda can check in a single round-trip whether all three datasets are now present without a separate `GetItem` call.

Items carry a TTL (`expires_at`) so a partial-upload batch that never completes is automatically cleaned up after `TTL_HOURS` (default 24 hours).

### Atomic Trigger Guard

After all three files are confirmed present, a conditional `update_item` sets `triggered = True` only if the attribute does not already exist:

```python
ConditionExpression="attribute_not_exists(triggered)"
```

If EventBridge delivers an event more than once (at-least-once delivery guarantee), two Lambda invocations may both see all three files present. Without this guard, both would call `start_execution` and fire duplicate pipeline runs. The conditional update ensures only the first invocation wins — the second catches `ConditionalCheckFailedException` and exits silently.

## Configuration

| Environment Variable | Source | Purpose |
| --- | --- | --- |
| `BATCH_TRACKER_TABLE` | Terraform inject | DynamoDB table name |
| `STATE_MACHINE_ARN` | Terraform inject | Step Functions ARN to invoke |
| `TTL_HOURS` | Terraform inject (default `24`) | Partial-batch item lifetime |

## Dead-Letter Queue

Failed Lambda invocations (after Lambda's built-in retry) are delivered to `ecom-lakehouse-dev-aggregation-dlq`. Monitor this queue in the AWS console — a message here means a file landed in S3 but the Lambda could not process it. Common causes: DynamoDB throttling, Step Functions execution limit reached, or an unparseable S3 key.

## IAM Role

The aggregation Lambda role (`ecom-lakehouse-dev-lambda-aggregation-role`) holds three inline policies:

- `BatchTrackerReadWrite` — `dynamodb:UpdateItem` and `dynamodb:GetItem` on the batch tracker table only.
- `StartEtlExecution` — `states:StartExecution` on the ETL state machine only.
- `SendToDLQ` — `sqs:SendMessage` on the aggregation DLQ only.

Plus `AWSLambdaBasicExecutionRole` (managed) for CloudWatch Logs.

## EventBridge Wiring

EventBridge invokes the Lambda via a resource-based policy (`aws_lambda_permission.eventbridge_invoke_aggregation`), not an IAM role grant. The permission is scoped to the specific EventBridge rule ARN — no other EventBridge rule in the account can invoke this function.

---

## Lambda 2 — Slack Notification Forwarder

## Overview

The Slack notification forwarder is a Python Lambda function that sits between the SNS pipeline alerts topic and a Slack incoming webhook. When any pipeline event is published to SNS — whether a per-stage progress update from a Glue job or a pipeline-level success/failure from Step Functions — SNS invokes this Lambda, which reformats the message into a Slack attachment and HTTP-POSTs it to the configured webhook URL. This document covers the Lambda configuration, the embedded Python handler, the SNS-to-Lambda subscription chain, the color-coded message routing logic, and the opt-in gating mechanism.

---

## Opt-In Gate

The entire Slack stack — the Lambda function, its IAM role, its CloudWatch log group, the SNS subscription, and the Lambda permission — is gated on a single Terraform variable:

```hcl
locals {
  slack_enabled = var.slack_webhook_url != "" ? 1 : 0
}
```

Every Slack resource uses `count = local.slack_enabled`. When `var.slack_webhook_url` is an empty string (the default), none of these resources are created. When the variable is set to a real Slack webhook URL, all resources are created together as a unit.

**Why gate instead of creating with an empty URL:** Without the gate, the Lambda would be created with `SLACK_WEBHOOK_URL = ""`. Every SNS message would invoke the function, which would call `urllib.request.urlopen("")` and raise `ValueError: unknown url type: ''`. This would log an error to CloudWatch on every single pipeline event, burning Lambda invocations and producing 30+ CloudWatch error records per pipeline run for a feature nobody enabled. The gate ensures the Slack stack is either fully operational or fully absent.

---

## The Lambda Function

```hcl
resource "aws_lambda_function" "slack_notifier" {
  count            = local.slack_enabled
  function_name    = "${local.name_prefix}-slack-notifier"
  role             = aws_iam_role.lambda_slack_role[0].arn
  filename         = data.archive_file.slack_notifier[0].output_path
  source_code_hash = data.archive_file.slack_notifier[0].output_base64sha256
  handler          = "slack_notifier.handler"
  runtime          = "python3.12"
  timeout          = 10

  environment {
    variables = {
      SLACK_WEBHOOK_URL = var.slack_webhook_url
    }
  }
}
```

**`runtime = "python3.12"`**: The handler uses only Python standard library modules (`json`, `os`, `urllib.request`) — no third-party packages required. Python 3.12 is the most recent stable Lambda runtime at the time of writing and carries no additional dependencies.

**`timeout = 10`**: The handler makes one outbound HTTP POST to the Slack webhook. Slack's API typically responds in under 1 second. The 10-second timeout provides generous headroom for Slack API slowdowns without leaving the Lambda invocation running indefinitely if Slack is unreachable. Step Functions does not wait for the Lambda — SNS invokes it asynchronously — so a hung invocation has no impact on pipeline execution, but it does consume Lambda duration billing.

**`source_code_hash`**: Terraform recalculates the MD5 of the zip archive on every `apply`. If the embedded Python handler code has changed, the hash changes, Terraform re-deploys the Lambda, and AWS atomically replaces the function code. If the code has not changed, no update occurs.

**`SLACK_WEBHOOK_URL` as environment variable**: The webhook URL is a sensitive value (`sensitive = true` on the Terraform variable). It is stored as a Lambda environment variable rather than hardcoded in the handler source. This means the handler source can be read safely (it contains no secrets), and the webhook URL can be rotated by updating the Terraform variable and re-applying without changing any application code.

### The Slack Handler

The Python handler is embedded directly in `terraform/lambda.tf` as a Terraform `archive_file` data source with inline content:

```python
import json
import os
import urllib.request

def handler(event, context):
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]

    for record in event.get("Records", []):
        sns_msg = record["Sns"]
        subject = sns_msg.get("Subject", "Lakehouse ETL Notification")
        message = sns_msg["Message"]

        upper = subject.upper()
        if "FAILED" in upper or "ERROR" in upper:
            color, icon = "#d9534f", ":x:"
        elif "SUCCESS" in upper:
            color, icon = "#36a64f", ":white_check_mark:"
        elif "STARTED" in upper:
            color, icon = "#3aa3e3", ":hourglass_flowing_sand:"
        else:
            color, icon = "#cccccc", ":information_source:"

        payload = {
            "attachments": [{
                "color":  color,
                "title":  f"{icon}  {subject}",
                "text":   message,
                "footer": "AWS Lakehouse ETL Pipeline",
            }]
        }

        data = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
```

### `event.get("Records", [])` — The SNS Event Structure

When SNS invokes a Lambda subscriber, the event payload is always a `Records` list even for a single message. Each record in the list contains a `Sns` object with the published message. The `for record in event.get("Records", [])` loop handles the case where SNS delivers a batch of messages in a single invocation (SNS can batch up to 10 records). In practice, SNS delivers pipeline alerts one at a time, but the loop is structurally correct regardless.

The `Sns` object within each record:

```json
{
  "Sns": {
    "Subject": "[dev] orders-etl — SUCCESS: Validate",
    "Message": "Stage 'Validate' completed in 12.4s.\nread=850 | valid=849 | rejected=1",
    "Timestamp": "2026-06-15T13:43:13.000Z",
    "TopicArn": "arn:aws:sns:eu-west-1:123456789012:ecom-lakehouse-dev-pipeline-alerts"
  }
}
```

### Color-Coded Message Routing

The handler checks the SNS subject (uppercased) for keywords and assigns a Slack color and emoji accordingly:

| Subject keyword | Hex color | Slack color | Emoji | Meaning |
| --- | --- | --- | --- | --- |
| `FAILED` or `ERROR` | `#d9534f` | Red | `:x:` | Stage or pipeline failure |
| `SUCCESS` | `#36a64f` | Green | `:white_check_mark:` | Stage or pipeline success |
| `STARTED` | `#3aa3e3` | Blue | `:hourglass_flowing_sand:` | Stage beginning |
| (none of the above) | `#cccccc` | Grey | `:information_source:` | Informational |

The keyword check uses the uppercased subject so `"failed"`, `"FAILED"`, and `"Failed"` all match identically. The `STARTED` keyword is checked last because a subject line like `"STARTED: Validate"` does not contain `FAILED` or `SUCCESS`, so it correctly falls through to the blue/hourglass branch.

**Why separate STARTED from SUCCESS with a distinct color:** An earlier version of the handler used only green/red routing — blue did not exist. A `STARTED` event has no `SUCCESS` in its subject, so it fell through to the red/failure branch and appeared in Slack with a red error icon next to `"STARTED: Read"`. This was confusing — an operator seeing a red banner would assume something failed. The three-color scheme makes the state machine of a pipeline run visually readable in Slack: blue (in progress) → green (done) or red (failed).

### The Slack Attachment Payload

```python
payload = {
    "attachments": [{
        "color":  color,                        # left border color
        "title":  f"{icon}  {subject}",         # bold header line
        "text":   message,                      # message body (multi-line)
        "footer": "AWS Lakehouse ETL Pipeline", # small footer text
    }]
}
```

Slack's `attachments` API renders the color as a colored left border on the message card. The `title` shows the icon emoji and the full SNS subject. The `text` shows the message body — for stage events this includes the elapsed time and row count metrics; for pipeline-level events it includes the batch name and execution name.

The outbound request uses `urllib.request` from the Python standard library — no `requests`, no `boto3`, no external dependencies. This keeps the Lambda deployment package small (no `requirements.txt`, no pip install step) and eliminates the risk of dependency version conflicts or supply-chain issues in the notification layer.

`urllib.request.urlopen(req, timeout=5)` — the 5-second timeout on the HTTP call is separate from the Lambda 10-second timeout. It ensures that a Slack API outage does not hold the Lambda open for the full 10 seconds per record. If Slack does not respond within 5 seconds, `urllib.error.URLError` is raised. The handler does not catch this exception, so it propagates to Lambda, which marks the invocation as a failure and logs the error to CloudWatch. SNS does not retry Lambda invocations for synchronous failures — the message is lost if Slack is down. This is acceptable: pipeline alert delivery is best-effort, and a failed Slack notification does not affect the pipeline.

---

## IAM — Minimal Role

```hcl
resource "aws_iam_role" "lambda_slack_role" {
  count = local.slack_enabled
  name  = "${local.name_prefix}-lambda-slack-role"

  assume_role_policy = jsonencode({
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  count      = local.slack_enabled
  role       = aws_iam_role.lambda_slack_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
```

`AWSLambdaBasicExecutionRole` is the AWS managed policy that grants:

- `logs:CreateLogGroup`
- `logs:CreateLogStream`
- `logs:PutLogEvents`

This is the minimum permission set required for a Lambda to write to CloudWatch Logs. The function does not call any AWS APIs — it only reads its environment variable and makes an outbound HTTPS call to `hooks.slack.com`. No S3, no SNS, no DynamoDB, no IAM beyond what `AWSLambdaBasicExecutionRole` provides.

---

## SNS → Lambda Subscription Chain

Two resources connect SNS to Lambda:

**Lambda permission** — grants SNS the right to invoke the function:

```hcl
resource "aws_lambda_permission" "sns_invoke_slack" {
  count         = local.slack_enabled
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.slack_notifier[0].function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.pipeline_alerts.arn
}
```

`source_arn` scopes the permission to the specific pipeline alerts topic. Without `source_arn`, any SNS topic in the account could invoke this Lambda — an unintended over-grant.

**SNS subscription** — subscribes the Lambda to the topic:

```hcl
resource "aws_sns_topic_subscription" "slack_lambda" {
  count     = local.slack_enabled
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.slack_notifier[0].arn
}
```

`protocol = "lambda"` tells SNS to invoke the function synchronously for each message. No confirmation step is required for Lambda subscriptions (unlike email subscriptions, which require clicking a confirmation link before messages are delivered).

The two resources must be created in the correct order: the Lambda permission must exist before SNS attempts to invoke the function. Terraform infers this from the resource reference — `aws_lambda_function.slack_notifier[0].function_name` in the permission depends on the function, and `aws_lambda_function.slack_notifier[0].arn` in the subscription depends on the function. Terraform resolves these dependencies automatically.

---

## The Full Notification Flow

```text
Glue Job (SnsNotifier._publish)
        │
        │  sns:Publish
        ▼
SNS Topic: ecom-lakehouse-dev-pipeline-alerts
        │
        ├──── Email subscription ──────────────────────────────▶ Inbox
        │       (if var.alert_email is set)
        │
        └──── Lambda subscription
                │
                │  lambda:InvokeFunction
                ▼
         slack_notifier.handler
                │
                │  HTTPS POST (urllib.request)
                ▼
         Slack Incoming Webhook
                │
                ▼
         Slack Channel
           ┌────────────────────────────────────────┐
           │ 🔵 [dev] orders-etl — STARTED: Validate │
           ├────────────────────────────────────────┤
           │ Stage 'Validate' started in ...        │
           │ AWS Lakehouse ETL Pipeline             │
           └────────────────────────────────────────┘
```

Step Functions publishes to the same SNS topic using its native `arn:aws:states:::sns:publish` integration — the flow is identical from the SNS topic downward. The Lambda receives Step Functions messages and Glue job messages through the same subscription and applies the same color routing logic to both.

---

## CloudWatch Logs for the Lambda

```hcl
resource "aws_cloudwatch_log_group" "slack_notifier" {
  count             = local.slack_enabled
  name              = "/aws/lambda/${aws_lambda_function.slack_notifier[0].function_name}"
  retention_in_days = 14
}
```

14-day retention (shorter than the 30-day retention on Glue and Step Functions log groups) because Lambda logs for this function are low-value after a short debugging window. The function has no business logic — it either successfully POSTs to Slack or it does not. If it fails, the error is visible in CloudWatch within minutes and resolved quickly. Retaining Lambda logs for 30 days would accumulate thousands of `START RequestId... END RequestId... REPORT ...` records that serve no purpose after the immediate debugging window.

Without this explicit `aws_cloudwatch_log_group` resource, Lambda auto-creates the log group with no retention policy and logs accumulate indefinitely. The explicit resource gives Terraform control over the log group's lifecycle, including its deletion when `terraform destroy` runs.
