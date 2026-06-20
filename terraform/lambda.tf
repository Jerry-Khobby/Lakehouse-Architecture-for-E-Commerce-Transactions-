
# LAMBDA — Aggregation trigger (ALWAYS ACTIVE)
# Receives S3 Object Created events from EventBridge, tracks which files have
# landed for each batch in DynamoDB, and fires a single Step Functions execution
# once all three files (products, orders, order_items) for the batch are present.


resource "aws_sqs_queue" "aggregation_dlq" {
  name                      = "${local.name_prefix}-aggregation-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_iam_role" "lambda_aggregation_role" {
  name = "${local.name_prefix}-lambda-aggregation-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "aggregation_basic_execution" {
  role       = aws_iam_role.lambda_aggregation_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "aggregation_dynamodb" {
  name = "${local.name_prefix}-aggregation-dynamodb-policy"
  role = aws_iam_role.lambda_aggregation_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "BatchTrackerReadWrite"
      Effect = "Allow"
      Action = ["dynamodb:UpdateItem", "dynamodb:GetItem"]
      Resource = [aws_dynamodb_table.batch_tracker.arn]
    }]
  })
}

resource "aws_iam_role_policy" "aggregation_sfn" {
  name = "${local.name_prefix}-aggregation-sfn-policy"
  role = aws_iam_role.lambda_aggregation_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "StartEtlExecution"
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.etl_pipeline.arn]
    }]
  })
}

resource "aws_iam_role_policy" "aggregation_dlq_send" {
  name = "${local.name_prefix}-aggregation-dlq-policy"
  role = aws_iam_role.lambda_aggregation_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "SendToDLQ"
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = [aws_sqs_queue.aggregation_dlq.arn]
    }]
  })
}

data "archive_file" "aggregation_lambda" {
  type        = "zip"
  output_path = "${path.module}/../aggregation_lambda.zip"

  source {
    filename = "handler.py"
    content  = <<-PYTHON
import json
import os
import re
import time
import boto3
from botocore.exceptions import ClientError

EXPECTED_DATASETS = {"products", "orders", "order_items"}
TABLE_NAME        = os.environ["BATCH_TRACKER_TABLE"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
TTL_HOURS         = int(os.environ.get("TTL_HOURS", "24"))

ddb = boto3.resource("dynamodb")
sfn = boto3.client("stepfunctions")


def resolve_dataset_and_batch(key):
    """Parse 'raw/products_apr_2025.csv' → ('products', 'apr_2025')."""
    filename = key.split("/")[-1].replace(".csv", "")
    match = re.match(r"^(products|orders|order_items)_(.+)$", filename)
    if not match:
        raise ValueError(f"Cannot parse dataset/batch from S3 key: {key}")
    return match.group(1), match.group(2)


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

    landed = response["Attributes"]
    landed_datasets = {k for k in landed if k in EXPECTED_DATASETS}

    if landed_datasets < EXPECTED_DATASETS:
        print(f"Batch {batch}: {len(landed_datasets)}/3 files landed. Waiting.")
        return

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
PYTHON
  }
}

resource "aws_lambda_function" "aggregation" {
  function_name    = "${local.name_prefix}-aggregation"
  role             = aws_iam_role.lambda_aggregation_role.arn
  filename         = data.archive_file.aggregation_lambda.output_path
  source_code_hash = data.archive_file.aggregation_lambda.output_base64sha256
  handler          = "handler.handler"
  runtime          = "python3.12"
  timeout          = 30

  environment {
    variables = {
      BATCH_TRACKER_TABLE = aws_dynamodb_table.batch_tracker.name
      STATE_MACHINE_ARN   = aws_sfn_state_machine.etl_pipeline.arn
      TTL_HOURS           = tostring(var.batch_tracker_ttl_hours)
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.aggregation_dlq.arn
  }
}

resource "aws_cloudwatch_log_group" "aggregation_lambda" {
  name              = "/aws/lambda/${aws_lambda_function.aggregation.function_name}"
  retention_in_days = 14
}

resource "aws_lambda_permission" "eventbridge_invoke_aggregation" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aggregation.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.raw_csv_upload.arn
}


# LAMBDA — Slack notification forwarder (OPTIONAL)
# SNS publishes to the pipeline_alerts topic; this Lambda receives the message
# and POSTs it to the Slack incoming-webhook URL.
#
# The entire Slack stack is gated on var.slack_webhook_url being set. Without
# the gate the Lambda is created with an empty SLACK_WEBHOOK_URL and every
# pipeline alert invokes a function that throws on urllib.urlopen("") — burning
# invocations and littering CloudWatch with errors for a feature nobody enabled.
# This mirrors the count guard already used on the email subscription in main.tf.


locals {
  slack_enabled = var.slack_webhook_url != "" ? 1 : 0
}

data "archive_file" "slack_notifier" {
  count       = local.slack_enabled
  type        = "zip"
  output_path = "${path.module}/../slack_notifier.zip"

  source {
    filename = "slack_notifier.py"
    content  = <<-PYTHON
import json
import os
import urllib.request

def handler(event, context):
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]

    for record in event.get("Records", []):
        sns_msg  = record["Sns"]
        subject  = sns_msg.get("Subject", "Lakehouse ETL Notification")
        message  = sns_msg["Message"]

        # Stages now publish a live feed: STARTED on entry, then SUCCESS or
        # FAILED on exit. Colour/icon by state so a "STARTED" event is not
        # mistaken for a failure (the old green-or-red split coloured it red).
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
PYTHON
  }
}

# -- IAM role for the Lambda ---------------------------------------------------
resource "aws_iam_role" "lambda_slack_role" {
  count = local.slack_enabled
  name  = "${local.name_prefix}-lambda-slack-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
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

# -- Lambda function -----------------------------------------------------------
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

resource "aws_cloudwatch_log_group" "slack_notifier" {
  count             = local.slack_enabled
  name              = "/aws/lambda/${aws_lambda_function.slack_notifier[0].function_name}"
  retention_in_days = 14
}

# -- Allow SNS to invoke the Lambda --------------------------------------------
resource "aws_lambda_permission" "sns_invoke_slack" {
  count         = local.slack_enabled
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.slack_notifier[0].function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.pipeline_alerts.arn
}

# -- Subscribe the Lambda to the SNS topic ------------------------------------
resource "aws_sns_topic_subscription" "slack_lambda" {
  count     = local.slack_enabled
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.slack_notifier[0].arn
}
