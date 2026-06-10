
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
