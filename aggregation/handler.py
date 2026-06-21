"""
Aggregation Lambda handler.

Receives S3 Object Created events from EventBridge, records each landed file
in DynamoDB, and fires a single Step Functions execution once all three files
for the batch (products, orders, order_items) are present.
"""

import json
import os
import re
import time

import boto3
from botocore.exceptions import ClientError

EXPECTED_DATASETS = {"products", "orders", "order_items"}
TABLE_NAME = os.environ["BATCH_TRACKER_TABLE"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
TTL_HOURS = int(os.environ.get("TTL_HOURS", "24"))

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
    key = event["detail"]["object"]["key"]

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
            "batch": batch,
            "files": {
                "products": landed["products"],
                "orders": landed["orders"],
                "order_items": landed["order_items"],
            },
        }),
    )
    print(f"Batch {batch}: execution '{execution_name}' started.")
