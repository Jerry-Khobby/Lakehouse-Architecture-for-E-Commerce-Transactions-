"""
Unit tests for the aggregation Lambda handler (aggregation/handler.py).

The handler receives S3 Object Created events from EventBridge, records each
file in DynamoDB, and fires a single Step Functions execution once all three
files for the batch are present. These tests validate that logic without
making any real AWS calls.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

# Set required env vars before importing the module — they are read at module
# load time and will raise KeyError if absent.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("BATCH_TRACKER_TABLE", "test-batch-tracker")
os.environ.setdefault("STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123:stateMachine:test")
os.environ.setdefault("TTL_HOURS", "24")

import aggregation.handler as handler_module  # noqa: E402


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_event(bucket, key):
    return {"detail": {"bucket": {"name": bucket}, "object": {"key": key}}}


def make_context(request_id="abc12345-xxxx-yyyy-zzzz-000000000000"):
    ctx = MagicMock()
    ctx.aws_request_id = request_id
    return ctx


def make_client_error(code):
    return ClientError({"Error": {"Code": code, "Message": "test"}}, "UpdateItem")


ALL_THREE_ATTRS = {
    "batch_id": "apr_2025",
    "products": "raw/products_apr_2025.csv",
    "orders": "raw/orders_apr_2025.csv",
    "order_items": "raw/order_items_apr_2025.csv",
    "expires_at": 9999999999,
}


# ── resolve_dataset_and_batch ─────────────────────────────────────────────────


def test_resolve_products_key():
    assert handler_module.resolve_dataset_and_batch("raw/products_apr_2025.csv") == ("products", "apr_2025")


def test_resolve_orders_key():
    assert handler_module.resolve_dataset_and_batch("raw/orders_may_2025.csv") == ("orders", "may_2025")


def test_resolve_order_items_key():
    assert handler_module.resolve_dataset_and_batch("raw/order_items_apr_2025.csv") == ("order_items", "apr_2025")


def test_resolve_raises_on_unknown_dataset():
    with pytest.raises(ValueError, match="Cannot parse"):
        handler_module.resolve_dataset_and_batch("raw/customers_apr_2025.csv")


def test_resolve_raises_when_batch_label_is_missing():
    # "products.csv" has no underscore-separated batch label
    with pytest.raises(ValueError, match="Cannot parse"):
        handler_module.resolve_dataset_and_batch("raw/products.csv")


def test_resolve_raises_on_completely_unrelated_key():
    with pytest.raises(ValueError, match="Cannot parse"):
        handler_module.resolve_dataset_and_batch("lakehouse-dwh/products/part-0.parquet")


# ── handler — partial batches ─────────────────────────────────────────────────


def test_handler_one_file_landed_does_not_start_execution():
    one_file_attrs = {"batch_id": "apr_2025", "products": "raw/products_apr_2025.csv", "expires_at": 9999999999}

    with patch.object(handler_module, "ddb") as mock_ddb, patch.object(handler_module, "sfn") as mock_sfn:
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table
        mock_table.update_item.return_value = {"Attributes": one_file_attrs}

        handler_module.handler(make_event("test-bucket", "raw/products_apr_2025.csv"), make_context())

        mock_sfn.start_execution.assert_not_called()


def test_handler_two_files_landed_does_not_start_execution():
    two_file_attrs = {
        "batch_id": "apr_2025",
        "products": "raw/products_apr_2025.csv",
        "orders": "raw/orders_apr_2025.csv",
        "expires_at": 9999999999,
    }

    with patch.object(handler_module, "ddb") as mock_ddb, patch.object(handler_module, "sfn") as mock_sfn:
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table
        mock_table.update_item.return_value = {"Attributes": two_file_attrs}

        handler_module.handler(make_event("test-bucket", "raw/orders_apr_2025.csv"), make_context())

        mock_sfn.start_execution.assert_not_called()


# ── handler — complete batch ──────────────────────────────────────────────────


def test_handler_third_file_starts_exactly_one_execution():
    with patch.object(handler_module, "ddb") as mock_ddb, patch.object(handler_module, "sfn") as mock_sfn:
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table
        mock_table.update_item.side_effect = [
            {"Attributes": ALL_THREE_ATTRS},  # first call: record file + return ALL_NEW
            {"Attributes": {**ALL_THREE_ATTRS, "triggered": True}},  # second call: atomic guard
        ]

        handler_module.handler(make_event("test-bucket", "raw/order_items_apr_2025.csv"), make_context())

        mock_sfn.start_execution.assert_called_once()


def test_handler_execution_input_contains_all_three_file_keys():
    with patch.object(handler_module, "ddb") as mock_ddb, patch.object(handler_module, "sfn") as mock_sfn:
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table
        mock_table.update_item.side_effect = [
            {"Attributes": ALL_THREE_ATTRS},
            {"Attributes": {**ALL_THREE_ATTRS, "triggered": True}},
        ]

        handler_module.handler(make_event("test-bucket", "raw/order_items_apr_2025.csv"), make_context())

        payload = json.loads(mock_sfn.start_execution.call_args[1]["input"])
        assert payload["bucket"] == "test-bucket"
        assert payload["batch"] == "apr_2025"
        assert payload["files"]["products"] == "raw/products_apr_2025.csv"
        assert payload["files"]["orders"] == "raw/orders_apr_2025.csv"
        assert payload["files"]["order_items"] == "raw/order_items_apr_2025.csv"


def test_handler_execution_name_uses_batch_label_and_request_id_prefix():
    with patch.object(handler_module, "ddb") as mock_ddb, patch.object(handler_module, "sfn") as mock_sfn:
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table
        mock_table.update_item.side_effect = [
            {"Attributes": ALL_THREE_ATTRS},
            {"Attributes": {**ALL_THREE_ATTRS, "triggered": True}},
        ]

        handler_module.handler(make_event("test-bucket", "raw/order_items_apr_2025.csv"), make_context("abc12345-rest"))

        name = mock_sfn.start_execution.call_args[1]["name"]
        assert name == "apr_2025-abc12345"


# ── handler — duplicate event guard ──────────────────────────────────────────


def test_handler_duplicate_event_skips_execution():
    """EventBridge at-least-once delivery: a second invocation after triggered=True is set
    must not fire a second Step Functions execution."""
    with patch.object(handler_module, "ddb") as mock_ddb, patch.object(handler_module, "sfn") as mock_sfn:
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table
        mock_table.update_item.side_effect = [
            {"Attributes": ALL_THREE_ATTRS},  # first call succeeds
            make_client_error("ConditionalCheckFailedException"),  # guard rejects duplicate
        ]

        handler_module.handler(make_event("test-bucket", "raw/order_items_apr_2025.csv"), make_context())

        mock_sfn.start_execution.assert_not_called()


def test_handler_propagates_unexpected_dynamodb_error():
    """A DynamoDB error that is not ConditionalCheckFailedException must re-raise
    so Lambda retries and routes to the DLQ after exhausting retries."""
    with patch.object(handler_module, "ddb") as mock_ddb:
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table
        mock_table.update_item.side_effect = [
            {"Attributes": ALL_THREE_ATTRS},
            make_client_error("ProvisionedThroughputExceededException"),
        ]

        with pytest.raises(ClientError):
            handler_module.handler(make_event("test-bucket", "raw/order_items_apr_2025.csv"), make_context())
