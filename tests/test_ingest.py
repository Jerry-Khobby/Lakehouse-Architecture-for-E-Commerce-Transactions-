"""
Unit tests for the batch ingestion entrypoint (ingestion.ingest).

These cover the orchestration logic introduced with the single-batch trigger:
the structured Step Functions input, execution-name sanitisation, and the
explicit error handling around StartExecution.
"""

import json
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from ingestion.ingest import BATCH, DATASETS, build_execution_name, start_etl_batch


def test_datasets_cover_the_three_relational_tables():
    assert set(DATASETS) == {"products", "orders", "order_items"}


def test_build_execution_name_is_sanitized_and_bounded():
    name = build_execution_name(BATCH)
    assert name.startswith(f"{BATCH}-")
    assert len(name) <= 80
    assert all(char.isalnum() or char in "-_" for char in name)


def test_start_etl_batch_sends_structured_input_and_returns_arn():
    sfn = MagicMock()
    sfn.start_execution.return_value = {"executionArn": "arn:aws:states:::execution:x"}
    files = {"products": "raw/products.csv"}

    arn = start_etl_batch(sfn, "arn:sm", "data-bucket", files)

    assert arn == "arn:aws:states:::execution:x"
    kwargs = sfn.start_execution.call_args[1]
    assert kwargs["stateMachineArn"] == "arn:sm"
    payload = json.loads(kwargs["input"])
    assert payload == {"bucket": "data-bucket", "batch": BATCH, "files": files}


def test_start_etl_batch_exits_when_execution_already_exists():
    sfn = MagicMock()
    sfn.start_execution.side_effect = ClientError(
        {"Error": {"Code": "ExecutionAlreadyExists", "Message": "dup"}}, "StartExecution"
    )
    with pytest.raises(SystemExit):
        start_etl_batch(sfn, "arn:sm", "data-bucket", {"products": "raw/products.csv"})


def test_start_etl_batch_exits_on_other_client_error():
    sfn = MagicMock()
    sfn.start_execution.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "no"}}, "StartExecution"
    )
    with pytest.raises(SystemExit):
        start_etl_batch(sfn, "arn:sm", "data-bucket", {"products": "raw/products.csv"})
