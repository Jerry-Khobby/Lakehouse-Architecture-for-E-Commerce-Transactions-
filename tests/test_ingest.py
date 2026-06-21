"""
Unit tests for the batch ingestion entrypoint (ingestion.ingest) and the
shared pipeline utilities (ingestion.pipeline).

run_ingestion() now only uploads files — EventBridge + the aggregation Lambda
handle the Step Functions trigger. start_etl_batch() and build_execution_name()
are retained as manual-override utilities and are tested here to protect the
emergency-trigger path.
"""

import json
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from ingestion.ingest import BATCH, DATASETS
from ingestion.pipeline import build_execution_name, start_etl_batch


def test_datasets_cover_the_three_relational_tables():
    assert set(DATASETS) == {"products", "orders", "order_items"}


def test_datasets_keys_carry_batch_label():
    for dataset, spec in DATASETS.items():
        assert BATCH in spec["key"], (
            f"S3 key for '{dataset}' must contain the batch label '{BATCH}' "
            f"so the aggregation Lambda can parse it. Got: {spec['key']}"
        )


def test_build_execution_name_is_sanitized_and_bounded():
    name = build_execution_name(BATCH)
    assert name.startswith(f"{BATCH}-")
    assert len(name) <= 80
    assert all(char.isalnum() or char in "-_" for char in name)


def test_start_etl_batch_sends_structured_input_and_returns_arn():
    sfn = MagicMock()
    sfn.start_execution.return_value = {"executionArn": "arn:aws:states:::execution:x"}
    files = {
        "products": "raw/products_apr_2025.csv",
        "orders": "raw/orders_apr_2025.csv",
        "order_items": "raw/order_items_apr_2025.csv",
    }

    arn = start_etl_batch(sfn, "arn:sm", "data-bucket", BATCH, files)

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
        start_etl_batch(sfn, "arn:sm", "data-bucket", BATCH, {"products": "raw/products_apr_2025.csv"})


def test_start_etl_batch_exits_on_other_client_error():
    sfn = MagicMock()
    sfn.start_execution.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "no"}}, "StartExecution"
    )
    with pytest.raises(SystemExit):
        start_etl_batch(sfn, "arn:sm", "data-bucket", BATCH, {"products": "raw/products_apr_2025.csv"})
