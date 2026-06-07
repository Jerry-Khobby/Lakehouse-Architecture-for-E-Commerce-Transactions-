"""
Unit tests for order_items_job.validate().

order_items has the most complex validation: composite primary key,
business rule checks on behavioural flags, and referential integrity checks
against external Delta tables. Referential checks are bypassed in unit tests
because DeltaTable.isDeltaTable is mocked to return False in conftest.py.
"""

from unittest.mock import patch

import pytest

from glue_jobs.order_items_job import READ_SCHEMA, validate

_PATCH = "glue_jobs.order_items_job.write_rejected"


def _df(spark, rows):
    return spark.createDataFrame(rows, READ_SCHEMA)


def _row(item_id="1", order_id="ORD-001", user_id="USER-1",
         days=None, product_id="5", cart_order="1",
         reordered="0", ts="2025-04-01T10:00:00", date="2025-04-01"):
    return (item_id, order_id, user_id, days, product_id, cart_order, reordered, ts, date)


def test_all_valid_items_pass(spark, fake_args):
    df = _df(spark, [_row("1", "ORD-001"), _row("2", "ORD-002")])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-001", spark)
    assert result.count() == 2


def test_null_item_id_in_composite_key_is_rejected(spark, fake_args):
    df = _df(spark, [
        _row(item_id=None, order_id="ORD-001"),
        _row(item_id="2",  order_id="ORD-002"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-002", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_null_order_id_in_composite_key_is_rejected(spark, fake_args):
    df = _df(spark, [
        _row(item_id="1", order_id=None),
        _row(item_id="2", order_id="ORD-002"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-003", spark)
    assert result.count() == 1


def test_invalid_reordered_flag_is_rejected(spark, fake_args):
    df = _df(spark, [
        _row("1", "ORD-001", reordered="2"),   # must be 0 or 1
        _row("2", "ORD-002", reordered="0"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-004", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_non_positive_cart_order_is_rejected(spark, fake_args):
    df = _df(spark, [
        _row("1", "ORD-001", cart_order="0"),   # must be > 0
        _row("2", "ORD-002", cart_order="1"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-005", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_out_of_range_days_since_prior_order_is_rejected(spark, fake_args):
    df = _df(spark, [
        _row("1", "ORD-001", days="400"),    # exceeds MAX_DAYS_SINCE_PRIOR = 365
        _row("2", "ORD-002", days="30"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-006", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_null_days_since_prior_is_allowed(spark, fake_args):
    df = _df(spark, [_row("1", "ORD-001", days=None)])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-007", spark)
    assert result.count() == 1
    assert result.collect()[0]["days_since_prior_order"] is None


def test_invalid_timestamp_format_is_rejected(spark, fake_args):
    df = _df(spark, [
        _row("1", "ORD-001", ts="bad-timestamp"),
        _row("2", "ORD-002", ts="2025-04-01T10:00:00"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-008", spark)
    assert result.count() == 1


def test_referential_check_skipped_when_delta_tables_absent(spark, fake_args):
    # DeltaTable.isDeltaTable → False (conftest mock).
    # All rows pass through the ref integrity steps unchanged.
    df = _df(spark, [_row("1", "ORD-001"), _row("2", "ORD-002")])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-009", spark)
    assert result.count() == 2
