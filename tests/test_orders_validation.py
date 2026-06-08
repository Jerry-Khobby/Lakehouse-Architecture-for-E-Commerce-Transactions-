"""
Unit tests for orders_job.validate().

The orders pipeline reads temporal and monetary columns as strings (READ_SCHEMA)
and casts them during validation. Tests verify both rejection rules and that
the returned DataFrame has correctly typed columns (Timestamp, Decimal, Date).
"""

from decimal import Decimal
from unittest.mock import patch

from glue_jobs.orders_job import READ_SCHEMA, validate

_PATCH = "glue_jobs.orders_job.write_rejected"


def _df(spark, rows):
    return spark.createDataFrame(rows, READ_SCHEMA)


def _row(
    order_id="ORD-001", user_id="USER-1", ts="2025-04-01T10:00:00", amount="99.99", date="2025-04-01", order_num=1
):
    return (order_num, order_id, user_id, ts, amount, date)


def test_all_valid_orders_pass(spark, fake_args):
    df = _df(spark, [_row("ORD-001"), _row("ORD-002", order_num=2)])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-001")
    assert result.count() == 2


def test_null_order_id_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            (1, None, "USER-1", "2025-04-01T10:00:00", "99.99", "2025-04-01"),
            (2, "ORD-002", "USER-2", "2025-04-01T11:00:00", "50.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-002")
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_blank_order_id_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            (1, "   ", "USER-1", "2025-04-01T10:00:00", "99.99", "2025-04-01"),
            (2, "ORD-002", "USER-2", "2025-04-01T11:00:00", "50.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-003")
    assert result.count() == 1


def test_invalid_timestamp_format_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            (1, "ORD-001", "USER-1", "not-a-timestamp", "99.99", "2025-04-01"),
            (2, "ORD-002", "USER-2", "2025-04-01T10:00:00", "50.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-004")
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_negative_amount_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            (1, "ORD-001", "USER-1", "2025-04-01T10:00:00", "-50.00", "2025-04-01"),
            (2, "ORD-002", "USER-2", "2025-04-01T10:00:00", "200.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-005")
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_invalid_amount_format_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            (1, "ORD-001", "USER-1", "2025-04-01T10:00:00", "not-a-number", "2025-04-01"),
            (2, "ORD-002", "USER-2", "2025-04-01T10:00:00", "100.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-006")
    assert result.count() == 1


def test_intra_batch_dedup_keeps_latest_by_timestamp(spark, fake_args):
    df = _df(
        spark,
        [
            (1, "ORD-001", "USER-1", "2025-04-01T10:00:00", "100.00", "2025-04-01"),
            (2, "ORD-001", "USER-1", "2025-04-01T12:00:00", "110.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-007")
    assert result.count() == 1
    assert result.collect()[0]["total_amount"] == Decimal("110.00")


def test_valid_order_has_correctly_typed_columns(spark, fake_args):
    from pyspark.sql.types import DateType, DecimalType, TimestampType

    df = _df(spark, [_row()])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-008")
    assert result.count() == 1
    schema_map = {f.name: f.dataType for f in result.schema.fields}
    assert isinstance(schema_map["order_timestamp"], TimestampType)
    assert isinstance(schema_map["total_amount"], DecimalType)
    assert isinstance(schema_map["date"], DateType)


def test_null_user_id_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            (1, "ORD-001", None, "2025-04-01T10:00:00", "99.99", "2025-04-01"),
            (2, "ORD-002", "USER-2", "2025-04-01T11:00:00", "50.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-009")
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_null_total_amount_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            (1, "ORD-001", "USER-1", "2025-04-01T10:00:00", None, "2025-04-01"),
            (2, "ORD-002", "USER-2", "2025-04-01T11:00:00", "50.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-010")
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_future_timestamp_is_rejected(spark, fake_args):
    from datetime import datetime, timedelta, timezone

    future = (datetime.now(timezone.utc) + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%S")
    df = _df(
        spark,
        [
            (1, "ORD-001", "USER-1", future, "99.99", "2025-04-01"),
            (2, "ORD-002", "USER-2", "2025-04-01T10:00:00", "50.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-011")
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_date_timestamp_mismatch_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            (1, "ORD-001", "USER-1", "2025-04-01T10:00:00", "99.99", "2025-04-02"),
            (2, "ORD-002", "USER-2", "2025-04-01T10:00:00", "50.00", "2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-012")
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"
