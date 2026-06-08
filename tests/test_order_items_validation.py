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


def _row(
    item_id="1",
    order_id="ORD-001",
    user_id="USER-1",
    days=None,
    product_id="5",
    cart_order="1",
    reordered="0",
    ts="2025-04-01T10:00:00",
    date="2025-04-01",
):
    return (item_id, order_id, user_id, days, product_id, cart_order, reordered, ts, date)


def test_all_valid_items_pass(spark, fake_args):
    df = _df(spark, [_row("1", "ORD-001"), _row("2", "ORD-002")])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-001", spark)
    assert result.count() == 2


def test_null_item_id_in_composite_key_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row(item_id=None, order_id="ORD-001"),
            _row(item_id="2", order_id="ORD-002"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-002", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_null_order_id_in_composite_key_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row(item_id="1", order_id=None),
            _row(item_id="2", order_id="ORD-002"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-003", spark)
    assert result.count() == 1


def test_invalid_reordered_flag_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", reordered="2"),  # must be 0 or 1
            _row("2", "ORD-002", reordered="0"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-004", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_non_positive_cart_order_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", cart_order="0"),  # must be > 0
            _row("2", "ORD-002", cart_order="1"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-005", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_out_of_range_days_since_prior_order_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", days="400"),  # exceeds MAX_DAYS_SINCE_PRIOR = 365
            _row("2", "ORD-002", days="30"),
        ],
    )
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
    df = _df(
        spark,
        [
            _row("1", "ORD-001", ts="bad-timestamp"),
            _row("2", "ORD-002", ts="2025-04-01T10:00:00"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-008", spark)
    assert result.count() == 1


def test_referential_check_skipped_in_non_strict_mode(spark, fake_args):
    # DeltaTable.isDeltaTable → False (conftest mock) and fake_args sets
    # STRICT_REFERENTIAL_INTEGRITY=false, so the integrity steps are skipped and
    # all rows pass through unchanged.
    df = _df(spark, [_row("1", "ORD-001"), _row("2", "ORD-002")])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-009", spark)
    assert result.count() == 2


def test_referential_check_raises_when_strict_and_upstream_missing(spark, fake_args):
    # In production (strict mode) a missing upstream Delta table means the
    # products/orders job failed, so order_items must abort rather than admit
    # orphan rows. DeltaTable.isDeltaTable → False (conftest mock) triggers it.
    strict_args = dict(fake_args)
    strict_args["STRICT_REFERENTIAL_INTEGRITY"] = "true"
    df = _df(spark, [_row("1", "ORD-001")])
    with patch(_PATCH, return_value=0):
        with pytest.raises(RuntimeError, match="referential integrity"):
            validate(df, strict_args, "run-018", spark)


def test_invalid_date_format_is_rejected(spark, fake_args):
    # A non-null but unparseable date must be rejected, not silently dropped.
    df = _df(
        spark,
        [
            _row("1", "ORD-001", date="01-04-2025"),  # wrong format → unparseable
            _row("2", "ORD-002", date="2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-019", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_null_user_id_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", user_id=None),
            _row("2", "ORD-002"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-010", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_null_required_field_product_id_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", product_id=None),
            _row("2", "ORD-002"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-011", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_null_required_field_order_timestamp_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", ts=None),
            _row("2", "ORD-002"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-012", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_invalid_id_format_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row(item_id="not-a-number", order_id="ORD-001"),
            _row(item_id="2", order_id="ORD-002"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-013", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_invalid_product_id_value_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", product_id="0"),
            _row("2", "ORD-002", product_id="5"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-014", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_future_timestamp_is_rejected(spark, fake_args):
    from datetime import datetime, timedelta, timezone

    future = (datetime.now(timezone.utc) + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%S")
    df = _df(
        spark,
        [
            _row("1", "ORD-001", ts=future),
            _row("2", "ORD-002", ts="2025-04-01T10:00:00"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-015", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_date_timestamp_mismatch_is_rejected(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", ts="2025-04-01T10:00:00", date="2025-04-02"),
            _row("2", "ORD-002", ts="2025-04-01T10:00:00", date="2025-04-01"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-016", spark)
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ORD-002"


def test_intra_batch_dedup_on_composite_key_keeps_latest(spark, fake_args):
    df = _df(
        spark,
        [
            _row("1", "ORD-001", ts="2025-04-01T10:00:00"),
            _row("1", "ORD-001", ts="2025-04-01T12:00:00"),
            _row("2", "ORD-002"),
        ],
    )
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-017", spark)
    assert result.count() == 2
