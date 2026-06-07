"""
Unit tests for products_job.validate().

Each test exercises one validation rule in isolation.
write_rejected is patched to a no-op — tests assert on the returned valid
DataFrame only, not on what was written to S3.
"""

from unittest.mock import patch

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from glue_jobs.products_job import PRODUCTS_SCHEMA, validate

# Nullable version of PRODUCTS_SCHEMA used when we need to insert NULL values
# into the product_id column for testing the null-PK rejection rule.
_NULLABLE_SCHEMA = StructType([
    StructField("product_id",    IntegerType(), nullable=True),
    StructField("department_id", IntegerType(), nullable=False),
    StructField("department",    StringType(),  nullable=False),
    StructField("product_name",  StringType(),  nullable=False),
])

_PATCH = "glue_jobs.products_job.write_rejected"


def _df(spark, rows, schema=PRODUCTS_SCHEMA):
    return spark.createDataFrame(rows, schema)


def test_all_valid_rows_pass(spark, fake_args):
    df = _df(spark, [
        (1, 10, "Electronics", "Laptop"),
        (2, 20, "Books", "Python Guide"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-001")
    assert result.count() == 2


def test_null_product_id_is_rejected(spark, fake_args):
    df = _df(spark, [
        (None, 10, "Electronics", "Laptop"),
        (2,    20, "Books",       "Python Guide"),
    ], schema=_NULLABLE_SCHEMA)
    with patch(_PATCH, return_value=1):
        result = validate(df, fake_args, "run-002")
    assert result.count() == 1
    assert result.collect()[0]["product_id"] == 2


def test_non_positive_ids_are_rejected(spark, fake_args):
    df = _df(spark, [
        (0,  1, "Electronics", "Laptop"),
        (-1, 2, "Books",       "Guide"),
        (3,  3, "Food",        "Bread"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-003")
    assert result.count() == 1
    assert result.collect()[0]["product_id"] == 3


def test_empty_string_fields_are_rejected(spark, fake_args):
    df = _df(spark, [
        (1, 1, "  ",   "Laptop"),    # blank department
        (2, 2, "Books", "   "),      # blank product_name
        (3, 3, "Food",  "Bread"),    # valid
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-004")
    assert result.count() == 1
    assert result.collect()[0]["product_id"] == 3


def test_intra_batch_duplicates_keep_one_per_product(spark, fake_args):
    df = _df(spark, [
        (1, 1, "Electronics", "Laptop"),
        (1, 1, "Electronics", "Laptop v2"),   # duplicate product_id
        (2, 2, "Books",       "Guide"),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-005")
    assert result.count() == 2
    product_ids = {row["product_id"] for row in result.collect()}
    assert product_ids == {1, 2}


def test_string_fields_are_trimmed_on_output(spark, fake_args):
    df = _df(spark, [
        (1, 1, "  Electronics  ", "  Laptop  "),
    ])
    with patch(_PATCH, return_value=0):
        result = validate(df, fake_args, "run-006")
    row = result.collect()[0]
    assert row["department"] == "Electronics"
    assert row["product_name"] == "Laptop"
