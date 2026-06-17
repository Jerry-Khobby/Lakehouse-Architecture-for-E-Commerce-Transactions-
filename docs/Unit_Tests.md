# Unit Tests — Structure, Local PySpark, and Mock Strategies

## Overview

The test suite covers validation logic, schema enforcement, deduplication, and timestamp handling for all three Glue jobs. Tests run locally and in CI using a plain PySpark `SparkSession` — no Glue runtime, no AWS credentials, no live S3 access. Delta MERGE operations are not unit-tested (they require the full Delta JAR and a running Delta table); instead, the `validate()` functions are tested independently since they contain all the business logic. This document covers the test layout, how the local Spark session is configured, and the mock strategies for S3 and boto3 calls.

---

## Test Layout

```
tests/
├── conftest.py                  — shared pytest fixtures (SparkSession, sample DataFrames)
├── test_constants.py            — timestamp format string, threshold constants
├── test_helpers.py              — utility function tests (log_counts, build_execution_name, clean_cell)
├── test_products_validation.py  — all 5 validation checks for products_job
├── test_orders_validation.py    — all 5 validation checks for orders_job (including timestamp cast)
├── test_order_items_validation.py — all 14 checks for order_items_job
├── test_deduplication.py        — Window function dedup, rank vs row_number, composite key
├── test_common.py               — archive_source_file, write_rejected, update_catalog_table
└── test_ingestion.py            — xlsx_to_csv, clean_cell, build_execution_name
```

---

## Local PySpark Without a Glue Runtime

Glue jobs run on a `GlueContext`-managed Spark session in production. The unit tests cannot use `GlueContext` because the `awsglue` package is only available in the Glue runtime environment — it is not installable via pip. Tests use a plain `SparkSession` instead.

### `conftest.py` — Session Fixture

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR,console")
        .getOrCreate()
    )
    yield session
    session.stop()
```

**`master("local[2]")`**: Runs Spark locally with 2 threads — one for task parallelism, one for the driver. `local[1]` runs single-threaded, which can produce ordering artefacts in Window function tests. `local[2]` is the minimum for testing partition-level operations correctly.

**`spark.sql.shuffle.partitions = 2`**: The default is 200, which generates 200 output files even for a 10-row test DataFrame. Setting it to 2 reduces test execution time significantly and avoids filling the local temp directory with tiny files.

**`spark.sql.session.timeZone = UTC`**: Matches the production Spark session configuration. Tests that assert `order_timestamp` values after casting must use the same timezone as the production job or they will produce different results on different developer machines.

**`spark.ui.enabled = false`**: Disables the Spark web UI (port 4040). In CI environments with multiple parallel test workers, each worker tries to bind to port 4040 and subsequent workers fail with `BindException`. Disabling the UI also speeds up session creation.

**`scope="session"`**: The `SparkSession` is created once per `pytest` session and shared across all test modules. Creating a new `SparkSession` for each test function is possible but takes 5–10 seconds per session — for 100+ tests this adds minutes to the test run. The `scope="session"` fixture creates one session at the start and destroys it at the end.

### No Delta Lake in Unit Tests

The Delta Lake JAR (`delta-core` for Spark 3.x) is a 40+ MB dependency that requires a compatible Hadoop version. Adding it to `requirements-dev.txt` would make local test setup fragile and slow. More importantly, Delta MERGE logic is inherently integration-level — it requires a live Delta table, Delta log, and S3 (or local filesystem). Testing the MERGE semantics at the unit level would require mocking the Delta API surface, which would not test real Delta behaviour.

The unit tests deliberately exclude Delta operations. What is unit-tested instead:
- `validate()` — all validation checks, producing valid and rejected DataFrames
- `ensure_delta_table()` — the guard condition (mocked `isDeltaTable()`)
- `update_catalog_table()` — the `spark.sql()` call (mocked SparkSession)
- `archive_source_file()` — S3 API calls (mocked boto3)

The Delta MERGE itself is covered by integration tests that run against a real Glue environment (a separate pipeline that is not part of the local test suite).

---

## Test Structure — Products Validation

```python
# tests/test_products_validation.py
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from glue_jobs.products_job import validate, PRODUCTS_SCHEMA

def test_null_product_id_rejected(spark):
    data = [(None, 1, "produce", "Apple"), (42, 1, "produce", "Banana")]
    df = spark.createDataFrame(data, schema=PRODUCTS_SCHEMA)
    valid, rejected = validate(df)
    assert valid.count() == 1
    assert rejected.count() == 1
    assert rejected.collect()[0]["rejection_reason"] == "null_product_id"

def test_invalid_id_rejected(spark):
    data = [(0, 1, "produce", "Apple"), (42, 1, "produce", "Banana")]
    df = spark.createDataFrame(data, schema=PRODUCTS_SCHEMA)
    valid, rejected = validate(df)
    assert valid.count() == 1
    reason = rejected.collect()[0]["rejection_reason"]
    assert reason == "invalid_id_value"

def test_empty_string_rejected(spark):
    data = [(42, 1, "  ", "Apple"), (43, 1, "produce", "Banana")]
    df = spark.createDataFrame(data, schema=PRODUCTS_SCHEMA)
    valid, rejected = validate(df)
    assert valid.count() == 1
    assert rejected.collect()[0]["rejection_reason"] == "empty_string_field"

def test_intra_batch_dedup_stable_ordering(spark):
    # Two rows with same product_id — lower department_id wins
    data = [
        (42, 2, "dairy", "Cheese"),
        (42, 1, "produce", "Apple"),  # lower department_id — should win
    ]
    df = spark.createDataFrame(data, schema=PRODUCTS_SCHEMA)
    valid, rejected = validate(df)
    assert valid.count() == 1
    assert valid.collect()[0]["department"] == "produce"
    assert rejected.collect()[0]["rejection_reason"] == "intra_batch_duplicate"

def test_clean_batch_passes_all_checks(spark):
    data = [(1, 1, "produce", "Apple"), (2, 2, "dairy", "Milk")]
    df = spark.createDataFrame(data, schema=PRODUCTS_SCHEMA)
    valid, rejected = validate(df)
    assert valid.count() == 2
    assert rejected is None or rejected.count() == 0
```

### One Assert Per Test

Each test verifies exactly one concept: the null PK rejection, the invalid ID rejection, etc. This follows the AmaliTech single-assert rule. A test called `test_all_validations_work` that checks five things at once produces a failure message that says "test_all_validations_work failed" — providing no information about which validation failed. One-concept tests produce `test_null_product_id_rejected FAILED`, immediately locating the problem.

---

## Timestamp Tests — The Bug That Motivated Them

```python
# tests/test_constants.py
from scripts.constants import TIMESTAMP_FMT

def test_timestamp_format_uses_t_separator():
    """Regression: was "%Y-%m-%d %H:%M:%S" (space) — rejected all May 2025 orders."""
    assert "T" in TIMESTAMP_FMT
    assert " " not in TIMESTAMP_FMT

def test_timestamp_format_matches_glue_job_format():
    from glue_jobs.orders_job import TIMESTAMP_FORMAT
    # Python strftime format and Java SimpleDateFormat are different syntaxes
    # but both must produce the T-separator ISO 8601 form
    import datetime
    sample = datetime.datetime(2025, 4, 15, 8, 30, 0)
    python_formatted = sample.strftime(TIMESTAMP_FMT)
    assert python_formatted == "2025-04-15T08:30:00"
```

```python
# tests/test_helpers.py
from glue_jobs.orders_job import TIMESTAMP_FORMAT
from pyspark.sql import functions as F

def test_timestamp_cast_succeeds_with_t_separator(spark):
    df = spark.createDataFrame([("2025-04-15T08:30:00",)], ["order_timestamp"])
    result = df.withColumn("ts", F.to_timestamp("order_timestamp", TIMESTAMP_FORMAT))
    assert result.collect()[0]["ts"] is not None

def test_timestamp_cast_fails_with_space_separator(spark):
    """Space separator must NOT parse — validates the format string is strict."""
    df = spark.createDataFrame([("2025-04-15 08:30:00",)], ["order_timestamp"])
    result = df.withColumn("ts", F.to_timestamp("order_timestamp", TIMESTAMP_FORMAT))
    assert result.collect()[0]["ts"] is None   # cast fails → null → rejected as unparseable
```

These three tests existed before the May 2025 bug was fixed but had hardcoded space-separator expected values — they were testing the wrong behaviour. After the fix (changing `TIMESTAMP_FMT` to T-separator), the tests were updated to assert the correct format. They now serve as regression tests that will fail immediately if someone changes the format back.

---

## Order Items — `STRICT_REFERENTIAL_INTEGRITY` in Tests

The referential integrity checks (`_filter_by_product_ref`, `_filter_by_order_ref`) read live Delta tables from S3. Unit tests cannot use live Delta tables. The `STRICT_REFERENTIAL_INTEGRITY` Glue job argument controls whether these checks run:

```python
# tests/test_order_items_validation.py
from glue_jobs.order_items_job import validate

TEST_ARGS = {
    "STRICT_REFERENTIAL_INTEGRITY": "false",   # Skip Delta reads
    "PRODUCTS_TABLE_PATH": "s3://unused/",
    "ORDERS_TABLE_PATH":   "s3://unused/",
}

def test_null_composite_key_rejected(spark):
    data = [(None, "ord-001", "usr-1", 42, 1, 0, None, "2025-04-15T08:30:00", "2025-04-15")]
    df = spark.createDataFrame(data, schema=ORDER_ITEMS_SCHEMA)
    valid, rejected = validate(df, args=TEST_ARGS, spark=spark)
    assert rejected.filter("rejection_reason = 'null_id'").count() == 1

def test_composite_key_dedup(spark):
    # Two rows with same (id, order_id) — later timestamp wins
    data = [
        (1, "ord-001", "usr-1", 42, 1, 0, None, "2025-04-15T09:00:00", "2025-04-15"),
        (1, "ord-001", "usr-1", 42, 2, 0, None, "2025-04-15T08:00:00", "2025-04-15"),  # earlier — loses
    ]
    df = spark.createDataFrame(data, schema=ORDER_ITEMS_SCHEMA)
    valid, rejected = validate(df, args=TEST_ARGS, spark=spark)
    assert valid.count() == 1
    assert valid.collect()[0]["add_to_cart_order"] == 1   # the row with the later timestamp
```

The 12 structural checks are fully covered without live Delta access. The referential integrity checks are covered separately in integration tests.

---

## Mock Strategies for S3 and boto3

### Mocking `archive_source_file()` — `unittest.mock.patch`

```python
# tests/test_common.py
from unittest.mock import MagicMock, patch, call
from glue_jobs.utils.common import archive_source_file
from botocore.exceptions import ClientError

def test_archive_copies_then_deletes():
    s3_mock = MagicMock()
    archive_source_file(
        s3_client=s3_mock,
        source_bucket="src-bucket",
        source_key="raw/apr_2025/orders/orders.csv",
        archive_bucket="src-bucket",
        archive_key="archived/orders/apr_2025/orders.csv",
        account_id="123456789012",
    )
    s3_mock.copy_object.assert_called_once_with(
        CopySource={"Bucket": "src-bucket", "Key": "raw/apr_2025/orders/orders.csv"},
        Bucket="src-bucket",
        Key="archived/orders/apr_2025/orders.csv",
        ExpectedBucketOwner="123456789012",
    )
    s3_mock.delete_object.assert_called_once_with(
        Bucket="src-bucket",
        Key="raw/apr_2025/orders/orders.csv",
        ExpectedBucketOwner="123456789012",
    )

def test_archive_non_fatal_on_client_error():
    """A ClientError on copy must not raise — pipeline must continue."""
    s3_mock = MagicMock()
    s3_mock.copy_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": ""}}, "CopyObject"
    )
    # Should NOT raise
    archive_source_file(
        s3_client=s3_mock,
        source_bucket="src",
        source_key="raw/file.csv",
        archive_bucket="src",
        archive_key="archived/file.csv",
        account_id="123456789012",
    )
    # delete_object must NOT be called if copy failed
    s3_mock.delete_object.assert_not_called()
```

`MagicMock()` replaces the boto3 S3 client. `assert_called_once_with()` verifies both the call was made and the exact arguments — including `ExpectedBucketOwner`. The `ClientError` test verifies the non-fatal behaviour documented in [Archival_Strategy.md](Archival_Strategy.md) and that `delete_object` is not called if `copy_object` fails (preventing data loss).

### Mocking `write_rejected()` — `moto`

For tests that need a real S3 API response (not just argument verification), `moto` provides an in-memory S3 implementation:

```python
import boto3
import moto

@moto.mock_s3
def test_write_rejected_writes_parquet(spark):
    # Create the mock bucket
    s3 = boto3.client("s3", region_name="eu-west-1")
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    rejected_df = spark.createDataFrame(
        [("null_product_id", 42)],
        ["rejection_reason", "product_id"],
    )
    write_rejected(
        spark=spark,
        rejected_df=rejected_df,
        dataset="products",
        run_id="jr_test123",
        s3_bucket="test-bucket",
    )

    # Verify an object was written under the expected prefix
    objects = s3.list_objects_v2(Bucket="test-bucket", Prefix="rejected/products/")
    assert objects["KeyCount"] > 0
```

`moto` intercepts all `boto3` calls within the `@moto.mock_s3` context and routes them to an in-memory S3 store. No real AWS credentials or network access are needed. The test verifies that `write_rejected()` actually wrote an object to the correct prefix — a level of confidence that `MagicMock` cannot provide.

### Mocking `update_catalog_table()` — SparkSession `sql` Patch

```python
from unittest.mock import patch, MagicMock
from glue_jobs.utils.common import update_catalog_table

def test_update_catalog_runs_correct_sql(spark):
    with patch.object(spark, "sql") as mock_sql:
        update_catalog_table(
            spark=spark,
            database="ecom_lakehouse",
            table_name="orders",
            table_path="s3://bucket/lakehouse-dwh/orders/",
        )
        mock_sql.assert_called_once_with(
            "CREATE TABLE IF NOT EXISTS `ecom_lakehouse`.`orders` "
            "USING DELTA LOCATION 's3://bucket/lakehouse-dwh/orders/'"
        )
```

Patching `spark.sql` prevents the test from needing a real Hive metastore. The test verifies the exact SQL string — including backtick quoting on the database and table names — because incorrect quoting would produce SQL injection vulnerability or `ParseException` in production.

---

## `requirements-dev.txt`

```
pyspark==3.3.2         # Match Glue 4.0 PySpark version exactly
pytest==8.x
pytest-cov
moto[s3]==5.x
ruff
mypy
openpyxl               # For ingestion xlsx tests
boto3                  # For mock targets
```

`pyspark==3.3.2` matches the version in AWS Glue 4.0 exactly. Running tests on PySpark 3.5 while the production job runs on 3.3.2 can hide compatibility issues: a function available in 3.5 but not 3.3.2 would pass CI but fail in production. Pinning to the exact Glue version catches these mismatches at test time.

---

## Coverage Targets

```ini
# pytest.ini or pyproject.toml
[tool.pytest.ini_options]
addopts = "--cov=glue_jobs --cov=ingestion --cov-report=term-missing --cov-fail-under=90"
```

The AmaliTech standard targets 100% statement and branch coverage. The 90% threshold in CI (`--cov-fail-under=90`) is a pragmatic floor that excludes the `if __name__ == "__main__"` blocks and the Glue runtime-specific `build_spark_session()` branches that cannot run locally. The validation functions themselves (the business logic core) are expected to reach 100% — any uncovered branch in `validate()` represents a validation rule that has no test for its failure case.
