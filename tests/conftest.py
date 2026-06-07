"""
conftest.py — pytest fixtures and module-level mocks for all lakehouse unit tests.

awsglue and delta.tables are mocked in sys.modules BEFORE any job module is
imported. This allows the job files (which import from awsglue at the top level)
to be loaded in a standard Python environment without the Glue runtime.

DeltaTable.isDeltaTable is mocked to return False so that referential integrity
checks inside order_items_job.validate() are bypassed in unit tests. Those
checks require live Delta tables and belong to integration tests.
"""

import sys
from unittest.mock import MagicMock

import pytest

# ── Mock AWS Glue runtime modules ──────────────────────────────────────────────
# These packages only exist in the Glue execution environment.
for _mod in ("awsglue", "awsglue.utils", "awsglue.context", "awsglue.job"):
    sys.modules[_mod] = MagicMock()

# ── Mock Delta Lake ────────────────────────────────────────────────────────────
# Unit tests cover validation logic only — no Delta reads or writes.
# isDeltaTable → False causes _filter_by_product_ref and _filter_by_order_ref
# to log a warning and return the DataFrame unchanged, which is correct for
# a unit test that does not provision actual Delta tables.
_delta_mock = MagicMock()
_delta_mock.tables.DeltaTable.isDeltaTable.return_value = False
sys.modules["delta"] = _delta_mock
sys.modules["delta.tables"] = _delta_mock.tables


# ── SparkSession fixture ───────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession shared across all tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("lakehouse-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Shared fake args fixture ───────────────────────────────────────────────────

@pytest.fixture
def fake_args():
    """Minimal job args dict accepted by all validate() functions."""
    return {
        "JOB_NAME":           "test-job",
        "DATA_BUCKET":        "test-bucket",
        "SCRIPTS_BUCKET":     "test-scripts-bucket",
        "ENVIRONMENT":        "test",
        "DATABASE_NAME":      "test_db",
        "DATASET":            "test",
        "RAW_KEY":            "raw/test.csv",
        "RAW_PREFIX":         "raw/",
        "PROCESSED_PREFIX":   "lakehouse-dwh/",
        "ARCHIVED_PREFIX":    "archived/",
        "REJECTED_PREFIX":    "rejected/",
        "FLAGGED_PREFIX":     "flagged/",
        "MERGE_KEYS":         "product_id",
        "MERGE_KEYS_LIST":    ["product_id"],
        "PARTITION_COLS":     "department",
        "PARTITION_COLS_LIST": ["department"],
        "SNS_TOPIC_ARN":      "arn:aws:sns:us-east-1:000000000000:test-topic",
    }
