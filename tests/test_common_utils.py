"""
Unit tests for shared utilities in glue_jobs.utils.common.

These are pure-Python functions with no Spark or AWS dependencies
so they run quickly and need no fixtures.
"""

import logging

from glue_jobs.utils.common import log_counts, s3_path


class TestS3Path:
    def test_builds_correct_uri_with_suffix(self):
        assert s3_path("my-bucket", "raw/", "test.csv") == "s3://my-bucket/raw/test.csv"

    def test_strips_leading_slash_from_suffix(self):
        assert s3_path("my-bucket", "raw/", "/test.csv") == "s3://my-bucket/raw/test.csv"

    def test_without_suffix_returns_prefix_only(self):
        assert s3_path("my-bucket", "raw/") == "s3://my-bucket/raw"

    def test_strips_trailing_slash_from_prefix(self):
        assert s3_path("my-bucket", "raw", "test.csv") == "s3://my-bucket/raw/test.csv"

    def test_nested_suffix_path(self):
        assert (
            s3_path("my-bucket", "lakehouse-dwh/", "products")
            == "s3://my-bucket/lakehouse-dwh/products"
        )


class TestLogCounts:
    def test_normal_pass_rate(self, caplog):
        with caplog.at_level(logging.INFO, logger="lakehouse.common"):
            log_counts("products:validate", 100, 90, 10)
        assert "pass_rate=90.0%" in caplog.text
        assert "total_read=100" in caplog.text
        assert "valid=90" in caplog.text
        assert "rejected=10" in caplog.text

    def test_zero_total_does_not_raise(self, caplog):
        with caplog.at_level(logging.INFO, logger="lakehouse.common"):
            log_counts("orders:validate", 0, 0, 0)
        assert "pass_rate=0.0%" in caplog.text

    def test_perfect_pass_rate(self, caplog):
        with caplog.at_level(logging.INFO, logger="lakehouse.common"):
            log_counts("order_items:validate", 50, 50, 0)
        assert "pass_rate=100.0%" in caplog.text
