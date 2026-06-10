"""
Unit tests for shared utilities in glue_jobs.utils.common.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from pyspark.sql.types import StringType, StructField, StructType

from glue_jobs.utils.common import (
    archive_source_file,
    build_spark_session,
    ensure_delta_table,
    log_counts,
    parse_args,
    s3_path,
    update_catalog_table,
    write_rejected,
)


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
        assert s3_path("my-bucket", "lakehouse-dwh/", "products") == "s3://my-bucket/lakehouse-dwh/products"


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


class TestWriteRejected:
    _SIMPLE_SCHEMA = StructType([StructField("id", StringType(), True)])

    def test_returns_zero_for_empty_df(self, spark, fake_args):
        empty = spark.createDataFrame([], self._SIMPLE_SCHEMA)
        assert write_rejected(empty, fake_args, "run-001", "test_reason") == 0

    @patch("pyspark.sql.readwriter.DataFrameWriter.parquet")
    def test_returns_row_count_for_non_empty_df(self, mock_parquet, spark, fake_args):
        df = spark.createDataFrame([("1",), ("2",), ("3",)], self._SIMPLE_SCHEMA)
        result = write_rejected(df, fake_args, "run-002", "null_pk")
        assert result == 3
        mock_parquet.assert_called_once()

    @patch("pyspark.sql.readwriter.DataFrameWriter.parquet")
    def test_uses_scalar_rejection_reason_when_no_reason_col(self, mock_parquet, spark, fake_args):
        df = spark.createDataFrame([("A",)], self._SIMPLE_SCHEMA)
        write_rejected(df, fake_args, "run-003", "my_reason")
        mock_parquet.assert_called_once()

    @patch("pyspark.sql.readwriter.DataFrameWriter.parquet")
    def test_uses_per_row_reason_col_when_provided(self, mock_parquet, spark, fake_args):
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("reason", StringType(), True),
            ]
        )
        df = spark.createDataFrame([("1", "bad_value")], schema)
        result = write_rejected(df, fake_args, "run-004", "fallback", reason_col="reason")
        assert result == 1
        mock_parquet.assert_called_once()



class TestArchiveSourceFile:
    def _make_clients(self):
        mock_s3 = MagicMock()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        return mock_s3, mock_sts

    def _client_factory(self, mock_s3, mock_sts):
        return lambda svc, **kw: mock_s3 if svc == "s3" else mock_sts

    def test_copies_then_deletes_source_on_success(self, fake_args):
        mock_s3, mock_sts = self._make_clients()
        with patch("boto3.client", side_effect=self._client_factory(mock_s3, mock_sts)):
            archive_source_file(fake_args)
        mock_s3.copy_object.assert_called_once()
        mock_s3.delete_object.assert_called_once()

    def test_does_not_raise_when_copy_fails(self, fake_args):
        mock_s3, mock_sts = self._make_clients()
        mock_s3.copy_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "copy_object"
        )
        with patch("boto3.client", side_effect=self._client_factory(mock_s3, mock_sts)):
            archive_source_file(fake_args)


class TestUpdateCatalogTable:
    def test_calls_spark_sql_with_correct_statement(self, fake_args):
        mock_spark = MagicMock()
        update_catalog_table(fake_args, "orders", "s3://b/orders/", spark=mock_spark)
        mock_spark.sql.assert_called_once()
        sql_text = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql_text
        assert "`test_db`.`orders`" in sql_text
        assert "USING DELTA" in sql_text
        assert "s3://b/orders/" in sql_text

    def test_uses_getorcreate_when_spark_not_provided(self, fake_args):
        mock_spark = MagicMock()
        with patch("glue_jobs.utils.common.SparkSession") as mock_ss:
            mock_ss.builder.getOrCreate.return_value = mock_spark
            update_catalog_table(fake_args, "orders", "s3://b/orders/")
        mock_spark.sql.assert_called_once()

    def test_raises_when_spark_sql_fails(self, fake_args):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("AnalysisException: AccessDenied")
        with pytest.raises(Exception, match="AccessDenied"):
            update_catalog_table(fake_args, "orders", "s3://b/orders/", spark=mock_spark)


class TestBuildSparkSession:
    def _make_mocks(self, extensions="io.delta.sql.DeltaSparkSessionExtension"):
        mock_sc = MagicMock()
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = extensions
        mock_glue_ctx = MagicMock()
        mock_glue_ctx.spark_session = mock_spark
        mock_job = MagicMock()
        return mock_sc, mock_spark, mock_glue_ctx, mock_job

    def test_raises_when_delta_extension_is_missing(self):
        mock_sc, _, mock_glue_ctx, _ = self._make_mocks(extensions="")
        with patch("glue_jobs.utils.common.SparkContext") as mock_sc_cls:
            mock_sc_cls.getOrCreate.return_value = mock_sc
            with patch("glue_jobs.utils.common.GlueContext", return_value=mock_glue_ctx):
                with pytest.raises(RuntimeError, match="Delta Lake extensions not loaded"):
                    build_spark_session("test-job")

    def test_returns_four_tuple_when_delta_extension_present(self):
        mock_sc, mock_spark, mock_glue_ctx, mock_job = self._make_mocks()
        with patch("glue_jobs.utils.common.SparkContext") as mock_sc_cls:
            mock_sc_cls.getOrCreate.return_value = mock_sc
            with patch("glue_jobs.utils.common.GlueContext", return_value=mock_glue_ctx):
                with patch("glue_jobs.utils.common.Job", return_value=mock_job):
                    sc, glue_ctx, spark, job = build_spark_session("test-job")
        assert sc is mock_sc
        assert glue_ctx is mock_glue_ctx
        assert spark is mock_spark
        assert job is mock_job


class TestParseArgs:
    _VALID_RAW = {
        "JOB_NAME": "test-job",
        "DATA_BUCKET": "my-bucket",
        "SCRIPTS_BUCKET": "scripts-bucket",
        "ENVIRONMENT": "test",
        "DATABASE_NAME": "test_db",
        "DATASET": "products",
        "RAW_KEY": "raw/products.csv",
        "RAW_PREFIX": "raw/",
        "PROCESSED_PREFIX": "lakehouse-dwh/",
        "ARCHIVED_PREFIX": "archived/",
        "REJECTED_PREFIX": "rejected/",
        "FLAGGED_PREFIX": "flagged/",
        "MERGE_KEYS": "product_id",
        "PARTITION_COLS": "department",
        "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:000000000000:test",
    }

    def test_returns_dict_with_split_list_fields(self):
        with patch("glue_jobs.utils.common.getResolvedOptions", return_value=dict(self._VALID_RAW)):
            result = parse_args()
        assert result["MERGE_KEYS_LIST"] == ["product_id"]
        assert result["PARTITION_COLS_LIST"] == ["department"]

    def test_handles_composite_merge_keys(self):
        raw = dict(self._VALID_RAW)
        raw["MERGE_KEYS"] = "id, order_id"
        with patch("glue_jobs.utils.common.getResolvedOptions", return_value=raw):
            result = parse_args()
        assert result["MERGE_KEYS_LIST"] == ["id", "order_id"]

    def test_raises_when_data_bucket_is_blank(self):
        raw = dict(self._VALID_RAW)
        raw["DATA_BUCKET"] = "   "
        with patch("glue_jobs.utils.common.getResolvedOptions", return_value=raw):
            with pytest.raises(ValueError, match="DATA_BUCKET"):
                parse_args()

    def test_raises_when_dataset_is_empty(self):
        raw = dict(self._VALID_RAW)
        raw["DATASET"] = ""
        with patch("glue_jobs.utils.common.getResolvedOptions", return_value=raw):
            with pytest.raises(ValueError, match="DATASET"):
                parse_args()


class TestEnsureDeltaTable:
    def test_skips_init_when_table_already_exists(self, spark):
        from glue_jobs.orders_job import ORDERS_SCHEMA

        with patch("glue_jobs.utils.common.DeltaTable.isDeltaTable", return_value=True):
            with patch.object(spark, "createDataFrame") as mock_create:
                ensure_delta_table(spark, "s3://b/orders/", ORDERS_SCHEMA, ["date"])
        mock_create.assert_not_called()

    def test_creates_table_when_it_does_not_exist(self, spark):
        from glue_jobs.orders_job import ORDERS_SCHEMA

        mock_df = MagicMock()
        with patch("glue_jobs.utils.common.DeltaTable.isDeltaTable", return_value=False):
            with patch.object(spark, "createDataFrame", return_value=mock_df):
                ensure_delta_table(spark, "s3://b/orders/", ORDERS_SCHEMA, ["date"])
        mock_df.write.format.assert_called_once_with("delta")

    def test_creates_table_without_partition_cols(self, spark):
        from glue_jobs.orders_job import ORDERS_SCHEMA

        mock_df = MagicMock()
        with patch("glue_jobs.utils.common.DeltaTable.isDeltaTable", return_value=False):
            with patch.object(spark, "createDataFrame", return_value=mock_df):
                ensure_delta_table(spark, "s3://b/orders/", ORDERS_SCHEMA, [])
        mock_df.write.format.assert_called_once_with("delta")
