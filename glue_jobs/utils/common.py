"""
common.py — Shared utilities for all Lakehouse Glue ETL jobs.

Provides:
  - Spark + Delta session initialisation
  - Job argument parsing
  - Rejected-record writer
  - S3 file archiver (copy raw → archived, then delete source)
  - Delta table initialiser  ← FIX: seeds empty DataFrame instead of
                                     DeltaTable.createIfNotExists() to avoid
                                     "partitioning when schema not defined" on Glue 4.0
  - Glue Data Catalog table registrar
"""

import sys
import logging
from datetime import datetime, timezone
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

# Logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("lakehouse.common")


# Spark / Glue session


def build_spark_session(job_name: str) -> tuple:
    """
    Initialise SparkContext, GlueContext, SparkSession and Job.

    Delta Lake extensions are activated via --conf args that Terraform
    injects into every Glue job's default_arguments:
      spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
      spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog

    Returns: (sc, glue_ctx, spark, job)
    """
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    # Pin the session timezone so timestamp casts and the future-timestamp
    # cutoff comparisons in the jobs are unambiguous regardless of the worker's
    # local zone.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    active_extensions = spark.conf.get("spark.sql.extensions", "")
    if "DeltaSparkSessionExtension" not in active_extensions:
        raise RuntimeError(
            "Delta Lake extensions not loaded. Check --conf spark.sql.extensions in Glue job default_arguments."
        )

    job = Job(glue_ctx)
    job.init(job_name, {})

    logger.info("Spark session ready. Delta extensions: %s", active_extensions)
    return sc, glue_ctx, spark, job


# Argument parsing


REQUIRED_ARGS = [
    "JOB_NAME",
    "DATA_BUCKET",
    "SCRIPTS_BUCKET",
    "ENVIRONMENT",
    "DATABASE_NAME",
    "DATASET",
    "RAW_KEY",  # exact S3 key for this dataset, from the Step Functions batch input $.files.<dataset>
    "RAW_PREFIX",
    "PROCESSED_PREFIX",
    "ARCHIVED_PREFIX",
    "REJECTED_PREFIX",
    "FLAGGED_PREFIX",  # soft-flagged records that pass but need analyst review
    "MERGE_KEYS",  # comma-separated e.g. "order_id" or "id,order_id"
    "PARTITION_COLS",  # comma-separated e.g. "date" or "department"
    "SNS_TOPIC_ARN",  # SNS topic for stage-level alerts
]


def parse_args() -> dict:
    """
    Parse all Glue --ARG parameters into a plain dict.
    Splits MERGE_KEYS and PARTITION_COLS into lists.
    Validates that no critical key is blank.
    """
    raw = getResolvedOptions(sys.argv, REQUIRED_ARGS)

    raw["MERGE_KEYS_LIST"] = [k.strip() for k in raw["MERGE_KEYS"].split(",") if k.strip()]
    raw["PARTITION_COLS_LIST"] = [c.strip() for c in raw["PARTITION_COLS"].split(",") if c.strip()]

    for key in ("DATA_BUCKET", "DATASET", "RAW_KEY", "DATABASE_NAME"):
        if not raw.get(key, "").strip():
            raise ValueError(f"Required job argument --{key} is empty or missing.")

    logger.info(
        "Job args parsed | dataset=%s | raw_key=%s | environment=%s",
        raw["DATASET"],
        raw["RAW_KEY"],
        raw["ENVIRONMENT"],
    )
    return raw


# Rejected-record writer


def write_rejected(
    df: DataFrame,
    args: dict,
    job_run_id: str,
    rejection_reason: str,
    reason_col: Optional[str] = None,
) -> int:
    """
    Write rejected rows to s3://<DATA_BUCKET>/rejected/<DATASET>/<date>/<run_id>/

    If reason_col is provided that column already contains per-row reasons.
    Otherwise rejection_reason (scalar string) is added as a new column.
    Three audit columns are always appended: _rejected_at, _job_run_id, _source_key.

    Returns the count of rows written (0 = nothing written, short-circuits early).
    """
    count = df.count()
    if count == 0:
        return 0

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = (
        f"s3://{args['DATA_BUCKET']}/"
        f"{args['REJECTED_PREFIX'].rstrip('/')}/{args['DATASET']}/"
        f"{run_date}/{job_run_id}/"
    )

    out_df = (
        df.withColumn(
            "rejection_reason",
            F.col(reason_col) if reason_col else F.lit(rejection_reason),
        )
        .withColumn("_rejected_at", F.current_timestamp())
        .withColumn("_job_run_id", F.lit(job_run_id))
        .withColumn("_source_key", F.lit(args["RAW_KEY"]))
    )

    out_df.write.mode("append").parquet(output_path)

    logger.warning(
        "Wrote %d rejected rows | reason=%s | path=%s",
        count,
        rejection_reason,
        output_path,
    )
    return count


# S3 file archiver


def archive_source_file(args: dict) -> None:
    """
    Move the source CSV from raw/ → archived/ after a successful Delta commit.

    Uses boto3 copy_object → delete_object.
    ExpectedBucketOwner is set on both calls to guard against bucket-confusion
    attacks — both source and destination live in the same account-owned bucket.

    This function logs but does NOT re-raise on ClientError so that a failed
    archive never marks an otherwise-successful pipeline run as failed.
    """
    s3 = boto3.client("s3")
    sts = boto3.client("sts")
    bucket = args["DATA_BUCKET"]
    source_key = args["RAW_KEY"]
    account_id = sts.get_caller_identity()["Account"]

    filename = source_key.split("/")[-1]
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest_key = f"{args['ARCHIVED_PREFIX'].rstrip('/')}/{args['DATASET']}/{run_date}/{filename}"

    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": source_key},
            Key=dest_key,
            ExpectedBucketOwner=account_id,
        )
        logger.info(
            "Archived: s3://%s/%s → s3://%s/%s",
            bucket,
            source_key,
            bucket,
            dest_key,
        )

        s3.delete_object(
            Bucket=bucket,
            Key=source_key,
            ExpectedBucketOwner=account_id,
        )
        logger.info("Deleted source: s3://%s/%s", bucket, source_key)

    except ClientError:
        # Log but do not re-raise: a failed archive must not fail the pipeline
        # when the Delta write already committed successfully.
        logger.exception(
            "Archive step failed for s3://%s/%s",
            bucket,
            source_key,
        )


# Delta table initialiser


def ensure_delta_table(
    spark: SparkSession,
    table_path: str,
    schema: StructType,
    partition_cols: List[str],
) -> None:
    """
    Ensure the Delta table exists at table_path before any MERGE runs.

    WHY this exists:
        DeltaTable.merge() on a non-existent path raises AnalysisException.
        We must seed the table before the first merge.

    WHY we write an empty DataFrame instead of DeltaTable.createIfNotExists():
        On Glue 4.0 / Delta 2.x, calling .createIfNotExists().addColumns(schema)
        .partitionedBy(...) raises:
            AnalysisException: It is not allowed to specify partitioning
            when the table schema is not defined.
        Writing an empty DataFrame avoids this entirely — Spark writes the
        Delta transaction log (_delta_log/00000000000000000000.json) with the
        schema and partition spec derived from the DataFrame's schema and the
        .partitionBy() call, which Delta accepts unconditionally.

    The function is fully idempotent: isDeltaTable() returns True on every
    subsequent run and the body is skipped with no I/O cost.
    """
    if DeltaTable.isDeltaTable(spark, table_path):
        logger.info("Delta table already exists at %s — skipping init.", table_path)
        return

    logger.info("Initialising Delta table at %s …", table_path)

    empty_df = spark.createDataFrame([], schema)

    writer = empty_df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(table_path)

    # Catalog registration is deferred to update_catalog_table() in main(),
    # which uses boto3 directly and avoids spark.sql.warehouse.dir being unset
    # on Glue 4.0 (which causes "Can not create a Path from an empty string"
    # when Spark SQL executes CREATE TABLE without an explicit warehouse dir).
    logger.info("Delta table initialised at %s", table_path)


# Glue Data Catalog registrar


def update_catalog_table(
    args: dict,
    table_name: str,
    table_path: str,
    schema: StructType,
    partition_cols: List[str],
) -> None:
    """
    Update (or create) the Glue Data Catalog table after a successful Delta write.

    Uses the boto3 Glue client directly rather than Spark SQL ALTER TABLE
    because the boto3 path works identically across all Glue versions and
    doesn't require an active SparkSession when called from outside a job.

    Column list is built from the StructType, with partition columns separated
    into PartitionKeys as the Glue API requires.
    """
    glue = boto3.client("glue", region_name=_get_region())
    database = args["DATABASE_NAME"]

    non_partition_cols = [f for f in schema.fields if f.name not in partition_cols]
    partition_fields = [f for f in schema.fields if f.name in partition_cols]

    def _to_glue_type(dtype) -> str:
        from pyspark.sql.types import (
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            BooleanType,
            TimestampType,
            DateType,
            DecimalType,
        )

        mapping = {
            StringType: "string",
            IntegerType: "int",
            LongType: "bigint",
            DoubleType: "double",
            FloatType: "float",
            BooleanType: "boolean",
            TimestampType: "timestamp",
            DateType: "date",
        }
        if isinstance(dtype, DecimalType):
            return f"decimal({dtype.precision},{dtype.scale})"
        return mapping.get(type(dtype), "string")

    table_input = {
        "Name": table_name,
        "Description": (f"Delta Lake table managed by Lakehouse ETL — {args['ENVIRONMENT']}"),
        "StorageDescriptor": {
            "Columns": [{"Name": f.name, "Type": _to_glue_type(f.dataType)} for f in non_partition_cols],
            "Location": table_path,
            "InputFormat": "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {
                    "path": table_path,
                    "serialization.format": "1",
                },
            },
        },
        "PartitionKeys": [{"Name": f.name, "Type": _to_glue_type(f.dataType)} for f in partition_fields],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "delta",
            "spark.sql.sources.provider": "delta",
            # Athena engine v3 Delta Lake reader requires an explicit "path" parameter.
            # It does NOT fall back to StorageDescriptor.Location, even when that is set.
            # Without this, Athena raises: DELTA_LAKE_INVALID_SCHEMA: No path property defined.
            "path": table_path,
            "delta.minReaderVersion": "1",
            "delta.minWriterVersion": "2",
            "lakeformation.arn": "",
        },
    }

    try:
        glue.update_table(DatabaseName=database, TableInput=table_input)
        logger.info("Catalog table updated: %s.%s", database, table_name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        logger.info("Catalog table created: %s.%s", database, table_name)
    except ClientError:
        # Log but do not re-raise — a catalog update failure must not fail
        # the pipeline when data is already safely written to Delta.
        logger.exception("Catalog update failed for %s.%s", database, table_name)

    # Deregister from Lake Formation if it was registered — LF governance
    # causes COLUMN_NOT_FOUND on JOINs by applying column-level filters
    # that block named column resolution even when IAM allows full access.
    try:
        lf = boto3.client("lakeformation", region_name=_get_region())
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        region = _get_region()
        lf.deregister_resource(ResourceArn=f"arn:aws:glue:{region}:{account_id}:table/{database}/{table_name}")
        logger.info("Deregistered %s.%s from Lake Formation", database, table_name)
    except lf.exceptions.EntityNotFoundException:
        pass
    except ClientError as exc:
        logger.warning("LF deregister skipped — %s", exc)


# Helpers


# EC2 IMDSv2 link-local address — intentional, not a configurable host.
_EC2_METADATA_HOST = "http://169.254.169.254"  # noqa: S1313


def _get_region() -> str:
    """
    Resolve the AWS region from EC2 IMDSv2 (Glue runs on EC2).
    Falls back to us-east-1 if the metadata service is unreachable.
    Uses IMDSv2 (token-gated) rather than the deprecated v1 path.
    """
    try:
        import urllib.request

        token_req = urllib.request.Request(
            f"{_EC2_METADATA_HOST}/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},
            method="PUT",
        )
        with urllib.request.urlopen(token_req, timeout=2) as resp:
            token = resp.read().decode()

        region_req = urllib.request.Request(
            f"{_EC2_METADATA_HOST}/latest/meta-data/placement/region",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(region_req, timeout=2) as resp:
            return resp.read().decode()
    except Exception:
        return "us-east-1"


def s3_path(bucket: str, prefix: str, suffix: str = "") -> str:
    """Build a clean s3:// URI, stripping redundant slashes."""
    prefix = prefix.rstrip("/")
    if suffix:
        return f"s3://{bucket}/{prefix}/{suffix.lstrip('/')}"
    return f"s3://{bucket}/{prefix}"


def log_counts(label: str, total: int, valid: int, rejected: int) -> None:
    """Emit a one-line structured validation summary to CloudWatch."""
    logger.info(
        "%s | total_read=%d | valid=%d | rejected=%d | pass_rate=%.1f%%",
        label,
        total,
        valid,
        rejected,
        (valid / total * 100) if total > 0 else 0.0,
    )
