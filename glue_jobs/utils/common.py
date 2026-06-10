"""
common.py — Shared utilities for all Lakehouse Glue ETL jobs.

Provides:
  - Spark + Delta session initialisation
  - Job argument parsing
  - Rejected-record writer
  - S3 file archiver (copy raw → archived, then delete source)
  - Delta table initialiser (seeds empty DataFrame for first-run compatibility)
  - Glue Data Catalog table registrar (boto3 — avoids LF Describe intercept)
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
    "RAW_KEY",
    "RAW_PREFIX",
    "PROCESSED_PREFIX",
    "ARCHIVED_PREFIX",
    "REJECTED_PREFIX",
    "FLAGGED_PREFIX",
    "MERGE_KEYS",
    "PARTITION_COLS",
    "SNS_TOPIC_ARN",
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

    for key in ("DATA_BUCKET", "DATASET", "RAW_KEY", "DATABASE_NAME", "PROCESSED_PREFIX"):
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
    dest_key = f"{args['ARCHIVED_PREFIX'].rstrip('/')}/{args['DATASET']}/" f"{run_date}/{filename}"

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

    Writes an empty DataFrame on first run to seed the Delta transaction log.
    Fully idempotent — isDeltaTable() returns True on subsequent runs and the
    body is skipped with no I/O cost.
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

    logger.info("Delta table initialised at %s", table_path)


# Glue Data Catalog registrar — Spark SQL via DeltaCatalog
#
# CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION goes through the
# DeltaCatalog connector, which uses the Glue job's IAM role directly.
# The companion Terraform resource aws_lakeformation_permissions
# "glue_role_database" grants CREATE_TABLE + DESCRIBE on the database, and
# "glue_role_alter_tables" grants ALL (including DESCRIBE + ALTER) on all
# tables — together these cover both first-run creation and re-registration.
#
# DROP TABLE IF EXISTS is intentionally absent: CREATE TABLE IF NOT EXISTS
# is a no-op when the table already exists, so there is no stale-schema risk
# and no need for DROP permission.


def update_catalog_table(
    args: dict,
    table_name: str,
    table_path: str,
    spark: SparkSession = None,
) -> None:
    """
    Register (or re-register) the Delta table in the Glue Data Catalog
    via Spark SQL. Schema is read automatically from the Delta transaction log.
    """
    database = args["DATABASE_NAME"]

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    full_table = f"`{database}`.`{table_name}`"

    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table}
            USING DELTA
            LOCATION '{table_path}'
        """)
        logger.info("Catalog table registered: %s", full_table)

    except Exception:
        logger.exception("Catalog registration failed for %s", full_table)
        raise


# Helpers


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
