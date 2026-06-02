"""
common.py — Shared utilities for all Lakehouse Glue ETL jobs.

Provides:
  - Spark + Delta session initialisation
  - Job argument parsing
  - Rejected-record writer
  - S3 file archiver (copy raw → archived, then delete source)
  - Delta table initialiser (idempotent first-run safe)
  - Glue Data Catalog table registrar
"""

import sys
import logging
from datetime import datetime, timezone
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError

# Glue context imports — only available inside a Glue job runtime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable



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
    already injects into every Glue job's default_arguments:
      spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
      spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog

    The Glue Data Catalog metastore integration is activated by
    --enable-glue-datacatalog (also set in Terraform).

    Returns:
        (sc, glue_ctx, spark, job) — all four Glue/Spark handles
    """
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    # Confirm Delta extensions loaded
    active_extensions = spark.conf.get(
        "spark.sql.extensions", ""
    )
    if "DeltaSparkSessionExtension" not in active_extensions:
        raise RuntimeError(
            "Delta Lake extensions not loaded. "
            "Check --conf spark.sql.extensions in Glue job default_arguments."
        )

    job = Job(glue_ctx)
    job.init(job_name, {})

    logger.info("Spark session ready. Delta extensions: %s", active_extensions)
    return sc, glue_ctx, spark, job


# ─────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ─────────────────────────────────────────────────────────────────────────────

# All argument keys that every job must receive (either from Glue
# default_arguments or from the Step Functions per-execution override).
REQUIRED_ARGS = [
    "JOB_NAME",
    "DATA_BUCKET",
    "SCRIPTS_BUCKET",
    "ENVIRONMENT",
    "DATABASE_NAME",
    "DATASET",
    "RAW_KEY",          # exact S3 key of the triggering file, passed by Step Functions
    "RAW_PREFIX",
    "PROCESSED_PREFIX",
    "ARCHIVED_PREFIX",
    "REJECTED_PREFIX",
    "MERGE_KEYS",       # comma-separated, e.g. "order_id" or "id,order_id"
    "PARTITION_COLS",   # comma-separated, e.g. "date" or "department"
]


def parse_args() -> dict:
    """
    Parse and return all Glue job arguments as a plain dict.

    Splits MERGE_KEYS and PARTITION_COLS into lists for convenience.
    Validates that none of the required keys are empty.
    """
    raw = getResolvedOptions(sys.argv, REQUIRED_ARGS)

    # Normalise list-typed args
    raw["MERGE_KEYS_LIST"] = [k.strip() for k in raw["MERGE_KEYS"].split(",") if k.strip()]
    raw["PARTITION_COLS_LIST"] = [c.strip() for c in raw["PARTITION_COLS"].split(",") if c.strip()]

    # Validate nothing critical is blank
    for key in ("DATA_BUCKET", "DATASET", "RAW_KEY", "DATABASE_NAME"):
        if not raw.get(key, "").strip():
            raise ValueError(f"Required job argument --{key} is empty or missing.")

    logger.info(
        "Job args parsed | dataset=%s | raw_key=%s | environment=%s",
        raw["DATASET"], raw["RAW_KEY"], raw["ENVIRONMENT"],
    )
    return raw


# ─────────────────────────────────────────────────────────────────────────────
# Rejected-record writer
# ─────────────────────────────────────────────────────────────────────────────

def write_rejected(
    df: DataFrame,
    args: dict,
    job_run_id: str,
    rejection_reason: str,
    reason_col: Optional[str] = None,
) -> int:
    """
    Write rejected rows to the rejected/ S3 prefix.

    If `reason_col` is provided, that column already contains per-row
    rejection reasons (used when multiple checks are combined). Otherwise
    the scalar `rejection_reason` string is added as a new column.

    Output path:
        s3://<DATA_BUCKET>/rejected/<DATASET>/<YYYY-MM-DD>/<job_run_id>/

    Returns the count of rejected rows written.
    """
    count = df.count()
    if count == 0:
        return 0

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = (
        f"s3://{args['DATA_BUCKET']}/"
        f"{args['REJECTED_PREFIX']}{args['DATASET']}/"
        f"{run_date}/{job_run_id}/"
    )

    if reason_col:
        out_df = df.withColumn("rejection_reason", F.col(reason_col))
    else:
        out_df = df.withColumn("rejection_reason", F.lit(rejection_reason))

    # Add metadata columns for auditability
    out_df = (
        out_df
        .withColumn("_rejected_at", F.current_timestamp())
        .withColumn("_job_run_id", F.lit(job_run_id))
        .withColumn("_source_key", F.lit(args["RAW_KEY"]))
    )

    out_df.write.mode("append").parquet(output_path)

    logger.warning(
        "Wrote %d rejected rows | reason=%s | path=%s",
        count, rejection_reason, output_path,
    )
    return count


# ─────────────────────────────────────────────────────────────────────────────
# S3 file archiver
# ─────────────────────────────────────────────────────────────────────────────

def archive_source_file(args: dict) -> None:
    """
    Move the source CSV from raw/ to archived/ after a successful Delta commit.

    Performs: S3 copy_object → S3 delete_object.
    Only call this AFTER the Delta merge has completed successfully.

    Source: s3://<DATA_BUCKET>/<RAW_KEY>
    Dest:   s3://<DATA_BUCKET>/archived/<DATASET>/<YYYY-MM-DD>/<filename>
    """
    s3 = boto3.client("s3")
    bucket = args["DATA_BUCKET"]
    source_key = args["RAW_KEY"]

    filename = source_key.split("/")[-1]
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest_key = (
        f"{args['ARCHIVED_PREFIX']}{args['DATASET']}/"
        f"{run_date}/{filename}"
    )

    try:
        # Copy to archived/
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": source_key},
            Key=dest_key,
        )
        logger.info("Archived: s3://%s/%s → s3://%s/%s", bucket, source_key, bucket, dest_key)

        # Delete the original from raw/
        s3.delete_object(Bucket=bucket, Key=source_key)
        logger.info("Deleted source: s3://%s/%s", bucket, source_key)

    except ClientError as exc:
        # Log but do not re-raise — a failed archive is not a reason to
        # mark the pipeline as failed when the Delta write succeeded.
        logger.error(
            "Archive step failed for s3://%s/%s — %s",
            bucket, source_key, exc,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Delta table initialiser
# ─────────────────────────────────────────────────────────────────────────────

def ensure_delta_table(
    spark: SparkSession,
    table_path: str,
    schema: StructType,
    partition_cols: List[str],
    table_name: str,
    database_name: str,
) -> None:
    """
    Create the Delta table at `table_path` if it does not yet exist.

    Uses DeltaTable.createIfNotExists() so this is fully idempotent —
    safe to call on every job run without risk of data loss.

    Also registers the table in the Glue Data Catalog (via Spark SQL
    CREATE TABLE IF NOT EXISTS … USING DELTA) so the first run is
    immediately queryable by Athena without waiting for a crawler.
    """
    if DeltaTable.isDeltaTable(spark, table_path):
        logger.info("Delta table already exists at %s — skipping init.", table_path)
        return

    logger.info("Initialising Delta table at %s …", table_path)

    builder = (
        DeltaTable.createIfNotExists(spark)
        .location(table_path)
        .addColumns(schema)
    )
    if partition_cols:
        builder = builder.partitionedBy(*partition_cols)

    builder.execute()

    # Register in Glue Data Catalog so Athena can query it immediately
    partition_clause = (
        f"PARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{database_name}`.`{table_name}`
        USING DELTA
        {partition_clause}
        LOCATION '{table_path}'
    """)

    logger.info(
        "Delta table initialised and registered as %s.%s",
        database_name, table_name,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Glue Data Catalog registrar
# ─────────────────────────────────────────────────────────────────────────────

def update_catalog_table(
    args: dict,
    table_name: str,
    table_path: str,
    schema: StructType,
    partition_cols: List[str],
) -> None:
    """
    Update the Glue Data Catalog table metadata after a successful Delta write.

    This ensures the catalog reflects the current schema and location
    so Athena queries always read the latest table definition.

    Uses boto3 Glue client directly (more reliable than Spark SQL ALTER TABLE
    across all Glue versions).
    """
    glue = boto3.client("glue", region_name=_get_region())
    database = args["DATABASE_NAME"]

    # Build column list from Spark schema (excluding partition columns)
    non_partition_cols = [f for f in schema.fields if f.name not in partition_cols]
    partition_fields = [f for f in schema.fields if f.name in partition_cols]

    def _spark_type_to_glue(dtype) -> str:
        """Map Spark DataType to Glue/Hive type string."""
        from pyspark.sql.types import (
            StringType, IntegerType, LongType, DoubleType,
            FloatType, BooleanType, TimestampType, DateType,
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

    glue_cols = [
        {"Name": f.name, "Type": _spark_type_to_glue(f.dataType)}
        for f in non_partition_cols
    ]
    glue_partition_keys = [
        {"Name": f.name, "Type": _spark_type_to_glue(f.dataType)}
        for f in partition_fields
    ]

    table_input = {
        "Name": table_name,
        "Description": f"Delta Lake table managed by Lakehouse ETL — {args['ENVIRONMENT']}",
        "StorageDescriptor": {
            "Columns": glue_cols,
            "Location": table_path,
            "InputFormat": "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            },
        },
        "PartitionKeys": glue_partition_keys,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "delta",
            "spark.sql.sources.provider": "delta",
            "spark.sql.sources.schema.numParts": "1",
        },
    }

    try:
        glue.update_table(DatabaseName=database, TableInput=table_input)
        logger.info("Catalog table updated: %s.%s", database, table_name)
    except glue.exceptions.EntityNotFoundException:
        # Table doesn't exist in catalog yet — create it
        glue.create_table(DatabaseName=database, TableInput=table_input)
        logger.info("Catalog table created: %s.%s", database, table_name)
    except ClientError as exc:
        logger.error("Catalog update failed for %s.%s — %s", database, table_name, exc)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_region() -> str:
    """Return the AWS region from EC2 instance metadata or default to us-east-1."""
    try:
        import urllib.request
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},
            method="PUT",
        )
        with urllib.request.urlopen(token_req, timeout=2) as resp:
            token = resp.read().decode()
        region_req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/placement/region",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(region_req, timeout=2) as resp:
            return resp.read().decode()
    except Exception:
        return "us-east-1"


def s3_path(bucket: str, prefix: str, suffix: str = "") -> str:
    """Construct a clean s3:// path, stripping double slashes."""
    prefix = prefix.rstrip("/")
    if suffix:
        return f"s3://{bucket}/{prefix}/{suffix.lstrip('/')}"
    return f"s3://{bucket}/{prefix}"


def log_counts(label: str, total: int, valid: int, rejected: int) -> None:
    """Emit a structured count summary log line."""
    logger.info(
        "%s | total_read=%d | valid=%d | rejected=%d | pass_rate=%.1f%%",
        label, total, valid, rejected,
        (valid / total * 100) if total > 0 else 0.0,
    )