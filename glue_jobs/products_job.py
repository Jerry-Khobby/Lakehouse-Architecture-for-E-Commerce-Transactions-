"""
products_job.py — Glue ETL job for the products dimension table.

Pipeline:
  1. Read CSV from s3://<DATA_BUCKET>/<RAW_KEY>
  2. Enforce schema (FAILFAST — corrupt files raise immediately)
  3. Validate: null primary key, null required fields, invalid IDs,
     empty strings, intra-batch duplicates
  4. Write rejected rows to rejected/products/
  5. Delta MERGE into lakehouse-dwh/products/ partitioned by department
  6. Archive source file raw/ → archived/products/
  7. Update Glue Data Catalog table

Merge key  : product_id          (set by Terraform --MERGE_KEYS)
Partition  : department           (set by Terraform --PARTITION_COLS)
Delta path : s3://<DATA>/<PROCESSED_PREFIX>products/
"""

import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)
from delta.tables import DeltaTable

# Glue provides awsglue on the classpath
from awsglue.utils import getResolvedOptions

from glue_jobs.utils.common import (
    build_spark_session,
    parse_args,
    write_rejected,
    archive_source_file,
    ensure_delta_table,
    update_catalog_table,
    s3_path,
    log_counts,
    logger,
)
from glue_jobs.utils.monitor import PipelineMonitor
from glue_jobs.utils.notifier import SnsNotifier

# Schema
PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), nullable=False),
        StructField("department_id", IntegerType(), nullable=False),
        StructField("department", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
    ]
)

TABLE_NAME = "products"


# Stage 1 — Read
def read_source(spark, args: dict) -> DataFrame:
    """
    Read the CSV file identified by RAW_KEY.

    mode=FAILFAST: Spark raises AnalysisException immediately if any row
    cannot be cast to the declared schema, rather than silently producing
    nulls. This catches truncated or misencoded files before validation.

    The schema is fully declared — no inferSchema — so the read cost is
    O(1) in schema detection time.
    """
    source_path = f"s3://{args['DATA_BUCKET']}/{args['RAW_KEY']}"
    logger.info("Reading products CSV from %s", source_path)

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("enforceSchema", "true")
        .schema(PRODUCTS_SCHEMA)
        .load(source_path)
    )

    count = df.count()
    logger.info("Read %d raw rows from %s", count, source_path)
    return df


# Stage 2 — Validation


def validate(df: DataFrame, args: dict, job_run_id: str) -> DataFrame:
    """
    Run all validation checks and split into valid / rejected DataFrames.

    Checks applied (in order — all are non-destructive to the DataFrame):
      1. Null product_id          → rejection_reason = "null_product_id"
      2. Null required fields     → rejection_reason = "null_required_field:<col>"
      3. Invalid ID values (≤ 0)  → rejection_reason = "invalid_id_value"
      4. Empty / whitespace-only  → rejection_reason = "empty_string_field"
      5. Intra-batch duplicates   → rejection_reason = "intra_batch_duplicate"

    All rejected rows are written to s3://rejected/products/ and excluded
    from the returned valid DataFrame. The valid DataFrame is guaranteed:
      - No null product_id, department_id, department, product_name
      - product_id and department_id are positive integers
      - product_name and department are non-empty strings
      - Unique product_id within this batch (one row per product)
    """
    total = df.count()

    # ── Check 1: null primary key ──────────────────────────────────────────
    null_pk = df.filter(F.col("product_id").isNull())
    valid_df = df.filter(F.col("product_id").isNotNull())

    if null_pk.count() > 0:
        write_rejected(null_pk, args, job_run_id, "null_product_id")

    # ── Check 2: null required fields ─────────────────────────────────────
    required_fields = ["department_id", "department", "product_name"]
    for col in required_fields:
        null_rows = valid_df.filter(F.col(col).isNull())
        if null_rows.count() > 0:
            write_rejected(null_rows, args, job_run_id, f"null_required_field:{col}")
        valid_df = valid_df.filter(F.col(col).isNotNull())

    # ── Check 3: invalid ID values (must be positive integers > 0) ────────
    invalid_ids = valid_df.filter((F.col("product_id") <= 0) | (F.col("department_id") <= 0))
    if invalid_ids.count() > 0:
        write_rejected(invalid_ids, args, job_run_id, "invalid_id_value")
    valid_df = valid_df.filter((F.col("product_id") > 0) & (F.col("department_id") > 0))

    # ── Check 4: empty or whitespace-only string fields ───────────────────
    string_fields = ["department", "product_name"]
    for col in string_fields:
        empty_rows = valid_df.filter(F.trim(F.col(col)) == "")
        if empty_rows.count() > 0:
            write_rejected(empty_rows, args, job_run_id, f"empty_string_field:{col}")
        valid_df = valid_df.filter(F.trim(F.col(col)) != "")

    # Trim whitespace from string columns now that empties are removed
    valid_df = valid_df.withColumn("department", F.trim(F.col("department")))
    valid_df = valid_df.withColumn("product_name", F.trim(F.col("product_name")))

    # ── Check 5: intra-batch deduplication ────────────────────────────────
    # Keep one row per product_id. Products has no recency column, so order by
    # the dimension attributes for a STABLE, reproducible choice — re-running
    # the same file always keeps the same row. (monotonically_increasing_id()
    # encodes the Spark partition index, so it is not reproducible across
    # re-runs or repartitioning.) Subsequent duplicates go to rejected/.
    window = Window.partitionBy("product_id").orderBy(
        F.col("department_id"),
        F.col("product_name"),
    )
    ranked = valid_df.withColumn("_row_rank", F.row_number().over(window))

    intra_dupes = ranked.filter(F.col("_row_rank") > 1).drop("_row_rank")
    if intra_dupes.count() > 0:
        write_rejected(intra_dupes, args, job_run_id, "intra_batch_duplicate")

    valid_df = ranked.filter(F.col("_row_rank") == 1).drop("_row_rank")

    valid_count = valid_df.count()
    total_rejected = total - valid_count
    log_counts("products:validate", total, valid_count, total_rejected)

    return valid_df


# Stage 3 — Delta MERGE (upsert)
def merge_into_delta(spark, valid_df: DataFrame, args: dict) -> str:
    """
    Merge (upsert) the valid batch into the products Delta table.

    Merge logic:
      MATCHED     → UPDATE SET * (product name or department may change)
      NOT MATCHED → INSERT *

    Products is a dimension table — full overwrite on match is correct.
    There is no timestamp guard needed here (unlike orders/order_items)
    because the newest file load should always win for dimension updates.

    Partitioned by `department` for query performance.
    """
    table_path = s3_path(
        args["DATA_BUCKET"],
        args["PROCESSED_PREFIX"],
        TABLE_NAME,
    )

    # Initialise the table if this is the first run
    ensure_delta_table(
        spark=spark,
        table_path=table_path,
        schema=PRODUCTS_SCHEMA,
        partition_cols=args["PARTITION_COLS_LIST"],
    )

    delta_table = DeltaTable.forPath(spark, table_path)

    logger.info(
        "Merging %d valid product rows into %s",
        valid_df.count(),
        table_path,
    )

    (
        delta_table.alias("target")
        .merge(
            valid_df.alias("source"),
            "target.product_id = source.product_id",
        )
        # Dimension update — take the latest value, but only when an attribute
        # actually changed so that re-running an identical batch is a true
        # no-op and does not append empty commits to the Delta log.
        .whenMatchedUpdateAll(
            condition=(
                "source.department_id <> target.department_id "
                "OR source.department <> target.department "
                "OR source.product_name <> target.product_name"
            )
        )
        .whenNotMatchedInsertAll()  # new product — insert it
        .execute()
    )

    # Log Delta table history for the most recent operation
    history = delta_table.history(1).select("version", "operation", "operationMetrics")
    history.show(truncate=False)
    logger.info("Delta merge complete for products at %s", table_path)

    return table_path


# Main entrypoint
def main():
    _, _, spark, job = build_spark_session(getResolvedOptions(sys.argv, ["JOB_NAME"])["JOB_NAME"])

    args = parse_args()
    job_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    monitor = PipelineMonitor(
        args["JOB_NAME"],
        SnsNotifier(args["SNS_TOPIC_ARN"], args["ENVIRONMENT"]),
    )

    try:
        with monitor.stage("Read"):
            raw_df = read_source(spark, args)

        with monitor.stage("Validate"):
            valid_df = validate(raw_df, args, job_run_id)

        if valid_df.count() == 0:
            logger.warning("All rows in %s were rejected. No Delta merge.", args["RAW_KEY"])
            return

        with monitor.stage("Delta Merge"):
            table_path = merge_into_delta(spark, valid_df, args)

        with monitor.stage("Archive"):
            archive_source_file(args)

        with monitor.stage("Catalog Update"):
            update_catalog_table(
                args=args,
                table_name=TABLE_NAME,
                table_path=table_path,
                schema=PRODUCTS_SCHEMA,
                partition_cols=args["PARTITION_COLS_LIST"],
            )

        monitor.log_summary()

    except Exception as exc:
        logger.exception(
            "products_job FAILED | raw_key=%s | run_id=%s | error=%s",
            args.get("RAW_KEY", "unknown"),
            job_run_id,
            exc,
        )
        raise

    finally:
        job.commit()


if __name__ == "__main__":
    main()
