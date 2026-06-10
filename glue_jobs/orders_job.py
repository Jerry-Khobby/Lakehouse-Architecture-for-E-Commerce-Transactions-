"""
orders_job.py — Glue ETL job for the orders fact table.

Pipeline:
  1. Read CSV from s3://<DATA_BUCKET>/<RAW_KEY>
  2. Enforce schema — order_timestamp read as string, then cast for
     controlled error handling
  3. Validate:
       - Null order_id (primary key)
       - Null user_id (orphan order)
       - Invalid / future timestamps
       - Negative total_amount
       - Date vs timestamp consistency
       - Intra-batch deduplication (last-write-wins by order_timestamp)
  4. Write rejected rows to rejected/orders/
  5. Delta MERGE into lakehouse-dwh/orders/ with timestamp guard
     (source overwrites target only if source is newer)
  6. Archive source file raw/ → archived/orders/
  7. Update Glue Data Catalog table

Merge key  : order_id          (set by Terraform --MERGE_KEYS)
Partition  : date               (set by Terraform --PARTITION_COLS)
Delta path : s3://<DATA>/<PROCESSED_PREFIX>orders/
"""

import sys
from datetime import datetime, timezone, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType,
    DateType,
    DecimalType,
)
from delta.tables import DeltaTable

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

# Read schema: order_timestamp and date come in as strings so we control
# the cast and can catch bad formats explicitly.
READ_SCHEMA = StructType(
    [
        StructField("order_num", LongType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("order_timestamp", StringType(), nullable=True),
        StructField("total_amount", StringType(), nullable=True),
        StructField("date", StringType(), nullable=True),
    ]
)

# Storage schema: final typed columns written to Delta
ORDERS_SCHEMA = StructType(
    [
        StructField("order_num", LongType(), nullable=True),
        StructField("order_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("order_timestamp", TimestampType(), nullable=False),
        StructField("total_amount", DecimalType(12, 2), nullable=False),
        StructField("date", DateType(), nullable=False),
    ]
)

# Expected timestamp format in the CSV
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

# Maximum amount allowed before soft-flagging (not hard reject)
SOFT_FLAG_AMOUNT = 1_000_000.00

# Future timestamp tolerance (orders cannot be more than 1 hour ahead of now)
FUTURE_TOLERANCE_HOURS = 1

TABLE_NAME = "orders"


# Stage 1 — Read


def read_source(spark, args: dict) -> DataFrame:
    """
    Read the orders CSV using a string-typed schema for temporal fields.
    FAILFAST mode ensures corrupt rows are caught before processing begins.
    """
    source_path = f"s3://{args['DATA_BUCKET']}/{args['RAW_KEY']}"
    logger.info("Reading orders CSV from %s", source_path)

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("enforceSchema", "true")
        .schema(READ_SCHEMA)
        .load(source_path)
    )

    count = df.count()
    logger.info("Read %d raw rows from %s", count, source_path)
    return df


# Stage 2 — Validation


def validate(df: DataFrame, args: dict, job_run_id: str) -> DataFrame:
    """
    Run all validation checks on the orders batch.

    Checks applied (in order):
      1. Null order_id              → "null_order_id"
      2. Null user_id               → "null_user_id"
      3. Null / blank total_amount  → "null_total_amount"
      4. Cast total_amount to Decimal — cast failure → "invalid_total_amount_format"
      5. Negative total_amount      → "negative_total_amount"
      6. Cast order_timestamp to Timestamp — failure → "invalid_timestamp_format"
      7. Future order_timestamp     → "future_timestamp"
      8. Derive date from timestamp if null; mismatch → "date_timestamp_mismatch"
      9. Intra-batch dedup on order_id (keep latest by order_timestamp)

    Soft flag: total_amount > SOFT_FLAG_AMOUNT written to flagged/ prefix
    (not rejected — keeps data but marks for analyst review).

    Returns a valid DataFrame with fully-typed columns matching ORDERS_SCHEMA.
    """
    total = df.count()
    now_utc = datetime.now(timezone.utc)

    # ── Check 1: null primary key ──────────────────────────────────────────
    null_pk = df.filter(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""))
    valid_df = df.filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != ""))

    if null_pk.count() > 0:
        write_rejected(null_pk, args, job_run_id, "null_order_id")

    # ── Check 2: null user_id ─────────────────────────────────────────────
    null_user = valid_df.filter(F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""))
    valid_df = valid_df.filter(F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != ""))

    if null_user.count() > 0:
        write_rejected(null_user, args, job_run_id, "null_user_id")

    # ── Check 3: null total_amount ────────────────────────────────────────
    null_amt = valid_df.filter(F.col("total_amount").isNull() | (F.trim(F.col("total_amount")) == ""))
    valid_df = valid_df.filter(F.col("total_amount").isNotNull() & (F.trim(F.col("total_amount")) != ""))

    if null_amt.count() > 0:
        write_rejected(null_amt, args, job_run_id, "null_total_amount")

    # ── Check 4: cast total_amount to Decimal ─────────────────────────────
    valid_df = valid_df.withColumn(
        "_amount_cast",
        F.col("total_amount").cast(DecimalType(12, 2)),
    )
    bad_amount = valid_df.filter(F.col("_amount_cast").isNull())
    valid_df = valid_df.filter(F.col("_amount_cast").isNotNull())

    if bad_amount.count() > 0:
        write_rejected(bad_amount.drop("_amount_cast"), args, job_run_id, "invalid_total_amount_format")

    valid_df = valid_df.drop("total_amount").withColumnRenamed("_amount_cast", "total_amount")

    # ── Check 5: negative total_amount ───────────────────────────────────
    negative_amt = valid_df.filter(F.col("total_amount") < 0)
    valid_df = valid_df.filter(F.col("total_amount") >= 0)

    if negative_amt.count() > 0:
        write_rejected(negative_amt, args, job_run_id, "negative_total_amount")

    # Soft flag: unusually large amounts (> 1M) — written separately, not rejected
    large_amt = valid_df.filter(F.col("total_amount") > SOFT_FLAG_AMOUNT)
    if large_amt.count() > 0:
        flagged_path = f"s3://{args['DATA_BUCKET']}/{args['FLAGGED_PREFIX'].rstrip('/')}/orders/{job_run_id}/"
        large_amt.withColumn("flag_reason", F.lit("large_amount")).write.mode("append").parquet(flagged_path)
        logger.warning(
            "Soft-flagged %d orders with total_amount > %s | path=%s",
            large_amt.count(),
            SOFT_FLAG_AMOUNT,
            flagged_path,
        )

    # ── Check 6: cast order_timestamp to Timestamp ───────────────────────
    valid_df = valid_df.withColumn(
        "_ts_cast",
        F.to_timestamp(F.col("order_timestamp"), TIMESTAMP_FORMAT),
    )
    bad_ts = valid_df.filter(F.col("_ts_cast").isNull())
    valid_df = valid_df.filter(F.col("_ts_cast").isNotNull())

    if bad_ts.count() > 0:
        write_rejected(bad_ts.drop("_ts_cast"), args, job_run_id, "invalid_timestamp_format")

    valid_df = valid_df.drop("order_timestamp").withColumnRenamed("_ts_cast", "order_timestamp")

    # ── Check 7: future timestamps (more than 1 hour ahead of now) ────────
    future_cutoff = now_utc + timedelta(hours=FUTURE_TOLERANCE_HOURS)
    future_ts = valid_df.filter(F.col("order_timestamp") > F.lit(future_cutoff))
    valid_df = valid_df.filter(F.col("order_timestamp") <= F.lit(future_cutoff))

    if future_ts.count() > 0:
        write_rejected(future_ts, args, job_run_id, "future_timestamp")

    # ── Check 8: date derivation and consistency ──────────────────────────
    # Derive the date from the (already validated) order_timestamp, then take
    # the provided date when present.
    valid_df = valid_df.withColumn("date_derived", F.to_date(F.col("order_timestamp")))
    valid_df = valid_df.withColumn(
        "_date_cast",
        F.when(F.col("date").isNull(), F.col("date_derived")).otherwise(F.to_date(F.col("date"), "yyyy-MM-dd")),
    )

    # Reject rows whose provided date is present but unparseable. Without this
    # branch the failed cast yields null, which makes BOTH the mismatch and the
    # keep filters below evaluate to null (never true) — so the row would be
    # neither rejected nor kept, i.e. silently dropped.
    bad_date = valid_df.filter(F.col("date").isNotNull() & F.col("_date_cast").isNull())
    if bad_date.count() > 0:
        write_rejected(bad_date.drop("date_derived", "_date_cast"), args, job_run_id, "invalid_date_format")
    valid_df = valid_df.filter(F.col("date").isNull() | F.col("_date_cast").isNotNull())

    # Reject rows where a parseable provided date disagrees with the timestamp.
    # After the bad_date filter both columns are non-null, so plain (in)equality
    # is unambiguous here.
    date_mismatch = valid_df.filter(F.col("_date_cast") != F.col("date_derived"))
    if date_mismatch.count() > 0:
        write_rejected(date_mismatch.drop("date_derived", "_date_cast"), args, job_run_id, "date_timestamp_mismatch")
    valid_df = valid_df.filter(F.col("_date_cast") == F.col("date_derived"))

    valid_df = valid_df.drop("date", "date_derived").withColumnRenamed("_date_cast", "date")

    # ── Check 9: intra-batch deduplication ────────────────────────────────
    # Per order_id keep the row with the latest order_timestamp.
    # Older duplicates go to rejected/ for audit.
    window = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc())
    ranked = valid_df.withColumn("_row_rank", F.row_number().over(window))

    intra_dupes = ranked.filter(F.col("_row_rank") > 1).drop("_row_rank")
    if intra_dupes.count() > 0:
        write_rejected(intra_dupes, args, job_run_id, "intra_batch_duplicate")

    valid_df = ranked.filter(F.col("_row_rank") == 1).drop("_row_rank")

    # Trim string columns
    valid_df = valid_df.withColumn("order_id", F.trim(F.col("order_id")))
    valid_df = valid_df.withColumn("user_id", F.trim(F.col("user_id")))

    valid_count = valid_df.count()
    total_rejected = total - valid_count
    log_counts("orders:validate", total, valid_count, total_rejected)

    return valid_df


# Stage 3 — Delta MERGE (upsert with timestamp guard)


def merge_into_delta(spark, valid_df: DataFrame, args: dict) -> str:
    """
    Merge (upsert) the valid orders batch into the orders Delta table.

    Merge logic:
      MATCHED AND source.order_timestamp > target.order_timestamp
                        → UPDATE SET *  (newer record wins)
      MATCHED AND source.order_timestamp <= target.order_timestamp
                        → no-op         (stale redelivery — do not overwrite)
      NOT MATCHED       → INSERT *

    The timestamp guard is the critical correctness guarantee: a file
    re-delivered from a previous day cannot overwrite a more recent upsert
    that is already in the Delta table.
    """
    table_path = s3_path(
        args["DATA_BUCKET"],
        args["PROCESSED_PREFIX"],
        TABLE_NAME,
    )

    ensure_delta_table(
        spark=spark,
        table_path=table_path,
        schema=ORDERS_SCHEMA,
        partition_cols=args["PARTITION_COLS_LIST"],
    )

    delta_table = DeltaTable.forPath(spark, table_path)

    logger.info(
        "Merging %d valid order rows into %s",
        valid_df.count(),
        table_path,
    )

    (
        delta_table.alias("target")
        .merge(
            valid_df.alias("source"),
            "target.order_id = source.order_id",
        )
        # Only update if the incoming record is newer
        .whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
        .whenNotMatchedInsertAll()
        .execute()
    )

    history = delta_table.history(1).select("version", "operation", "operationMetrics")
    history.show(truncate=False)
    logger.info("Delta merge complete for orders at %s", table_path)

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

        with monitor.stage("Catalog Update"):
            update_catalog_table(
                args=args,
                table_name=TABLE_NAME,
                table_path=table_path,
                spark=spark,
            )

        with monitor.stage("Archive"):
            archive_source_file(args)

        monitor.log_summary()

    except Exception as exc:
        logger.exception(
            "orders_job FAILED | raw_key=%s | run_id=%s | error=%s",
            args.get("RAW_KEY", "unknown"),
            job_run_id,
            exc,
        )
        raise

    finally:
        job.commit()


if __name__ == "__main__":
    main()
