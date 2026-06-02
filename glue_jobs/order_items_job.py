"""
order_items_job.py — Glue ETL job for the order_items fact table.

This is the most complex job in the pipeline. It validates a composite
primary key, enforces business rules on behavioural fields, and performs
cross-dataset referential integrity checks against both the products and
orders Delta tables.

Pipeline:
  1. Read CSV from s3://<DATA_BUCKET>/<RAW_KEY>
  2. Enforce read schema (timestamps and numerics as strings for safe cast)
  3. Validate:
       - Null composite key (id, order_id)
       - Null required fields
       - reordered flag must be 0 or 1
       - add_to_cart_order must be > 0
       - days_since_prior_order: 0–365 when non-null
       - Timestamp cast and future check
       - Date vs timestamp consistency
       - Referential integrity: product_id exists in products Delta table
       - Referential integrity: order_id exists in orders Delta table
       - Intra-batch dedup on (id, order_id) — keep latest by order_timestamp
  4. Write rejected rows to rejected/order_items/
  5. Delta MERGE into lakehouse-dwh/order_items/ with timestamp guard
  6. Archive source file raw/ → archived/order_items/
  7. Update Glue Data Catalog table

Merge key  : id, order_id       (composite — set by Terraform --MERGE_KEYS)
Partition  : date                (set by Terraform --PARTITION_COLS)
Delta path : s3://<DATA>/<PROCESSED_PREFIX>order_items/
"""

import sys
import logging
from datetime import datetime, timezone, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType,
    TimestampType, DateType,
)
from delta.tables import DeltaTable

from awsglue.utils import getResolvedOptions

from  glue_jobs.utils.common import (
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


# Schema


# Read schema: cast-sensitive columns come in as strings
READ_SCHEMA = StructType([
    StructField("id",                     StringType(), nullable=False),
    StructField("order_id",               StringType(), nullable=False),
    StructField("user_id",                StringType(), nullable=False),
    StructField("days_since_prior_order", StringType(), nullable=True),   # optional
    StructField("product_id",             StringType(), nullable=False),
    StructField("add_to_cart_order",      StringType(), nullable=False),
    StructField("reordered",              StringType(), nullable=False),
    StructField("order_timestamp",        StringType(), nullable=False),
    StructField("date",                   StringType(), nullable=True),
])

# Storage schema: final typed columns written to Delta
ORDER_ITEMS_SCHEMA = StructType([
    StructField("id",                     LongType(),      nullable=False),
    StructField("order_id",               StringType(),    nullable=False),
    StructField("user_id",                StringType(),    nullable=False),
    StructField("days_since_prior_order", IntegerType(),   nullable=True),
    StructField("product_id",             IntegerType(),   nullable=False),
    StructField("add_to_cart_order",      IntegerType(),   nullable=False),
    StructField("reordered",              IntegerType(),   nullable=False),
    StructField("order_timestamp",        TimestampType(), nullable=False),
    StructField("date",                   DateType(),      nullable=False),
])

TIMESTAMP_FORMAT         = "yyyy-MM-dd HH:mm:ss"
FUTURE_TOLERANCE_HOURS   = 1
MAX_DAYS_SINCE_PRIOR     = 365


TABLE_NAME = "order_items"



# Stage 1 — Read


def read_source(spark, args: dict) -> DataFrame:
    """Read the order_items CSV. All numeric/temporal columns read as strings
    so we control casting and can precisely log which rows fail and why."""
    source_path = f"s3://{args['DATA_BUCKET']}/{args['RAW_KEY']}"
    logger.info("Reading order_items CSV from %s", source_path)

    df = (
        spark.read
        .format("csv")
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


def _cast_numeric_fields(df: DataFrame, args: dict, job_run_id: str) -> DataFrame:
    """
    Cast all numeric string columns to their target types.
    Rows where any non-nullable numeric cast fails are rejected.
    days_since_prior_order is nullable so a null cast result is preserved.
    """
    # Cast id → LongType
    df = df.withColumn("_id_cast", F.col("id").cast(LongType()))
    bad_id = df.filter(F.col("_id_cast").isNull())
    if bad_id.count() > 0:
        write_rejected(bad_id.drop("_id_cast"), args, job_run_id, "invalid_id_format")
    df = df.filter(F.col("_id_cast").isNotNull()).drop("id").withColumnRenamed("_id_cast", "id")

    # Cast product_id → IntegerType
    df = df.withColumn("_pid_cast", F.col("product_id").cast(IntegerType()))
    bad_pid = df.filter(F.col("_pid_cast").isNull())
    if bad_pid.count() > 0:
        write_rejected(bad_pid.drop("_pid_cast"), args, job_run_id, "invalid_product_id_format")
    df = df.filter(F.col("_pid_cast").isNotNull()).drop("product_id").withColumnRenamed("_pid_cast", "product_id")

    # Cast add_to_cart_order → IntegerType
    df = df.withColumn("_cart_cast", F.col("add_to_cart_order").cast(IntegerType()))
    bad_cart = df.filter(F.col("_cart_cast").isNull())
    if bad_cart.count() > 0:
        write_rejected(bad_cart.drop("_cart_cast"), args, job_run_id, "invalid_add_to_cart_order_format")
    df = df.filter(F.col("_cart_cast").isNotNull()).drop("add_to_cart_order").withColumnRenamed("_cart_cast", "add_to_cart_order")

    # Cast reordered → IntegerType
    df = df.withColumn("_reorder_cast", F.col("reordered").cast(IntegerType()))
    bad_reorder = df.filter(F.col("_reorder_cast").isNull())
    if bad_reorder.count() > 0:
        write_rejected(bad_reorder.drop("_reorder_cast"), args, job_run_id, "invalid_reordered_format")
    df = df.filter(F.col("_reorder_cast").isNotNull()).drop("reordered").withColumnRenamed("_reorder_cast", "reordered")

    # Cast days_since_prior_order → IntegerType (nullable — null = first order)
    df = df.withColumn(
        "days_since_prior_order",
        F.col("days_since_prior_order").cast(IntegerType()),
    )

    return df


def validate(df: DataFrame, args: dict, job_run_id: str, spark: SparkSession) -> DataFrame:
    """
    Full validation suite for order_items.

    Checks applied (in order):
      1.  Null composite key (id OR order_id null/blank)
      2.  Null user_id
      3.  Null product_id / add_to_cart_order / reordered
      4.  Cast all numeric columns (reject on cast failure)
      5.  reordered must be 0 or 1
      6.  add_to_cart_order must be > 0
      7.  days_since_prior_order: when non-null must be 0–365
      8.  product_id must be > 0
      9.  Cast order_timestamp → reject on failure
      10. Future timestamp check
      11. Date derivation and consistency with order_timestamp
      12. Referential integrity: product_id must exist in products Delta table
      13. Referential integrity: order_id must exist in orders Delta table
      14. Intra-batch dedup on (id, order_id) — keep latest by order_timestamp

    Returns a fully-typed valid DataFrame matching ORDER_ITEMS_SCHEMA.
    """
    total   = df.count()
    now_utc = datetime.now(timezone.utc)

    # ── Check 1: null composite key ───────────────────────────────────────
    null_key = df.filter(
        F.col("id").isNull() | (F.trim(F.col("id")) == "") |
        F.col("order_id").isNull() | (F.trim(F.col("order_id")) == "")
    )
    valid_df = df.filter(
        F.col("id").isNotNull() & (F.trim(F.col("id")) != "") &
        F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")
    )
    if null_key.count() > 0:
        write_rejected(null_key, args, job_run_id, "null_composite_key")

    # ── Check 2: null user_id ─────────────────────────────────────────────
    null_user = valid_df.filter(F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""))
    valid_df  = valid_df.filter(F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != ""))
    if null_user.count() > 0:
        write_rejected(null_user, args, job_run_id, "null_user_id")

    # ── Check 3: null required non-nullable fields ────────────────────────
    required = ["product_id", "add_to_cart_order", "reordered", "order_timestamp"]
    for col in required:
        null_rows = valid_df.filter(F.col(col).isNull() | (F.trim(F.col(col)) == ""))
        if null_rows.count() > 0:
            write_rejected(null_rows, args, job_run_id, f"null_required_field:{col}")
        valid_df = valid_df.filter(F.col(col).isNotNull() & (F.trim(F.col(col)) != ""))

    # ── Check 4: cast numeric columns ─────────────────────────────────────
    valid_df = _cast_numeric_fields(valid_df, args, job_run_id)

    # ── Check 5: reordered must be exactly 0 or 1 ─────────────────────────
    invalid_reorder = valid_df.filter(~F.col("reordered").isin(0, 1))
    valid_df         = valid_df.filter(F.col("reordered").isin(0, 1))
    if invalid_reorder.count() > 0:
        write_rejected(invalid_reorder, args, job_run_id, "invalid_reordered_flag")

    # ── Check 6: add_to_cart_order must be positive ────────────────────────
    invalid_cart = valid_df.filter(F.col("add_to_cart_order") <= 0)
    valid_df      = valid_df.filter(F.col("add_to_cart_order") > 0)
    if invalid_cart.count() > 0:
        write_rejected(invalid_cart, args, job_run_id, "invalid_cart_order")

    # ── Check 7: days_since_prior_order range (0–365 when non-null) ────────
    invalid_days = valid_df.filter(
        F.col("days_since_prior_order").isNotNull() &
        (
            (F.col("days_since_prior_order") < 0) |
            (F.col("days_since_prior_order") > MAX_DAYS_SINCE_PRIOR)
        )
    )
    valid_df = valid_df.filter(
        F.col("days_since_prior_order").isNull() |
        (
            (F.col("days_since_prior_order") >= 0) &
            (F.col("days_since_prior_order") <= MAX_DAYS_SINCE_PRIOR)
        )
    )
    if invalid_days.count() > 0:
        write_rejected(invalid_days, args, job_run_id, "invalid_days_since_prior_order")

    # ── Check 8: product_id must be a positive integer ─────────────────────
    invalid_pid = valid_df.filter(F.col("product_id") <= 0)
    valid_df     = valid_df.filter(F.col("product_id") > 0)
    if invalid_pid.count() > 0:
        write_rejected(invalid_pid, args, job_run_id, "invalid_product_id_value")

    # ── Check 9: cast order_timestamp ─────────────────────────────────────
    valid_df = valid_df.withColumn(
        "_ts_cast",
        F.to_timestamp(F.col("order_timestamp"), TIMESTAMP_FORMAT),
    )
    bad_ts   = valid_df.filter(F.col("_ts_cast").isNull())
    valid_df = valid_df.filter(F.col("_ts_cast").isNotNull())
    if bad_ts.count() > 0:
        write_rejected(bad_ts.drop("_ts_cast"), args, job_run_id, "invalid_timestamp_format")
    valid_df = valid_df.drop("order_timestamp").withColumnRenamed("_ts_cast", "order_timestamp")

    # ── Check 10: future timestamp ────────────────────────────────────────
    future_cutoff = now_utc + timedelta(hours=FUTURE_TOLERANCE_HOURS)
    future_ts = valid_df.filter(F.col("order_timestamp") > F.lit(future_cutoff))
    valid_df   = valid_df.filter(F.col("order_timestamp") <= F.lit(future_cutoff))
    if future_ts.count() > 0:
        write_rejected(future_ts, args, job_run_id, "future_timestamp")

    # ── Check 11: date derivation and consistency ─────────────────────────
    valid_df = valid_df.withColumn("_date_derived", F.to_date(F.col("order_timestamp")))
    valid_df = valid_df.withColumn(
        "_date_cast",
        F.when(F.col("date").isNull(), F.col("_date_derived"))
         .otherwise(F.to_date(F.col("date"), "yyyy-MM-dd")),
    )
    date_mismatch = valid_df.filter(
        F.col("date").isNotNull() &
        (F.col("_date_cast") != F.col("_date_derived"))
    )
    valid_df = valid_df.filter(
        F.col("date").isNull() |
        (F.col("_date_cast") == F.col("_date_derived"))
    )
    if date_mismatch.count() > 0:
        write_rejected(
            date_mismatch.drop("_date_derived", "_date_cast"),
            args, job_run_id, "date_timestamp_mismatch",
        )
    valid_df = valid_df.drop("date", "_date_derived").withColumnRenamed("_date_cast", "date")

    # ── Check 12: referential integrity — product_id ──────────────────────
    products_path = s3_path(args["DATA_BUCKET"], args["PROCESSED_PREFIX"], "products")
    if DeltaTable.isDeltaTable(spark, products_path):
        known_products = (
            spark.read.format("delta").load(products_path)
            .select(F.col("product_id").alias("_known_pid"))
            .distinct()
        )
        # Left anti-join: rows in valid_df whose product_id is NOT in products
        unknown_products = valid_df.join(
            known_products,
            valid_df["product_id"] == known_products["_known_pid"],
            how="left_anti",
        )
        if unknown_products.count() > 0:
            write_rejected(unknown_products, args, job_run_id, "unknown_product_id")
        # Keep only rows where product_id is known
        valid_df = valid_df.join(
            known_products,
            valid_df["product_id"] == known_products["_known_pid"],
            how="inner",
        ).drop("_known_pid")
    else:
        logger.warning(
            "Products Delta table not found at %s — skipping referential check for product_id.",
            products_path,
        )

    # ── Check 13: referential integrity — order_id ────────────────────────
    orders_path = s3_path(args["DATA_BUCKET"], args["PROCESSED_PREFIX"], "orders")
    if DeltaTable.isDeltaTable(spark, orders_path):
        known_orders = (
            spark.read.format("delta").load(orders_path)
            .select(F.col("order_id").alias("_known_oid"))
            .distinct()
        )
        unknown_orders = valid_df.join(
            known_orders,
            valid_df["order_id"] == known_orders["_known_oid"],
            how="left_anti",
        )
        if unknown_orders.count() > 0:
            write_rejected(unknown_orders, args, job_run_id, "unknown_order_id")
        valid_df = valid_df.join(
            known_orders,
            valid_df["order_id"] == known_orders["_known_oid"],
            how="inner",
        ).drop("_known_oid")
    else:
        logger.warning(
            "Orders Delta table not found at %s — skipping referential check for order_id.",
            orders_path,
        )

    # ── Check 14: intra-batch deduplication ───────────────────────────────
    # Composite key dedup: (id, order_id) — keep the record with the latest
    # order_timestamp within the batch (last-write-wins for re-submissions).
    window   = Window.partitionBy("id", "order_id").orderBy(F.col("order_timestamp").desc())
    ranked   = valid_df.withColumn("_row_rank", F.row_number().over(window))

    intra_dupes = ranked.filter(F.col("_row_rank") > 1).drop("_row_rank")
    if intra_dupes.count() > 0:
        write_rejected(intra_dupes, args, job_run_id, "intra_batch_duplicate")

    valid_df = ranked.filter(F.col("_row_rank") == 1).drop("_row_rank")

    # Trim string identifier columns
    valid_df = valid_df.withColumn("order_id", F.trim(F.col("order_id")))
    valid_df = valid_df.withColumn("user_id",  F.trim(F.col("user_id")))

    valid_count    = valid_df.count()
    total_rejected = total - valid_count
    log_counts("order_items:validate", total, valid_count, total_rejected)

    return valid_df



# Stage 3 — Delta MERGE (upsert with timestamp guard)


def merge_into_delta(spark, valid_df: DataFrame, args: dict) -> str:
    """
    Merge (upsert) valid order_items into the Delta table.

    Composite key merge on (id, order_id).
    Timestamp guard prevents stale redeliveries from overwriting newer data.

    Merge logic:
      MATCHED AND source.order_timestamp > target.order_timestamp
                        → UPDATE SET *
      NOT MATCHED       → INSERT *
    """
    table_path = s3_path(
        args["DATA_BUCKET"],
        args["PROCESSED_PREFIX"],
        TABLE_NAME,
    )

    ensure_delta_table(
        spark=spark,
        table_path=table_path,
        schema=ORDER_ITEMS_SCHEMA,
        partition_cols=args["PARTITION_COLS_LIST"],
        table_name=TABLE_NAME,
        database_name=args["DATABASE_NAME"],
    )

    delta_table = DeltaTable.forPath(spark, table_path)

    logger.info(
        "Merging %d valid order_item rows into %s",
        valid_df.count(), table_path,
    )

    (
        delta_table.alias("target")
        .merge(
            valid_df.alias("source"),
            # Composite key join condition
            "target.id = source.id AND target.order_id = source.order_id",
        )
        # Only overwrite if incoming record is newer (stale-delivery guard)
        .whenMatchedUpdateAll(
            condition="source.order_timestamp > target.order_timestamp"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    history = delta_table.history(1).select("version", "operation", "operationMetrics")
    history.show(truncate=False)
    logger.info("Delta merge complete for order_items at %s", table_path)

    return table_path



# Main entrypoint
def main():
    sc, glue_ctx, spark, job = build_spark_session(
        getResolvedOptions(sys.argv, ["JOB_NAME"])["JOB_NAME"]
    )
    args       = parse_args()
    job_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    try:
        # Stage 1 — Read
        raw_df = read_source(spark, args)

        # Stage 2 — Validate (passes spark for referential integrity checks)
        valid_df = validate(raw_df, args, job_run_id, spark)

        if valid_df.count() == 0:
            logger.warning(
                "All rows in %s were rejected. No Delta merge will run.",
                args["RAW_KEY"],
            )
            job.commit()
            return

        # Stage 3 — Delta Merge
        table_path = merge_into_delta(spark, valid_df, args)

        # Stage 4 — Archive source file
        archive_source_file(args)

        # Stage 5 — Update Glue Data Catalog
        update_catalog_table(
            args=args,
            table_name=TABLE_NAME,
            table_path=table_path,
            schema=ORDER_ITEMS_SCHEMA,
            partition_cols=args["PARTITION_COLS_LIST"],
        )

        logger.info(
            "order_items_job completed successfully | raw_key=%s | run_id=%s",
            args["RAW_KEY"], job_run_id,
        )

    except Exception as exc:
        logger.exception(
            "order_items_job FAILED | raw_key=%s | run_id=%s | error=%s",
            args.get("RAW_KEY", "unknown"), job_run_id, exc,
        )
        raise

    finally:
        job.commit()


if __name__ == "__main__":
    main()