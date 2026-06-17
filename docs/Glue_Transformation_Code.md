# Glue Transformation Code — Job Structure and Stage Pattern

## Overview

The pipeline has three Glue jobs — `products_job.py`, `orders_job.py`, and `order_items_job.py` — each following the same five-stage structure: Read, Validate, Delta Merge, Catalog Update, Archive. Every stage is wrapped in a `PipelineMonitor.stage()` context manager that fires SNS notifications on start, success, and failure. This document walks through the structure of each job, the stage pattern, and the design decisions that keep each job consistent with the others.

---

## The Five-Stage Pattern

Every job's `main()` function follows this skeleton:

```python
def main() -> None:
    args = parse_args([...])
    spark = build_spark_session(args["JOB_NAME"])
    notifier = SnsNotifier(
        topic_arn=args["SNS_TOPIC_ARN"],
        job_name=args["JOB_NAME"],
        environment=args.get("ENVIRONMENT", "dev"),
    )
    monitor = PipelineMonitor(notifier=notifier)

    with monitor.stage("Read"):
        df = read_source(spark, args)

    with monitor.stage("Validate"):
        valid_df, rejected_df = validate(df, ...)
        log_counts(df, valid_df, rejected_df, dataset=DATASET)
        if rejected_df is not None:
            write_rejected(spark, rejected_df, ...)

    with monitor.stage("Delta Merge"):
        ensure_delta_table(spark, table_path, SCHEMA, PARTITION_COLS)
        merge_into_delta(spark, valid_df, table_path)

    with monitor.stage("Catalog Update"):
        update_catalog_table(spark, database, TABLE_NAME, table_path)

    with monitor.stage("Archive"):
        archive_source_file(s3_client, source_bucket, source_key, archive_bucket, archive_key)

    monitor.log_summary()
```

### Why Stages Are Separate Context Managers

Each `with monitor.stage("Name"):` block is independent. If the `Validate` stage raises an exception, the `Delta Merge` stage never executes — the monitor catches the exception, fires an SNS failure notification for the Validate stage, and re-raises it. The Step Functions Catch block intercepts the job failure and branches to `NotifyFailure`. The Step Functions execution records which stage name was in progress at the time of failure via the `StageReport` stored in the monitor.

Grouping all code into one block would produce less specific failure information — "the job failed" instead of "the Validate stage failed." The stage-level granularity allows CloudWatch log searches that filter by stage name, and SNS messages that tell an operator exactly where to look.

### `monitor.log_summary()`

`log_summary()` runs unconditionally after all five stages. It prints a timing table to the CloudWatch log showing elapsed time per stage:

```
PIPELINE SUMMARY ─────────────────────────────────────
Stage             Duration     Status
───────────────────────────────────────────────────────
Read              0:00:08      SUCCESS
Validate          0:00:22      SUCCESS
Delta Merge       0:01:14      SUCCESS
Catalog Update    0:00:03      SUCCESS
Archive           0:00:02      SUCCESS
───────────────────────────────────────────────────────
Total             0:01:49      SUCCESS
```

`log_summary()` never publishes to SNS — the Step Functions state machine sends the pipeline-level success or failure notification. Publishing to SNS here would create a duplicate message. The summary is CloudWatch-only.

---

## `products_job.py` — Dimension Load

### Constants

```python
DATASET            = "products"
TABLE_NAME         = "products"
PARTITION_COLS     = ["department"]
TIMESTAMP_FORMAT   = None   # products has no timestamp column

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",    IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=False),
    StructField("department",    StringType(),  nullable=False),
    StructField("product_name",  StringType(),  nullable=False),
])
```

All four columns are `nullable=False`. Products is a reference dimension — every field is required for the row to be meaningful. A product with a null name cannot be displayed in a catalogue.

### `read_source()`

```python
def read_source(spark: SparkSession, source_path: str) -> DataFrame:
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(PRODUCTS_SCHEMA)
        .load(source_path)
    )
```

Products uses a single schema (no two-schema design) because it has no timestamp column requiring controlled casting. Every column is either an integer or a string — types that FAILFAST handles correctly without special treatment.

### `validate()`

Five checks applied in sequence. Each check separates invalid rows into a named bucket, collects all invalid rows, and returns the clean remainder:

```python
def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    rejected_buckets = []

    # 1. Null primary key
    null_pk = df.filter(F.col("product_id").isNull())
    rejected_buckets.append(null_pk.withColumn("rejection_reason", F.lit("null_product_id")))
    df = df.filter(F.col("product_id").isNotNull())

    # 2. Null required fields
    null_req = df.filter(
        F.col("department_id").isNull() | F.col("department").isNull() | F.col("product_name").isNull()
    )
    rejected_buckets.append(null_req.withColumn("rejection_reason", F.lit("null_required_field")))
    df = df.subtract(null_req)

    # 3. Invalid IDs
    invalid_ids = df.filter((F.col("product_id") <= 0) | (F.col("department_id") <= 0))
    rejected_buckets.append(invalid_ids.withColumn("rejection_reason", F.lit("invalid_id_value")))
    df = df.subtract(invalid_ids)

    # 4. Empty strings
    empty_str = df.filter((F.trim(F.col("department")) == "") | (F.trim(F.col("product_name")) == ""))
    rejected_buckets.append(empty_str.withColumn("rejection_reason", F.lit("empty_string_field")))
    df = df.subtract(empty_str)

    # 5. Intra-batch dedup (stable ordering, NOT monotonically_increasing_id)
    window_spec = Window.partitionBy("product_id").orderBy(
        F.col("department_id").asc(), F.col("product_name").asc()
    )
    ranked = df.withColumn("_rank", F.rank().over(window_spec))
    dupes = ranked.filter(F.col("_rank") > 1).drop("_rank")
    rejected_buckets.append(dupes.withColumn("rejection_reason", F.lit("intra_batch_duplicate")))
    df = ranked.filter(F.col("_rank") == 1).drop("_rank")

    rejected_df = reduce(DataFrame.unionAll, rejected_buckets) if rejected_buckets else None
    return df, rejected_df
```

### `merge_into_delta()`

```python
def merge_into_delta(delta_table: DeltaTable, valid_df: DataFrame) -> None:
    (
        delta_table.alias("target")
        .merge(valid_df.alias("source"), "target.product_id = source.product_id")
        .whenMatchedUpdateAll(condition=(
            "source.department_id <> target.department_id "
            "OR source.department <> target.department "
            "OR source.product_name <> target.product_name"
        ))
        .whenNotMatchedInsertAll()
        .execute()
    )
    delta_table.history(1).select("version", "operation", "operationMetrics").show(truncate=False)
```

The change-detection condition in `whenMatchedUpdateAll` means this MERGE is a true no-op on an identical re-run. The `history(1).show()` call logs the `operationMetrics` to CloudWatch after every execution — visible in the Glue continuous CloudWatch log for the Delta Merge stage.

---

## `orders_job.py` — Fact Load with Timestamp Casting

### Constants and Dual Schema

```python
DATASET          = "orders"
TABLE_NAME       = "orders"
PARTITION_COLS   = ["date"]
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

READ_SCHEMA = StructType([
    StructField("order_num",       LongType(),         nullable=True),
    StructField("order_id",        StringType(),       nullable=False),
    StructField("user_id",         StringType(),       nullable=False),
    StructField("order_timestamp", StringType(),       nullable=True),  # held as string for explicit cast
    StructField("total_amount",    DecimalType(12, 2), nullable=False),
    StructField("date",            StringType(),       nullable=True),  # held as string for explicit cast
])

ORDERS_SCHEMA = StructType([
    StructField("order_num",       LongType(),         nullable=True),
    StructField("order_id",        StringType(),       nullable=False),
    StructField("user_id",         StringType(),       nullable=False),
    StructField("order_timestamp", TimestampType(),    nullable=False),
    StructField("total_amount",    DecimalType(12, 2), nullable=False),
    StructField("date",            DateType(),         nullable=False),
])
```

`orders_job.py` reads with `READ_SCHEMA` (timestamps as strings) and casts during validation. The Delta table is initialised with `ORDERS_SCHEMA` (timestamps as proper types). The two schemas serve different purposes: `READ_SCHEMA` prevents FAILFAST from aborting the entire job on a bad timestamp row, while `ORDERS_SCHEMA` defines the committed Delta table structure.

### `validate()` — Casting Step

After the structural null checks, validation casts the timestamp and date columns:

```python
df = df.withColumn(
    "order_timestamp",
    F.to_timestamp(F.col("order_timestamp"), TIMESTAMP_FORMAT)
)
df = df.withColumn("date", F.to_date(F.col("date")))
```

Rows where `order_timestamp` is null after this cast but was non-null before are rejected as `"unparseable_timestamp"`. The same applies to `date`. This pattern preserves the original string value in the rejected record rather than replacing it with null before writing — an operator can see `"2025-04-15 08:30:00"` (space separator) in the rejection file and immediately understand that the source system used the wrong format.

### `merge_into_delta()`

```python
(
    delta_table.alias("target")
    .merge(valid_df.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
    .whenNotMatchedInsertAll()
    .execute()
)
```

Single-column merge key. Timestamp guard prevents stale re-deliveries from overwriting newer committed state.

---

## `order_items_job.py` — Fact Load with Referential Integrity

### Schema — The Nullable Exception

```python
ORDER_ITEMS_SCHEMA = StructType([
    StructField("id",                     IntegerType(), nullable=False),
    StructField("order_id",               StringType(),  nullable=False),
    StructField("user_id",                StringType(),  nullable=False),
    StructField("product_id",             IntegerType(), nullable=False),
    StructField("add_to_cart_order",      IntegerType(), nullable=False),
    StructField("reordered",              IntegerType(), nullable=False),
    StructField("days_since_prior_order", IntegerType(), nullable=True),  # null = first order
    StructField("order_timestamp",        StringType(),  nullable=True),
    StructField("date",                   StringType(),  nullable=True),
])
```

`days_since_prior_order` is the only intentionally nullable column across all three jobs. Its null carries business meaning: the user has no prior order, so there is no prior date to count from.

### `validate()` — Referential Integrity Gate

The 14-check validation includes a conditional gate on `STRICT_REFERENTIAL_INTEGRITY`:

```python
if _strict_referential_integrity(args):
    valid_df, product_rejects = _filter_by_product_ref(valid_df, products_path, spark)
    rejected_rows.append(product_rejects)

    valid_df, order_rejects = _filter_by_order_ref(valid_df, orders_path, spark)
    rejected_rows.append(order_rejects)
```

The gate exists because the referential checks require live Delta tables. Unit tests set `STRICT_REFERENTIAL_INTEGRITY = "false"` to skip them. In production the flag defaults to `"true"`. This is the only place in any of the three jobs where a validation check is conditional — all other checks run unconditionally.

### `merge_into_delta()` — Composite Key

```python
(
    delta_table.alias("target")
    .merge(
        valid_df.alias("source"),
        "target.id = source.id AND target.order_id = source.order_id",
    )
    .whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
    .whenNotMatchedInsertAll()
    .execute()
)
```

The composite key `(id, order_id)` is unique across the dataset. `id` alone is the line item sequence number within an order — item 1, item 2, item 3 — and repeats across orders. `order_id` alone covers all line items of one order. Together they uniquely identify one order line item across all orders.

---

## `PipelineMonitor` — The Stage Context Manager

```python
class PipelineMonitor:
    def __init__(self, notifier: SnsNotifier) -> None:
        self.notifier = notifier
        self._stages: list[StageReport] = []

    @contextmanager
    def stage(self, name: str):
        report = StageReport(name=name)
        self._stages.append(report)
        self.notifier.notify_started(stage_name=name)
        try:
            yield report
            report.mark_succeeded()
            self.notifier.notify_succeeded(stage_name=name, report=report)
        except Exception as exc:
            report.mark_failed(exc)
            self.notifier.notify_failed(stage_name=name, report=report, exc=exc)
            raise
```

### Entry — `notify_started`

When the `with monitor.stage("Validate"):` block is entered, `notify_started()` fires immediately. An SNS message goes to the pipeline topic with subject `[dev] orders_job — STARTED: Validate`. This appears in CloudWatch and (if Lambda is configured) in Slack as a blue hourglass notification.

### Clean Exit — `notify_succeeded`

When the `with` block exits normally (no exception), `report.mark_succeeded()` records the duration and `notify_succeeded()` fires with subject `[dev] orders_job — SUCCESS: Validate`.

### Exception — `notify_failed` then re-raise

When an exception propagates out of the `with` block, `notify_failed()` fires with subject `[dev] orders_job — FAILED: Validate`, then the exception is re-raised unchanged. Glue catches the re-raised exception and marks the job run as FAILED. Step Functions receives the `FAILED` job status and the Catch block routes to `NotifyFailure`. The re-raise is essential — without it, the stage would swallow the exception and the next stage would attempt to run with an invalid DataFrame.

### `StageReport.record(**metrics)`

Within a stage block, the `report` yielded by the context manager can accumulate metrics:

```python
with monitor.stage("Validate") as report:
    valid_df, rejected_df = validate(df)
    report.record(
        total=df.count(),
        valid=valid_df.count(),
        rejected=rejected_df.count() if rejected_df else 0,
    )
```

`record()` stores key-value pairs that are included in the SNS success message body as `"total=850 | valid=848 | rejected=2"`. This gives the SNS subscriber (an email recipient or the Lambda Slack notifier) a summary of what happened in each stage without needing to read the CloudWatch log.

---

## Job Caller Functions — Top-to-Bottom Vertical Ordering

All three jobs follow the Clean Code vertical formatting rule: caller functions appear above callee functions in the file. `main()` is at the top (it calls everything), followed by `read_source()`, `validate()`, `merge_into_delta()`, and finally the utility helpers. Reading the file top-to-bottom follows the execution flow.

```
main()                    ← top-level orchestrator
  └─ read_source()        ← called first
  └─ validate()           ← called second
        └─ _check_nulls()       ← helper called by validate
        └─ _check_ids()         ← helper called by validate
        └─ _filter_by_product_ref()  ← order_items only
        └─ _filter_by_order_ref()    ← order_items only
  └─ merge_into_delta()   ← called third
  └─ (catalog, archive from common.py)
```

Functions deeper in the call tree appear lower in the file. This is the newspaper metaphor from the Clean Code standard: the headline is at the top, the detail is at the bottom.
