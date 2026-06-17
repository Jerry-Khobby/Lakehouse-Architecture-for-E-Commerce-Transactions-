# Rejected Records Strategy — Storage, Audit Columns, and Lifecycle

## Overview

Every invalid row separated during validation is written to the `rejected/` prefix on the data bucket — not discarded, not logged as a counter, but physically preserved as a Parquet file alongside the original row data and four audit columns. This document covers the `write_rejected()` function in `common.py`, the exact directory structure under `rejected/`, what the four audit columns contain and why each exists, and the 60-day S3 lifecycle rule that expires old rejection records.

---

## Why Rejected Rows Are Stored, Not Dropped

Dropping invalid rows silently is the most dangerous thing a data pipeline can do. It produces a dataset that looks complete but is not, and there is no record of what was lost. The effects compound over time — a `pass_rate` of 98% on every batch means 2% of data is silently missing, and by month 12, the Silver layer is missing a cumulative 21% of the original data.

Storing rejected rows serves three purposes:

1. **Diagnosis:** The full row is preserved. An operator can open the rejected Parquet file, see the exact problematic value, and determine whether it is a source system bug, a format change, or an encoding error.
2. **Recovery:** If a rejection was caused by a pipeline bug (wrong timestamp format, overly strict validation rule), the original rows are available for reprocessing after the fix. No data is permanently lost.
3. **Audit:** The `_job_run_id` and `_source_key` columns link each rejected row back to the specific Glue job execution and the specific source file. An audit question like "what happened to order 12345" can be answered by querying `rejected/orders/` for that order ID.

---

## `write_rejected()` Implementation

```python
def write_rejected(
    spark: SparkSession,
    rejected_df: DataFrame,
    dataset: str,
    run_id: str,
    s3_bucket: str,
    source_key: str = "",
) -> None:
    if rejected_df is None or rejected_df.rdd.isEmpty():
        logger.info("No rejected rows for dataset '%s'. Skipping write.", dataset)
        return

    now = datetime.utcnow()
    date_partition = now.strftime("%Y-%m-%d")

    output_path = f"s3://{s3_bucket}/rejected/{dataset}/{date_partition}/{run_id}/"

    enriched = (
        rejected_df
        .withColumn("_rejected_at", F.lit(now.isoformat()).cast(TimestampType()))
        .withColumn("_job_run_id", F.lit(run_id))
        .withColumn("_source_key", F.lit(source_key))
    )

    enriched.write.mode("overwrite").parquet(output_path)
    logger.info(
        "Wrote %d rejected rows for dataset '%s' to %s",
        rejected_df.count(),
        dataset,
        output_path,
    )
```

The function returns immediately if `rejected_df` is empty — it does not write a zero-row Parquet file. An empty rejection file would pollute the `rejected/` prefix and cause Athena queries over the prefix to open files with no data.

---

## Directory Structure

```
s3://<data-bucket>/rejected/
├── products/
│   ├── 2025-04-30/
│   │   └── jr_abc123def456/        ← Glue job run ID
│   │       └── part-00000-xyz.parquet
│   └── 2025-05-31/
│       └── jr_ghi789jkl012/
│           └── part-00000-abc.parquet
├── orders/
│   ├── 2025-04-30/
│   │   └── jr_abc123def456/
│   │       └── part-00000-def.parquet
│   └── 2025-05-31/
│       └── jr_ghi789jkl012/
│           └── part-00000-ghi.parquet
└── order_items/
    ├── 2025-04-30/
    │   └── jr_abc123def456/
    │       └── part-00000-jkl.parquet
    └── 2025-05-31/
        └── jr_ghi789jkl012/
            └── part-00000-mno.parquet
```

### Directory Levels

**`rejected/<dataset>/`** — One subdirectory per dataset (`products`, `orders`, `order_items`). Athena or a Glue crawler can register each dataset's rejection prefix as a separate table, allowing queries like "all rejected orders" without scanning products or order_items rejections.

**`rejected/<dataset>/<date>/`** — The calendar date of the Glue job execution (UTC), formatted as `YYYY-MM-DD`. This is the date the rejection happened, not the date of the data being processed. A rejection from reprocessing April data in June has `date = 2025-06-15` — the date the pipeline ran, not the April data date. This date partition supports the S3 lifecycle rule: objects older than 60 days (by their `date/` prefix) are expired automatically.

**`rejected/<dataset>/<date>/<run_id>/`** — The Glue job run ID (`jr_...`). This is the finest granularity — one directory per individual Glue job execution. Within a single pipeline run, three Glue jobs run: products, orders, order_items. Each writes to its own `<run_id>/` directory. A pipeline re-run produces a new `<run_id>/` directory under the same `<date>/` prefix — the previous rejection records are not overwritten.

---

## The Four Audit Columns

Every rejected row has four columns appended before it is written to Parquet. These columns do not exist in the source CSV — they are added by `write_rejected()`.

### `rejection_reason`

**Type:** `StringType()`  
**Source:** Set by the validation check that identified the row as invalid

The rejection reason is a machine-readable string identifying which specific validation rule the row failed. Examples:

| Value | Meaning |
|---|---|
| `null_product_id` | `product_id` was null |
| `null_required_field` | A non-nullable column was null |
| `invalid_id_value` | An ID column was zero or negative |
| `empty_string_field` | A string column was empty or whitespace-only |
| `unparseable_timestamp` | `order_timestamp` could not be parsed with the declared format |
| `invalid_total_amount` | `total_amount` was zero or negative |
| `invalid_add_to_cart_order` | `add_to_cart_order` was zero or negative |
| `invalid_reordered_flag` | `reordered` was not 0 or 1 |
| `invalid_days_since_prior` | `days_since_prior_order` was negative |
| `intra_batch_duplicate` | A duplicate key within the CSV batch |
| `unknown_product_id` | Referential integrity failure — product not in Delta table |
| `unknown_order_id` | Referential integrity failure — order not in Delta table |

Using a machine-readable string (not a human-readable sentence) allows Athena queries to group and count by rejection reason without pattern matching:

```sql
SELECT rejection_reason, COUNT(*) as count
FROM rejected_orders
WHERE date = '2025-04-30'
GROUP BY rejection_reason
ORDER BY count DESC;
```

### `_rejected_at`

**Type:** `TimestampType()` (stored as UTC ISO 8601)  
**Source:** `datetime.utcnow()` at the time `write_rejected()` executes

The exact UTC timestamp when the row was written to `rejected/`. This is not the timestamp of the source data — it is the pipeline's processing timestamp. Combined with `_job_run_id`, it provides a precise anchor for when the rejection was observed.

The underscore prefix (`_rejected_at`) is a convention indicating this is a pipeline-generated audit column, not a column from the source data. Analysts querying the rejection table can immediately distinguish source columns from pipeline metadata columns.

### `_job_run_id`

**Type:** `StringType()`  
**Source:** The Glue job run ID passed into `write_rejected()` from `main()`

The Glue job run ID (`jr_abc123def456`) uniquely identifies the specific Glue job execution that produced this rejection. Every Glue job execution has a unique run ID assigned by the Glue service. This ID can be used to:

- Look up the CloudWatch log stream for the run: `"/aws-glue/jobs/output"` filtered by `job_run_id = "jr_abc123def456"`
- Correlate the rejection record with the job's CloudWatch metrics for that run
- Identify whether a rejection pattern is tied to a specific problematic execution (one-off) or appears across multiple runs (systematic bug)

### `_source_key`

**Type:** `StringType()`  
**Source:** The S3 key of the source CSV file from which the row was read

The S3 object key of the raw CSV file in the `raw/` prefix, e.g.:
```
raw/apr_2025/orders/orders_apr_2025.csv
```

This column links the rejected row back to the source file. If an operator investigates a rejection and needs to see the surrounding rows in context, `_source_key` tells them exactly which file to open. After the pipeline run, the source file is archived to `archived/<dataset>/<batch>/` — but the `_source_key` still identifies the original S3 key path, which can be used to find the archived copy.

---

## 60-Day Lifecycle Expiry

Rejection records have operational value for diagnosis and recovery, but they are not permanent analytical data. Retaining them indefinitely grows the `rejected/` prefix without limit. The pipeline applies an S3 lifecycle rule that expires rejection records 60 days after their creation date:

```hcl
rule {
  id     = "expire-rejected-records"
  status = "Enabled"

  filter {
    prefix = "rejected/"
  }

  expiration {
    days = 60
  }
}
```

### Why 60 Days

60 days covers two complete monthly pipeline cycles. If the April batch runs on April 30 and a rejection is discovered on May 15, there are 45 days remaining before expiry — enough time for investigation and reprocessing. If a rejection from April is not investigated by June 30 (60 days later), it is unlikely to require reprocessing — the delta between 60-day-old rejected data and current data would require a full restatement of the Silver layer, which is a separate incident response process, not a routine correction.

The 60-day window is a balance between storage cost (every rejected row is a full original row plus audit columns, stored as Parquet — a 100-record rejection file is approximately 20 KB) and operational safety (enough time to catch and act on systematic rejection issues before the evidence expires).

### How the Lifecycle Rule Interacts with the Directory Structure

S3 lifecycle rules evaluate object age based on the `Last-Modified` timestamp of each S3 object, not on the key path. A file at `rejected/orders/2025-04-30/jr_abc123def456/part-00000.parquet` written on 2025-04-30 expires 60 days later on 2025-06-29 — regardless of the `2025-04-30` in its key path.

The `<date>/` level in the path is useful for Athena partition pruning and human navigation, but the lifecycle rule expires objects based on actual object age, not the date embedded in the path.

---

## Querying Rejected Records with Athena

To register the rejection prefix as an Athena-queryable table, a Glue crawler can be pointed at `s3://<data-bucket>/rejected/orders/`. Alternatively, a manual `CREATE EXTERNAL TABLE` in the Glue catalog:

```sql
CREATE EXTERNAL TABLE rejected_orders (
  order_num        BIGINT,
  order_id         STRING,
  user_id          STRING,
  order_timestamp  STRING,
  total_amount     DECIMAL(12,2),
  date             STRING,
  rejection_reason STRING,
  _rejected_at     TIMESTAMP,
  _job_run_id      STRING,
  _source_key      STRING
)
PARTITIONED BY (rejection_date STRING)
STORED AS PARQUET
LOCATION 's3://<data-bucket>/rejected/orders/';
```

Once registered, rejection diagnostics are straightforward:

```sql
-- Most common rejection reasons for today's orders run
SELECT rejection_reason, COUNT(*) AS count
FROM rejected_orders
WHERE rejection_date = '2025-04-30'
GROUP BY rejection_reason
ORDER BY count DESC;

-- Find all rejected rows from a specific job run
SELECT *
FROM rejected_orders
WHERE _job_run_id = 'jr_abc123def456';

-- Check for the unparseable timestamp bug
SELECT order_timestamp, rejection_reason, _source_key
FROM rejected_orders
WHERE rejection_reason = 'unparseable_timestamp'
LIMIT 20;
```
