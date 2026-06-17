# Data Validation Rules — Per-Dataset Checks, Rejections, and Flagging

## Overview

Every Glue job validates its input before running a Delta MERGE. Validation is not optional and cannot be bypassed — invalid rows are separated from valid rows, written to `rejected/` or `flagged/` with audit metadata, and counted in the CloudWatch log. The MERGE runs only on the valid subset. This document lists every validation check applied to each dataset, the exact rejection reason string written to the audit record, and the distinction between hard rejection (to `rejected/`) and soft flagging (to `flagged/`).

---

## Destination Routing

Before the per-dataset checks, it is important to understand where invalid rows go.

### `rejected/` — Hard Rejections

A row goes to `rejected/` when it fails a structural or referential integrity rule: the row is provably wrong or cannot be trusted. Examples include a null primary key (a product with no `product_id` cannot be identified), a negative ID (impossible in the source system), a referential violation (an order item that references a product that does not exist). These rows are written as Parquet to `s3://<data-bucket>/rejected/<dataset>/<date>/<run_id>/` with four audit columns appended. The original row data is preserved so the rejection can be diagnosed and the source corrected.

### `flagged/` — Soft Flags for Review

A row goes to `flagged/` when it passes all structural checks but exhibits suspicious characteristics that a downstream analyst should review. An order with a `total_amount` of zero when it has line items, or an order item with `add_to_cart_order = 0`, are examples where the row is not provably wrong but warrants human review. Flagged rows are still merged into the Delta table — they represent valid data that may have a business explanation. The `flagged/` prefix serves as a side-channel audit trail, not a rejection queue.

---

## Products Validation — 5 Checks

`products_job.py` applies five validation checks in sequence. Each check produces a subset of rows with a `rejection_reason` label; those rows are collected and written to `rejected/products/` at the end of the validate stage.

### Check 1 — Null Primary Key

```python
null_pk = df.filter(F.col("product_id").isNull())
```

**Rejection reason:** `"null_product_id"`

A row with a null `product_id` cannot be merged into the Delta table — the MERGE key is `product_id` and a null key cannot match or uniquely identify anything. These rows are immediately separated before any other check runs. A null `product_id` indicates a CSV encoding problem or a source system defect.

### Check 2 — Null Required Fields

```python
null_required = df.filter(
    F.col("department_id").isNull() |
    F.col("department").isNull() |
    F.col("product_name").isNull()
)
```

**Rejection reason:** `"null_required_field"`

The products schema declares all four columns as `nullable=False`. A row missing any of the three non-PK required fields cannot be stored with schema enforcement active — Delta would reject the write at the Parquet level. These rows are separated at the validation layer before they reach the MERGE.

### Check 3 — Invalid IDs (Zero or Negative)

```python
invalid_ids = df.filter(
    (F.col("product_id") <= 0) | (F.col("department_id") <= 0)
)
```

**Rejection reason:** `"invalid_id_value"`

Both `product_id` and `department_id` are integer surrogate keys assigned by the source system. The source system uses auto-incrementing positive integers starting at 1. A zero or negative value is impossible in the source system's ID generation scheme and indicates data corruption or a test record that escaped production filtering. These IDs cannot be meaningfully resolved in any join or lookup.

### Check 4 — Empty Strings

```python
empty_strings = df.filter(
    (F.trim(F.col("department")) == "") |
    (F.trim(F.col("product_name")) == "")
)
```

**Rejection reason:** `"empty_string_field"`

A row where `department` or `product_name` is an empty string or whitespace-only passes the null check but is semantically invalid — a product with no name or no department classification cannot be displayed in the catalogue or used in aggregations. `F.trim()` strips leading and trailing whitespace before comparing to `""`, so a row containing only spaces is caught as well as a true empty string.

### Check 5 — Intra-Batch Deduplication

```python
window_spec = (
    Window.partitionBy("product_id")
    .orderBy(F.col("department_id").asc(), F.col("product_name").asc())
)
df = df.withColumn("_rank", F.rank().over(window_spec)).filter(F.col("_rank") == 1).drop("_rank")
```

**Rejection reason:** `"intra_batch_duplicate"` (assigned to non-rank-1 rows before filtering)

If a `product_id` appears more than once in the CSV, all rows after rank 1 are treated as duplicates. The deduplication uses a deterministic stable ordering (`department_id` ascending, then `product_name` ascending as tiebreaker) so that the same row is always selected across re-runs. `monotonically_increasing_id()` was explicitly rejected as an ordering column because Spark assigns different values on different runs — using it would make the "winning" row non-deterministic and break idempotency.

---

## Orders Validation

`orders_job.py` applies validation checks after reading the CSV with a `READ_SCHEMA` that holds `order_timestamp` and `date` as `StringType` for controlled casting. The casting step itself is part of validation — a row that cannot be cast is rejected.

### Null Primary Key

**Rejection reason:** `"null_order_id"`

`order_id` is a UUID string. A null `order_id` cannot be used as a MERGE key.

### Null Required Fields

**Rejection reason:** `"null_required_field"`

Covers: `user_id`, `order_timestamp`, `total_amount`, `date`.

### Invalid Timestamp Format

```python
cast_df = df.withColumn(
    "order_timestamp",
    F.to_timestamp(F.col("order_timestamp"), TIMESTAMP_FORMAT)
)
bad_timestamp = cast_df.filter(F.col("order_timestamp").isNull() & original_df.col("order_timestamp").isNotNull())
```

**Rejection reason:** `"unparseable_timestamp"`

`TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"` — the format uses a literal `T` separator between date and time. A timestamp like `"2025-04-15 08:30:00"` (space separator) fails to parse and returns null after `to_timestamp()`. The check compares the post-cast null against the pre-cast non-null to identify rows where casting failed. This was the root cause of the May 2025 all-rejection bug: `constants.py` had `TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"` (space), but `orders_job.py` expected the T-separator format. Every order in the May batch was rejected with `"unparseable_timestamp"`.

### Negative or Zero Total Amount

**Rejection reason:** `"invalid_total_amount"`

`total_amount` is a `Decimal(12,2)` representing order value. A zero or negative total is impossible for a completed order in the source system.

### Intra-Batch Deduplication

```python
window_spec = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc())
```

**Rejection reason:** `"intra_batch_duplicate"`

Last-write-wins by `order_timestamp` within the batch. The most recent version of a duplicate `order_id` survives.

---

## Order Items Validation — 14 Checks

`order_items_job.py` is the most complex validator. It applies 14 checks split into structural checks (rows are provably invalid) and referential integrity checks (rows reference entities that do not exist in other Delta tables).

### Structural Checks (9 checks)

**Null composite primary key components**
- `"null_id"` — `id` is null
- `"null_order_id"` — `order_id` is null

**Null required fields**
- `"null_required_field"` — any of: `user_id`, `product_id`, `add_to_cart_order`, `reordered`, `order_timestamp`, `date` is null
- Note: `days_since_prior_order` is nullable — null is valid for the first order of a user

**Invalid ID values**
- `"invalid_id_value"` — `id <= 0`
- `"invalid_product_id"` — `product_id <= 0`

**Invalid cart order**
- `"invalid_add_to_cart_order"` — `add_to_cart_order <= 0` (item position in cart must be a positive integer; position 0 does not exist)

**Invalid reordered flag**
- `"invalid_reordered_flag"` — `reordered` is not in `{0, 1}` (binary indicator, only 0 or 1 are valid)

**Invalid days since prior order**
- `"invalid_days_since_prior"` — `days_since_prior_order < 0` (negative days is impossible; null is allowed for first orders, zero is allowed if the previous order was same-day, but a negative value is impossible)

**Unparseable timestamp**
- `"unparseable_timestamp"` — same casting check as orders; `order_timestamp` fails `to_timestamp()` with `TIMESTAMP_FORMAT`

**Intra-batch deduplication**
- `"intra_batch_duplicate"` — duplicate `(id, order_id)` pairs; last-write-wins by `order_timestamp` descending

### Referential Integrity Checks (2 checks, conditional)

These checks are controlled by the `STRICT_REFERENTIAL_INTEGRITY` Glue job argument (defaults to `"true"`; set to `"false"` in unit tests to avoid needing live Delta tables).

**Unknown product reference**
- `"unknown_product_id"` — the `product_id` in the order item does not exist in the products Delta table

**Unknown order reference**
- `"unknown_order_id"` — the `order_id` in the order item does not exist in the orders Delta table

These checks use Spark `left_anti` join against the respective Delta tables. See [Referential_Integrity.md](Referential_Integrity.md) for the full implementation.

### Validation Count Summary

```
total_read     = rows read from CSV before validation
valid          = rows passed all 14 checks
rejected       = total invalid rows (sum of all rejection buckets)
pass_rate      = valid / total_read * 100
```

`log_counts()` in `common.py` writes this as a structured log line:
```
total_read=2540 | valid=2538 | rejected=2 | pass_rate=99.92%
```

---

## Validation Failure Thresholds

There is no configurable "minimum pass rate" threshold in this pipeline. A batch with `valid=0` is not explicitly blocked — the MERGE executes with an empty source DataFrame, which is a safe no-op. However, a `pass_rate` of 0% in the CloudWatch log triggers an SNS alert (via the `PipelineMonitor` stage failure or the structured log line) that prompts manual investigation. The `log_counts()` output is always written regardless of the pass rate, providing a persistent audit record of what was received vs what was processed.

---

## Validation Check Reference

| Dataset | Check | Rejection Reason | Destination |
|---|---|---|---|
| products | Null product_id | `null_product_id` | rejected/ |
| products | Null required field | `null_required_field` | rejected/ |
| products | ID ≤ 0 | `invalid_id_value` | rejected/ |
| products | Empty string | `empty_string_field` | rejected/ |
| products | Intra-batch dup | `intra_batch_duplicate` | rejected/ |
| orders | Null order_id | `null_order_id` | rejected/ |
| orders | Null required field | `null_required_field` | rejected/ |
| orders | Unparseable timestamp | `unparseable_timestamp` | rejected/ |
| orders | Total amount ≤ 0 | `invalid_total_amount` | rejected/ |
| orders | Intra-batch dup | `intra_batch_duplicate` | rejected/ |
| order_items | Null id | `null_id` | rejected/ |
| order_items | Null order_id | `null_order_id` | rejected/ |
| order_items | Null required field | `null_required_field` | rejected/ |
| order_items | id ≤ 0 | `invalid_id_value` | rejected/ |
| order_items | product_id ≤ 0 | `invalid_product_id` | rejected/ |
| order_items | add_to_cart_order ≤ 0 | `invalid_add_to_cart_order` | rejected/ |
| order_items | reordered not in {0,1} | `invalid_reordered_flag` | rejected/ |
| order_items | days_since_prior < 0 | `invalid_days_since_prior` | rejected/ |
| order_items | Unparseable timestamp | `unparseable_timestamp` | rejected/ |
| order_items | Intra-batch dup | `intra_batch_duplicate` | rejected/ |
| order_items | Unknown product_id | `unknown_product_id` | rejected/ |
| order_items | Unknown order_id | `unknown_order_id` | rejected/ |
| orders (suspicious) | Zero total with items | *(reason TBD)* | flagged/ |
