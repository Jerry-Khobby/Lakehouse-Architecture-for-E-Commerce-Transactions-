# MERGE / Upsert Logic — Per-Dataset Semantics

## Overview

Each of the three Glue jobs runs a `DeltaTable.merge()` after validation. The merge key, the match condition, and the update strategy differ by dataset type. Products is a dimension table — full attribute replacement on match. Orders and order_items are fact tables — timestamp-guarded updates that prevent stale re-deliveries from overwriting newer committed data. This document covers the exact MERGE logic for each dataset, what each `operationMetrics` field means in the Delta history, and why the designs differ.

---

## Products — Dimension Table MERGE

```python
(
    delta_table.alias("target")
    .merge(
        valid_df.alias("source"),
        "target.product_id = source.product_id",
    )
    .whenMatchedUpdateAll(
        condition=(
            "source.department_id <> target.department_id "
            "OR source.department <> target.department "
            "OR source.product_name <> target.product_name"
        )
    )
    .whenNotMatchedInsertAll()
    .execute()
)
```

### Merge Key — `product_id`

`product_id` is a single-column integer primary key for the products dimension. Every product in the pipeline has a unique `product_id`. The MERGE join condition `target.product_id = source.product_id` identifies whether a product already exists in the Delta table.

### `whenMatchedUpdateAll` with Change Detection

If a `product_id` already exists in the target, the row is updated — but **only if at least one attribute has changed**. The condition:

```
source.department_id <> target.department_id
OR source.department <> target.department
OR source.product_name <> target.product_name
```

evaluates all three mutable columns. If all three are identical in source and target, the condition is `false` and `whenMatchedUpdateAll` is not executed for that row. The row in the target is left unchanged.

**Why the change detection condition matters:**

Without it, every re-run of the same `products.csv` would update all matching rows even though nothing changed. Delta MERGE with `whenMatchedUpdateAll()` (no condition) marks every matched row as "updated" — Delta physically rewrites the Parquet files for every touched partition, appends a new log entry with `add` and `remove` actions for those files, and increments the table version. For a 500-product table partitioned into 10 department directories, this is 10 Parquet file rewrites and 10 `add`/`remove` pairs in the log on every pipeline run — even if the product catalogue did not change at all.

With the condition, a true re-run of identical data produces: `numTargetRowsInserted=0`, `numTargetRowsUpdated=0`. No Parquet files are rewritten. The Delta log may receive a new version entry, but it contains no `add` or `remove` actions — it is a metadata-only commit that costs almost nothing.

### `whenNotMatchedInsertAll`

New products (a `product_id` in the source that does not exist in the target) are inserted unconditionally. There is no timestamp guard on new products because there is nothing in the target to conflict with — a new product is simply added.

### No Timestamp Guard

Products has no `last_updated` or `updated_at` timestamp column. The products dimension is managed by the source system — the assumption is that the latest `products.csv` reflects the current truth. If a product's name changes, the new CSV contains the correct name and the pipeline should overwrite the old one. A timestamp guard would be counterproductive: it would require tracking when each product attribute last changed, which the source data does not provide.

---

## Orders — Fact Table MERGE with Timestamp Guard

```python
(
    delta_table.alias("target")
    .merge(
        valid_df.alias("source"),
        "target.order_id = source.order_id",
    )
    .whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
    .whenNotMatchedInsertAll()
    .execute()
)
```

### Merge Key — `order_id`

`order_id` is a UUID string that uniquely identifies one customer order. Every order in the `orders` table has exactly one row identified by its `order_id`. The MERGE join condition `target.order_id = source.order_id` finds existing orders in the Delta table.

### `whenMatchedUpdateAll` with Timestamp Guard

If an `order_id` already exists in the target, the row is updated **only if the incoming record has a later `order_timestamp` than the one already stored**:

```
source.order_timestamp > target.order_timestamp
```

This guard handles three distinct re-delivery scenarios:

**Scenario 1 — Same file re-ingested (idempotent re-run):**
Source and target have identical `order_timestamp` for every matched `order_id`. The condition `source.order_timestamp > target.order_timestamp` is `false` for all matched rows. No rows are updated. `numTargetRowsUpdated = 0`. The table is unchanged.

**Scenario 2 — Stale file re-delivered:**
The source contains `orders_apr_2025.csv` from three weeks ago, but the May pipeline run has already committed more recent data for some `order_id` values (e.g. order status changed from `pending` to `completed`). For those rows, `source.order_timestamp < target.order_timestamp`. The condition is `false`. The stale version does not overwrite the more recent committed state. The newer state is preserved.

**Scenario 3 — Corrected record re-delivered:**
A source system re-sends an order with a correction (changed `total_amount`) and a newer `order_timestamp` to indicate the correction is authoritative. `source.order_timestamp > target.order_timestamp` is `true` for that row. The row is updated with the corrected values.

### `whenNotMatchedInsertAll`

New orders (an `order_id` in the source not yet in the target) are inserted without any condition. The first time an order appears it is always a legitimate new record.

---

## Order Items — Fact Table MERGE with Composite Key and Timestamp Guard

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

### Merge Key — Composite `(id, order_id)`

Order items has a **composite primary key** — both `id` and `order_id` together uniquely identify one order line item. Neither column alone is sufficient:

- `id` is the line item number within an order: item 1, item 2, item 3. Every order has an item 1. `id = 1` appears thousands of times across different orders — it is not unique by itself.
- `order_id` identifies the order. But one order contains multiple items — `order_id = "abc"` appears once per line item, so it is also not unique by itself.

The combination `(id = 1, order_id = "abc")` — item 1 of order "abc" — is unique. The MERGE join condition requires both columns to match:

```
target.id = source.id AND target.order_id = source.order_id
```

If only `order_id` were used as the key, the MERGE would treat all items of the same order as a single logical row — a multi-item order would be "updated" to a single item on each MERGE pass.

### Timestamp Guard — Same Semantics as Orders

The `whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")` guard applies the same timestamp-wins logic as the orders table. Since `order_items.order_timestamp` is the timestamp of the parent order (shared across all items in the same order), re-running the same batch produces identical timestamps for all matched rows, making the condition false for all — no updates, no duplicates.

---

## `operationMetrics` — Understanding the Delta History Output

After each MERGE, the job logs the Delta table history:

```python
history = delta_table.history(1).select("version", "operation", "operationMetrics")
history.show(truncate=False)
```

The `operationMetrics` map contains these fields for a MERGE operation:

### `numSourceRows`

Total rows in the source DataFrame (the validated batch). For an April orders batch: `numSourceRows = 850`.

### `numTargetRowsInserted`

Rows matched by `whenNotMatchedInsertAll` — new keys not found in the target. On first pipeline run (empty target table): `numTargetRowsInserted = 850` (all 850 rows are new). On the May run adding 850 new orders: `numTargetRowsInserted = 850`. On an idempotent re-run of the same file: `numTargetRowsInserted = 0` (all keys already exist).

### `numTargetRowsUpdated`

Rows matched by `whenMatchedUpdateAll` **where the update condition was true**. On the May run where May orders update existing April order statuses: `numTargetRowsUpdated = N` (however many order_ids exist in both April and May with newer May timestamps). On an idempotent re-run with identical timestamps: `numTargetRowsUpdated = 0`.

### `numTargetRowsCopied`

Rows that were **matched** (the join condition was true) but **not updated** (the update condition was false) — and were physically located in the same Parquet file as an updated row, so they had to be rewritten alongside the updated rows to produce a consistent new file.

This metric is greater than zero when some rows in a partition are updated and others in the same file are not. Delta rewrites entire Parquet files when any row in that file changes. The rows that were not themselves updated but had to be included in the rewritten file are counted as `numTargetRowsCopied`.

**For an idempotent re-run:** `numTargetRowsUpdated = 0`. If no rows were updated, Delta does not need to rewrite any Parquet files, so `numTargetRowsCopied = 0` as well. The MERGE completes with all three counters at zero.

**For a partial update (some orders updated, some not):** `numTargetRowsUpdated = X`, `numTargetRowsCopied = Y`, where Y is the count of unchanged rows that were co-located in Parquet files with the X updated rows.

### `numTargetRowsDeleted`

Always `0` for this pipeline. No MERGE in this pipeline uses `whenNotMatchedBySource` or `deleteAll`. Records are never physically deleted from the Delta table by the pipeline — they persist indefinitely. Deletion would require a separate Delta operation outside the MERGE.

### `executionTimeMs`

Wall-clock time for the full MERGE execution in milliseconds. Useful for identifying slow MERGEs — a sudden increase relative to the previous run indicates either more data to process or a performance regression in the Spark execution plan.

---

## MERGE Behaviour Summary by Scenario

| Dataset | Scenario | `numTargetRowsInserted` | `numTargetRowsUpdated` | `numTargetRowsCopied` |
|---|---|---|---|---|
| products | First run | All valid rows | 0 | 0 |
| products | Identical re-run | 0 | 0 | 0 |
| products | Attribute changed | 0 | N changed products | M unchanged co-located |
| products | New product added | 1 | 0 | 0 |
| orders | First run | 850 | 0 | 0 |
| orders | Identical re-run | 0 | 0 | 0 |
| orders | May batch (new orders) | 850 new | K updated (newer ts) | M copied |
| orders | Stale file re-delivered | 0 | 0 | 0 |
| order_items | First run | 2540 | 0 | 0 |
| order_items | Identical re-run | 0 | 0 | 0 |
| order_items | New items for existing orders | N | 0 | 0 |
