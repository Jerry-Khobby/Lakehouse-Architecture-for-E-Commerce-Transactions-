# Deduplication Strategy — Intra-Batch and Cross-Batch

## Overview

Duplicate data enters the pipeline from two directions: within a single CSV file (two rows with the same primary key in the same batch), and across pipeline runs (the same file re-delivered in a subsequent run). Each requires a different deduplication mechanism. This document covers intra-batch deduplication using Spark Window functions, why `monotonically_increasing_id()` was rejected as an ordering key, cross-batch deduplication via the Delta MERGE key and update conditions, and the composite key handling required for order items.

---

## The Two Sources of Duplicates

### Intra-Batch Duplicates

A single CSV file can contain the same primary key more than once. This can happen because:
- The source system re-emits an updated version of a row in the same file export (e.g. an order whose status changed between the start and end of the export window appears twice: once as `pending` and once as `processing`)
- A join in the upstream ETL fanout produces duplicates before the file is written
- A file concatenation error duplicates a header or a block of rows

If intra-batch duplicates reach the MERGE, Delta raises:

```
DeltaUnsupportedOperationException: Cannot perform Merge as multiple source rows
matched and attempted to modify the same target row in the Delta table.
Cardinality violations are not allowed in MERGE operations.
```

Delta's MERGE requires a one-to-one relationship between source and target for matched rows. Multiple source rows matching the same target row is a cardinality violation — Delta does not know which source row should win and refuses to guess. The MERGE fails and nothing is committed.

Intra-batch deduplication resolves this before it reaches the MERGE.

### Cross-Batch Duplicates

A batch file already processed in run N is re-delivered in run N+1. This happens because:
- A pipeline failure in run N caused the Step Functions execution to fail, and the operator re-runs with the same input files
- An upstream system re-sends a month's extract because a downstream system reported a discrepancy
- A network retry in `ingest.py` uploads the same file twice (though `ingest.py` uses the same S3 key, so the second upload overwrites the first — but the file contents are identical)

Cross-batch duplicates would produce duplicate rows in the Delta table if the MERGE had no mechanism to detect them. The MERGE key and update conditions are the cross-batch deduplication mechanism.

---

## Intra-Batch Deduplication — Products

```python
window_spec = (
    Window.partitionBy("product_id")
    .orderBy(
        F.col("department_id").asc(),
        F.col("product_name").asc(),
    )
)
deduped = (
    df.withColumn("_rank", F.rank().over(window_spec))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)
```

### Window Function Mechanics

`Window.partitionBy("product_id")` groups all rows with the same `product_id` together into a window. `.orderBy(...)` assigns a rank to each row within that window. `F.rank()` assigns rank 1 to the row that sorts first within the partition. All rows after rank 1 are duplicates of the key and are filtered out.

For products, the ordering is `department_id` ascending, then `product_name` ascending. This is a secondary sort on attributes that are expected to be the same for all rows with the same `product_id` (a product does not change departments). If two rows do have different attributes, the one with the lower `department_id` wins. This is an arbitrary but deterministic tie-breaking rule — the same rule applied to the same data always selects the same row.

### Why `rank()` and Not `row_number()`

`F.rank()` assigns the same rank to rows that are identical on the ordering columns. If two rows with `product_id = 42` have the same `department_id` and `product_name`, both receive rank 1. `F.row_number()` would assign rank 1 to one and rank 2 to the other arbitrarily (Spark's internal processing order, which is not guaranteed to be deterministic across runs).

For products, using `rank()` means truly identical rows both receive rank 1 and both survive the filter. This is the correct behaviour — if both rows are truly identical (same key, same attributes), it does not matter which one survives, but `rank()` makes the selection explicit: keep one representative of the identical group.

The downstream `whenMatchedUpdateAll(condition=change_detection)` then handles truly identical rows gracefully: if both source and target have the same attributes, the change-detection condition is false and no update occurs.

---

## Intra-Batch Deduplication — Orders

```python
window_spec = (
    Window.partitionBy("order_id")
    .orderBy(F.col("order_timestamp").desc())
)
deduped = (
    df.withColumn("_rank", F.row_number().over(window_spec))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)
```

### Last-Write-Wins by Timestamp

Orders uses `row_number()` (not `rank()`) with `order_timestamp` descending. The row with the most recent `order_timestamp` receives `row_number = 1` and survives. All other rows for the same `order_id` are discarded.

**Why `row_number()` here instead of `rank()`:**

If two rows have the same `order_id` and the same `order_timestamp`, they are truly indistinguishable — neither is more recent than the other. `rank()` would assign both rank 1, and both would survive the filter. Two identical rows reaching the MERGE for the same `order_id` would again produce a cardinality violation. `row_number()` assigns distinct sequential numbers regardless of tie values — in a tie, Spark picks one arbitrarily but deterministically within a single execution. For orders, any tie-breaking is acceptable because tied rows are identical and either choice produces the same committed state.

**Why `order_timestamp` descending:**

The assumption is that if an `order_id` appears multiple times with different timestamps, the later timestamp represents the more current state of the order. An order that moved from `pending` (timestamped 08:00) to `processing` (timestamped 08:15) within the same export window appears twice — the 08:15 row is more current and should win.

---

## Intra-Batch Deduplication — Order Items

```python
window_spec = (
    Window.partitionBy("id", "order_id")
    .orderBy(F.col("order_timestamp").desc())
)
deduped = (
    df.withColumn("_rank", F.row_number().over(window_spec))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)
```

### The Composite Partition Key

`Window.partitionBy("id", "order_id")` partitions on both components of the composite primary key together. A window defined only on `"id"` would group item 1 from order A and item 1 from order B together — they share `id = 1` but are different line items. The composite `("id", "order_id")` correctly identifies that `(id=1, order_id="A")` and `(id=1, order_id="B")` are distinct rows and must not be compared for deduplication.

The same `order_timestamp` descending ordering applies: if two rows share the same `(id, order_id)` composite key, the one with the later timestamp survives.

---

## Why `monotonically_increasing_id()` Was Rejected

`monotonically_increasing_id()` is a Spark function that assigns a unique, strictly increasing 64-bit integer to each row in a DataFrame. It is sometimes used as an ordering key when no natural ordering column exists.

It was considered for the products deduplication (products has no timestamp column) and explicitly rejected. The reason is that `monotonically_increasing_id()` assigns values based on the DataFrame partition layout — the specific integer assigned to a row depends on how Spark physically partitioned the DataFrame during the read, which depends on the number of Spark executors, the file split size, and the input data layout. The same CSV file read twice under different Spark session configurations (different number of G.1X workers, different executor memory causing different task scheduling) can assign different `monotonically_increasing_id()` values to the same row.

This means the deduplication winner is non-deterministic: reading the same file on Monday selects product row A, reading the same file on Tuesday after a Glue capacity change selects product row B. Two consecutive pipeline runs with the same input produce different committed states — a violation of idempotency.

The stable ordering by `(department_id ASC, product_name ASC)` does not depend on Spark's physical execution. It is a data-driven ordering that produces the same result regardless of how many executors Spark uses.

---

## Cross-Batch Deduplication — Delta MERGE Key and Conditions

After intra-batch deduplication, the source DataFrame contains at most one row per primary key. The Delta MERGE then handles cross-batch deduplication through two mechanisms that differ by table type.

### Dimension Tables — Change Detection Condition (Products)

```python
.whenMatchedUpdateAll(condition=(
    "source.department_id <> target.department_id "
    "OR source.department <> target.department "
    "OR source.product_name <> target.product_name"
))
```

A re-run of the same `products.csv` produces source rows identical to what was already committed. The MERGE matches on `product_id`. For each match, the change-detection condition evaluates all three mutable attributes. Since source and target are identical, the condition is `false` for every matched row. `whenMatchedUpdateAll` does not execute. Zero updates, zero inserts, zero copies.

The change-detection condition is what makes the MERGE a true no-op for products on re-run. Without it, `whenMatchedUpdateAll()` (no condition) updates every matched row unconditionally — Delta rewrites the Parquet files even though nothing changed, incrementing the table version and consuming I/O for no purpose.

### Fact Tables — Timestamp Guard (Orders and Order Items)

```python
.whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
```

The merge key (`order_id` for orders, `(id, order_id)` for order_items) identifies existing rows. The timestamp guard decides whether to update. A re-run of the same file produces source rows with the same timestamps as what was committed. `source.order_timestamp > target.order_timestamp` is false for every matched row (equal timestamps do not satisfy strict greater-than). Zero updates.

No new primary keys appear in the source (all were committed on the first run). Zero inserts.

The combined result is zero changes to the Delta table — idempotency confirmed.

---

## Deduplication Mechanism Summary

| Dataset | Intra-Batch Key | Intra-Batch Ordering | Intra-Batch Function | Cross-Batch Mechanism |
|---|---|---|---|---|
| products | `product_id` | `department_id` ASC, `product_name` ASC | `rank()` | Change-detection condition on 3 attributes |
| orders | `order_id` | `order_timestamp` DESC | `row_number()` | Timestamp guard: `source > target` |
| order_items | `(id, order_id)` | `order_timestamp` DESC | `row_number()` | Timestamp guard: `source > target` |

| Mechanism | Handles | Does Not Handle |
|---|---|---|
| Window intra-batch dedup | Duplicate keys within one CSV file | Different keys with identical data (not a duplicate issue) |
| Change-detection condition | Identical re-runs for dimension tables | Source corrections with a later timestamp (handled by MERGE update) |
| Timestamp guard | Identical re-runs for fact tables; stale re-deliveries | Two source rows with different timestamps for the same key (intra-batch dedup handles this first) |
