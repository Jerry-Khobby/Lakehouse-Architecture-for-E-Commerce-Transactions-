# Idempotency in Delta Pipelines — Re-Run Safety

## Overview

An idempotent operation produces the same result regardless of how many times it is executed. For this pipeline, idempotency means: running the same three CSV files through the pipeline a second (or third, or tenth) time produces zero new rows in the Delta tables, zero duplicate records, and zero data corruption. The pipeline can be re-run safely after any failure — hardware fault, network timeout, Step Functions execution error, or any other interruption — without manual cleanup.

This document explains the mechanisms that make each dataset's pipeline stage idempotent, how to confirm idempotency from the Delta log, and what "intra-batch" deduplication does before the MERGE runs.

---

## The Two Layers of Idempotency

Idempotency in this pipeline is enforced at two distinct layers:

1. **Intra-batch deduplication** — removes duplicate rows from the incoming CSV *before* they reach the MERGE. Handles duplicates within a single file.
2. **Delta MERGE key + update condition** — ensures a row that already exists in the target is either updated only if the source is newer (facts), or not updated at all if nothing changed (dimensions). Handles re-delivery of the same file on a subsequent run.

Both layers are required. The MERGE alone does not protect against within-file duplicates: if the same `order_id` appears twice in the CSV with different values, a MERGE without deduplication would fail with `DeltaUnsupportedOperationException: Cannot perform Merge as multiple source rows matched` — Delta does not allow one target row to be updated by more than one source row. The intra-batch dedup ensures each merge key appears exactly once in the source DataFrame before the MERGE executes.

---

## Products — Dimension Idempotency

### Intra-Batch Deduplication

`products_job.py` deduplicates within the validated batch using a stable Window ordering:

```python
window_spec = (
    Window.partitionBy("product_id")
    .orderBy(F.col("department_id").asc(), F.col("product_name").asc())
)
df = df.withColumn("_rank", F.rank().over(window_spec)).filter(F.col("_rank") == 1).drop("_rank")
```

If `product_id = 42` appears twice in the CSV, the Window function assigns rank 1 to the row with the lower `department_id` (and then lower `product_name` as a tiebreaker). Only rank-1 rows survive. The ordering is deterministic — the same CSV processed twice always keeps the same row. `monotonically_increasing_id()` was explicitly rejected as an ordering criterion because it assigns different IDs on different Spark runs, which would make the dedup non-deterministic.

### MERGE Idempotency

The MERGE for products uses a change-detection condition:

```python
.whenMatchedUpdateAll(condition=(
    "source.department_id <> target.department_id "
    "OR source.department <> target.department "
    "OR source.product_name <> target.product_name"
))
.whenNotMatchedInsertAll()
```

**On a re-run of the same `products.csv`:**

Every `product_id` in the source already exists in the target (inserted on the first run). For every matched row, the change-detection condition evaluates whether any of the three attributes differ between source and target. Since the source is identical to what was used on the first run, all three attributes match: `source.department_id = target.department_id`, `source.department = target.department`, `source.product_name = target.product_name`. The condition resolves to `false`. `whenMatchedUpdateAll` does not execute for any row.

There are no new `product_id` values to insert (all already exist). `whenNotMatchedInsertAll` does not execute for any row.

**Result:**
```
numTargetRowsInserted = 0
numTargetRowsUpdated  = 0
numTargetRowsCopied   = 0
```

The products table is byte-for-byte identical after the re-run. No Parquet files are rewritten. The Delta log receives a new version entry, but it contains no `add` or `remove` file actions.

---

## Orders — Fact Idempotency

### Intra-Batch Deduplication

`orders_job.py` deduplicates by `order_id` within the validated batch:

```python
window_spec = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc())
df = df.withColumn("_rank", F.row_number().over(window_spec)).filter(F.col("_rank") == 1).drop("_rank")
```

If the same `order_id` appears twice in the CSV, the row with the later `order_timestamp` survives (last-write-wins within the file). The ordering is deterministic — timestamp descending, with `order_id` as the partition key, always selects the same row for the same file contents.

### MERGE Idempotency

```python
.whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
.whenNotMatchedInsertAll()
```

**On a re-run of the same `orders_apr_2025.csv`:**

Every `order_id` in the source already exists in the target (inserted on the first run). For every matched row, the timestamp guard evaluates: `source.order_timestamp > target.order_timestamp`. Since the source file is identical, both values are the same timestamp. `=` is not `>`. The condition is `false`. `whenMatchedUpdateAll` does not execute.

There are no new `order_id` values. `whenNotMatchedInsertAll` does not execute.

**Result:**
```
numTargetRowsInserted = 0
numTargetRowsUpdated  = 0
numTargetRowsCopied   = 0
```

The orders table is unchanged. The timestamp guard is the single mechanism that prevents stale re-deliveries from overwriting newer data and prevents idempotent re-runs from creating phantom updates.

---

## Order Items — Fact Idempotency with Composite Key

### Intra-Batch Deduplication

`order_items_job.py` deduplicates by the composite key `(id, order_id)`:

```python
window_spec = (
    Window.partitionBy("id", "order_id")
    .orderBy(F.col("order_timestamp").desc())
)
df = df.withColumn("_rank", F.row_number().over(window_spec)).filter(F.col("_rank") == 1).drop("_rank")
```

If line item 3 of order "abc" (`id=3, order_id="abc"`) appears twice in the CSV, the row with the later `order_timestamp` survives. The composite partition key ensures that `id=3` from order "abc" and `id=3` from order "xyz" are treated as separate rows — they have different `order_id` values and are not duplicates of each other.

### Why the Composite Key Prevents Row-Level Duplicates

Without a composite key, a MERGE keyed only on `order_id` would treat an entire order (all its line items) as a single logical row. The MERGE would fail or produce incorrect results — it cannot update a single target row to reflect five source rows.

Without a composite key, a MERGE keyed only on `id` would treat line item 1 of every order as the same row. Order A item 1 and Order B item 1 would collide.

The combination `(id, order_id)` — line item number within a specific order — is unique across the entire dataset. This composite key is the correct natural key for order items.

### MERGE Idempotency

```python
.whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
.whenNotMatchedInsertAll()
```

**On a re-run of the same `order_items_apr_2025.csv`:**

Every `(id, order_id)` pair in the source already exists in the target. For every matched composite key, `source.order_timestamp = target.order_timestamp` (same file, same timestamps). The guard `source.order_timestamp > target.order_timestamp` is `false`. Nothing is updated, nothing is inserted.

**Result:**
```
numTargetRowsInserted = 0
numTargetRowsUpdated  = 0
numTargetRowsCopied   = 0
```

---

## Confirming Idempotency from the Delta Log

The Delta log is the authoritative record of what happened on each run. After a re-run, read the last two versions:

```python
delta_table.history(2).select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
```

**Example output after first run then idempotent re-run:**

```
+-------+-------------------+---------+---------------------------------------------------+
|version|timestamp          |operation|operationMetrics                                   |
+-------+-------------------+---------+---------------------------------------------------+
|2      |2025-05-02 08:15:33|MERGE    |{numTargetRowsInserted -> 0,                       |
|       |                   |         | numTargetRowsUpdated -> 0,                        |
|       |                   |         | numTargetRowsCopied -> 0,                         |
|       |                   |         | numSourceRows -> 850}                             |
|1      |2025-04-30 09:22:11|MERGE    |{numTargetRowsInserted -> 850,                     |
|       |                   |         | numTargetRowsUpdated -> 0,                        |
|       |                   |         | numTargetRowsCopied -> 0,                         |
|       |                   |         | numSourceRows -> 850}                             |
+-------+-------------------+---------+---------------------------------------------------+
```

Version 1: First run, 850 rows inserted (all new). Version 2: Re-run, zero insertions, zero updates. The table data is identical at versions 1 and 2.

### Time Travel Verification

Delta's time travel feature allows reading the table at a specific version to compare:

```python
v1 = spark.read.format("delta").option("versionAsOf", 1).load(orders_path)
v2 = spark.read.format("delta").option("versionAsOf", 2).load(orders_path)

print(f"Version 1 row count: {v1.count()}")
print(f"Version 2 row count: {v2.count()}")
```

**Expected output:**
```
Version 1 row count: 850
Version 2 row count: 850
```

Identical row counts confirm no rows were added. To verify no rows changed:

```python
changed_rows = v1.exceptAll(v2).union(v2.exceptAll(v1))
print(f"Rows that differ between versions: {changed_rows.count()}")
```

**Expected output:**
```
Rows that differ between versions: 0
```

Zero differing rows confirms the re-run produced a byte-for-byte identical table state. This is the formal proof of idempotency using the Delta log.

---

## What Idempotency Does NOT Protect Against

Idempotency in this pipeline is scoped to re-delivery of the **same file** or the **same batch**. It does not protect against:

**Different files with overlapping keys:** If a new CSV file is delivered that contains `order_id` values that were already processed in a previous batch *but with different data* (corrected amounts, status updates), the timestamp guard determines the winner. A source row with a newer `order_timestamp` updates the existing row. A source row with an older or equal `order_timestamp` is silently ignored. This is the intended behaviour for corrections — the most recent timestamp wins.

**Schema changes:** If the source CSV adds a new column, Delta's schema enforcement raises `AnalysisException: A schema mismatch detected` before any MERGE executes. The table is unchanged. The pipeline fails cleanly. No partial writes occur.

**Data corruption upstream:** If the source system delivers a file where all `order_id` values are wrong (e.g. all blank), the validation stage in the Glue job rejects all rows as invalid. Zero rows reach the MERGE. The table is unchanged. Idempotency holds even when validation produces zero valid rows — the result is a clean no-op.

---

## Idempotency Summary

| Mechanism | Layer | Protects Against |
|---|---|---|
| Window-based intra-batch dedup (products) | Before MERGE | Duplicate `product_id` within the CSV |
| Window-based intra-batch dedup (orders) | Before MERGE | Duplicate `order_id` within the CSV |
| Composite key + Window dedup (order_items) | Before MERGE | Duplicate `(id, order_id)` within the CSV |
| Change-detection condition (products) | MERGE | Re-run of identical data, unchanged products |
| Timestamp guard (orders, order_items) | MERGE | Re-run of same file, stale re-deliveries |
| Composite MERGE key (order_items) | MERGE | Row-level duplicates across the composite key |
| Delta log history | Post-MERGE | Audit proof — `operationMetrics` shows zero on re-run |
| Time travel | Post-MERGE | Formal verification — row counts and diff across versions |
