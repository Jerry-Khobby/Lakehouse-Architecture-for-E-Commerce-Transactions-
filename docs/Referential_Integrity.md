# Referential Integrity — Left Anti-Join Against Live Delta Tables

## Overview

Order items reference two other entities: the product being purchased (`product_id` → products table) and the order the item belongs to (`order_id` → orders table). In a relational database, these references would be enforced by foreign key constraints at the storage engine level. S3-backed Delta Lake has no such constraints — nothing at the storage layer prevents an order item from referencing a `product_id` that does not exist.

This pipeline enforces referential integrity explicitly at the Glue job validation layer using Spark `left_anti` joins against the live Delta tables. An order item that references a non-existent product or a non-existent order is rejected and written to `rejected/order_items/` before the MERGE runs. This document explains how the checks work, why the `left_anti` join is the correct operation, the `STRICT_REFERENTIAL_INTEGRITY` flag that controls them, and why referential checks happen after structural checks.

---

## What `left_anti` Join Does

A `left_anti` join returns all rows from the left DataFrame that have **no match** in the right DataFrame on the join key. It is the set-theoretic complement of an inner join.

```python
# Returns every order_item row where product_id is NOT in the products table
unmatched = order_items_df.join(
    products_df.select("product_id"),
    on="product_id",
    how="left_anti"
)
```

If `order_items_df` contains `product_id = 999` and the products Delta table has no row with `product_id = 999`, that order item row appears in `unmatched`. If `product_id = 42` exists in both, the order item row is excluded from `unmatched` — it has a valid reference.

The inverse — valid rows — is obtained by the same operation reversed: an inner join or a `left_semi` join returns only the rows that do match:

```python
valid = order_items_df.join(
    products_df.select("product_id"),
    on="product_id",
    how="left_semi"
)
```

The pipeline uses two `left_anti` passes to isolate the invalid rows (for rejection), then the remaining rows are the valid set.

---

## `_filter_by_product_ref()` — Product Referential Check

```python
def _filter_by_product_ref(
    df: DataFrame,
    products_path: str,
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame]:
    products_df = spark.read.format("delta").load(products_path)
    known_products = products_df.select("product_id").distinct()

    invalid = df.join(known_products, on="product_id", how="left_anti")
    invalid = invalid.withColumn("rejection_reason", F.lit("unknown_product_id"))

    valid = df.join(known_products, on="product_id", how="left_semi")
    return valid, invalid
```

### What It Reads

`spark.read.format("delta").load(products_path)` reads the current committed state of the products Delta table from `s3://<data-bucket>/lakehouse-dwh/products/`. This is a live read — it uses the latest `_delta_log/` snapshot, not a cached version. If the products Glue job ran in the same Step Functions execution and committed new products before the order_items job starts, those new products are visible to this read.

`products_df.select("product_id").distinct()` reduces the products table to a single-column DataFrame of known product IDs. `.distinct()` is technically redundant here because `product_id` is the Delta table's merge key (unique by construction), but it makes the intent explicit and avoids any edge case from corrupted data in the products table itself.

### Why This Order Matters

The referential integrity check runs **after** the structural checks (null checks, range checks, dedup). This ordering is intentional:

1. A null `product_id` would fail the null check (Check 3 in order_items validation) and be rejected with `"null_required_field"` before reaching the referential check. A null `product_id` joining against `known_products` on `product_id` would produce all rows as unmatched — every order item would appear invalid, which is wrong.

2. A `product_id <= 0` would fail the invalid ID check and be rejected before reaching this join. A fabricated negative ID `product_id = -1` clearly does not exist in the products table, but rejecting it here with `"unknown_product_id"` would be misleading — the real reason is that the value is structurally invalid, not that the product is unknown.

By the time `_filter_by_product_ref()` runs, the DataFrame contains only rows with valid (non-null, positive) `product_id` values. Any row rejected here failed a genuine referential check — the ID is structurally valid but does not exist in the products catalogue.

### The `left_anti` Join and the EventBridge Decision

This join was the technical root cause that ruled out using EventBridge S3 triggers for this pipeline. If EventBridge fired a Step Functions execution for each uploaded file independently:

- The `products.csv` upload triggers execution A (products job runs, MERGE inserts products)
- The `orders.csv` upload triggers execution B concurrently
- The `order_items.csv` upload triggers execution C concurrently

Execution C (order_items job) runs its referential integrity check against the products Delta table at the moment it executes. If the products MERGE from execution A has not yet committed by the time execution C reads the products table, the `left_anti` join finds an empty products table. Every single order item has `product_id` that does not match an empty set — 100% of order items are rejected as `"unknown_product_id"`.

This is a silent total rejection. The pipeline reports success (all three jobs completed), the Step Functions execution succeeds, but the `order_items` Delta table is empty. No error is raised because rejecting rows is normal behaviour — the pipeline cannot distinguish "all rows rejected because the reference table was empty" from "all rows genuinely reference invalid products." The explicit trigger pattern in `ingest.py` (upload all three files, then call `sfn:StartExecution` once) eliminates this race condition entirely.

---

## `_filter_by_order_ref()` — Order Referential Check

```python
def _filter_by_order_ref(
    df: DataFrame,
    orders_path: str,
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame]:
    orders_df = spark.read.format("delta").load(orders_path)
    known_orders = orders_df.select("order_id").distinct()

    invalid = df.join(known_orders, on="order_id", how="left_anti")
    invalid = invalid.withColumn("rejection_reason", F.lit("unknown_order_id"))

    valid = df.join(known_orders, on="order_id", how="left_semi")
    return valid, invalid
```

The pattern is identical to the product check. `orders_path` points to `s3://<data-bucket>/lakehouse-dwh/orders/`. This read must see the committed result of the orders Glue job MERGE from the current execution.

### Step Functions Execution Order Dependency

The order_items job runs after both the products job and the orders job have committed their MERGEs. The Step Functions state machine enforces this sequencing:

```
[ProcessProducts] → [ProcessOrders] → [ProcessOrderItems]
```

Not parallel, not concurrent — strictly sequential. By the time `ProcessOrderItems` starts, `ProcessOrders` has successfully committed. `orders_path` contains the current batch's orders. Every `order_id` from the current `order_items.csv` that belongs to the current batch will find its matching `order_id` in the orders Delta table.

An order item whose `order_id` is not in the orders table is genuinely invalid — it references an order that has never been committed to the pipeline's Silver layer. The rejection is correct.

---

## `_strict_referential_integrity()` — The Test Gate

```python
def _strict_referential_integrity(args: dict) -> bool:
    return args.get("STRICT_REFERENTIAL_INTEGRITY", "true").lower() == "true"
```

The referential integrity checks read live Delta tables. Unit tests cannot run against real Delta tables without setting up a full Spark + Delta + S3 environment. To allow unit testing of the validation logic without live Delta dependencies, the referential checks are conditional on this flag.

```python
if _strict_referential_integrity(args):
    valid_df, product_rejects = _filter_by_product_ref(valid_df, products_path, spark)
    rejected_rows = rejected_rows.union(product_rejects)

    valid_df, order_rejects = _filter_by_order_ref(valid_df, orders_path, spark)
    rejected_rows = rejected_rows.union(order_rejects)
```

**In production:** `STRICT_REFERENTIAL_INTEGRITY` is not set in the Glue job arguments, so it defaults to `"true"`. Both referential checks execute.

**In unit tests:** The test fixture sets `STRICT_REFERENTIAL_INTEGRITY = "false"` in the args dict. Both referential checks are skipped. Tests can validate all 12 structural checks without needing a live Delta environment.

This flag is an escape hatch for testing only. It must never be set to `"false"` in a production Glue job run.

---

## What Happens to Referentially Invalid Rows

Rows rejected by either referential check are passed to `write_rejected()` in `common.py` alongside all other rejected rows:

```python
write_rejected(spark, rejected_rows, dataset="order_items", run_id=job_run_id, s3_bucket=bucket)
```

The rejected Parquet files contain the full original row plus the `rejection_reason` column (`"unknown_product_id"` or `"unknown_order_id"`), `_rejected_at`, `_job_run_id`, and `_source_key`. The `_source_key` identifies the S3 key of the source CSV that contained the invalid row.

This allows an operator to:
1. Open the rejected Parquet file in Athena
2. Filter by `rejection_reason = "unknown_product_id"`
3. Find the specific `product_id` values that were unrecognised
4. Investigate whether the products CSV was missing entries or whether the order items CSV contained genuine data errors

See [Rejected_Records_Strategy.md](Rejected_Records_Strategy.md) for the full storage structure and lifecycle policy.

---

## Why Not Enforce at the Delta Write Layer

Delta Lake's schema enforcement does not support foreign key constraints. Delta validates column types and nullable constraints but has no concept of "this column's value must exist in another table." Enforcing referential integrity at the write layer would require a Delta constraint feature that does not exist in Delta Lake 2.x.

Even if it were available, enforcing it at write time would cause the MERGE to fail the entire batch rather than separating the invalid rows. A single order item with an unknown `product_id` would block all 2,540 order items from being merged. The validation-first pattern (validate → separate valid from invalid → MERGE valid-only) is the correct approach for pipelines that need to process partial batches gracefully.
