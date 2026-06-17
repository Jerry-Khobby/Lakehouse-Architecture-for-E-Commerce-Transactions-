# Partitioning Strategy — Why Each Table Partitions the Way It Does

## Overview

Partitioning divides a Delta table's data into separate S3 prefixes based on the value of one or more columns. Each distinct value of the partition column becomes its own directory under the table path. When Athena or a Glue job queries with a filter on the partition column, S3 lists only the matching directories rather than scanning all data files — this is partition pruning. Choosing the right partition column is therefore a performance decision: the wrong choice can create thousands of tiny partitions that are slower to list than to scan, or produce one giant partition that provides no pruning benefit at all.

This pipeline partitions `products` by `department`, `orders` by `date`, and `order_items` by `date`. Each decision is explained below.

---

## Products — Partitioned by `department`

```python
PRODUCTS_PARTITION_COLS = ["department"]
```

The `products` table is a dimension table containing product catalogue entries. Each product belongs to one department (e.g. `produce`, `dairy`, `beverages`, `bakery`).

### Why `department`

**Cardinality:** There are approximately 10–20 distinct department values in the e-commerce catalogue. 10–20 partitions is the ideal range for a small dimension table — low enough that Athena's S3 `ListObjects` call to discover partitions is fast, high enough that common queries filtered by department scan only their relevant partition.

**Query pattern:** Analytical queries against the products dimension almost always filter by department:
- "All products in the `produce` department"
- "Average product name length by department"
- Joins from `order_items` that look up product details and then group by department

With `department` as the partition column, Athena's query planner pushes the `WHERE department = 'produce'` predicate down to the partition prune step — it reads only `lakehouse-dwh/products/department=produce/` and never opens the other 9–19 directories.

**Data volume:** The products table is small — a few hundred to a few thousand rows. With 10 partitions, each partition contains ~50–200 rows. Each Parquet file is under a few KB. Athena and Glue are designed for much larger workloads, but the partitioning still saves the overhead of unnecessary file opens.

### Why NOT user_id or product_id

`product_id` is an integer primary key with potentially thousands of distinct values (one per product). Partitioning by `product_id` would create thousands of tiny single-row Parquet files — one directory per product. S3 `ListObjects` on a prefix with thousands of directories is slower than scanning a single unpartitioned file. File-per-row is an anti-pattern called "small file problem" and is the most common partitioning mistake.

---

## Orders — Partitioned by `date`

```python
ORDERS_PARTITION_COLS = ["date"]
```

The `orders` table is a fact table containing customer order records. Each order has a `date` column derived from `order_timestamp` — the calendar date of the order.

### Why `date`

**Cardinality:** A monthly batch covers approximately 30–31 days. After six months of data, the table has ~180 date partitions. After one year, ~365. This is manageable — Athena lists the partitions once per query and prunes based on the `WHERE date = ...` or `WHERE date BETWEEN ... AND ...` filter.

**Query pattern:** Orders are inherently time-series data. Every analytical query over orders has a time dimension:
- "Total revenue for April 2025"
- "Order count per day last week"
- "Average order value by month"

Without date partitioning, a query for April revenue scans the entire orders table — all months, all years. With `date` partitioning, Athena opens only the 30 directories matching April dates and reads nothing from the March or May data.

**Growth characteristics:** One new date partition is added per day in each new monthly batch. The partition count grows predictably and linearly. At 1,000 partitions (approximately 3 years of data), `ListObjects` latency is still negligible relative to the actual data scan time.

**Delta MERGE alignment:** The MERGE writes new rows into their date partitions. When a May batch inserts orders dated 2025-05-01 through 2025-05-31, Delta writes 31 new partition directories. Each MERGE operation touches only the newly inserted partitions and leaves all previous date partitions physically unchanged. This is also why the `numTargetRowsCopied` metric is low for orders — same-date rows going into the same partition are written together, not scattered across every partition.

### Why NOT user_id or order_status

`user_id` — A large e-commerce platform can have millions of users. Partitioning by `user_id` creates millions of tiny partitions, one per user. This is the extreme version of the small-file problem. Athena cannot prune effectively because almost no analytical query filters for a single user ID. Aggregation queries still touch all partitions. The `ListObjects` overhead alone would exceed the scan time.

`order_status` — There are typically 3–5 distinct status values (`pending`, `processing`, `shipped`, `delivered`, `cancelled`). Low cardinality means almost every query touches every status partition. The pruning benefit is near-zero. Additionally, order statuses change over time — an order moves from `pending` to `delivered`. Delta MERGE with `order_status` as a partition column would require rewriting the row across partition boundaries, which Delta handles by deleting the row from the old partition and inserting it in the new one. This produces excessive partition rewriting on every status-change update. `date` is immutable — an order placed on 2025-04-15 always has `date = 2025-04-15`, regardless of status changes.

---

## Order Items — Partitioned by `date`

```python
ORDER_ITEMS_PARTITION_COLS = ["date"]
```

The `order_items` table uses the same `date` partition column as the `orders` table.

### Why the Same `date` Column as Orders

**Query alignment with the parent table:** The most common join in this dataset is `order_items JOIN orders ON order_items.order_id = orders.order_id`. If both tables are partitioned by `date` using the same column (derived from the same source `order_timestamp` for the same order), a query filtered by date can prune both tables' partitions simultaneously:

```sql
SELECT o.order_id, SUM(oi.add_to_cart_order) as items_count
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.date = DATE '2025-04-15'
  AND oi.date = DATE '2025-04-15'
GROUP BY o.order_id
```

Athena opens `lakehouse-dwh/orders/date=2025-04-15/` and `lakehouse-dwh/order_items/date=2025-04-15/` — only two prefixes. Without `date` partitioning on order items, the join would scan every order item regardless of date.

**Intra-batch alignment:** Within a single pipeline run, all order items in a batch have a `date` derived from the same `order_timestamp` distribution as their parent orders. Items for April 2025 orders have April 2025 dates. The May batch writes to May date partitions. The `date` column ties the order items to the same temporal slice as the orders, making partition alignment exact.

**Composite key and partition safety:** The composite MERGE key is `(id, order_id)`. The `date` column is not part of the MERGE key — it is used only for partitioning. This is intentional: `date` is derived from `order_timestamp`, which is the same for all items in an order, and `order_timestamp` is the timestamp guard field. Using `date` in the merge key would create a compound constraint (same `id`, same `order_id`, same `date`) that is equivalent to the existing composite key since `date` is functionally determined by `order_timestamp`.

### Why NOT product_id

`product_id` has moderate cardinality — a few hundred to a few thousand distinct values. Partitioning order items by `product_id` would create one directory per product, with each product's directory containing order items from all dates. A time-series query ("order items for the past week") could not prune by product — it would need to open all product directories and scan for the date filter inside each one. The date-based time-series access pattern dominates analytical workloads; product-based access is better served by predicate pushdown at the file level (Parquet column statistics) rather than partition pruning.

---

## Athena Scan Performance Implications

### Partition Pruning vs Full Table Scan

Without partitioning, an Athena query with `WHERE date = '2025-04-15'` on the orders table scans every Parquet file in `lakehouse-dwh/orders/`. After 12 months of data (365 daily batches of ~850 orders each), that is approximately 310,000 order rows spread across 365 Parquet files. Athena opens every file, reads the date column, and discards rows that do not match. All 365 files are read.

With `date` partitioning, the same query opens exactly one directory: `lakehouse-dwh/orders/date=2025-04-15/`. Athena reads only the Parquet files in that directory — approximately 1 file containing ~850 rows. The other 364 directories are never opened. Athena reads 1/365th of the data.

### Bytes Scanned and Cost

Athena charges $5.00 per terabyte scanned (as of 2024). Partition pruning directly reduces the bytes scanned:

- Full table scan of 12 months of orders (310,000 rows, ~50 MB as Parquet with compression): **$0.00025** per query
- Single-day query with partition pruning (~850 rows, ~140 KB): approximately **$0.0000007** per query

The absolute cost difference is small at this dataset size — the orders table in this pipeline is a development/training dataset, not a production-scale billion-row table. The ratio (1/365th of bytes scanned) scales directly to production: a 100 GB orders table costs $0.50 per full scan vs. $0.0014 per single-day partitioned query.

### Glue MERGE Partition Alignment

Partition pruning also benefits the Glue MERGE operation. When Delta executes a MERGE, Spark reads only the partitions that could contain matching rows. For `orders`, the MERGE key is `order_id`. If the source batch contains orders from April 2025 (`date = 2025-04-01` through `2025-04-30`), Delta's partition pruning reads only April partitions from the target. March and earlier partitions cannot contain matching `order_id` values from the April batch (new April orders are `whenNotMatched` inserts, not updates to March rows). In practice, the optimizer prunes based on statistics in the Delta log, but the partition-aligned writes ensure that April's data is physically separated from March's data, making this optimization possible.

---

## Partitioning Decision Summary

| Table | Partition Column | Distinct Values | Rationale |
|---|---|---|---|
| products | `department` | ~10–20 | Low cardinality, natural query filter for dimension lookups |
| orders | `date` | ~365/year | Time-series, immutable column, aligns with common query filter |
| order_items | `date` | ~365/year | Matches parent orders for partition-aligned joins |

| Rejected Option | Reason Rejected |
|---|---|
| products by `product_id` | High cardinality → thousands of single-row partitions (small file problem) |
| orders by `user_id` | Very high cardinality → millions of tiny partitions, no pruning benefit |
| orders by `order_status` | Low cardinality with mutable values → constant cross-partition rewrites on status change |
| order_items by `product_id` | Moderate cardinality, but product-based access is rare; date access dominates |
