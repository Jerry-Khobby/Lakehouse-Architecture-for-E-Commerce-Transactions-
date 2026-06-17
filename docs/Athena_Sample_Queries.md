# Athena Sample Queries — Reading Delta Tables Through the Glue Catalog

## Overview

Athena engine version 3 reads Delta tables natively. The Glue Data Catalog entries registered by `update_catalog_table()` and the Glue crawlers expose the three Silver layer Delta tables as standard SQL tables. Queries use ordinary SQL — no special Delta syntax is required for reads. This document covers how Athena resolves Delta tables, partition pruning behaviour, and a set of analytical queries that span from single-table aggregations to three-way cross-dataset joins, including a time travel query using Athena's Delta version-as-of syntax.

---

## How Athena Reads Delta Tables Through the Catalog

When Athena queries `ecom_lakehouse_dev.orders`, the execution path is:

1. **Catalog lookup**: Athena resolves `ecom_lakehouse_dev.orders` against the Glue Data Catalog, finding the table's `LOCATION` (`s3://<data-bucket>/lakehouse-dwh/orders/`) and `TABLE_FORMAT` (`DELTA`)
2. **Delta log snapshot**: Athena reads `_delta_log/` at the table location, reconstructing the current snapshot — the set of Parquet files that belong to the latest committed version
3. **Partition pruning**: If the query has a predicate on `date` (the partition column), Athena evaluates the `partitionValues` in the Delta log's `add` entries and skips S3 prefixes whose partition values do not match
4. **Column projection**: Athena reads only the columns referenced in the query from the Parquet files, using Parquet column metadata to skip irrelevant column groups
5. **Query execution**: Athena processes the filtered, projected data using its distributed query engine

The critical requirement is `enforce_workgroup_configuration = true` on the Athena workgroup — this prevents clients from overriding the output location or encryption, and pins the engine version to 3. Queries submitted with the wrong engine version (engine 2) do not support native Delta reads and would fail with `HIVE_UNKNOWN_ERROR: Delta Lake format is not supported`.

### Table Reference Format

```sql
-- Standard unqualified reference (requires workgroup to have the correct database set):
SELECT * FROM orders LIMIT 5;

-- Fully qualified (preferred — unambiguous regardless of workgroup default):
SELECT * FROM ecom_lakehouse_dev.orders LIMIT 5;
```

All queries in this document use fully qualified table names.

---

## Schema Reference

```
ecom_lakehouse_dev.products
  product_id     INT        NOT NULL  — partition key context: department
  department_id  INT        NOT NULL
  department     VARCHAR    NOT NULL  [partition column]
  product_name   VARCHAR    NOT NULL

ecom_lakehouse_dev.orders
  order_num      BIGINT
  order_id       VARCHAR    NOT NULL
  user_id        VARCHAR    NOT NULL
  order_timestamp TIMESTAMP NOT NULL
  total_amount   DECIMAL(12,2) NOT NULL
  date           DATE       NOT NULL  [partition column]

ecom_lakehouse_dev.order_items
  id                      INT      NOT NULL  [composite key: (id, order_id)]
  order_id                VARCHAR  NOT NULL
  user_id                 VARCHAR  NOT NULL
  product_id              INT      NOT NULL
  add_to_cart_order       INT      NOT NULL  — position in cart (1 = first item added)
  reordered               INT      NOT NULL  — 1 if previously purchased, 0 if not
  days_since_prior_order  INT               — NULL for first-ever order
  order_timestamp         TIMESTAMP NOT NULL
  date                    DATE     NOT NULL  [partition column]
```

---

## Single-Table Queries

### Order Volume and Revenue by Date

```sql
SELECT
    date,
    COUNT(DISTINCT order_id)          AS order_count,
    SUM(total_amount)                 AS total_revenue,
    ROUND(AVG(total_amount), 2)       AS avg_order_value,
    MIN(total_amount)                 AS min_order,
    MAX(total_amount)                 AS max_order
FROM ecom_lakehouse_dev.orders
WHERE date BETWEEN DATE '2025-04-01' AND DATE '2025-04-30'
GROUP BY date
ORDER BY date;
```

**Partition pruning**: `WHERE date BETWEEN DATE '2025-04-01' AND DATE '2025-04-30'` prunes to only the 30 April date partitions. Athena opens zero files outside `date=2025-04-*/`. Without this predicate, all historical date partitions would be scanned.

**Expected result shape**: One row per calendar day in April. Days with no orders do not appear (use a date spine LEFT JOIN if zero-order days must be represented).

---

### Daily Order Count Trend (with 7-Day Moving Average)

```sql
SELECT
    date,
    order_count,
    ROUND(
        AVG(order_count) OVER (
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    ) AS moving_avg_7d
FROM (
    SELECT
        date,
        COUNT(DISTINCT order_id) AS order_count
    FROM ecom_lakehouse_dev.orders
    WHERE date >= DATE '2025-04-01'
    GROUP BY date
)
ORDER BY date;
```

Athena engine v3 supports window functions (`OVER`, `ROWS BETWEEN`). The inner subquery aggregates daily counts; the outer query applies the 7-day rolling average. The `WHERE date >= DATE '2025-04-01'` predicate prunes partitions.

---

### Products Catalogue by Department

```sql
SELECT
    department,
    COUNT(product_id)  AS product_count,
    MIN(product_id)    AS min_product_id,
    MAX(product_id)    AS max_product_id
FROM ecom_lakehouse_dev.products
GROUP BY department
ORDER BY product_count DESC;
```

**Partition pruning on products**: `department` is the products partition column. A query filtered by a specific department (`WHERE department = 'produce'`) reads only `lakehouse-dwh/products/department=produce/`. Without a `WHERE` clause, all 10–20 department partitions are scanned — but the products table is small enough that a full scan is fast.

---

### Repeat Purchase Rate

```sql
SELECT
    CASE
        WHEN days_since_prior_order IS NULL THEN 'First-time buyers'
        ELSE 'Repeat buyers'
    END                                        AS buyer_type,
    COUNT(DISTINCT order_id)                   AS order_count,
    ROUND(
        COUNT(DISTINCT order_id) * 100.0
        / SUM(COUNT(DISTINCT order_id)) OVER (), 1
    )                                          AS pct_of_total
FROM ecom_lakehouse_dev.order_items
GROUP BY
    CASE
        WHEN days_since_prior_order IS NULL THEN 'First-time buyers'
        ELSE 'Repeat buyers'
    END;
```

`days_since_prior_order IS NULL` identifies a user's first-ever order in the dataset (no prior order to count days from). The window function `SUM(...) OVER ()` computes the grand total for the percentage calculation without a second pass.

---

## Cross-Dataset Joins

### Top Reordered Products

```sql
SELECT
    p.department,
    p.product_name,
    COUNT(*)                      AS times_purchased,
    SUM(oi.reordered)             AS times_reordered,
    ROUND(
        SUM(oi.reordered) * 100.0 / COUNT(*), 1
    )                             AS reorder_rate_pct
FROM ecom_lakehouse_dev.order_items oi
JOIN ecom_lakehouse_dev.products p
    ON oi.product_id = p.product_id
GROUP BY
    p.department,
    p.product_name
HAVING COUNT(*) >= 5                        -- exclude products purchased fewer than 5 times
ORDER BY reorder_rate_pct DESC
LIMIT 25;
```

**Join mechanics**: `order_items` is partitioned by `date`; `products` is partitioned by `department`. This is not a partition-aligned join (different partition columns). Athena performs a hash join — `products` (small) is broadcast to all nodes processing `order_items` (large). The join executes entirely in Athena without data movement back to S3.

`HAVING COUNT(*) >= 5` excludes products that appear too rarely in the dataset for the reorder rate to be meaningful. A product purchased once with `reordered = 1` would show a 100% reorder rate — technically accurate but not analytically useful.

---

### Average Order Value by Department

```sql
SELECT
    p.department,
    COUNT(DISTINCT o.order_id)            AS orders_containing_dept_product,
    ROUND(AVG(o.total_amount), 2)         AS avg_order_value,
    ROUND(SUM(o.total_amount), 2)         AS total_revenue_from_orders,
    COUNT(oi.id)                          AS total_items_sold
FROM ecom_lakehouse_dev.orders o
JOIN ecom_lakehouse_dev.order_items oi
    ON o.order_id = oi.order_id
   AND o.date = oi.date                   -- partition-aligned join condition
JOIN ecom_lakehouse_dev.products p
    ON oi.product_id = p.product_id
GROUP BY p.department
ORDER BY total_revenue_from_orders DESC;
```

**Partition-aligned join**: `AND o.date = oi.date` adds the partition column to the join condition. Both `orders` and `order_items` are partitioned by `date`. Athena can evaluate this join partition-by-partition — data for `date=2025-04-15` in `orders` only joins with data for `date=2025-04-15` in `order_items`. This eliminates cross-partition shuffle for the `orders ↔ order_items` join, significantly reducing bytes scanned and query time at scale.

**Interpretation note**: `avg_order_value` is the average total order amount for orders that contain at least one product from that department — not the average spend per department item. A single order containing both `produce` and `dairy` items contributes to both departments' average. This is intentional for "which department's orders tend to be higher-value?" analysis.

---

### Customer Purchasing Frequency Analysis

```sql
WITH order_frequency AS (
    SELECT
        user_id,
        COUNT(DISTINCT order_id)   AS total_orders,
        MIN(date)                  AS first_order_date,
        MAX(date)                  AS last_order_date,
        DATE_DIFF('day', MIN(date), MAX(date)) AS days_active
    FROM ecom_lakehouse_dev.orders
    GROUP BY user_id
)
SELECT
    CASE
        WHEN total_orders = 1                     THEN '1 order (one-time)'
        WHEN total_orders BETWEEN 2 AND 4         THEN '2–4 orders (occasional)'
        WHEN total_orders BETWEEN 5 AND 9         THEN '5–9 orders (regular)'
        WHEN total_orders >= 10                   THEN '10+ orders (loyal)'
    END                                           AS frequency_segment,
    COUNT(user_id)                                AS customer_count,
    ROUND(AVG(total_orders), 1)                   AS avg_orders_in_segment,
    ROUND(AVG(days_active), 0)                    AS avg_days_between_first_and_last
FROM order_frequency
GROUP BY 1
ORDER BY MIN(total_orders);
```

Athena engine v3 supports CTEs (`WITH`), `DATE_DIFF`, and `CASE` expressions. This query does not require joining with `order_items` or `products` — it operates entirely on `orders`, which keeps partition pruning effective if a `WHERE date` clause is added to the CTE.

---

### Most Popular Items by Cart Position

```sql
SELECT
    add_to_cart_order            AS cart_position,
    p.product_name,
    p.department,
    COUNT(*)                     AS times_added_at_this_position
FROM ecom_lakehouse_dev.order_items oi
JOIN ecom_lakehouse_dev.products p
    ON oi.product_id = p.product_id
WHERE add_to_cart_order <= 5    -- first 5 items added to cart only
GROUP BY
    add_to_cart_order,
    p.product_name,
    p.department
ORDER BY
    add_to_cart_order,
    times_added_at_this_position DESC;
```

`add_to_cart_order = 1` is the first item placed in the cart — typically a staple product. `add_to_cart_order = 5` is the fifth. Filtering to the first 5 positions reveals the "anchor" products customers add first, which is useful for home page placement and promotional strategies.

---

## Delta Time Travel Queries

Athena engine v3 supports reading a Delta table at a specific historical version or timestamp using `FOR VERSION AS OF` and `FOR TIMESTAMP AS OF`.

### Read Table State at a Specific Delta Version

```sql
-- Count rows after the first pipeline run (version 1 = first MERGE commit)
SELECT COUNT(*) AS row_count_at_v1
FROM ecom_lakehouse_dev.orders FOR VERSION AS OF 1;

-- Compare with current version to confirm idempotency after a re-run
SELECT COUNT(*) AS current_row_count
FROM ecom_lakehouse_dev.orders;
```

If both counts are identical, no rows were added by the re-run — idempotency confirmed from Athena without needing PySpark.

### Read Table State at a Specific Timestamp

```sql
-- What did the products table look like before the May batch updated department names?
SELECT *
FROM ecom_lakehouse_dev.products FOR TIMESTAMP AS OF TIMESTAMP '2025-04-30 23:59:59'
WHERE department = 'frozen'
ORDER BY product_id;
```

`FOR TIMESTAMP AS OF` reads the Delta snapshot that was current at the given UTC timestamp. Useful for auditing what a report would have shown on a specific date, or for comparing pre- and post-update states after a data correction.

### Detect What Changed Between Two Versions

```sql
-- Rows in version 2 that were not in version 1 (newly inserted)
SELECT 'inserted' AS change_type, order_id, total_amount, date
FROM ecom_lakehouse_dev.orders FOR VERSION AS OF 2

EXCEPT

SELECT 'inserted', order_id, total_amount, date
FROM ecom_lakehouse_dev.orders FOR VERSION AS OF 1

UNION ALL

-- Rows in version 1 that are not in version 2 (updated or deleted)
SELECT 'removed' AS change_type, order_id, total_amount, date
FROM ecom_lakehouse_dev.orders FOR VERSION AS OF 1

EXCEPT

SELECT 'removed', order_id, total_amount, date
FROM ecom_lakehouse_dev.orders FOR VERSION AS OF 2;
```

This pattern reveals the exact diff between two Delta versions using standard SQL set operations. For the idempotent re-run case, both `EXCEPT` results return zero rows.

---

## Query Performance Tips

### Always Filter on the Partition Column

```sql
-- SLOW: scans all date partitions (full table scan)
SELECT * FROM ecom_lakehouse_dev.orders WHERE order_id = 'abc-123';

-- FAST: prunes to one date partition, then filters within it
SELECT * FROM ecom_lakehouse_dev.orders
WHERE date = DATE '2025-04-15'
  AND order_id = 'abc-123';
```

Athena cannot prune by `order_id` (not a partition column). Adding the known `date` value reduces the scan from all partitions to one.

### Use `EXPLAIN` to Verify Partition Pruning

```sql
EXPLAIN
SELECT COUNT(*) FROM ecom_lakehouse_dev.orders
WHERE date = DATE '2025-04-15';
```

The output includes `TableScan ... partitions=1` when Athena correctly identifies and prunes to a single partition. `partitions=N` (where N is the total partition count) indicates no pruning — the predicate is not on the partition column or uses a non-sargable expression.

### Avoid Functions on Partition Columns

```sql
-- BREAKS partition pruning: CAST prevents pushdown
WHERE CAST(date AS VARCHAR) = '2025-04-15'

-- CORRECT: compare DATE literal to DATE column
WHERE date = DATE '2025-04-15'
```

Wrapping a partition column in a function (CAST, DATE_FORMAT, YEAR, etc.) prevents Athena from using the partition metadata for pruning — it must scan all partitions and apply the function row-by-row.
