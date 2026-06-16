# Schema Design and Decisions

This document explains every schema decision in this project: why each table is classified the way it is, why each field has the type it has, why temporal columns are read as strings before being cast, and why the partition and merge key choices were made. Each decision is traced to a concrete consequence in the code or the query engine.

---

## The Fundamental Division: Dimension vs Fact

Before explaining individual tables, the distinction that drives the entire schema design needs to be established.

**A dimension table** is a reference dataset that describes the entities in your business domain. It is relatively small, changes infrequently, and is shared across many fact records. Every analytical query that involves a dimension table uses it to look up attributes (names, categories, labels) to attach to measured events.

**A fact table** is a transactional dataset that records what happened, when it happened, and what it involved. It is large, grows with every period, and references dimension tables through foreign keys to provide context for the measurements.

This distinction is not a convention imposed from outside — it emerges from the data itself. Products describe what is sold. Orders and order_items record what was sold, when, and to whom. The separation determines merge semantics, partition strategies, and whether a timestamp guard is needed.

---

## Table 1 — `products` (Dimension)

### Schema

```python
PRODUCTS_SCHEMA = StructType([
    StructField("product_id",    IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=False),
    StructField("department",    StringType(),  nullable=False),
    StructField("product_name",  StringType(),  nullable=False),
])
```

### Why Products Is a Dimension Table

Products do not record events — they describe things that exist in the catalogue. A product does not "happen" at a point in time. It exists, it may change its name or department assignment, and it may be retired. There is no concept of "the 14:32 version of a product" — only "the current version."

Concretely:
- 1,000 rows, fixed size. Unlike orders (which grow every month), the product catalogue does not grow with transaction volume.
- No timestamp column. There is no `created_at` or `updated_at` on products. When a product row is re-ingested with a changed name, the new name should simply win — there is no need to check which row is "newer."
- Re-used across batches. Every monthly order_items batch references the same product catalogue. Products is the stable reference that orders and order_items point into.

### Why `product_id` and `department_id` Are `IntegerType` Not `LongType`

`LongType` is a 64-bit signed integer (range: –2^63 to 2^63–1). `IntegerType` is 32-bit (range: –2^31 to ~2.1 billion). Product IDs in this system range from 1 to 1,000. Department IDs range from 1 to 10. Using `LongType` for these values would consume twice the storage per row per file without any gain.

For orders, `order_num` is `LongType` because it is a sequential counter that could eventually exceed 2 billion over a long-running platform. For products, `IntegerType` is the correct choice and will remain correct unless the catalogue grows to 2 billion items, which is not a realistic concern.

### Why `department` and `product_name` Are `StringType` Not `VarcharType`

Spark's `StringType` maps to UTF-8 unbounded text. There is no schema-level length constraint. Length validation happens in the validation stage code (`F.trim(F.col("product_name")) != ""`), not in the type system. This is deliberate: a type system constraint that rejects a row silently is harder to audit than an explicit validation check that writes the row to `rejected/` with a named reason. Constraints belong in validation logic where they can be observed, not in schema declarations where they silently fail.

### Why `nullable=False` on All Columns

Every column is declared `nullable=False`. This does not prevent null values from appearing in the source CSV — CSV files have no schema enforcement. What it does is signal intent to Spark's optimizer and to anyone reading the schema: no column in this table is ever expected to contain a null in the processed zone. The validation stage enforces this explicitly: any null in any column sends the row to `rejected/` before the MERGE runs. The `nullable=False` declaration and the validation code agree.

### Why Partitioned by `department`

The product catalogue has exactly 10 departments (produce, dairy eggs, snacks, beverages, frozen, bakery, meat seafood, pantry, deli, personal care). Low-cardinality partitioning on a small, stable reference table. When an analyst or Glue job queries products with a department filter, Athena skips 9 out of 10 partition directories entirely:

```sql
SELECT * FROM products WHERE department = 'produce'
-- Athena reads only: lakehouse-dwh/products/department=produce/
-- Skips: 9 other department directories
```

Without this partition, Athena reads all 1,000 product rows to answer a department-filtered query. With it, Athena reads ~100 rows.

### Merge Key and Merge Semantics

**Merge key:** `product_id`

The MERGE condition is:
```python
.whenMatchedUpdateAll(condition=(
    "source.department_id <> target.department_id OR "
    "source.department <> target.department OR "
    "source.product_name <> target.product_name"
))
.whenNotMatchedInsertAll()
```

This change-detection condition means: only write an update to the Delta log if something actually changed. If the same products file is ingested twice (e.g. the pipeline is re-run after a failure), every row hits the MATCHED branch, the condition is false for every row (nothing changed), and the MERGE produces zero Delta log entries. This is a true no-op — no storage is written, no new snapshot version is produced.

Without this condition, `.whenMatchedUpdateAll()` would write an update for every matched row even if the data is identical, producing a new Delta snapshot on every re-run and growing the `_delta_log/` unnecessarily.

There is **no timestamp guard** on the products MERGE. Dimension tables do not have a timestamp ordering. The last batch load wins. If a product's name changed between April and May, the May load correctly overwrites the April value.

---

## Table 2 — `orders` (Fact)

### Schema

```python
ORDERS_SCHEMA = StructType([
    StructField("order_num",       LongType(),        nullable=True),
    StructField("order_id",        StringType(),      nullable=False),
    StructField("user_id",         StringType(),      nullable=False),
    StructField("order_timestamp", TimestampType(),   nullable=False),
    StructField("total_amount",    DecimalType(12,2), nullable=False),
    StructField("date",            DateType(),        nullable=False),
])
```

### Why `order_num` Is `LongType` and `nullable=True`

`order_num` is a sequential counter within a batch file (1, 2, 3, … 800 for 800 orders). It is `LongType` rather than `IntegerType` because row counters accumulate across months. A platform with hundreds of thousands of monthly orders would exhaust `IntegerType` (max ~2.1 billion) in measurable time. `LongType` handles quadrillions of rows.

`nullable=True` is the one exception to the general pattern. `order_num` is a row-number label from the source file, not a business key. It has no semantic meaning in the Delta table and is not used in any join or filter. If a source file does not include it, the row is still valid. Allowing null here avoids rejecting otherwise-valid orders because a counter column is missing.

### Why `order_id` Is `StringType`

Order IDs use a prefixed string format (`ord_3a1f9c`, `ord_00042`, etc.). These are not integers and cannot be stored as integers without losing the prefix. Even if the current format happened to be purely numeric, using `StringType` for a business key is the safer choice: if the source system ever changes its ID format (adds a prefix, switches to UUIDs), the schema does not break.

### Why `total_amount` Is `DecimalType(12,2)` Not `DoubleType` or `FloatType`

Monetary values must not use floating-point types. IEEE 754 double-precision cannot exactly represent most decimal fractions. `99.99 + 0.01` in double arithmetic can produce `100.00000000000001`. When these values are summed across thousands of orders and compared against expected totals, floating-point rounding errors accumulate and produce incorrect results.

`DecimalType(12,2)` stores values as exact scaled integers:
- Precision 12: up to 12 significant digits total.
- Scale 2: exactly 2 digits after the decimal point.
- Maximum value: 9,999,999,999.99 — sufficient for any realistic e-commerce order.
- Exact arithmetic: `99.99 + 0.01 = 100.00` without rounding error.

The trade-off is that `DecimalType` is slightly slower than `DoubleType` for arithmetic because it cannot use hardware floating-point. For this workload (monthly batch, not real-time computation), this is irrelevant.

The cast from string to `DecimalType` happens in validation with explicit null checking:
```python
valid_df = valid_df.withColumn("_amount_cast", F.col("total_amount").cast(DecimalType(12,2)))
bad_amount = valid_df.filter(F.col("_amount_cast").isNull())
# bad_amount rows → rejected as "invalid_total_amount_format"
```
A cast failure (e.g. `"$99.99"` with a dollar sign, or `"N/A"`) produces null, which is caught and rejected. The row is not silently converted to zero or dropped without audit.

### Why `order_timestamp` Is `StringType` in the Read Schema but `TimestampType` in the Storage Schema

The Read schema for orders declares `order_timestamp` as `StringType`:
```python
READ_SCHEMA = StructType([
    StructField("order_timestamp", StringType(), nullable=True),
    ...
])
```

The Storage schema declares it as `TimestampType`:
```python
ORDERS_SCHEMA = StructType([
    StructField("order_timestamp", TimestampType(), nullable=False),
    ...
])
```

The two-step approach exists for a specific reason: controlled error handling with an audit trail.

If Spark reads `order_timestamp` directly as `TimestampType` with `mode=FAILFAST`, any row with a malformed timestamp raises immediately and aborts the entire job — you lose context about which row failed. If read with `mode=PERMISSIVE`, malformed timestamps become null silently — you lose the row with no audit trail.

By reading as string first, then casting explicitly:
```python
valid_df = valid_df.withColumn(
    "_ts_cast",
    F.to_timestamp(F.col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)
bad_ts = valid_df.filter(F.col("_ts_cast").isNull())
write_rejected(bad_ts, args, job_run_id, "invalid_timestamp_format")
```
The bad row is captured with its original string value, written to `rejected/` with `rejection_reason = "invalid_timestamp_format"`, and the rest of the batch continues processing. This is the required behaviour for a production pipeline: failures are isolated, logged, and queryable.

### Why the Timestamp Format Is `yyyy-MM-dd'T'HH:mm:ss`

ISO 8601 format with a literal `T` separator. The `'T'` in the Spark format string means literal character `T`, not a format code. This matches the output of the data generator after the `TIMESTAMP_FMT` fix in `scripts/constants.py` (`"%Y-%m-%dT%H:%M:%S"`) and is the standard interchange format for datetime values in JSON and CSV systems.

The `date` column is also read as `StringType` then cast to `DateType` with `yyyy-MM-dd` format, for the same controlled-error-handling reason.

### Why There Are Both `order_timestamp` and `date` Columns

`order_timestamp` is the precise moment of the order (e.g. `2025-05-15T14:23:11`). `date` is the calendar date of the order (`2025-05-15`). Two reasons both exist:

**1. Partition performance.** The table is partitioned by `date`. Athena partition pruning works on the partition column directly. If the table stored only `order_timestamp` and derived the date at query time with `DATE(order_timestamp)`, Athena would need to scan all partitions and apply the function to every row. With a dedicated `date` column matching the partition key, `WHERE date = '2025-05-15'` prunes to exactly one partition directory without scanning anything else.

**2. Consistency validation.** The `date` column is validated against `order_timestamp` in the pipeline:
```python
date_derived = F.to_date(F.col("order_timestamp"))
date_mismatch = valid_df.filter(F.col("_date_cast") != date_derived)
write_rejected(date_mismatch, args, job_run_id, "date_timestamp_mismatch")
```
If the source system sends a row where `date = 2025-05-16` but `order_timestamp = 2025-05-15T14:23:11`, the row is rejected. This catches data quality issues in the source — a sign of either a timezone bug or a data entry error.

### Merge Key and Merge Semantics

**Merge key:** `order_id`

The merge uses a timestamp guard:
```python
.whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
.whenNotMatchedInsertAll()
```

The guard means:
- A new order (not in the Delta table) → INSERT.
- An existing order re-delivered with a newer timestamp → UPDATE (correction to a prior record).
- An existing order re-delivered with an older timestamp → no-op (stale re-delivery, do not overwrite).

This is the idempotency guarantee for facts. Re-running the May batch after the June batch does not corrupt June's data.

### Why Partitioned by `date`

Orders are a time-series. Analytical queries almost always filter by date range. Partitioning by `date` means:
- `WHERE date = '2025-05-15'` reads one partition directory, skipping all others.
- `WHERE date BETWEEN '2025-05-01' AND '2025-05-31'` reads 31 partition directories, skipping April and prior months.
- The partition column `date` is a `DateType` in the storage schema, which Athena understands natively for range comparisons without function calls.

---

## Table 3 — `order_items` (Fact, Composite Key)

### Schema

```python
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
```

### Why a Composite Primary Key (`id`, `order_id`)

The `id` field is a sequential row counter within a batch file. For the April batch it runs from 1 to ~2,500. For the May batch it also starts from 1 and runs to ~2,500. If `id` alone were the merge key, May's row `id=1` would overwrite April's row `id=1` — which belongs to a completely different order and product.

The correct business key is the combination of `id` (the row within a batch) and `order_id` (which batch/order the row belongs to). Together they uniquely identify an item across all batches:

```python
delta_table.alias("target").merge(
    valid_df.alias("source"),
    "target.id = source.id AND target.order_id = source.order_id"
)
```

An alternative design would be a globally unique `item_id` generated by the source system, but this project works with the source data as given. The composite key is the correct interpretation of the source schema.

### Why `reordered` Is `IntegerType` Not `BooleanType`

The `reordered` flag represents whether the customer has ordered this product before (1 = yes, 0 = no). It is semantically boolean but represented as an integer in the source data. Using `IntegerType` preserves the source representation exactly and avoids a type conversion that could fail silently if a source row sends `2` or `true` (string) instead of `1`.

The validation stage enforces the binary constraint explicitly:
```python
invalid_reorder = valid_df.filter(~F.col("reordered").isin(0, 1))
write_rejected(invalid_reorder, args, job_run_id, "invalid_reordered_flag")
```
A value of `5` (seeded as a dirty row in the test data) is caught here with a named reason rather than being silently accepted into the Delta table as an unexplained integer.

### Why `days_since_prior_order` Is `nullable=True`

This field records how many days elapsed since the customer's previous order. For a customer placing their very first order, this value is semantically undefined — there is no prior order. The source system correctly omits it (empty string in CSV, which the pipeline treats as null).

`nullable=True` reflects the domain: null here means "this is the customer's first order," not "this is a data quality problem." The validation stage checks only that non-null values are within range (0–365):
```python
invalid_days = valid_df.filter(
    F.col("days_since_prior_order").isNotNull() &
    ((F.col("days_since_prior_order") < 0) | (F.col("days_since_prior_order") > 365))
)
```
A null value passes this check correctly.

### Why `order_timestamp` and `date` Are Copied from the Parent Order

Each order_item carries `order_timestamp` and `date` that duplicate the parent order's values. Every item in an order has the same timestamp and date as the order itself. This redundancy is intentional for two reasons:

**1. Self-contained Delta table.** Athena can query order_items by date directly without joining to orders:
```sql
SELECT product_id, COUNT(*) AS items_sold
FROM order_items
WHERE date BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY product_id
```
Without the copied `date` column, this query would require a join to orders, doubling the data scanned.

**2. Delta MERGE timestamp guard.** The MERGE for order_items uses the same timestamp guard as orders:
```python
.whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
```
Without a timestamp column on the order_items row itself, there would be no basis for this guard. Joining back to orders to get the timestamp during the MERGE would be complex and expensive.

### Merge Key and Semantics for Order Items

```python
.merge(valid_df.alias("source"), "target.id = source.id AND target.order_id = source.order_id")
.whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
.whenNotMatchedInsertAll()
```

The timestamp guard here means: if an order_item with the same composite key is re-ingested (from a corrected source file), the newer version wins. If the same file is re-delivered unchanged, every matched row's timestamp is equal to the target's timestamp — the condition `source.order_timestamp > target.order_timestamp` is false for all rows — and the MERGE is a true no-op.

---

## Cross-Schema Design Decisions

### Why Explicit `StructType` Schemas Instead of `inferSchema`

Every Glue job declares a complete `StructType` before reading the CSV. Spark's `inferSchema=True` is not used anywhere. Three reasons:

**1. Cost.** `inferSchema` reads the entire file (or a configurable sample fraction) to detect types. For a CSV in S3, this is additional S3 API calls and data transfer before processing even begins. An explicit schema costs zero for type detection.

**2. Correctness.** `inferSchema` infers from a sample. If the sample for a numeric column happens to contain only integers but the full file contains one decimal value, Spark infers `LongType` and the decimal row either fails or loses its fractional part silently. An explicit schema applies to every row.

**3. Stability.** `inferSchema` is non-deterministic across Glue worker configurations and PySpark versions. The same file can produce different inferred types depending on partition assignment and sampling random seed. An explicit schema is always the schema you declared.

### Why `mode=FAILFAST` at Read Time

`FAILFAST` raises an `AnalysisException` immediately if any row cannot be coerced to the declared schema. This catches:
- Files with the wrong number of columns (truncated, missing delimiter).
- Files with a header row that does not match the expected column names.
- Encoding corruption that produces binary garbage in a field.

The alternative, `PERMISSIVE` mode, would produce nulls for uncastable fields without raising. Those nulls would then need to be caught in the validation stage, but the original string value would already be lost — the null carries no information about what was in the field. `FAILFAST` surfaces the problem immediately, at the right abstraction layer (the read boundary), where it can be handled by the job's exception handler and reported to CloudWatch and SNS.

### Uniform Temporal Handling Across Tables

All three tables handle temporal columns identically:
1. Read as `StringType`.
2. Cast explicitly with `F.to_timestamp(col, format)` or `F.to_date(col, format)`.
3. Check for null cast result → write to `rejected/` with named reason.
4. Rename the cast column to replace the original string column.
5. Store as `TimestampType` / `DateType` in the Delta table.

This consistency means the same `write_rejected()` utility works identically across all three jobs, the same rejection reasons appear in the `rejected/` zone, and the same Athena queries can audit timestamp failures across any dataset.

### Why the Soft Flag on `total_amount`

Orders with `total_amount > 1,000,000` are written to `flagged/orders/` but are NOT rejected — they remain in the valid batch and are committed to the Delta table. A $1,200,000 order is technically valid data. It may represent a bulk corporate purchase, a data entry error (extra zero), or a genuine large transaction. The pipeline cannot determine which.

The soft flag is the correct design: keep the data in the analytical store (it may be correct and important), but signal to the analyst team that this row warrants review. Rejecting it would hide potentially real business activity. Accepting it silently would miss potentially erroneous data. The flagged zone is the third option.

This pattern — hard reject for clear violations, soft flag for anomalies — is reusable for other future rules (unusual user IDs, orders from future dates that are just slightly over the 1-hour tolerance, etc.).
