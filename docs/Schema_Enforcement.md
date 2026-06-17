# Schema Enforcement — FAILFAST Mode, Declared StructTypes, and Why inferSchema Is Banned

## Overview

Every Glue job reads its source CSV with a declared `StructType` schema passed to the Spark `DataFrameReader` and `FAILFAST` parse mode enabled. `inferSchema` is never used anywhere in this pipeline. This document explains what `FAILFAST` mode does versus the alternatives, how the two-schema design for orders (`READ_SCHEMA` vs `ORDERS_SCHEMA`) handles controlled casting, why schema inference is a correctness hazard, and how Delta Lake adds a second layer of schema enforcement at write time.

---

## `FAILFAST` Parse Mode

```python
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(READ_SCHEMA)
    .load(source_path)
)
```

Spark's CSV reader has three parse modes for handling malformed records:

### `PERMISSIVE` (Spark default)

When a value cannot be cast to the declared column type, Spark replaces the value with `null` and continues reading. A CSV row like `"abc123,not-a-number,produce,Widget"` where column 2 is declared `IntegerType` would silently produce a row with `department_id = null`.

`PERMISSIVE` is the Spark default because it never fails — it will read any CSV file. This makes it incorrect for a validation pipeline: the null-replacement is silent, the row appears to have been read successfully, and the null then triggers the null-check validation rule with `"null_required_field"` as the rejection reason. The original value (`"not-a-number"`) is lost — the rejection record shows a null, not the actual corrupt value. Diagnosing the source defect becomes harder.

More critically, `PERMISSIVE` does not distinguish between "this value was null in the source" and "this value could not be parsed so Spark nulled it." Both arrive in the DataFrame as null — there is no way to tell them apart without the original file.

### `DROPMALFORMED`

Silently drops any row where a value cannot be cast. The row disappears from the DataFrame with no rejection record, no counter, and no indication that it was ever read. A CSV with 1,000 rows where 50 have type mismatches produces a DataFrame with 950 rows. The pipeline commits 950 rows to the Delta table, logs `total_read = 950`, and reports success. The 50 lost rows are permanently gone with no audit trail.

`DROPMALFORMED` is the most dangerous option for a pipeline with correctness requirements. It destroys data silently.

### `FAILFAST`

Raises `SparkException: Malformed CSV record` and immediately aborts the Spark job when a value cannot be cast to the declared type. The CSV read fails before any rows are returned.

```
SparkException: Malformed CSV record: abc123,not-a-number,produce,Widget
```

`FAILFAST` is the correct mode because:

1. **It surfaces format problems immediately.** If the source system changes its CSV format — a column type changes, a new column is inserted in the wrong position, a decimal separator changes from `.` to `,` — the job fails loudly at the read stage. The CloudWatch log, SNS alert, and Step Functions failure notification all fire. The problem is visible.

2. **It prevents null contamination.** No silently-nulled values reach the validation layer. Every null in the validation DataFrame is a genuine null from the source, not a Spark parse failure disguised as a null.

3. **It preserves correctness.** A partial read (some rows parsed, some failed) is worse than a total failure. `FAILFAST` ensures the pipeline either reads all rows correctly or reads nothing.

The trade-off is that a single malformed row in a 1,000-row CSV fails the entire job. This is acceptable because:
- The source files are produced by a structured source system, not by ad-hoc human data entry
- A type mismatch in one row typically indicates a systemic format change, not an isolated anomaly
- The failed job produces an SNS alert, which prompts investigation of the source format before the pipeline is re-run

---

## Declared `StructType` Schemas

Each Glue job declares its schema as a `StructType` constant. The schema is never inferred at runtime.

### Products Schema

```python
PRODUCTS_SCHEMA = StructType([
    StructField("product_id",   IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=False),
    StructField("department",   StringType(),  nullable=False),
    StructField("product_name", StringType(),  nullable=False),
])
```

All four columns are `nullable=False`. This is the strictest possible schema declaration. The `nullable=False` flag propagates into the Delta table schema when `ensure_delta_table()` writes the empty seed DataFrame — the Delta log records these nullability constraints, and subsequent writes that violate them raise `AnalysisException`.

### The Two-Schema Design for Orders

Orders uses two schemas — `READ_SCHEMA` and `ORDERS_SCHEMA` — to handle controlled timestamp casting:

```python
READ_SCHEMA = StructType([
    StructField("order_num",       LongType(),    nullable=True),
    StructField("order_id",        StringType(),  nullable=False),
    StructField("user_id",         StringType(),  nullable=False),
    StructField("order_timestamp", StringType(),  nullable=True),   # ← StringType for controlled cast
    StructField("total_amount",    DecimalType(12, 2), nullable=False),
    StructField("date",            StringType(),  nullable=True),   # ← StringType for controlled cast
])

ORDERS_SCHEMA = StructType([
    StructField("order_num",       LongType(),    nullable=True),
    StructField("order_id",        StringType(),  nullable=False),
    StructField("user_id",         StringType(),  nullable=False),
    StructField("order_timestamp", TimestampType(), nullable=False), # ← TimestampType after cast
    StructField("total_amount",    DecimalType(12, 2), nullable=False),
    StructField("date",            DateType(),    nullable=False),   # ← DateType after cast
])
```

**Why `order_timestamp` is read as `StringType`:**

If `order_timestamp` is declared as `TimestampType` in the read schema with `FAILFAST` mode, Spark's default timestamp parser is used for casting. Spark's CSV timestamp parser may or may not handle the `yyyy-MM-dd'T'HH:mm:ss` format correctly depending on the PySpark version and locale settings — the behaviour can vary. More importantly, if the cast fails with `FAILFAST`, the entire job aborts and no rejection record is written. The invalid row is lost.

By reading `order_timestamp` as `StringType`, the CSV read always succeeds (any string is a valid string). The cast from string to timestamp then happens explicitly in the validation layer:

```python
df = df.withColumn(
    "order_timestamp",
    F.to_timestamp(F.col("order_timestamp"), TIMESTAMP_FORMAT)
)
```

`F.to_timestamp()` with an explicit format string returns `null` (not an exception) when parsing fails. The validation layer then identifies null `order_timestamp` values that were non-null in the original DataFrame — those are `"unparseable_timestamp"` rejections with the original string value preserved in the rejection record.

This two-step approach (read as string, cast explicitly, detect cast failures as nulls) produces rejection records that contain the actual bad timestamp string, making source diagnosis possible.

### Order Items Schema

```python
ORDER_ITEMS_SCHEMA = StructType([
    StructField("id",                      IntegerType(), nullable=False),
    StructField("order_id",                StringType(),  nullable=False),
    StructField("user_id",                 StringType(),  nullable=False),
    StructField("product_id",              IntegerType(), nullable=False),
    StructField("add_to_cart_order",       IntegerType(), nullable=False),
    StructField("reordered",               IntegerType(), nullable=False),
    StructField("days_since_prior_order",  IntegerType(), nullable=True),  # ← nullable: null = first order
    StructField("order_timestamp",         StringType(),  nullable=True),   # ← StringType for controlled cast
    StructField("date",                    StringType(),  nullable=True),   # ← StringType for controlled cast
])
```

`days_since_prior_order` is the only intentionally nullable column. A null value for this field means the order is the user's first order — there is no prior order to count days from. This is a semantically meaningful null, not a data quality problem. Declaring it `nullable=True` ensures the null validation check does not incorrectly reject valid first-order rows.

---

## Why `inferSchema` Is Never Used

Spark's `inferSchema=True` (or `.option("inferSchema", "true")`) reads the CSV twice: once to sample rows and infer column types, then again to read the full dataset with the inferred schema. It is tempting because it requires no upfront schema declaration. It is wrong for production pipelines for four reasons.

### Reason 1 — Type Inference Is Heuristic, Not Guaranteed

Spark infers types from sampled rows. If `product_id` happens to be `1`, `2`, `5` in the first 100 rows, Spark infers `IntegerType`. If the first 100 rows had `001`, `002`, `005` (zero-padded strings), Spark infers `StringType`. The inferred type is a guess based on the sample — it can be wrong for rows not in the sample.

For the same CSV file, a different Spark version, a different sample size, or a different random sample can produce a different schema. Schema inference is non-deterministic across environments.

### Reason 2 — Schema Drift Is Silent

If the source system changes a column type (e.g. `total_amount` changes from no-decimal integers to decimal values), `inferSchema` adapts silently. The pipeline continues processing with the new inferred type. No alert fires, no test breaks. The data in the Delta table silently changes type for new rows while old rows retain the old type, producing a mixed-schema Delta table that Athena queries fail on inconsistently.

With a declared schema, the same type change causes `FAILFAST` to abort the job immediately, producing a visible failure that forces investigation.

### Reason 3 — `FAILFAST` and `inferSchema` Are Incompatible

`inferSchema` requires Spark to read rows and infer types before applying the schema. `FAILFAST` mode rejects rows that do not match the schema. These two options have contradictory requirements — `FAILFAST` validates against a schema that `inferSchema` has not yet determined. In practice, combining them either does nothing (inference runs before validation) or produces unexpected behaviour. They should never be combined.

### Reason 4 — Performance

`inferSchema` reads the CSV twice. For large CSVs on S3, this doubles the network I/O and S3 API call cost of the read step. A declared schema eliminates the sampling pass entirely — Spark reads the file once with the known schema applied.

---

## Delta's Second Layer of Schema Enforcement

The Glue job schema declaration (FAILFAST + StructType) enforces schema at the CSV read layer. Delta Lake enforces schema a second time at the write layer.

When `ensure_delta_table()` initialises the Delta table, it writes an empty DataFrame with the declared schema. The Delta log records the schema in its `metaData` block. Every subsequent write (the MERGE) must match this registered schema.

If a code change accidentally altered `ORDERS_SCHEMA` (e.g. changed `total_amount` from `Decimal(12,2)` to `DoubleType`), the MERGE would raise:

```
AnalysisException: A schema mismatch detected when writing to the Delta table
 - Table schema: total_amount: decimal(12,2)
 - Data schema:  total_amount: double
```

The MERGE fails. The Delta table is unchanged. No corrupted data is written. This second layer of enforcement catches bugs introduced during code changes that the CSV-layer validation cannot detect — schema mismatches between the validation layer's declared schema and the Delta table's registered schema.

---

## Schema Enforcement Summary

| Layer | Mechanism | What It Catches |
|---|---|---|
| CSV read | `FAILFAST` mode + declared `StructType` | Type mismatches in raw CSV, malformed records |
| Explicit casting | `F.to_timestamp()` + null detection | Unparseable timestamps (preserves original string for audit) |
| Delta write | Delta schema registration | Schema drift between validation layer and Delta table |
| Delta write | `nullable=False` propagation | Attempts to write null values into non-nullable columns |
