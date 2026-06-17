# Delta Table Initialisation — The Empty DataFrame Seed

## Overview

Before any MERGE can run, a Delta table must already exist at the target path. On first pipeline run, the `lakehouse-dwh/` prefix is empty. This document covers the `ensure_delta_table()` function in `common.py`, why it seeds an empty DataFrame rather than using `DeltaTable.createIfNotExists()`, the specific `AnalysisException` that alternative approach causes in Glue 4.0, and how the initialisation is fully idempotent across all subsequent runs.

---

## The Problem — `DeltaTable.forPath()` on a Non-Existent Path

Every Glue job calls `DeltaTable.forPath(spark, table_path)` before running the MERGE:

```python
delta_table = DeltaTable.forPath(spark, table_path)

delta_table.alias("target")
    .merge(source_df.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll(...)
    .whenNotMatchedInsertAll()
    .execute()
```

`DeltaTable.forPath()` loads an existing Delta table from the specified S3 path. On first pipeline run, there is no `_delta_log/` at that path. Calling `forPath()` on an empty prefix raises:

```
AnalysisException: <path> is not a Delta table.
```

The MERGE never executes. The pipeline fails immediately after the validate stage.

---

## `ensure_delta_table()` — The Fix

```python
def ensure_delta_table(
    spark: SparkSession,
    table_path: str,
    schema: StructType,
    partition_cols: List[str],
) -> None:
    if DeltaTable.isDeltaTable(spark, table_path):
        logger.info("Delta table already exists at %s — skipping init.", table_path)
        return

    logger.info("Initialising Delta table at %s …", table_path)

    empty_df = spark.createDataFrame([], schema)

    writer = empty_df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(table_path)

    logger.info("Delta table initialised at %s", table_path)
```

**`DeltaTable.isDeltaTable(spark, table_path)`** — checks for the existence of `_delta_log/` at the given path. Returns `True` if a valid Delta log is present, `False` otherwise. This is a metadata-only check — it does not read any Parquet files.

If `False`, the function creates an empty DataFrame using the job's full schema and writes it in Delta format with `mode("overwrite")`. This produces:
- `_delta_log/00000000000000000000.json` — the initial commit, recording zero data files and the schema
- No Parquet files (zero rows were written)

After this write, `DeltaTable.forPath(spark, table_path)` succeeds. The subsequent MERGE finds an empty target table and inserts all valid source rows via `whenNotMatchedInsertAll`.

---

## Why Not `DeltaTable.createIfNotExists()`

The Delta Lake API provides a builder for conditional table creation:

```python
DeltaTable.createIfNotExists(spark) \
    .tableName("orders") \
    .addColumn("order_id", StringType()) \
    .addColumn(...) \
    .partitionedBy("date") \
    .execute()
```

This looks like the natural solution. It was not used for three specific reasons.

### Reason 1 — Glue 4.0 AnalysisException

`DeltaTable.createIfNotExists()` is a higher-level builder that internally calls `CREATE TABLE IF NOT EXISTS` through the Spark catalog. In Glue 4.0 (PySpark 3.3.2, Delta Lake 2.1.x), calling this on a path-based table while the `DeltaCatalog` connector is active produces:

```
AnalysisException: Table or view not found: orders;
'UnresolvedRelation [orders], [], false
```

The error arises because `createIfNotExists()` attempts to look up the table by name in the catalog (using the `tableName()` call), but the table is registered as a path-based external table, not a named catalog table. The catalog has not yet registered it at the point `ensure_delta_table()` is called — registration happens after the MERGE, in the `Catalog Update` stage. The builder cannot reconcile the path-based Delta log with the catalog lookup.

Using a direct `empty_df.write.format("delta").save(table_path)` bypasses the catalog entirely. It writes to a raw S3 path and creates the Delta log without touching the Glue Data Catalog. The catalog registration happens separately in `update_catalog_table()` after the MERGE commits.

### Reason 2 — Schema and Partition Registration

`createIfNotExists()` builder requires manually listing every column and its type:

```python
.addColumn("order_id", StringType(), nullable=False)
.addColumn("order_timestamp", TimestampType(), nullable=False)
...
```

The `ensure_delta_table()` approach takes the `schema` argument directly (the `StructType` already defined in the job, e.g. `ORDERS_SCHEMA`) and writes an empty DataFrame with that schema. The Delta log records the schema automatically from the DataFrame. There is no risk of the schema definition in `createIfNotExists()` drifting from `ORDERS_SCHEMA` — they are the same object.

```python
empty_df = spark.createDataFrame([], schema)   # schema = ORDERS_SCHEMA
```

The partition columns (`partition_cols = ["date"]`) are passed directly to `.partitionBy(*partition_cols)`, which registers the partition spec in the Delta log metadata. The initial Delta log entry records both the schema and the partition spec:

```json
{
  "metaData": {
    "schemaString": "{\"fields\":[{\"name\":\"order_id\",...}]}",
    "partitionColumns": ["date"]
  }
}
```

This partition registration is important: when the MERGE inserts the first real rows, Spark can immediately apply partition pruning using the spec from the log, rather than inferring partitions from file names.

### Reason 3 — Simplicity and Reliability

`empty_df.write.format("delta").mode("overwrite").partitionBy(...).save(path)` is straightforward standard Spark DataFrame API — the same API used everywhere else in the pipeline. It has no known bugs in the Glue 4.0 / Delta 2.x combination and does not depend on catalog state. `createIfNotExists()` is a higher-level abstraction that adds catalog interaction as a side effect. For a function whose only job is "ensure a Delta log exists at this path," the lower-level approach is more reliable.

---

## Why `mode("overwrite")` Is Safe Here

`mode("overwrite")` might look risky — it sounds like it could overwrite existing data. It is safe here because:

1. The `isDeltaTable()` guard runs first. If the table already exists, the function returns immediately and `mode("overwrite")` is never reached.
2. The DataFrame being written is empty — zero rows. There is no data to overwrite even if the mode were somehow reached on an existing table.

The `mode("overwrite")` is needed rather than `mode("append")` because `append` on a non-existent Delta path also raises `AnalysisException` — it expects a pre-existing schema to append to. `overwrite` creates the table if it does not exist.

---

## Idempotency Across Runs

The `isDeltaTable()` guard makes `ensure_delta_table()` a true no-op on every run after the first:

| Run | `isDeltaTable()` result | Action taken | I/O cost |
|---|---|---|---|
| First pipeline run | `False` | Writes empty DataFrame, creates `_delta_log/00000000000000000000.json` | One Delta log write (tiny) |
| Second run (May batch) | `True` | Returns immediately | Zero |
| Third run (re-run) | `True` | Returns immediately | Zero |
| Any subsequent run | `True` | Returns immediately | Zero |

The check is a single S3 `ListObjects` call to check for `_delta_log/`. It costs one API call and returns in milliseconds. There is no penalty for calling `ensure_delta_table()` on every job run — it is designed to be called unconditionally at the start of the MERGE stage.

---

## What the Initial Delta Log Entry Contains

After `ensure_delta_table()` runs on the `orders` table:

**`_delta_log/00000000000000000000.json`:**

```json
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  },
  "metaData": {
    "id": "a1b2c3d4-e5f6-...",
    "format": {"provider": "parquet", "options": {}},
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"order_num\",\"type\":\"long\",\"nullable\":true},{\"name\":\"order_id\",\"type\":\"string\",\"nullable\":false},{\"name\":\"user_id\",\"type\":\"string\",\"nullable\":false},{\"name\":\"order_timestamp\",\"type\":\"timestamp\",\"nullable\":false},{\"name\":\"total_amount\",\"type\":\"decimal(12,2)\",\"nullable\":false},{\"name\":\"date\",\"type\":\"date\",\"nullable\":false}]}",
    "partitionColumns": ["date"],
    "createdTime": 1718450000000
  }
}
```

No `add` entries (no Parquet files written). The `metaData` block records the schema and partition columns. This is the foundation that makes the subsequent MERGE work — Delta reads this metadata to know the table's schema and how to apply partition-aware writes.
