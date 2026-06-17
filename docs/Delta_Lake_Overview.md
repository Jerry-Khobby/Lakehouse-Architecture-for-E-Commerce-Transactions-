# Delta Lake — What It Is, Why It Was Chosen, and ACID on S3

## Overview

Delta Lake is an open-source storage layer that sits on top of object storage (S3, Azure Blob, GCS) and adds database-level reliability to plain file storage. It does this by maintaining a transaction log alongside the data files. This document explains what Delta Lake physically is, what it adds over plain Parquet, how ACID guarantees are implemented on S3, and why it was the correct choice for this e-commerce pipeline.

---

## What Delta Lake Is Physically

A Delta table is not a single file or a special format. It is a directory on S3 containing two things:

```
s3://ecom-lakehouse-dev-data-<account>/lakehouse-dwh/orders/
├── _delta_log/
│   ├── 00000000000000000000.json   ← version 0: empty table seed
│   ├── 00000000000000000001.json   ← version 1: April batch MERGE
│   └── 00000000000000000002.json   ← version 2: May batch MERGE
│
├── date=2025-04-01/
│   └── part-00000-abc123.snappy.parquet
├── date=2025-04-02/
│   └── part-00000-def456.snappy.parquet
└── ... (one directory per day)
```

**The Parquet files** are ordinary columnar data files — identical to what plain Parquet writes. Any tool that can read Parquet can read the individual files. There is nothing proprietary in the data format.

**The `_delta_log/` directory** is what makes it a Delta table. Each JSON file in the log is one committed transaction. It records:

```json
{
  "add": [
    {
      "path": "date=2025-04-01/part-00000-abc123.snappy.parquet",
      "size": 45678,
      "partitionValues": {"date": "2025-04-01"},
      "dataChange": true,
      "stats": "{\"numRecords\":28,\"minValues\":{...},\"maxValues\":{...}}"
    }
  ],
  "remove": [],
  "commitInfo": {
    "operation": "MERGE",
    "operationMetrics": {
      "numTargetRowsInserted": "850",
      "numTargetRowsUpdated": "0",
      "numTargetRowsCopied": "0"
    }
  }
}
```

When Athena or a Glue job reads the table, it does not scan every Parquet file in the directory. It reads the Delta log in version order, building the current snapshot — the exact set of Parquet files that belong to the latest committed state. Files that were marked as `remove`d by a previous MERGE are physically still on S3 but are invisible to readers because the log excludes them from the current snapshot.

---

## What Plain Parquet Cannot Do

Before Delta Lake, the common pattern for S3-based analytics was to write Parquet files directly to an S3 prefix and query them with Athena or a Glue crawler. This works for append-only, immutable datasets. For a pipeline that needs to upsert, correct, or update records, plain Parquet has fundamental limitations:

### No Upsert

Plain Parquet has no MERGE operation. To update an existing record in a Parquet dataset, you must:
1. Read the entire partition into memory
2. Apply the update in Spark
3. Overwrite the entire partition file

This is a full partition rewrite for every update, regardless of how many rows changed. For the `orders` table with 30 daily partitions and 850 rows per batch, updating one row requires rewriting the entire `date=2025-04-15/` partition.

Delta's MERGE reads only the affected partitions, writes new Parquet files for only the rows that changed, and marks the old files as removed in the log. Unchanged rows in unchanged partitions are not touched.

### No ACID Guarantees

If a Glue job writing Parquet fails midway through a partition overwrite, S3 contains a mix of old and new data with no way to distinguish them. A concurrent reader sees a corrupted partial state. There is no rollback.

Delta commits atomically. A job either commits the entire transaction to the Delta log, or it does not. If the job crashes after writing some Parquet files but before writing the log entry, those files exist on S3 but are not in the log — readers never see them, and the table state is unchanged from the previous version. The next successful run writes new files and a valid log entry.

### No Time Travel

Plain Parquet has no version history. Once a partition is overwritten, the previous state is gone.

Delta's log records every version. `delta_table.history()` returns the full audit trail. `spark.read.format("delta").option("versionAsOf", 1).load(path)` reads the exact state of the table after the first commit. This is used in this pipeline for idempotency verification — reading version N before and version N+1 after a re-run and confirming row counts are identical proves no duplicates were inserted.

### No Schema Enforcement

Writing to a plain Parquet prefix with `mode("append")` does not check whether the new file's schema matches the existing files. A file with a wrong column type or a missing column appends silently, producing a mixed-schema dataset that Athena queries fail on.

Delta enforces schema on every write. If the incoming DataFrame schema does not match the registered Delta schema, the write raises `AnalysisException: A schema mismatch detected` before any data is written.

---

## ACID Guarantees on S3

ACID stands for Atomicity, Consistency, Isolation, and Durability. Delta Lake delivers all four on S3 without requiring a running database server.

### Atomicity

A MERGE either fully commits or produces no effect. The commit mechanism:

1. The Glue job writes new Parquet data files to S3 (these files exist but are not yet referenced by the log).
2. The job reads the current latest Delta log version (e.g. `00000000000000000001.json`).
3. The job attempts to write the next version (`00000000000000000002.json`) with `add` entries pointing to the new files.
4. If the write succeeds, the transaction is committed. Readers picking up the new log version see the new files.
5. If the job crashes between steps 1 and 3, the orphaned Parquet files from step 1 are never referenced by any log version — they are invisible to all readers and will be cleaned up by Delta vacuum. The table state remains at version 1.

### Consistency

The Delta schema is stored in the log. Every write is checked against it. No write can produce a state that violates the schema or partition definition recorded in the log.

### Isolation

Delta uses **optimistic concurrency control**. Multiple readers can read any version of the table simultaneously. Writers attempt to commit a new version and detect conflicts only if another writer has committed the same version in the interim. In this pipeline, `max_concurrent_runs = 1` on each Glue job means only one writer can be running per table at a time — conflicts are an operational safeguard rather than a runtime expectation.

### Durability

Once a log entry is written to S3 and S3 acknowledges the write (strong read-after-write consistency, available on all S3 regions since December 2020), the transaction is durable. S3's 11-nines durability guarantee applies to both the Parquet data files and the Delta log JSON files.

---

## `S3SingleDriverLogStore` — Required for S3

```hcl
"--conf" = "... --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore ..."
```

Delta Lake's default log store is designed for HDFS, which provides atomic rename operations. S3 does not support atomic rename — `CopyObject` followed by `DeleteObject` is not atomic, and two concurrent writers can both successfully write the same log version number, producing a split-brain log.

`S3SingleDriverLogStore` is Delta's S3-specific log store implementation. It uses a different commit protocol suited to S3's eventual consistency model (pre-2020) and its strong consistency model (post-2020). It serialises commits through a single driver and uses a list-before-write check to detect concurrent writes. Without this configuration, concurrent Delta writes on S3 can produce log corruption — two writers both commit `00000000000000000002.json` with different content, and subsequent readers see an inconsistent table.

This class is set as a `--conf` argument in `glue_jobs.tf` and is loaded by the Spark session before any Delta operation executes. `build_spark_session()` in `common.py` verifies the Delta extension is loaded on startup and raises `RuntimeError` immediately if it is not.

---

## Why Delta Lake Over Alternatives

| Option | Why not chosen |
|---|---|
| Plain Parquet | No upsert, no ACID, no time travel, no schema enforcement |
| Apache Hudi | Heavier operational footprint, less native Athena engine 3 support at the time |
| Apache Iceberg | Excellent choice, but Delta Lake has tighter Glue 4.0 integration and first-class DeltaCatalog connector |
| Redshift/DynamoDB | Vendor lock-in, always-on cost, not open format, defeats the Lakehouse model |

Delta Lake with Athena engine version 3 is the path of least friction for a Glue + Athena Lakehouse on AWS. The DeltaCatalog Spark connector, the `S3SingleDriverLogStore`, and native Delta support in Athena engine 3 are all production-ready as of 2024. The same open Parquet files remain queryable by any other tool that supports Delta — DuckDB, Trino, Databricks — with no migration cost.
