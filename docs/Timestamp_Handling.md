# Timestamp Handling — String Read, Explicit Cast, UTC Session Timezone

## Overview

Timestamps in this pipeline are treated with more care than any other column type. `order_timestamp` and `date` are read from CSV as `StringType`, cast explicitly with a declared format string, and any cast failure is surfaced as a named rejection reason rather than a silent null. The Spark session timezone is locked to UTC. A future-timestamp tolerance check rejects records with timestamps too far ahead of the job's execution time. This document explains each decision, the format string itself, and the root cause of the May 2025 all-rejection bug that motivated the current design.

---

## Why Timestamps Are Not Read as `TimestampType` Directly

A naive approach declares the timestamp column as `TimestampType` in the CSV read schema:

```python
# What was NOT done:
READ_SCHEMA = StructType([
    ...
    StructField("order_timestamp", TimestampType(), nullable=False),
    ...
])
df = spark.read.format("csv").option("mode", "FAILFAST").schema(READ_SCHEMA).load(path)
```

This has two problems.

### Problem 1 — FAILFAST Aborts the Entire Job on One Bad Row

With `FAILFAST` mode and `TimestampType` in the schema, Spark attempts to cast each `order_timestamp` cell to a timestamp as it reads the CSV. If any cell cannot be parsed, `FAILFAST` raises `SparkException: Malformed CSV record` and aborts the entire read. The job fails before a single row has been returned.

The consequence is that a single malformed timestamp in a 10,000-row CSV file causes the entire batch to be rejected. No rows are committed. No rejected record is written — there is nothing to write because the read never completed. The only information available is the exception message, which typically contains the raw row string but not the specific column value that failed.

With `StringType` in the read schema, the CSV read always succeeds: any string is a valid string. The malformed timestamp reaches the validation layer as a string, where the explicit cast identifies it, and the rejection record preserves the original bad value (`"2025-04-15 08:30:00"` with a space) alongside the rejection reason (`"unparseable_timestamp"`). The remaining 9,999 valid rows are committed normally.

### Problem 2 — Spark's Default Timestamp Parser Is Version-Dependent

Before PySpark 3.0, `spark.read.csv` with `TimestampType` used Joda-Time's timestamp parsing. In PySpark 3.0+, it uses Java's `DateTimeFormatter`. The two parsers differ in which formats they accept. A format that worked in Spark 2.x can silently fail to parse (returning null in PERMISSIVE mode, aborting in FAILFAST) in Spark 3.x without any code change. Glue 4.0 uses PySpark 3.3.2.

With an explicit `F.to_timestamp(col, format_string)` call, the parsing behaviour is exactly what the format string specifies, regardless of PySpark version or parser implementation changes.

---

## `TIMESTAMP_FORMAT`

```python
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
```

This string is used in two places:

```python
# Parsing: string → TimestampType
df = df.withColumn(
    "order_timestamp",
    F.to_timestamp(F.col("order_timestamp"), TIMESTAMP_FORMAT)
)

# Writing (ingestion layer, constants.py)
TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S"
```

### Format String Breakdown

| Token | Meaning | Example |
|---|---|---|
| `yyyy` | 4-digit year | `2025` |
| `-` | Literal hyphen | `-` |
| `MM` | 2-digit month (01–12) | `04` |
| `-` | Literal hyphen | `-` |
| `dd` | 2-digit day (01–31) | `15` |
| `'T'` | Literal capital T (quoted to prevent interpretation) | `T` |
| `HH` | 2-digit hour in 24-hour format (00–23) | `08` |
| `:` | Literal colon | `:` |
| `mm` | 2-digit minute (00–59) | `30` |
| `:` | Literal colon | `:` |
| `ss` | 2-digit second (00–59) | `00` |

The single quotes around `'T'` in the Spark format string (`yyyy-MM-dd'T'HH:mm:ss`) prevent Spark from interpreting `T` as a format code. Without the quotes, `T` would be interpreted as a pattern character — in Java's `DateTimeFormatter`, uppercase `T` is not a standard symbol and the format would fail to compile. The quotes make it a literal character.

### The Root Cause of the May 2025 All-Rejection Bug

The ingestion layer (`scripts/constants.py`) contained:

```python
# Before the fix:
TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"   # space separator
```

The `ingest.py` script used this constant when generating test data — all order timestamps in the CSV were written in the format `"2025-04-15 08:30:00"` with a space between the date and time components.

`orders_job.py` declared:

```python
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"   # T separator
```

When `F.to_timestamp(F.col("order_timestamp"), TIMESTAMP_FORMAT)` ran against `"2025-04-15 08:30:00"`, the format string expected a `T` at position 10 and found a space instead. The parse returned `null`. The null-check then rejected every order as `"unparseable_timestamp"`. `valid = 0`, `pass_rate = 0.0%`. The Delta table received zero inserts.

The fix: changed `constants.py` to `TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S"` (T separator). Three affected test files (`test_constants.py` line 52, `test_helpers.py` lines 28 and 33) that had hardcoded space-separator expected strings were updated to match.

The fact that this bug produced `valid = 0` and `pass_rate = 0.0%` but no job failure demonstrates why `log_counts()` exists and why the pass rate is a critical monitoring metric. A batch that completes with `numTargetRowsInserted = 0` but reports no errors is the most insidious class of pipeline failure — silent data loss.

---

## UTC Session Timezone

```python
.config("spark.sql.session.timeZone", "UTC")
```

This configuration is set in `build_spark_session()` in `common.py` and applies to all timestamp operations in the Spark session.

### What Happens Without UTC

Glue workers run on EC2 instances. The JVM timezone on an EC2 instance defaults to the timezone of the AWS region in which it runs. For `us-east-1` (US East, Northern Virginia), the JVM default timezone may be `America/New_York` (UTC-5 in winter, UTC-4 in summer with DST).

Without `spark.sql.session.timeZone = UTC`:

```python
F.to_timestamp(F.lit("2025-04-15T08:30:00"), "yyyy-MM-dd'T'HH:mm:ss")
# In UTC:         2025-04-15T08:30:00Z     ← stored in Delta
# In US/Eastern:  2025-04-15T12:30:00Z     ← stored in Delta (4-hour offset for EDT)
```

The same source string `"2025-04-15T08:30:00"` produces a different stored timestamp depending on the worker's regional timezone. A job run on a `us-east-1` worker stores `12:30Z`. The same job run on a `eu-west-1` worker (UTC+1) stores `07:30Z`. The timestamps in the Delta table are incorrect and inconsistent across runs in different regions.

With `spark.sql.session.timeZone = UTC`, `F.to_timestamp("2025-04-15T08:30:00", format)` always stores `2025-04-15T08:30:00Z` — a consistent, timezone-unambiguous value regardless of which region the Glue worker runs in.

### Date Column Consistency

The same UTC requirement applies to the `date` column:

```python
df = df.withColumn("date", F.to_date(F.col("date")))
```

`F.to_date()` respects the session timezone. A date string `"2025-04-15"` near midnight can shift to `"2025-04-14"` if the session timezone is offset behind UTC. With UTC, `"2025-04-15"` is always stored as the date `2025-04-15`.

The `date` column is used as the partition column for `orders` and `order_items`. A timezone-shifted date would place a row in the wrong partition directory, breaking Athena partition pruning for queries that filter by date. UTC session timezone prevents this.

---

## Future Timestamp Tolerance

Orders and order_items validate that `order_timestamp` is not unreasonably far in the future relative to the Glue job's execution time.

```python
MAX_FUTURE_SECONDS = 3600   # 1 hour

now_utc = datetime.utcnow()
future_threshold = now_utc + timedelta(seconds=MAX_FUTURE_SECONDS)

future_timestamps = df.filter(
    F.col("order_timestamp") > F.lit(future_threshold)
)
rejected_buckets.append(
    future_timestamps.withColumn("rejection_reason", F.lit("future_timestamp"))
)
df = df.subtract(future_timestamps)
```

### Why Future Timestamps Are Rejected

An `order_timestamp` of `2099-01-01T00:00:00` is structurally valid — it parses correctly, it is a non-null timestamp, and it satisfies the `>` timestamp guard in the MERGE. It would be committed to the Delta table and appear in every Athena query with a date far in the future until corrected.

Future timestamps indicate:
- A source system clock that is wildly misconfigured
- A test record with a placeholder date that escaped production filtering  
- A data entry error (year typo: `2205` instead of `2025`)

The 1-hour tolerance (`MAX_FUTURE_SECONDS = 3600`) accommodates clock skew between the source system and the Glue worker. If the source system's clock is 10 minutes ahead of UTC, orders timestamped "10 minutes in the future" from the worker's perspective are legitimate orders, not errors. Orders timestamped more than 1 hour in the future are rejected as `"future_timestamp"`.

The 1-hour boundary is a business decision, not a technical requirement. It can be tuned via a Glue job argument if the source system's clock skew is larger in practice.

### Relationship to the MERGE Timestamp Guard

The future timestamp rejection and the MERGE timestamp guard are complementary controls on different layers:

| Control | Layer | Purpose |
|---|---|---|
| Future timestamp rejection | Validation (before MERGE) | Prevents timestamps far in the future from being committed at all |
| MERGE timestamp guard `source > target` | MERGE (Delta write) | Prevents stale re-deliveries from overwriting newer committed data |

A record rejected as `"future_timestamp"` never reaches the MERGE. A record with a timestamp that is valid (within the 1-hour tolerance) but older than what is already in the Delta table for that merge key is handled by the MERGE timestamp guard — it is silently skipped (not updated, not rejected).

---

## Timestamp Handling Summary

| Step | Mechanism | Purpose |
|---|---|---|
| CSV read | `StringType` for timestamp columns | Prevent FAILFAST abort on one bad timestamp; preserve original string for audit |
| Explicit cast | `F.to_timestamp(col, TIMESTAMP_FORMAT)` | Controlled parsing with a declared format string |
| Cast failure detection | Null after cast on non-null input | Produces `"unparseable_timestamp"` rejection with original string preserved |
| UTC session timezone | `spark.sql.session.timeZone = UTC` | Consistent timestamp storage regardless of Glue worker region |
| Future tolerance | Reject if `> now + 1 hour` | Catch clock errors and test data before they reach the Delta table |
| MERGE guard | `source.order_timestamp > target.order_timestamp` | Cross-batch deduplication; stale delivery protection |
