# Common Utilities Module — `glue_jobs/utils/common.py`

## Overview

`common.py` is the shared utility module imported by all three Glue jobs. It provides the functions that every job needs but that have nothing to do with the specific dataset being processed: building the Spark session, parsing Glue job arguments, initialising Delta tables, writing rejected records, registering tables in the Glue catalog, archiving processed source files, and logging row counts. This document covers each function: what it does, why it is designed the way it is, and the key implementation decisions.

---

## `build_spark_session()`

```python
def build_spark_session(job_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(job_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    _verify_delta_loaded(spark)
    return spark
```

### The Four-Config Chain

These four configuration entries form a chain: each one enables a different layer of Delta Lake integration in Glue 4.0.

**`spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension`**

Registers Delta Lake's SQL extensions with Spark's SQL parser. Without this, Delta-specific SQL syntax (`DESCRIBE HISTORY`, `CONVERT TO DELTA`, vacuum commands issued via `spark.sql(...)`) raises `ParseException: Unsupported command`. This must be the first Delta configuration applied.

**`spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog`**

Replaces Spark's default in-memory catalog with the DeltaCatalog. The DeltaCatalog understands Delta table metadata: when `spark.sql("CREATE TABLE IF NOT EXISTS ...")` or `spark.sql("DESCRIBE TABLE ...")` is called, the catalog looks at the `_delta_log/` rather than relying on a Hive-style catalog entry. Without this configuration, `spark.sql("USING DELTA")` in DDL statements fails with `NoSuchDatabaseException` or `AnalysisException`.

**`spark.delta.logStore.class = S3SingleDriverLogStore`**

Overrides Delta's default log store (designed for HDFS atomic rename) with the S3-specific implementation. S3 does not support atomic rename — `CopyObject` + `DeleteObject` is not a single atomic operation. Two concurrent writers can both write the same log version number without S3 raising a conflict. `S3SingleDriverLogStore` uses a serialised commit protocol with list-before-write conflict detection, preventing split-brain log corruption. Without this, concurrent writes to the same Delta table on S3 can corrupt the transaction log. See [Delta_Lake_Overview.md](Delta_Lake_Overview.md) for the full explanation.

**`spark.hadoop.hive.metastore.client.factory.class = AWSGlueDataCatalogHiveClientFactory`**

Wires Spark's Hive metastore client to the AWS Glue Data Catalog. When `spark.sql("CREATE TABLE IF NOT EXISTS \`ecom_lakehouse\`.\`orders\`...")` executes, Spark needs a metastore to register the table entry. Without this configuration, Spark uses an embedded in-memory Hive metastore that does not persist across Glue job runs — the catalog entry disappears when the job ends. With this configuration, Spark writes to the Glue Data Catalog, which persists permanently and is queryable by Athena. See [Glue_Data_Catalog.md](Glue_Data_Catalog.md) for the full catalog integration flow.

**`spark.sql.session.timeZone = UTC`**

Sets the Spark SQL session timezone to UTC. Without this, `F.to_timestamp()` interprets timestamp strings using the JVM's default timezone, which varies by the Glue worker's host region. A Glue job running on a worker in `us-east-1` might interpret `"2025-04-15T08:30:00"` as `2025-04-15T08:30:00-05:00` (EST), storing `2025-04-15T13:30:00Z` in the Delta table. Setting UTC ensures that `"2025-04-15T08:30:00"` is always stored as `2025-04-15T08:30:00Z` regardless of which AWS region the Glue worker runs in. See [Timestamp_Handling.md](Timestamp_Handling.md) for the full timezone discussion.

### `_verify_delta_loaded()`

```python
def _verify_delta_loaded(spark: SparkSession) -> None:
    extensions = spark.conf.get("spark.sql.extensions", "")
    if "DeltaSparkSessionExtension" not in extensions:
        raise RuntimeError(
            "Delta Lake extensions are not loaded. "
            "Ensure --datalake-formats delta is set in the Glue job configuration."
        )
```

A defensive check run immediately after the session is built. If the Glue job was mis-configured (missing `--datalake-formats delta` in the Terraform `default_arguments`), the Delta JAR is not on the classpath and the `DeltaSparkSessionExtension` config has no effect. Without this check, the job proceeds, the MERGE fails with a cryptic `ClassNotFoundException`, and the error message does not indicate that Delta was not loaded. The `RuntimeError` raised here produces a clear error message in the CloudWatch log and the SNS failure notification.

---

## `parse_args()`

```python
def parse_args(required_keys: list[str]) -> dict[str, str]:
    from awsglue.utils import getResolvedOptions
    import sys

    all_keys = required_keys + ["JOB_NAME"]
    args = getResolvedOptions(sys.argv, all_keys)
    return args
```

Glue passes job arguments as command-line arguments to the Python process. `getResolvedOptions()` is the AWS Glue SDK function for reading these. Each job calls `parse_args()` with its specific required argument names:

```python
# products_job.py
args = parse_args([
    "SOURCE_BUCKET",
    "SOURCE_KEY",
    "DATA_BUCKET",
    "PROCESSED_DATA_PREFIX",
    "GLUE_DATABASE",
    "SNS_TOPIC_ARN",
    "ENVIRONMENT",
])
```

The `JOB_NAME` key is always appended — it is required by Glue for job bookmarking and is used as the `appName` in `build_spark_session()` and the job name in SNS notification subjects.

`parse_args()` does not validate the values — it only reads them. A missing required key causes `getResolvedOptions()` to raise `awsglue.utils.GlueArgumentError` immediately, before any Spark code runs. This fails fast with a clear error message rather than failing later with a `KeyError` buried in job logic.

---

## `ensure_delta_table()`

```python
def ensure_delta_table(
    spark: SparkSession,
    table_path: str,
    schema: StructType,
    partition_cols: list[str],
) -> None:
    if DeltaTable.isDeltaTable(spark, table_path):
        return

    empty_df = spark.createDataFrame([], schema)
    writer = empty_df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(table_path)
```

Seeds an empty Delta table at `table_path` if none exists. `DeltaTable.isDeltaTable()` is a metadata-only check (one S3 `ListObjects` call to look for `_delta_log/`) that costs milliseconds. On every run after the first, the function returns immediately with no I/O. This function is called unconditionally at the start of the Delta Merge stage — there is no penalty for calling it on every run.

The full rationale (why not `DeltaTable.createIfNotExists()`, why `mode("overwrite")` is safe with the guard) is covered in [Delta_Table_Initialisation.md](Delta_Table_Initialisation.md).

---

## `write_rejected()`

```python
def write_rejected(
    spark: SparkSession,
    rejected_df: DataFrame,
    dataset: str,
    run_id: str,
    s3_bucket: str,
    source_key: str = "",
) -> None:
    if rejected_df is None or rejected_df.rdd.isEmpty():
        return

    now = datetime.utcnow()
    output_path = f"s3://{s3_bucket}/rejected/{dataset}/{now.strftime('%Y-%m-%d')}/{run_id}/"

    enriched = (
        rejected_df
        .withColumn("_rejected_at",  F.lit(now.isoformat()).cast(TimestampType()))
        .withColumn("_job_run_id",   F.lit(run_id))
        .withColumn("_source_key",   F.lit(source_key))
    )
    enriched.write.mode("overwrite").parquet(output_path)
```

Writes the rejected rows DataFrame — already tagged with `rejection_reason` by the validation layer — to the `rejected/` prefix as a Parquet file. The four audit columns (`rejection_reason` from validate, `_rejected_at`, `_job_run_id`, `_source_key` from this function) make the rejection record self-contained. See [Rejected_Records_Strategy.md](Rejected_Records_Strategy.md) for the full directory structure and lifecycle policy.

The function returns immediately on an empty DataFrame rather than writing a zero-row Parquet file. A zero-row file at the output path would appear in Athena table scans and open/close with no data — unnecessary S3 API overhead with no value.

---

## `archive_source_file()`

```python
def archive_source_file(
    s3_client,
    source_bucket: str,
    source_key: str,
    archive_bucket: str,
    archive_key: str,
    account_id: str,
) -> None:
    try:
        s3_client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Bucket=archive_bucket,
            Key=archive_key,
            ExpectedBucketOwner=account_id,
        )
        s3_client.delete_object(
            Bucket=source_bucket,
            Key=source_key,
            ExpectedBucketOwner=account_id,
        )
        logger.info("Archived %s/%s → %s/%s", source_bucket, source_key, archive_bucket, archive_key)
    except ClientError as exc:
        logger.warning(
            "Archive failed for %s/%s: %s — continuing without archive.",
            source_bucket,
            source_key,
            exc.response["Error"]["Code"],
        )
```

### Copy-then-Delete, Not Rename

S3 has no rename operation. Moving a file from `raw/` to `archived/` requires two API calls: `CopyObject` (writes the file to the new location) followed by `DeleteObject` (removes the original). If `CopyObject` succeeds but `DeleteObject` fails, the file exists in both locations — a duplicate, not a loss. This is acceptable: the pipeline treats the raw file as already processed (the MERGE committed), and on the next run `ensure_delta_table()` finds the Delta table already initialised and the MERGE deduplicates the re-ingested rows.

The reverse is not acceptable: if `DeleteObject` ran first and `CopyObject` failed, the source file would be permanently lost with no archived copy.

### `ExpectedBucketOwner`

Both API calls include `ExpectedBucketOwner=account_id`. This parameter causes S3 to reject the call if the bucket is not owned by the specified AWS account ID. Without it, a bucket name collision (extremely unlikely but possible for public bucket names) or a misconfigured Terraform resource could cause the copy or delete to operate on the wrong account's bucket. `ExpectedBucketOwner` is a safety guard against cross-account operation.

### Non-Fatal by Design

`archive_source_file()` runs in the Archive stage, which is the last stage after the MERGE has already committed. The pipeline's data contract — reading from `raw/`, transforming, writing to `lakehouse-dwh/` — has been fulfilled by this point. A failed archive is an operational problem (the source file remains in `raw/` instead of `archived/`) but not a data correctness problem.

Raising on archive failure would cause Step Functions to report the entire pipeline run as FAILED — including the successful MERGE — and trigger the failure notification, implying that data was not committed when it was. The `logger.warning()` logs the failure for investigation. A future pipeline run processes the file again; the MERGE deduplicates the re-processed rows.

---

## `update_catalog_table()`

```python
def update_catalog_table(
    spark: SparkSession,
    database: str,
    table_name: str,
    table_path: str,
) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table_name}` "
        f"USING DELTA LOCATION '{table_path}'"
    )
    logger.info("Catalog table registered: %s.%s → %s", database, table_name, table_path)
```

Registers the Delta table in the Glue Data Catalog under `database.table_name` pointing to the S3 path. `CREATE TABLE IF NOT EXISTS` is idempotent — if the table is already registered with the correct location, the statement is a no-op. If the table was dropped or was never registered, it is registered now.

The Glue Data Catalog registration is what makes the table visible to Athena. Without this step, Athena cannot query the Delta table — Athena resolves table names through the catalog, not by scanning S3 paths directly. The registration runs after every MERGE (Catalog Update stage) because Delta's schema can evolve over time; a `CREATE TABLE IF NOT EXISTS` after each run ensures the catalog always reflects the current Delta schema.

The backtick quoting on `` `database` `` and `` `table_name` `` prevents SQL injection if the database or table name strings were ever user-controlled. They are pipeline constants, not user input, but the quoting is the correct practice for dynamic SQL identifiers.

---

## `log_counts()`

```python
def log_counts(
    source_df: DataFrame,
    valid_df: DataFrame,
    rejected_df: DataFrame | None,
    dataset: str,
) -> None:
    total    = source_df.count()
    valid    = valid_df.count()
    rejected = rejected_df.count() if rejected_df is not None else 0
    pass_rate = (valid / total * 100) if total > 0 else 0.0

    logger.info(
        "[%s] total_read=%d | valid=%d | rejected=%d | pass_rate=%.2f%%",
        dataset,
        total,
        valid,
        rejected,
        pass_rate,
    )
```

Writes a single structured log line to CloudWatch for every validation run. The format is consistent across all three jobs — the same fields in the same order — so a Logs Insights query can parse it uniformly:

```
filter @message like "total_read="
| parse @message "total_read=* | valid=* | rejected=* | pass_rate=*%" as total, valid, rejected, pass_rate
| stats avg(pass_rate) by bin(5m)
```

`log_counts()` calls `.count()` on three DataFrames. Each `.count()` triggers a Spark job. For large DataFrames this would be expensive, but the three counts are unavoidable — the validation layer needs to know how many rows passed and failed, and this is the point where that information is reported. The three `count()` calls run sequentially (not in parallel) because they are independent single-aggregate actions on fully materialised DataFrames after the validation filter operations.
