# Archival Strategy — Moving Source Files After a Committed MERGE

## Overview

After each Glue job's Delta MERGE commits successfully, the source CSV file is moved from the `raw/` prefix to `archived/`. This happens in the Archive stage — the fifth and final stage of each job, after the Delta Merge and Catalog Update stages have both completed. The archival is a copy-then-delete operation using boto3, not a rename. It is non-fatal: a failed archive does not fail the pipeline, because the data has already been committed to the Silver layer. This document covers the archival implementation, the directory structure under `archived/`, why archival is deliberately last, and how the non-fatal design prevents a secondary operation from masking a successful data commit.

---

## Why Archival Runs Last

The five stages run in order: Read → Validate → Delta Merge → Catalog Update → Archive.

The Archive stage is placed after the Delta Merge and Catalog Update stages for a specific reason: by the time `archive_source_file()` runs, the data from the source CSV has been durably committed to the Delta table and the Glue catalog entry has been updated. The source file's job is done.

If archival ran before the MERGE, two problems arise:

**Problem 1 — Data loss on failure between archive and commit:** If the source file were archived (deleted from `raw/`) before the MERGE committed, and then the MERGE failed (Spark exception, Glue worker crash, Delta conflict), the source file would be gone and the Delta table would be unchanged. The batch data would be permanently lost. There would be no file to re-run.

**Problem 2 — Ambiguous re-run state:** On a re-run after a failure, `ingest.py` would try to upload the file to its known S3 key. If that key were already in `archived/`, the operator would need to manually move it back to `raw/` before re-running. The pipeline would require manual intervention for every failed run.

Archival after a successful MERGE eliminates both problems. If the MERGE commits and then the Archive stage fails, the source file remains in `raw/`. On the next run:
- `ensure_delta_table()` finds the Delta table already initialised → no-op
- The MERGE runs against a valid existing table → the identical source file produces zero inserts, zero updates (idempotency)
- The Archive stage runs again and succeeds this time

The raw file being present on re-run is safe because idempotency ensures no duplicates are committed.

---

## `archive_source_file()` — Implementation

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
        logger.info(
            "Archived %s/%s → %s/%s",
            source_bucket, source_key,
            archive_bucket, archive_key,
        )
    except ClientError as exc:
        logger.warning(
            "Archive failed for %s/%s: %s — continuing without archive.",
            source_bucket,
            source_key,
            exc.response["Error"]["Code"],
        )
```

### Why Copy-Then-Delete, Not a Single Move

S3 has no `move` or `rename` API. Moving a file requires two separate API calls: `CopyObject` writes a new object to the destination key, then `DeleteObject` removes the source key. These are two independent S3 operations — they are not atomic.

The order is critical: **copy first, delete second**.

If `CopyObject` succeeds and `DeleteObject` fails, the file exists in both `raw/` and `archived/`. The source file is duplicated, not lost. On the next pipeline run, `ingest.py` uploads the same file to the same `raw/` key (overwriting the existing object — S3 `PutObject` is idempotent on the same key), and the MERGE deduplicates on re-run. The duplicate in `archived/` is eventually cleaned up by the lifecycle rule or overwritten by the next archive call to the same key.

If `DeleteObject` ran first and `CopyObject` failed, the file would be permanently gone from `raw/` with no copy in `archived/`. Recovery would require the operator to re-source the original file from outside the pipeline. The copy-first order eliminates this irreversible failure mode.

### `ExpectedBucketOwner`

Both `copy_object` and `delete_object` include `ExpectedBucketOwner=account_id`. This parameter causes S3 to reject the operation if the target bucket is not owned by the specified AWS account ID.

This guards against a bucket namespace confusion scenario: S3 bucket names are globally unique. If the destination bucket name (`archive_bucket`) were accidentally mis-named to match a bucket in a different AWS account (possible with corporate naming conventions or Terraform configuration errors), `CopyObject` without `ExpectedBucketOwner` would write the source file to that foreign bucket. With `ExpectedBucketOwner`, S3 returns `AccessDenied` (the foreign bucket is not owned by the pipeline account), the `ClientError` is caught, a warning is logged, and no data leaves the intended account.

### Non-Fatal `ClientError` Handling

```python
except ClientError as exc:
    logger.warning(
        "Archive failed for %s/%s: %s — continuing without archive.",
        source_bucket,
        source_key,
        exc.response["Error"]["Code"],
    )
```

The `except` block catches `ClientError` (the boto3 exception base class for all S3 API errors) and logs a warning rather than re-raising. The pipeline stage completes successfully after logging the warning. The Glue job exits with status `SUCCEEDED`. Step Functions proceeds to `NotifySuccess`.

**Why non-fatal:**

By the time the Archive stage runs, the Delta MERGE has committed. The pipeline's data contract — ingest from raw, transform, commit to Silver — is fulfilled. The source file remaining in `raw/` is an operational inconvenience, not a data correctness problem.

If `archive_source_file()` re-raised on `ClientError`, a transient S3 API error (S3 throttling, brief network interruption) would cause the Glue job to fail with status `FAILED`. Step Functions would receive the failure, route to `NotifyFailure`, and send an SNS alert saying the pipeline failed. An operator investigating the alert would find that the MERGE committed successfully but the pipeline reported failure — a confusing and misleading state. Worse, on re-run, the MERGE would run again (no-op via idempotency) and the Archive stage would succeed — but the failure notification would have already alarmed the operations team unnecessarily.

The log warning is sufficient: CloudWatch Insights queries over `logger.warning("Archive failed...")` lines reveal archive failures without the noise of false pipeline failures.

---

## `archived/` Prefix Structure

```
s3://<data-bucket>/archived/
├── products/
│   └── apr_2025/
│       └── products.csv
├── orders/
│   └── apr_2025/
│       └── orders_apr_2025.csv
└── order_items/
    └── apr_2025/
        └── order_items_apr_2025.csv
```

### Directory Levels

**`archived/<dataset>/`** — one directory per dataset. Mirrors the `raw/<batch>/<dataset>/` structure but inverted: dataset is the top level under `archived/`, batch is the second level. This organises files by their data type first, making it easy to find "all historical products files" by browsing `archived/products/` rather than scanning every batch directory.

**`archived/<dataset>/<batch>/`** — the batch identifier (`apr_2025`, `may_2025`, etc.) as the second level. The batch identifier is passed into the pipeline via the Step Functions execution input and propagated to the Glue jobs via Glue job arguments. `archive_source_file()` constructs the archive key:

```python
archive_key = f"archived/{dataset}/{batch}/{filename}"
```

Where `filename` is the basename of the source key (`products.csv`, `orders_apr_2025.csv`, `order_items_apr_2025.csv`).

**Why batch at the second level (not date):**

The `rejected/` and `flagged/` prefixes use calendar date as the second level because they are keyed by when the pipeline ran, not by the data's batch identity. Archive is different — the archived file represents a specific batch of data, and the batch identifier is the meaningful grouping. Looking up "what was in the April 2025 orders batch" navigates directly to `archived/orders/apr_2025/` rather than requiring knowledge of which calendar date the pipeline ran on.

### No Lifecycle Expiry on Archived Files

Unlike `rejected/` (60-day expiry), archived source files do not have an S3 lifecycle rule that deletes them. The archived file is the only record of the raw data as it was received. If a pipeline issue is discovered six months later and the root cause requires re-examining the original CSV, the archived file must still be present.

The cost is low: compressed CSV files for this pipeline are kilobytes to low megabytes each. A full year of monthly batches (12 files × 3 datasets = 36 files) is negligible storage at S3 pricing.

If storage cost becomes a concern at higher data volumes, a lifecycle rule transitioning archived files to S3 Glacier Instant Retrieval after 90 days would reduce cost with no operational impact — Glacier Instant Retrieval restores objects in milliseconds, making recovery access fast.

---

## Archive Key Construction

`ingest.py` passes the source S3 key to the Glue job as `--SOURCE_KEY`. The Glue job's `archive_source_file()` call constructs the archive key:

```python
# source_key = "raw/apr_2025/orders/orders_apr_2025.csv"
filename    = source_key.split("/")[-1]             # "orders_apr_2025.csv"
archive_key = f"archived/orders/{batch}/{filename}" # "archived/orders/apr_2025/orders_apr_2025.csv"
```

The archive key preserves the original filename. If the same file is re-archived (idempotent re-run after a previous archive failure), `CopyObject` overwrites the same archive key — S3 `CopyObject` is idempotent on the same key pair.

---

## Archive Coverage Across Jobs

| Glue Job | Source Key Pattern | Archive Key Pattern |
|---|---|---|
| products_job | `raw/<batch>/products/products.csv` | `archived/products/<batch>/products.csv` |
| orders_job | `raw/<batch>/orders/orders_<batch>.csv` | `archived/orders/<batch>/orders_<batch>.csv` |
| order_items_job | `raw/<batch>/order_items/order_items_<batch>.csv` | `archived/order_items/<batch>/order_items_<batch>.csv` |

All three jobs call `archive_source_file()` independently in their own Archive stage. The products job archives `products.csv` regardless of whether `orders.csv` has been archived yet. Each job's archive is self-contained — there is no cross-job dependency in the Archive stage, unlike the Validate stage (where order_items depends on products and orders Delta tables).
