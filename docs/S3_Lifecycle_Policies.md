# S3 Lifecycle Policies — Storage Tiering, Expiry, and Version Cleanup

## Overview

S3 lifecycle rules automate object transitions between storage classes and object expiry. This pipeline applies lifecycle rules to the data bucket targeting five distinct prefixes: `raw/`, `archived/`, `rejected/`, `flagged/`, and a bucket-wide rule for noncurrent object versions. Each rule reflects the access pattern and retention requirement for its prefix. This document covers every rule, its configuration, and the reasoning behind the storage class choice and timing.

---

## Background — S3 Storage Classes

| Storage Class | Retrieval Latency | Min Storage Duration | Cost vs STANDARD |
|---|---|---|---|
| STANDARD | Milliseconds | None | Baseline |
| STANDARD_IA | Milliseconds | 30 days | ~50% lower storage cost; per-GB retrieval fee |
| GLACIER_IR | Milliseconds | 90 days | ~68% lower storage cost; higher retrieval fee |
| GLACIER | 3–5 hours (standard) | 90 days | ~77% lower storage cost |
| DEEP_ARCHIVE | 12 hours | 180 days | ~95% lower storage cost |

**STANDARD_IA** (Infrequent Access): Same millisecond retrieval as STANDARD, lower storage cost, but a per-GB retrieval fee. Suitable for data accessed less than once per month.

**GLACIER_IR** (Glacier Instant Retrieval): Millisecond retrieval like STANDARD_IA but at significantly lower storage cost. Suitable for archival data that still needs occasional access without delay — e.g., source files that might be retrieved for debugging months after ingestion.

---

## Lifecycle Configuration Resource

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  # Rules declared below
}
```

All rules are on the data bucket. The other three buckets (scripts, athena_results, logs) have simpler policies or rely on S3's default 90-day object expiry for temporary objects.

---

## Rule 1 — `raw/` Transition to STANDARD_IA After 30 Days

```hcl
rule {
  id     = "raw-transition-to-ia"
  status = "Enabled"

  filter {
    prefix = "raw/"
  }

  transition {
    days          = 30
    storage_class = "STANDARD_IA"
  }
}
```

### Access Pattern

`raw/` holds the original CSV files uploaded by `ingest.py`. Each file is read once by its corresponding Glue job during the pipeline run (within hours of upload), then archived to `archived/` by the Archive stage. After a successful archive, the file in `raw/` is deleted by `s3.delete_object()` — it no longer exists. The lifecycle rule only applies to files that were not archived (Archive stage failure, manual uploads left in `raw/`, or files from a partial ingestion that was never completed).

### Why 30-Day Transition

The 30-day minimum storage duration of STANDARD_IA aligns with the monthly batch cadence. A file uploaded on April 30 that was not archived (Archive stage failed) will have been in `raw/` for at least 30 days by the time the May batch runs. Transitioning it to STANDARD_IA costs the same as storing it for 30 days at STANDARD (due to the minimum storage duration charge) — but from day 31 onward the storage cost drops by approximately 50%.

**Why not expiry?** Files in `raw/` that were not archived represent data that may not have been committed to the Delta table. Expiring them after 30 days would permanently delete source data that might need to be reprocessed. The transition to STANDARD_IA preserves the file indefinitely at low cost while allowing the pipeline to reprocess it if needed.

No expiry date is set on `raw/`. Files that are never reprocessed accumulate in STANDARD_IA indefinitely. This is an acceptable trade-off: the storage cost of a few CSV files at STANDARD_IA is negligible, and erasing source data automatically is a data governance risk.

---

## Rule 2 — `archived/` Two-Stage Tiering to Glacier

```hcl
rule {
  id     = "archived-tiered-storage"
  status = "Enabled"

  filter {
    prefix = "archived/"
  }

  transition {
    days          = 30
    storage_class = "STANDARD_IA"
  }

  transition {
    days          = 90
    storage_class = "GLACIER_IR"
  }
}
```

### Access Pattern

Archived files are the final resting state of processed source CSVs. They are accessed rarely — only when an operator needs to re-examine the original data for a specific batch, typically within a few months of ingestion. After 6–12 months, the probability of needing to retrieve a specific batch's source CSV drops to near zero.

### Stage 1 — STANDARD to STANDARD_IA at 30 Days

The first 30 days after archival, the file is in STANDARD. If the Archive stage completes on the same day as ingestion, the file enters `archived/` and may still be needed in the following days (e.g., to diagnose a rejection issue discovered after the pipeline run). The 30-day grace period in STANDARD ensures millisecond retrieval at no extra cost for the initial investigation window.

After 30 days, the file transitions to STANDARD_IA. It is still accessible in milliseconds but at lower storage cost.

### Stage 2 — STANDARD_IA to GLACIER_IR at 90 Days

After 90 days in `archived/`, the file transitions to GLACIER_IR. GLACIER_IR provides millisecond retrieval (unlike standard Glacier which requires 3–5 hours) at approximately 68% lower cost than STANDARD. The millisecond retrieval characteristic is important for `archived/` — an operator needing to retrieve a 6-month-old source file for debugging should not have to wait hours for a Glacier restore.

At 90 days total storage time (30 in STANDARD + 30 in STANDARD_IA + enough time to trigger the GLACIER_IR transition), the source file is 3 months old. It is extremely unlikely to be needed urgently at this point, but GLACIER_IR's instant retrieval maintains the option.

**Why not DEEP_ARCHIVE at 180 days?** DEEP_ARCHIVE (12-hour retrieval) would be appropriate for compliance-grade long-term retention where retrieval SLA is not a concern. For this pipeline's operational context — a development/training dataset — GLACIER_IR with millisecond retrieval is the better balance. A production deployment with formal retention policies might add a third transition to DEEP_ARCHIVE at 365 days.

No expiry date is set on `archived/`. See [Archival_Strategy.md](Archival_Strategy.md) for the reasoning.

---

## Rule 3 — `rejected/` Expiry at 60 Days

```hcl
rule {
  id     = "expire-rejected-records"
  status = "Enabled"

  filter {
    prefix = "rejected/"
  }

  expiration {
    days = 60
  }
}
```

### Why 60 Days

Rejected records have a clear operational lifecycle: they are written during a pipeline run, investigated within the following days or weeks if a data quality issue is suspected, and used for reprocessing recovery if the rejection was due to a pipeline bug. After 60 days (two complete monthly batch cycles), the window for meaningful investigation and recovery has closed. A rejection from April is unlikely to require action in July.

The 60-day rule permanently deletes the Parquet files under `rejected/` and the objects within them. Unlike `raw/` and `archived/`, there is no long-term retention value for rejected records — they represent bad data from a past batch. Storing them indefinitely would grow the `rejected/` prefix without bound at the monthly-batch cadence.

If a systematic data quality issue is discovered after 60 days (e.g., a validation rule that was too strict), the recovery path is to re-ingest from the archived source file (`archived/`) rather than from the expired rejection record.

---

## Rule 4 — `flagged/` Expiry at 90 Days

```hcl
rule {
  id     = "expire-flagged-records"
  status = "Enabled"

  filter {
    prefix = "flagged/"
  }

  expiration {
    days = 90
  }
}
```

### Why 90 Days (Not 60)

Flagged records are softer signals than rejections. A large-order flag (`flag_reason = "large_order_amount"`) requires a business analyst to review whether the order is legitimate — a process that may take longer than the 60-day rejected-record review window. Quarterly business reviews, monthly revenue reconciliations, and fraud investigation cycles typically operate on 60–90 day windows. The 90-day expiry gives a full quarter for flagged orders to be reviewed before the audit record expires.

The 30-day gap between `rejected/` expiry (60 days) and `flagged/` expiry (90 days) also reflects the difference in severity: rejections are always wrong and therefore lower urgency to retain; flags may be legitimate business events that need more time to validate.

---

## Rule 5 — Noncurrent Version Expiry

```hcl
rule {
  id     = "noncurrent-version-expiry"
  status = "Enabled"

  filter {}  # No prefix filter — applies to the entire bucket

  noncurrent_version_expiration {
    noncurrent_days           = 30
    newer_noncurrent_versions = 3
  }
}
```

### Versioning and Noncurrent Objects

The data bucket has versioning enabled:

```hcl
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}
```

With versioning enabled, overwriting or deleting an S3 object does not remove it — the old version becomes a "noncurrent" version, and a new current version takes its place. This is the safety net for the `archive_source_file()` copy-then-delete pattern: if `delete_object` is called on a versioned bucket without specifying a version ID, S3 places a delete marker rather than permanently removing the object. The file can be recovered by deleting the delete marker.

Without lifecycle management, noncurrent versions accumulate indefinitely. Every Delta MERGE that rewrites Parquet files generates noncurrent versions of the old Parquet files (the MERGE writes new Parquet files and S3-side deletes the old ones via Delta's remove log entries — but with versioning, those "deleted" files become noncurrent). Over months of pipeline runs, the `lakehouse-dwh/` prefix accumulates noncurrent Parquet versions that consume storage with no operational value.

### Configuration Explanation

**`noncurrent_days = 30`**: A noncurrent version is permanently deleted 30 days after it becomes noncurrent. The 30-day window is sufficient to recover from accidental overwrites detected within a month of occurrence.

**`newer_noncurrent_versions = 3`**: Even within 30 days, retain at most 3 noncurrent versions per object. If a Parquet file is overwritten 10 times in a single week (e.g., repeated pipeline reruns), only the 3 most recent noncurrent versions are kept. Older noncurrent versions are expired immediately, regardless of age. This caps the version count for high-churn objects — Delta log JSON files in `_delta_log/` are written once per pipeline run and may have many versions if the same batch is rerun frequently.

The `filter {}` (empty filter) applies the rule to every object in the bucket. Delta Parquet files in `lakehouse-dwh/`, CSV files in `raw/`, Parquet files in `rejected/` and `flagged/`, and archived files in `archived/` all benefit from noncurrent version cleanup.

---

## Lifecycle Rule Summary

| Prefix | Rule | Days | Storage Class / Action |
|---|---|---|---|
| `raw/` | Transition | 30 | → STANDARD_IA |
| `raw/` | Expiry | None | No automatic deletion |
| `archived/` | Transition | 30 | → STANDARD_IA |
| `archived/` | Transition | 90 | → GLACIER_IR |
| `archived/` | Expiry | None | No automatic deletion |
| `rejected/` | Expiry | 60 | Permanent deletion |
| `flagged/` | Expiry | 90 | Permanent deletion |
| `*` (bucket-wide) | Noncurrent version expiry | 30 (max 3 kept) | Permanent deletion of old versions |

---

## Cost Model at Pipeline Scale

For this development/training dataset, the lifecycle savings are modest in absolute dollar terms. At production scale the rationale is the same but the numbers matter:

**`lakehouse-dwh/` Parquet files** (STANDARD, no transition rule): Delta tables in the Silver layer are actively queried by Athena and read by Glue jobs. They must remain in STANDARD — STANDARD_IA's per-retrieval fee would apply on every Athena scan, making frequent analytical queries significantly more expensive than the storage savings justify. The Silver layer has no lifecycle transition rule for this reason.

**`raw/` and `archived/` CSV files** (transition to STANDARD_IA, then GLACIER_IR): After initial processing, these files are accessed rarely. The storage cost reduction (50% → 68% lower than STANDARD) is worth the per-retrieval fee that applies only on the rare occasion these files are accessed.

**`rejected/` and `flagged/`** (expire at 60 and 90 days): These files grow at a predictable rate proportional to rejection volume. Expiring them prevents unbounded storage growth for data with no long-term analytical value.
