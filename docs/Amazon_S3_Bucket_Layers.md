# Amazon S3 — Bucket Layers, Prefixes, and Data Lifecycle

## Overview

This project uses four S3 buckets. Each bucket has a dedicated role, a distinct security configuration, and its own lifecycle policy. Within the central data bucket, five prefixes act as logical zones that data passes through as it moves from raw landing to long-term archive. This document covers all four buckets, all five prefixes, encryption, versioning, access controls, and lifecycle tiering — with the exact Terraform configuration behind each decision.

---

## The Four Buckets

### Why Four Buckets Instead of One

Separating concerns into four buckets rather than one with prefixes achieves two things:

1. **Different lifecycle and access policies per concern.** Logs must be writable by the S3 log-delivery service, which requires a different ACL than the data bucket. Athena results need a short expiry (7 days) that would be dangerous on the same bucket as the data. Scripts need versioning for rollback but no lifecycle tiering because they are tiny.

2. **Blast-radius isolation.** A misconfigured lifecycle rule on one bucket cannot affect data in another. A policy change that accidentally grants public read on the Athena results bucket does not expose raw CSV files.

### Bucket 1 — Data Bucket (`ecom-lakehouse-dev-data-<account>`)

The central store. All pipeline data — raw CSVs, processed Delta tables, archived originals, rejected records, flagged rows — lives here under prefix-separated zones. Every Glue job reads from and writes to this bucket.

**Name construction:** `${project_name}-${environment}-data-${account_id}`. The account ID suffix guarantees global uniqueness without random suffixes that break reproducibility.

**Versioning:** Enabled. Every object write creates a new version. If a Glue job overwrites a Parquet file in a Delta table partition due to a merge or compaction, the previous version is retained and recoverable. Noncurrent versions expire after 30 days (configurable via `var.noncurrent_version_expiry_days`).

**Encryption:** AES-256 server-side encryption with `bucket_key_enabled = true`. Bucket Key reduces the number of AWS KMS API calls by generating a short-lived bucket-level key that is used to encrypt object keys, rather than calling KMS for each individual object. For a pipeline writing thousands of Parquet files per run, this has a measurable cost impact.

**Logging:** Access logs are delivered to the logs bucket at prefix `s3-access-logs/data-bucket/`.

**Public access:** All four block-public-access settings are set to `true` (`block_public_acls`, `block_public_policy`, `ignore_public_acls`, `restrict_public_buckets`). These are account-level guardrails that override any object-level public grant or bucket policy that might accidentally include `"Principal": "*"`.

**TLS policy:** A bucket policy denies any S3 action where `aws:SecureTransport = false`:
```json
{
  "Effect": "Deny",
  "Principal": "*",
  "Action": "s3:*",
  "Resource": ["arn:aws:s3:::bucket", "arn:aws:s3:::bucket/*"],
  "Condition": { "Bool": { "aws:SecureTransport": "false" } }
}
```
This is applied via `aws_s3_bucket_policy.tls_only` as a `for_each` across all four buckets. Any plaintext HTTP request (including from internal tools that forget to use HTTPS) is rejected at the S3 layer before it reaches IAM evaluation.

### Bucket 2 — Scripts Bucket (`ecom-lakehouse-dev-scripts-<account>`)

Stores all Glue ETL job scripts and the utility zip. The Glue job service reads scripts from here at runtime. This bucket has no lifecycle tiering — scripts are small text files and cost nothing to retain indefinitely.

**Versioning:** Enabled. When the `deploy.yml` GitHub Actions workflow uploads a new version of `orders_job.py`, the previous version is preserved with a version ID. If the new script causes job failures, the Glue job definition in Terraform can be rolled back to point at the previous S3 object version without re-deploying.

**Upload mechanism:** Terraform manages script uploads via `aws_s3_object` resources with `etag = filemd5(...)`. Terraform compares the local file's MD5 against the stored ETag on every `apply`. If the file changed, Terraform re-uploads it. If unchanged, the upload is skipped. This means scripts are only re-uploaded when they actually change, not on every `terraform apply`.

### Bucket 3 — Logs Bucket (`ecom-lakehouse-dev-logs-<account>`)

Receives S3 server-access logs from the data bucket, and Spark UI logs from Glue job runs. Created first in Terraform because the data bucket's `aws_s3_bucket_logging` resource references this bucket's ID — circular dependency is avoided by creating the target before the source.

**ACL:** `log-delivery-write`. This allows the S3 log-delivery service (which has its own AWS-internal principal) to write access log objects. The `BucketOwnerPreferred` ownership control is set so the bucket owner receives ownership of all log objects delivered by the external service.

**Lifecycle:** Access log objects expire after 90 days (configurable via `var.log_retention_days`). Spark UI log files are read-once for debugging; retaining them beyond 90 days is not useful.

### Bucket 4 — Athena Results Bucket (`ecom-lakehouse-dev-athena-results-<account>`)

Athena writes query result CSV and metadata files here after every query execution. The workgroup enforces `output_location = "s3://<athena-results-bucket>/query-results/"` and rejects any client-supplied output location — analysts cannot redirect results to arbitrary buckets.

**Lifecycle:** Query results expire after 7 days. Athena results are transient: analysts download or view them immediately, and stale results serve no purpose. The 7-day window provides a short recovery window if someone needs to retrieve a result they forgot to save.

**No versioning.** Results are generated on demand and re-runnable at any time. Versioning adds cost without benefit for ephemeral query output.

---

## The Five Prefixes Inside the Data Bucket

All five prefixes are initialised as empty S3 objects (`aws_s3_object` with `content = ""`) in Terraform. This ensures the prefix "folder" exists before any Glue job attempts to write into it — some AWS services behave unexpectedly when a prefix has never existed.

```
s3://ecom-lakehouse-dev-data-<account>/
├── raw/                  → Landing zone for source CSVs
├── lakehouse-dwh/        → Processed Delta Lake tables
├── archived/             → Source files after successful ingestion
├── rejected/             → Rows that failed validation
└── flagged/              → Rows that pass but need analyst attention
```

### Prefix 1 — `raw/`

**Purpose:** The immutable landing zone. Files arrive here as uploaded by `ingest.py`. Nothing in this prefix has been validated, transformed, or touched by Spark.

**What lands here:**

| File | Uploaded by | Lifecycle after ingestion |
|---|---|---|
| `products.csv` | `ingest.py` / `ingest_may_2025.py` | Moved to `archived/products/<date>/` |
| `orders_apr_2025.csv` | `ingest.py` | Moved to `archived/orders/<date>/` |
| `order_items_apr_2025.csv` | `ingest.py` | Moved to `archived/order_items/<date>/` |
| `orders_may_2025.csv` | `ingest_may_2025.py` | Moved to `archived/orders/<date>/` |
| `order_items_may_2025.csv` | `ingest_may_2025.py` | Moved to `archived/order_items/<date>/` |

**Lifecycle rule (`raw-ia-transition`):**
```hcl
rule {
  id     = "raw-ia-transition"
  status = "Enabled"
  filter { prefix = "raw/" }
  transition {
    days          = 30
    storage_class = "STANDARD_IA"
  }
}
```

In normal operation, a file stays in `raw/` for minutes — exactly as long as the Glue job needs to read it. The transition to Infrequent Access at 30 days exists as a cost backstop for the failure scenario where a Glue job never runs successfully and the file is never archived. Without this rule, a stuck file would accumulate Standard storage cost indefinitely.

**Why files are not deleted from `raw/` immediately:** The archive step runs at the end of the Glue job, after the Delta MERGE commits. If archiving fails (transient S3 error), the Delta data is already committed and safe — the file in `raw/` is just an orphan. The next pipeline run will re-process the same file, but because of MERGE idempotency, this is safe. Deleting the file before the MERGE commits would lose the source data if the MERGE fails.

### Prefix 2 — `lakehouse-dwh/`

**Purpose:** The processed zone. This is where the three Delta Lake tables live. It is the authoritative analytical store — the single version of truth downstream consumers query.

**Physical layout inside `lakehouse-dwh/`:**

```
lakehouse-dwh/
├── products/
│   ├── _delta_log/
│   │   ├── 00000000000000000000.json   ← initial commit (empty table)
│   │   ├── 00000000000000000001.json   ← first MERGE
│   │   └── 00000000000000000002.json   ← second MERGE (May batch)
│   ├── department=bakery/
│   │   └── part-00000-...snappy.parquet
│   ├── department=beverages/
│   └── ... (10 department partitions)
│
├── orders/
│   ├── _delta_log/
│   ├── date=2025-04-01/
│   ├── date=2025-04-02/
│   └── ... (one directory per calendar day)
│
└── order_items/
    ├── _delta_log/
    ├── date=2025-04-01/
    └── ...
```

**No lifecycle rule targets `lakehouse-dwh/` for expiry or tiering.** Delta tables accumulate Parquet files as MERGEs add new data and old versions are superseded. Introducing automatic tiering would interfere with Delta's transaction log, which expects its `_delta_log/*.json` files to be readable at consistent latency. The `noncurrent_version_expiry_days` rule applies here through versioning: old object versions (from S3 object overwrites during compaction) expire after 30 days, but the current version is always retained indefinitely.

**`_delta_log/` is the source of truth,** not the Parquet files themselves. Athena reads the log to determine which Parquet files belong to the current snapshot. Files that were logically removed by a MERGE (replaced by a compacted file) still exist in S3 but are invisible to readers because the log marks them as removed. Delta's vacuum operation (not triggered in this pipeline) is responsible for physically deleting them after a retention period.

### Prefix 3 — `archived/`

**Purpose:** Immutable audit archive of every source file that was successfully ingested. Once a file is archived, the raw/ copy is deleted, but the archive copy is permanent.

**Path pattern:**
```
archived/products/2026-06-15/products.csv
archived/orders/2026-06-15/orders_may_2025.csv
archived/order_items/2026-06-15/order_items_may_2025.csv
```

The `archive_source_file()` function in `common.py` constructs this path:
```python
filename = source_key.split("/")[-1]
run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
dest_key = f"{args['ARCHIVED_PREFIX'].rstrip('/')}/{args['DATASET']}/{run_date}/{filename}"
```

**Lifecycle rules (`archived-tiering`):**
```hcl
rule {
  id     = "archived-tiering"
  status = "Enabled"
  filter { prefix = "archived/" }
  transition {
    days          = 30
    storage_class = "STANDARD_IA"
  }
  transition {
    days          = 90
    storage_class = "GLACIER"
  }
}
```

IA at 30 days: archived files are queried frequently in the first weeks after ingestion for incident investigation. After a month, access drops to near zero.

Glacier at 90 days: archived files after 90 days are effectively permanent records. They are never retrieved except for regulatory audits or extreme incident recovery. Glacier costs ~$0.004/GB/month vs Standard's $0.023/GB/month — an 80% cost reduction for cold data that is retained indefinitely.

No expiry rule is set on `archived/`. These are permanent records of exactly what was ingested and when.

### Prefix 4 — `rejected/`

**Purpose:** Every row that fails any validation check is written here as Parquet with audit metadata. The rejected zone is the complete paper trail of data quality failures.

**Path pattern:**
```
rejected/orders/2026-06-15/20260615T134313/
rejected/order_items/2026-06-15/20260615T134313/
rejected/products/2026-06-15/20260615T134313/
```

The `write_rejected()` function in `common.py` builds this path:
```python
run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
output_path = (
    f"s3://{args['DATA_BUCKET']}/"
    f"{args['REJECTED_PREFIX'].rstrip('/')}/{args['DATASET']}/"
    f"{run_date}/{job_run_id}/"
)
```

**Every rejected Parquet file contains four audit columns** appended to the original row columns:

| Column | Type | Content |
|---|---|---|
| `rejection_reason` | String | Named code for the failing check (e.g. `invalid_timestamp_format`, `null_order_id`, `invalid_product_id`) |
| `_rejected_at` | Timestamp | When the row was written to rejected/ |
| `_job_run_id` | String | UTC timestamp of the job run (`20260615T134313`) |
| `_source_key` | String | S3 key of the source file (`raw/orders_may_2025.csv`) |

Rejected rows are stored as Parquet (not CSV) so they are directly queryable by Athena without a crawler or schema definition:
```sql
SELECT rejection_reason, COUNT(*) AS count, _source_key
FROM "s3://ecom-lakehouse-dev-data-<account>/rejected/orders/"
GROUP BY rejection_reason, _source_key
ORDER BY count DESC;
```

**Lifecycle rule (`expire-rejected`):**
```hcl
rule {
  id     = "expire-rejected"
  status = "Enabled"
  filter { prefix = "rejected/" }
  expiration { days = 60 }
}
```

60-day expiry. Rejected records are relevant for the operational investigation window immediately after a failed or partial batch. After 60 days, the data quality issues have either been resolved (new file ingested with fixes) or escalated to a longer-term data governance process that lives outside this pipeline. Retaining rejected records indefinitely would slowly accumulate large volumes of bad data that serves no operational purpose.

No Glacier transition is applied to rejected records. If investigation is needed, it happens within 60 days — there is no cold-storage access pattern for rejected data.

### Prefix 5 — `flagged/`

**Purpose:** Rows that pass all hard validation rules but contain values that warrant analyst review. The current rule is `total_amount > 1,000,000` for orders. These rows are committed to the Delta table (they are valid data), but also copied to `flagged/` with a `flag_reason` column for human review.

**Path pattern:**
```
flagged/orders/<job_run_id>/
```

**Lifecycle rule (`expire-flagged`):**
```hcl
rule {
  id     = "expire-flagged"
  status = "Enabled"
  filter { prefix = "flagged/" }
  expiration { days = 90 }
}
```

90 days rather than 60, because flagged rows represent legitimate business activity that may require investigation across a full quarter's reporting cycle. An analyst reviewing a flagged $1.2M order may need to compare it against the previous quarter's data, which requires the flagged record to persist longer than a simple validation failure.

---

## How the Lifecycle Policies Interact with Versioning

S3 versioning and lifecycle rules operate independently but interact in important ways for this bucket.

**Current version lifecycle:** Lifecycle rules with `expiration { days = N }` apply to current versions. The `rejected/` expiry at 60 days deletes the current version of rejected Parquet files after 60 days.

**Noncurrent version lifecycle:** The `expire-noncurrent-versions` rule applies globally:
```hcl
rule {
  id     = "expire-noncurrent-versions"
  status = "Enabled"
  filter { prefix = "" }
  noncurrent_version_expiration {
    noncurrent_days = 30
  }
}
```
When Glue writes a new Parquet file during a Delta MERGE — for example, compacting two small Parquet files in `date=2025-04-15/` into one — the replaced files become noncurrent versions. This rule expires them after 30 days, preventing indefinite accumulation of superseded Parquet fragments.

**Interaction:** A rejected Parquet file written today has a current version and no noncurrent versions. If it were somehow overwritten (unlikely for rejected files, which are append-only), the original would become noncurrent and expire in 30 days. The current version expires at 60 days by the `expire-rejected` rule. Both rules fire independently; the shorter one wins per-version.

---

## Object Ownership and Access Logging

The logs bucket uses `BucketOwnerPreferred` object ownership:
```hcl
resource "aws_s3_bucket_ownership_controls" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule { object_ownership = "BucketOwnerPreferred" }
}
```
S3 access log delivery uses a special internal AWS service principal that creates objects in the target bucket. Without `BucketOwnerPreferred`, the created log objects are owned by the delivery principal, not the bucket owner. This means the bucket owner's IAM policies cannot control the log objects. `BucketOwnerPreferred` ensures the account owns all objects, even those created by external principals.

The data bucket's logging configuration:
```hcl
resource "aws_s3_bucket_logging" "data" {
  bucket        = aws_s3_bucket.data.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3-access-logs/data-bucket/"
}
```
Every `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` call against the data bucket generates an access log entry. These logs are the audit trail for GDPR or SOC2 evidence: who accessed what file, at what time, from what IP address.
