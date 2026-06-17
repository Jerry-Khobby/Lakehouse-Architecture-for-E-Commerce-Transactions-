# Encryption in This Pipeline — At Rest and In Transit

## Overview

This pipeline applies encryption at two layers: data at rest (objects stored in S3 buckets) and data in transit (requests made over the network). Encryption at rest uses AES-256 server-side encryption applied automatically by S3 on every object write. Encryption in transit is enforced by the TLS-only bucket policy documented in `S3_Bucket_Policies.md`. This document covers the at-rest encryption configuration for each bucket, what `bucket_key_enabled` does and why it matters at pipeline scale, and what is and is not encrypted by default.

---

## Encryption at Rest — The Three Encrypted Buckets

Three of the four S3 buckets have explicit server-side encryption configured:

### Data Bucket

```hcl
resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}
```

The data bucket holds all pipeline data: raw CSVs in `raw/`, Delta Lake Parquet files in `lakehouse-dwh/`, archived source files in `archived/`, rejected row records in `rejected/`, and flagged order records in `flagged/`. This is the most sensitive bucket in the architecture — it contains customer order data, product information, and rejected records with full row data including monetary amounts and user identifiers. AES-256 SSE is the minimum appropriate encryption for this bucket.

### Scripts Bucket

```hcl
resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}
```

The scripts bucket holds Glue job Python scripts and the utility zip. While these files do not contain customer data, they contain the business logic of the pipeline. An attacker who can read the scripts bucket learns the exact validation rules, MERGE conditions, and rejection criteria — information that could be used to craft data that passes validation despite being fraudulent. More critically, if the scripts bucket were writable by an attacker, they could replace a job script with malicious code that exfiltrates data or corrupts the Delta tables. Encrypting at rest and enforcing TLS-only access addresses both risks.

### Athena Results Bucket

```hcl
resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}
```

Athena query results are CSV files containing actual query output — revenue totals, product rankings, order counts, row-level data from analytical queries. These results can be just as sensitive as the source data. The Athena workgroup also enforces encryption at the query level:

```hcl
result_configuration {
  output_location = "s3://${aws_s3_bucket.athena_results.id}/query-results/"
  encryption_configuration {
    encryption_option = "SSE_S3"
  }
}
```

This `encryption_configuration` in the workgroup and the bucket-level SSE configuration are complementary. The bucket SSE applies to all objects written to the bucket regardless of how they were written. The workgroup encryption configuration specifically tells Athena to request SSE-S3 when writing result files — which is redundant with the bucket default, but explicit. Together they ensure that even if a future Athena version or integration bypasses the bucket default, the workgroup configuration still requests encryption.

`enforce_workgroup_configuration = true` means the workgroup encryption setting cannot be overridden by a client who supplies their own `ResultConfiguration` with no encryption. Any client-supplied `ResultConfiguration.EncryptionConfiguration` is ignored — the workgroup setting wins.

---

## The Logs Bucket — No Explicit Encryption Configuration

The logs bucket does not have an explicit `aws_s3_bucket_server_side_encryption_configuration` resource:

```hcl
resource "aws_s3_bucket" "logs" {
  bucket        = local.logs_bucket_name
  force_destroy = var.environment != "prod"
}
# No encryption resource for logs bucket
```

S3 buckets created in AWS accounts after January 2023 have SSE-S3 enabled by default at the account level — S3 applies AES-256 to all objects regardless of whether a bucket-specific configuration exists. The logs bucket is therefore encrypted by this account-level default.

The explicit encryption configuration on the other three buckets is not strictly necessary under the new S3 default encryption behaviour — but it is present for two reasons:

1. **Explicit `bucket_key_enabled = true`** — the account-level default encryption does not automatically enable Bucket Key. This optimisation must be explicitly configured per-bucket (explained below).
2. **Documentation and auditability** — a `terraform plan` on a bucket with explicit SSE configuration shows `No changes` when the configuration is correct. A security auditor or compliance tool reading the Terraform state can confirm encryption is configured without needing to check account-level settings.

The logs bucket omits explicit configuration because it was provisioned first in Terraform (as the target for data bucket access logs) and the account-level default encryption provides the baseline. The operational logs it stores are less sensitive than customer data — they contain request metadata such as timestamps, IP addresses, and object keys, but not the object data itself. The risk profile does not require explicit key optimisation.

---

## What `bucket_key_enabled = true` Does

```hcl
rule {
  apply_server_side_encryption_by_default {
    sse_algorithm = "AES256"
  }
  bucket_key_enabled = true
}
```

Without a Bucket Key, S3 SSE-S3 makes one KMS API call per object for every write and read operation. For a Delta Lake pipeline writing thousands of Parquet files, this matters:

**Without Bucket Key (default):**
- Each `PutObject` call → one `GenerateDataKey` call to AWS KMS
- Each `GetObject` call → one `Decrypt` call to AWS KMS
- A Delta MERGE writing 100 new Parquet files → 100 KMS calls
- AWS KMS has per-account API rate limits. A busy pipeline can hit throttling

**With Bucket Key (`bucket_key_enabled = true`):**
- S3 generates a short-lived bucket-level key (the "Bucket Key") by making one KMS call
- All objects written during the Bucket Key's validity window are encrypted using that key
- Individual `PutObject` calls no longer each require a KMS API call
- KMS call volume drops by up to 99% for workloads with many small objects

For the `orders` table partitioned by `date`, a single Delta MERGE for a May 2025 batch creates one new Parquet file per date partition (up to 31 files for 31 days). Without Bucket Key, that is 31 KMS calls just for the orders MERGE. `lakehouse-dwh/` also accumulates `_delta_log/*.json` files — each log entry is a separate S3 object. With Bucket Key, all of these are encrypted using the shared short-lived key at a fraction of the KMS overhead.

**SSE-S3 vs SSE-KMS — why AES256 and not a customer-managed KMS key:**

`sse_algorithm = "AES256"` uses S3-managed keys (SSE-S3). An alternative would be `sse_algorithm = "aws:kms"` with a customer-managed KMS key (CMK). CMKs provide:
- A dedicated audit trail of every key usage in CloudTrail
- The ability to revoke access to all encrypted data by deleting or disabling the key
- Customer-controlled key rotation

For this project, SSE-S3 is the appropriate choice because:
- The pipeline does not have a regulatory requirement for customer-managed key rotation audit trails
- CMK costs $1/month per key plus $0.03 per 10,000 API calls — meaningful at pipeline scale
- Bucket Key applies to SSE-S3, reducing KMS overhead. CMK with Bucket Key still requires customer KMS calls for the bucket-level key generation itself

A future production deployment with SOC 2 or HIPAA compliance requirements would switch to `aws:kms` with a CMK and `bucket_key_enabled = true`.

---

## What AES-256 SSE Protects

Server-side encryption at rest protects data from physical-layer access to S3 storage. Specifically, it ensures that:

- AWS employees with physical access to storage hardware cannot read object data without the encryption key
- A data centre intrusion that involves disk theft cannot recover plaintext data
- An S3 internal storage failure that exposes raw block data to AWS systems does not expose readable customer data

**What SSE does not protect against:**

- An IAM principal with `s3:GetObject` and valid credentials — the object is decrypted transparently on read
- A misconfigured IAM policy that grants unintended `GetObject` access — SSE does not substitute for access control
- A request over HTTP (plaintext network) — the object is decrypted by S3 and then transmitted unencrypted to the requester (which is why the TLS bucket policy exists as a complementary control)

SSE and the TLS policy together cover the full threat model:
- SSE: data at rest on disk
- TLS policy: data in transit over the network

Neither alone is sufficient. Both together address the complete exposure surface.

---

## Encryption Applied to Every Object — How the Default Works

`apply_server_side_encryption_by_default` means the encryption configuration applies to every object written to the bucket, regardless of whether the writer specifies encryption headers in the `PutObject` request. A Glue job that writes a Parquet file without specifying `x-amz-server-side-encryption` gets SSE-S3 applied automatically by S3 before the object is stored.

The writer does not need to:
- Manage encryption keys
- Add encryption headers to every S3 API call
- Configure Spark or boto3 with encryption settings

This simplifies application code — `common.py`, `orders_job.py`, and all other pipeline code write to S3 with standard API calls and encryption happens transparently at the storage layer. The pipeline would continue writing encrypted data even if the encryption configuration were unknown to or forgotten by the application developers.

---

## Encryption Coverage Summary

| Bucket | Encryption configured | Bucket Key | Notes |
|---|---|---|---|
| Data | AES-256 SSE explicit | Yes | All pipeline data: Delta tables, CSVs, rejected/flagged records |
| Scripts | AES-256 SSE explicit | Yes | Glue job source code and utility zip |
| Athena Results | AES-256 SSE explicit | Yes | Query result CSVs; also enforced by workgroup `encryption_configuration` |
| Logs | AES-256 SSE (account default) | No | S3 server-access logs and Spark UI logs; lower sensitivity |

| Layer | Mechanism | Applied to |
|---|---|---|
| At rest | AES-256 SSE-S3 | All objects in all four buckets |
| In transit | TLS-only bucket policy (Deny on `aws:SecureTransport = false`) | All requests to all four buckets |
