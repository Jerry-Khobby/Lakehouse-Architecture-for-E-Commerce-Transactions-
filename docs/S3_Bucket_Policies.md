# S3 Bucket Policies — TLS Enforcement and Public Access Controls

## Overview

This project applies two complementary layers of S3 access control that operate independently: bucket policies (which enforce TLS-only access) and public access block settings (which prevent public exposure). Both are applied to all four S3 buckets. This document explains what each control does, why both are needed, and how they coexist without conflicting.

---

## The TLS-Only Bucket Policy

```hcl
locals {
  tls_only_buckets = {
    data           = { id = aws_s3_bucket.data.id,           arn = aws_s3_bucket.data.arn }
    scripts        = { id = aws_s3_bucket.scripts.id,        arn = aws_s3_bucket.scripts.arn }
    athena_results = { id = aws_s3_bucket.athena_results.id, arn = aws_s3_bucket.athena_results.arn }
    logs           = { id = aws_s3_bucket.logs.id,           arn = aws_s3_bucket.logs.arn }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = local.tls_only_buckets
  bucket   = each.value.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = {
        Bool = { "aws:SecureTransport" = "false" }
      }
    }]
  })
}
```

### What This Policy Does

The policy contains a single `Deny` statement with a condition. When an S3 API call arrives with `aws:SecureTransport = false` — meaning the request was made over plain HTTP rather than HTTPS — the policy denies the request before IAM evaluation proceeds. The denial applies to every S3 action (`s3:*`) on every object in the bucket (`arn/*`) and the bucket itself (`arn`).

`Principal = "*"` means the denial applies to every identity — IAM users, IAM roles, the AWS Glue service, the Step Functions service, anonymous requests, and even the bucket owner. There are no exceptions. A request made over HTTP is denied regardless of who is making it.

`Effect = "Deny"` — this is a hard denial, not a conditional allow. In AWS IAM evaluation logic, an explicit `Deny` overrides any `Allow`. Even if a principal has a fully permissive IAM policy (`s3:*` on `*`), this bucket policy `Deny` overrides it.

### Why HTTP Requests Are a Real Risk

S3 supports both HTTP and HTTPS endpoints. The HTTPS endpoint is `https://bucket-name.s3.region.amazonaws.com/key`. The HTTP endpoint is `http://bucket-name.s3.region.amazonaws.com/key`. AWS does not disable the HTTP endpoint by default.

If a tool, library, or misconfigured script makes an S3 request over HTTP rather than HTTPS:

- The data (CSV files, Parquet files, rejected records, query results) travels over the network unencrypted.
- The AWS credentials used to sign the request are transmitted in plaintext in the `Authorization` header.
- A network observer (on a shared VPC, a compromised router, or a cloud provider internal network position) can read the data and steal the credentials.

The server-side encryption configured on the buckets (AES-256) protects data at rest — on the S3 disks — but does nothing for data in transit. The TLS policy fills the in-transit gap.

### The Resource Covers Both Bucket and Object ARNs

```hcl
Resource = [each.value.arn, "${each.value.arn}/*"]
```

`each.value.arn` (the bare bucket ARN, e.g. `arn:aws:s3:::ecom-lakehouse-dev-data-123456789012`) covers bucket-level actions: `s3:ListBucket`, `s3:GetBucketLocation`, `s3:GetBucketPolicy`, `s3:PutBucketPolicy`, etc.

`"${each.value.arn}/*"` covers object-level actions: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, etc.

If only the object ARN were specified, a plaintext `ListBucket` request would not be denied (bucket-level action, not an object-level action). Including both ARNs ensures the policy covers the complete S3 API surface.

### Why a `Deny` Instead of a Conditional `Allow`

An alternative approach would be an `Allow` policy with `aws:SecureTransport = true` as a condition:

```json
{
  "Effect": "Allow",
  "Principal": "*",
  "Action": "s3:*",
  "Condition": { "Bool": { "aws:SecureTransport": "true" } }
}
```

This would only allow requests made over HTTPS. However, `Allow` in a bucket policy does not add permissions — IAM policies are still the primary mechanism for granting access. A bucket policy `Allow` only makes a difference for cross-account access or anonymous access. For same-account access, a bucket policy `Allow` for `Principal = "*"` grants public read — exactly what the public access blocks are designed to prevent.

The `Deny` approach is the correct pattern. It adds a restriction on top of any existing `Allow` grants rather than attempting to grant permissions that IAM already controls. The `Deny` applies unconditionally to all principals and coexists safely with the public access block settings.

---

## Public Access Block Settings

All four buckets have all four public access block settings enabled:

```hcl
resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
```

### What Each Setting Does

**`block_public_acls = true`:** Rejects any `PutBucketAcl` or `PutObjectAcl` call that would grant public access. Even if an IAM principal with `s3:PutBucketAcl` permission tries to set the bucket ACL to `public-read`, the request is rejected before it takes effect.

**`block_public_policy = true`:** Rejects any `PutBucketPolicy` call that would grant public access (i.e., a policy with `"Principal": "*"` and an `Allow` effect). This prevents the TLS policy itself from being accidentally replaced with a policy that opens the bucket publicly. Note: the TLS policy contains `"Principal": "*"` with a `Deny` effect — a `Deny` for all principals is not considered a "public access" grant because it restricts rather than opens access. AWS evaluates whether a policy grants public access based on `Allow` statements, not `Deny`.

**`ignore_public_acls = true`:** If any object already has a public ACL (from before the block settings were applied, or applied by a tool that bypassed the block), those ACLs are ignored. Requests that would be permitted by the public ACL are treated as if the ACL does not exist.

**`restrict_public_buckets = true`:** Restricts access to the bucket so that only the AWS account that owns the bucket and authorized AWS services can access it, regardless of any `Allow` statement in the bucket policy. This is the most aggressive of the four settings — it overrides bucket policies that would otherwise grant cross-account access.

### Why All Four Settings Are Needed

Each setting addresses a different attack surface:

| Setting | Blocks |
|---|---|
| `block_public_acls` | Future attempts to set public ACLs via API |
| `block_public_policy` | Future attempts to set public bucket policies |
| `ignore_public_acls` | Existing public ACLs already on objects/bucket |
| `restrict_public_buckets` | Cross-account policy grants that could expose data |

Enabling three of the four but not the fourth leaves a gap. For example, without `ignore_public_acls`, an object uploaded before the block settings were applied with `x-amz-acl: public-read` remains publicly readable even after the block is enabled.

---

## How TLS Policy and Public Access Blocks Coexist

The two controls are independent and complementary. They operate at different layers:

**Public access blocks** operate at the S3 control plane. They prevent the bucket from entering a "publicly accessible" state — they block policy and ACL changes that would grant public access.

**The TLS bucket policy** operates at request evaluation time. It evaluates conditions on live requests and denies them if `aws:SecureTransport = false`.

A request to the bucket from an IAM-authenticated principal goes through this evaluation:

```
Incoming S3 request
        │
        ▼
S3 checks public access block settings
(Is this request from a public/anonymous source? Is the bucket's public state being modified?)
        │
        ▼
Bucket policy evaluation (TLS policy)
Is aws:SecureTransport = false?
  YES → Deny (request rejected, never reaches IAM)
  NO  → continue
        │
        ▼
IAM policy evaluation
Does the principal have the required s3:Action permission?
  NO  → Deny
  YES → Allow
        │
        ▼
Request succeeds
```

The public access blocks and the TLS policy operate at different points in this chain. They do not interfere with each other. A valid HTTPS request from an authenticated IAM principal with appropriate IAM permissions passes through both controls without conflict.

### The TLS Policy Does Not Grant Public Access

The TLS policy has `Principal = "*"` and `Effect = "Deny"`. The `block_public_policy` setting rejects `PutBucketPolicy` calls that would grant public access. AWS evaluates whether a policy grants public access based on `Allow` statements with `Principal = "*"` or `Principal = {"AWS": "*"}`.

A `Deny` statement with `Principal = "*"` is not a public grant — it is a universal restriction. AWS's `block_public_policy` check does not block `Deny` policies for all principals. This means the `aws_s3_bucket_policy.tls_only` Terraform resource applies successfully even with `block_public_policy = true` on the bucket.

---

## Why These Controls Apply to the Logs Bucket Too

The logs bucket receives S3 server-access logs from the data bucket. Log delivery is performed by an AWS-internal S3 log delivery service principal, not by an IAM role. The TLS policy and public access blocks apply to the logs bucket for the same reasons they apply to all other buckets:

- Access logs contain request metadata including IP addresses, object keys, and request sizes. This is sensitive operational data.
- The logs bucket has `log-delivery-write` ACL, which allows the S3 log delivery service to write objects. Without `block_public_acls`, a misconfigured `PutBucketAcl` call could expand this ACL to public access.
- S3 access log delivery uses HTTPS internally — the TLS policy does not block legitimate log delivery.

Applying the same policy uniformly across all four buckets rather than selectively (e.g. "logs don't need TLS enforcement") follows the principle of consistent security posture. The cost of applying these controls is zero; the risk of omitting them for "low-value" buckets creates inconsistency that is easy to exploit.
