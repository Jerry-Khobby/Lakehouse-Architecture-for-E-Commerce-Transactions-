# Medallion Architecture — Storage Zones and Data Lifecycle

## What the Medallion Architecture Is

The Medallion Architecture (also called multi-hop or layered architecture) organises a Lakehouse into discrete zones. Data moves through each zone as it becomes progressively more reliable, structured, and analytics-ready. Each zone has a single, well-defined responsibility, which means a problem at one layer can be investigated and replayed without touching the others.

This project implements the pattern across **five S3 prefixes** within a single data bucket, each with a distinct purpose, access pattern, and lifecycle policy. The classic three-layer model (Bronze → Silver → Gold) maps to how data flows through this pipeline.

---

## The Five Zones at a Glance

```
s3://ecom-lakehouse-dev-data-<account>/
│
├── raw/                   ← BRONZE: Source files land here, untouched
├── lakehouse-dwh/         ← SILVER: Cleaned, validated Delta tables
├── archived/              ← Operational: Source files after successful ingestion
├── rejected/              ← Operational: Rows that failed validation
└── flagged/               ← Operational: Rows that pass but need analyst review
```

The analytics (Gold) layer is not a separate S3 prefix — it is Amazon Athena querying `lakehouse-dwh/` through the Glue Data Catalog. The Gold layer is a compute lens over Silver data, not a storage copy.

---

## Layer 1 — Bronze: `raw/`

### What It Is

The raw zone is the immutable landing area. Files arrive here exactly as the ingestion script uploads them — no transformation, no validation, no filtering. It is the ground truth of what was received and when.

### What Lives Here

| File | Uploaded by | S3 key |
|---|---|---|
| `products.csv` | `ingest.py` / `ingest_may_2025.py` | `raw/products.csv` |
| `orders_apr_2025.csv` | `ingest.py` | `raw/orders_apr_2025.csv` |
| `order_items_apr_2025.csv` | `ingest.py` | `raw/order_items_apr_2025.csv` |
| `orders_may_2025.csv` | `ingest_may_2025.py` | `raw/orders_may_2025.csv` |
| `order_items_may_2025.csv` | `ingest_may_2025.py` | `raw/order_items_may_2025.csv` |

### Why It Exists

The raw zone serves two purposes. First, it is the trigger for the pipeline: `ingest.py` uploads all three files for a batch, then fires a single Step Functions execution with the S3 keys as input. The Glue jobs read their source files from the keys passed in the execution input — they do not scan for new files; they are told exactly which file to process.

Second, it is the safety net. If a Glue job fails after reading but before the Delta MERGE commits, the source file is still in `raw/`. The pipeline can be re-run against the same file without re-uploading. The file is only removed from `raw/` after a successful Delta commit, in the `archive_source_file()` call at the very end of each job.

### How Data Enters This Zone

`ingestion/ingest.py` (for April) and `ingestion/ingest_may_2025.py` (for May) call `upload_dataset()` in `pipeline.py`, which:
- For `.csv` files: reads the bytes directly and calls `s3_client.put_object`.
- For `.xlsx` files: converts the active sheet to CSV in memory using `openpyxl` → `csv.writer` → UTF-8 bytes, then calls `s3_client.put_object`. No intermediate file is written to disk.

### Lifecycle Policy

Raw files are transitioned to S3 Infrequent Access storage class after 30 days. In normal operation a file stays in `raw/` for only minutes (the duration of one Glue job run) before being moved to `archived/`. The IA transition exists as a cost backstop in case a file is never archived due to a persistent pipeline failure.

---

## Layer 2 — Silver: `lakehouse-dwh/`

### What It Is

The processed zone is where clean, validated, deduplicated, typed data lives in ACID Delta Lake format. This is the authoritative store — the single version of truth for downstream analytics.

### What Lives Here

```
lakehouse-dwh/
├── products/
│   ├── _delta_log/           ← Delta transaction log (JSON commit files)
│   ├── department=bakery/
│   ├── department=beverages/
│   ├── department=dairy eggs/
│   ├── department=deli/
│   ├── department=frozen/
│   ├── department=meat seafood/
│   ├── department=pantry/
│   ├── department=personal care/
│   ├── department=produce/
│   └── department=snacks/
│
├── orders/
│   ├── _delta_log/
│   ├── date=2025-04-01/
│   ├── date=2025-04-02/
│   ├── ...
│   └── date=2025-05-31/
│
└── order_items/
    ├── _delta_log/
    ├── date=2025-04-01/
    ├── date=2025-04-02/
    ├── ...
    └── date=2025-05-31/
```

Each directory under a partition key contains one or more Parquet files. The `_delta_log/` directory contains numbered JSON files (one per transaction) that record every MERGE, INSERT, and schema change, providing a full audit trail and enabling time travel.

### Why Three Tables, Not One

The three tables reflect the three domain entities and their distinct access patterns:

**`products`** is a **dimension table** — a slowly changing reference catalogue. It has 1,000 rows and changes infrequently. It is partitioned by `department` because analytical queries almost always filter by department. The MERGE condition for products updates a row only if an attribute actually changed:
```python
.whenMatchedUpdateAll(condition=(
    "source.department_id <> target.department_id "
    "OR source.department <> target.department "
    "OR source.product_name <> target.product_name"
))
```
Re-running an identical file is a true no-op — no new Delta log entry is produced, no data is rewritten.

**`orders`** is a **fact table** — append-dominant, partitioned by `date`. It grows with every batch. The MERGE uses a timestamp guard so that a re-delivered older batch file cannot overwrite a newer record that is already committed:
```python
.whenMatchedUpdateAll(condition="source.order_timestamp > target.order_timestamp")
```

**`order_items`** is a **fact table** with a composite primary key (`id`, `order_id`). It is the largest table and has the most complex validation: in addition to its own field-level rules, it cross-references both `products` (via `product_id`) and `orders` (via `order_id`) against the Delta tables committed in the same execution. The same timestamp guard applies for updates.

### What the Glue Jobs Do Before Writing Here

Each job runs a staged validation pipeline. Every stage that finds bad rows writes those rows to `rejected/` before removing them from the working DataFrame. The order of checks within a stage matters — a null primary key is checked before type casting, because a null ID row with an unparseable timestamp should be rejected for the PK reason, not for the timestamp reason.

**Products validation stages (in order):**
1. Null `product_id` → reject as `null_product_id`
2. Null `department_id`, `department`, `product_name` → reject as `null_required_field:<col>`
3. `product_id ≤ 0` or `department_id ≤ 0` → reject as `invalid_id_value`
4. Empty or whitespace-only `department`, `product_name` → reject as `empty_string_field:<col>`
5. Intra-batch duplicates on `product_id` → reject older rows as `intra_batch_duplicate`

**Orders validation stages (in order):**
1. Null `order_id` → reject as `null_order_id`
2. Null `user_id` → reject as `null_user_id`
3. Null `total_amount` → reject as `null_total_amount`
4. Cast `total_amount` to `Decimal(12,2)` — cast failure → reject as `invalid_total_amount_format`
5. Negative `total_amount` → reject as `negative_total_amount`
6. Soft flag: `total_amount > 1,000,000` → write to `flagged/orders/` (not rejected — kept for analyst review)
7. Cast `order_timestamp` to Timestamp with format `yyyy-MM-dd'T'HH:mm:ss` — cast failure → reject as `invalid_timestamp_format`
8. Future timestamp (`> now + 1 hour`) → reject as `future_timestamp`
9. `date` column parsed and compared against timestamp-derived date — mismatch → reject as `date_timestamp_mismatch`
10. Intra-batch duplicate `order_id` → reject older rows as `intra_batch_duplicate`

**Order items validation stages (in order):**
1. Null composite key (`id` or `order_id`) → reject as `null_composite_key`
2. Null required fields → reject as `null_required_field:<col>`
3. Cast `id` and `product_id` to integers — failure → reject as `invalid_id_format`
4. `reordered` flag must be 0 or 1 → reject as `invalid_reordered_flag`
5. `add_to_cart_order` must be > 0 → reject as `invalid_add_to_cart_order`
6. `days_since_prior_order` must be 0–365 when non-null → reject as `invalid_days_since_prior`
7. Cast and validate `order_timestamp` (same format rule as orders) → `invalid_timestamp_format` / `future_timestamp`
8. Date consistency check → `date_timestamp_mismatch`
9. Referential integrity: `product_id` must exist in `lakehouse-dwh/products/` Delta table → reject orphans as `invalid_product_id`
10. Referential integrity: `order_id` must exist in `lakehouse-dwh/orders/` Delta table → reject orphans as `invalid_order_id`
11. Intra-batch dedup on `(id, order_id)` by `order_timestamp` → `intra_batch_duplicate`

The fact that `order_items` joins against live Delta tables is why the Step Functions execution is strictly ordered. `products` must be committed before `orders` runs; both must be committed before `order_items` runs. This is not a convention — it is enforced by the state machine's linear graph.

### Partitioning Strategy

**Products → `department`**: 10 distinct values, low cardinality. Queries that filter `WHERE department = 'produce'` skip 9 out of 10 partition directories entirely.

**Orders → `date`**: high cardinality (one partition per calendar day), but analytical queries almost always filter by date range (`WHERE date BETWEEN '2025-05-01' AND '2025-05-31'`). Athena reads only the required date partitions.

**Order items → `date`**: same date partitioning. Because order items are joined to orders on `order_id`, queries that filter orders by date usually apply the same date filter to order_items, making both partition prunes effective simultaneously.

### Delta Table Initialisation

On the very first run, `ensure_delta_table()` in `common.py` checks whether a Delta table already exists at the target path using `DeltaTable.isDeltaTable(spark, path)`. If it does not exist, it writes an empty DataFrame with the full declared schema to seed the `_delta_log/`. This is required because `DeltaTable.merge()` requires the target table to exist — it cannot create the table and merge in the same operation.

Every subsequent run skips this check with no I/O cost because `isDeltaTable()` reads a single S3 HEAD request against `_delta_log/`.

### Idempotency

Running the same batch file twice against the Silver layer is safe:
- Products: the change-detection condition means an identical file produces a no-op MERGE with no new Delta log entry.
- Orders and order_items: the timestamp guard means re-delivered older records do not overwrite newer ones. Identical records hit the MATCHED branch but the `source.order_timestamp > target.order_timestamp` condition is false, so no update is written.

This means the pipeline can be safely re-triggered after a failure without data corruption.

---

## Layer 3 — Gold: Athena over `lakehouse-dwh/`

### What It Is

The Gold layer is the analytics-facing view of the Silver data. In this architecture it is not a separate physical copy — it is Amazon Athena querying the Delta Parquet files in `lakehouse-dwh/` through the Glue Data Catalog. The Glue Data Catalog acts as the metadata bridge between the physical files and Athena's SQL engine.

### How the Catalog Bridge Works

Each Glue job calls `update_catalog_table()` at the end of every successful run:

```sql
CREATE TABLE IF NOT EXISTS `ecom_lakehouse_db`.`orders`
USING DELTA
LOCATION 's3://ecom-lakehouse-dev-data-<account>/lakehouse-dwh/orders/'
```

Athena's Federated Query engine (version 3) reads Delta tables natively. When an Athena query arrives, it:
1. Looks up the table in `ecom_lakehouse_db` and gets the S3 location.
2. Reads the Delta transaction log at `_delta_log/` to find the current snapshot — which Parquet files belong to the current version, and which were removed by prior compactions or overwrites.
3. Scans only the Parquet files in the current snapshot, respecting partition pruning based on the `WHERE` clause.

Because Athena reads the Delta log directly, the catalog entry does not need to be updated after every schema change — schema is always derived from the live Delta log.

### The AthenaValidation Step

Every Step Functions execution ends with an Athena smoke-test query before sending the success notification:

```sql
SELECT 'products' AS tbl, COUNT(*) AS row_count FROM ecom_lakehouse_db.products
UNION ALL SELECT 'orders', COUNT(*) FROM ecom_lakehouse_db.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM ecom_lakehouse_db.order_items;
```

This is a structural integrity gate. If the Glue jobs committed data but the catalog registration failed (a known failure mode when Lake Formation permissions are misconfigured), this query fails, which routes the execution to `NotifyFailure`. The pipeline is not declared successful until Athena can actually see and count rows in all three tables.

---

## Operational Zones

### `archived/`

After a successful Delta MERGE, `archive_source_file()` moves the source file from `raw/` to `archived/` using `boto3.copy_object` followed by `boto3.delete_object`. The destination key includes the dataset name, run date, and original filename:

```
archived/orders/2025-06-15/orders_may_2025.csv
```

This preserves a copy of exactly what was ingested and when. The archive zone is queryable — S3 Select or a separate Glue crawler could serve as a point-in-time audit trail independent of the Delta transaction log.

Lifecycle policy: IA after 30 days, Glacier after 90 days. This balances audit access speed against cost for data that is rarely queried after the first few weeks.

The archive step is intentionally non-fatal. If `copy_object` or `delete_object` fails (e.g. transient S3 throttling), `archive_source_file()` logs the exception but does not re-raise it. A failed archive should not mark an otherwise successful pipeline run as failed, because the Delta MERGE already committed. The file remains in `raw/` and the next pipeline run will process it again — but because of idempotency, this is safe.

### `rejected/`

Rows that fail any validation check are written here as Parquet with three audit columns appended:

| Column | Content |
|---|---|
| `rejection_reason` | String identifier for the failing check (e.g. `invalid_timestamp_format`, `invalid_order_id`) |
| `_rejected_at` | Timestamp of when the row was written to `rejected/` |
| `_job_run_id` | UTC timestamp of the job run (format `20260615T134313`) |
| `_source_key` | S3 key of the source file that contained this row |

Path pattern:
```
rejected/orders/2025-06-15/20260615T134313/
rejected/order_items/2025-06-15/20260615T134313/
```

Rejected rows are Parquet (not CSV), so they are directly queryable by Athena:
```sql
SELECT rejection_reason, COUNT(*) AS rejected_count, _source_key
FROM "s3://ecom-lakehouse-dev-data-<account>/rejected/orders/"
GROUP BY rejection_reason, _source_key
ORDER BY rejected_count DESC;
```

Lifecycle policy: rejected records expire after 60 days. They are not archived to Glacier because their primary use is operational investigation in the days immediately after a failed batch.

### `flagged/`

The flagged zone handles a different class of data: rows that pass all hard validation rules but contain values that warrant analyst review before being trusted for business decisions. The current rule is `total_amount > 1,000,000` for orders — technically valid (non-null, positive, parseable) but anomalous for a consumer e-commerce order.

Flagged rows are written to `flagged/orders/<run_id>/` as Parquet with a `flag_reason` column. Critically, they are **also** written to the Delta table as part of the valid batch. The flag is a soft warning, not a rejection. An analyst can query `flagged/` to review the rows and confirm or override them without the pipeline having withheld the data.

Lifecycle policy: flagged records expire after 90 days, giving analysts more time to review them than the 60-day rejection window, since the analytical workload is lower-urgency.

---

## Data Lifecycle Summary

| Zone | What enters | What exits | Retention |
|---|---|---|---|
| `raw/` | Source CSVs from `ingest.py` | Moved to `archived/` after successful Glue job | IA after 30d |
| `lakehouse-dwh/` | Valid rows via Delta MERGE | Never deleted (Delta log retains all versions) | Indefinite |
| `archived/` | Source CSVs moved from `raw/` after successful MERGE | Not deleted automatically | IA → Glacier |
| `rejected/` | Rows that failed any validation check | Expired after 60 days | 60 days |
| `flagged/` | Rows that pass validation but exceed soft thresholds | Expired after 90 days | 90 days |

---

## Why This Design, Not a Simpler One

**Why not write directly to Delta from `ingest.py`?**
`ingest.py` runs on a developer workstation or CI runner. It has minimal compute and no Spark runtime. Delta writes require the full Spark + Delta Lake stack, which is what Glue provides. Separating upload (lightweight, any machine) from transform (heavy, Glue cluster) means the ingestion script is simple, testable, and fast.

**Why not one big Delta table instead of three?**
Products, orders, and order_items have fundamentally different update patterns (dimension vs. fact), different primary keys, different partition strategies, different validation rules, and different foreign-key relationships. A single flat table would either denormalise all three (massive row duplication) or require complex struct columns (poor Athena compatibility). Three tables mapped to three Glue jobs makes each job independently testable, independently retryable, and independently queryable.

**Why not separate S3 buckets for each zone?**
Keeping all zones in one bucket simplifies the IAM policy surface: the Glue job role needs read/write on one bucket ARN, not five. Cross-bucket `copy_object` operations (for archiving) require source-bucket read and destination-bucket write permissions separately, adding IAM complexity. Prefix-level separation within one bucket achieves the same logical isolation at lower operational overhead. Lifecycle rules apply per-prefix, so cost control is identical.

**Why not use Hive-format tables instead of Delta?**
Hive tables on S3 are not ACID. A failed mid-write Glue job leaves partial Parquet files that Athena will happily query, returning corrupt or incomplete results with no error. Delta's transaction log guarantees that Athena only ever sees committed versions. The MERGE operation specifically requires Delta — Hive tables support INSERT OVERWRITE of a partition but not row-level upsert with a timestamp guard. Without Delta, implementing idempotent upserts would require reading the entire target partition, merging in memory, and overwriting — which is expensive and not atomic.
