# AWS Glue Data Catalog — Database Setup, Crawlers, and Table Registration

## Overview

The Glue Data Catalog is the metadata store that connects the pipeline's physical Delta Lake tables on S3 to the analytical layer in Athena. It holds database and table definitions — schema, location, format, partition layout — that Athena reads to resolve a SQL table reference like `ecom_lakehouse_db.orders` into a set of S3 Parquet files to scan. This document covers the catalog database configuration, the Lake Formation permission model layered on top of it, how each Glue job registers its table at runtime via Spark SQL, the three on-demand crawlers and how they differ from the primary registration path, and a one-time cleanup mechanism for stale catalog entries.

---

## The Catalog Database

```hcl
resource "aws_glue_catalog_database" "lakehouse" {
  name        = var.glue_database_name         # "ecom_lakehouse_db"
  description = "E-commerce lakehouse Delta Lake tables — ${var.environment}"
  catalog_id  = local.account_id

  location_uri = "s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}"

  create_table_default_permission {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}
```

### `location_uri` — the Non-Obvious Required Field

The `location_uri` field on the Glue database is frequently omitted in tutorials because it is optional for most catalog use cases. For this pipeline it is mandatory.

When a Glue job calls `update_catalog_table()`, which executes:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{database}`.`{table_name}`
    USING DELTA
    LOCATION '{table_path}'
""")
```

The DeltaCatalog connector (registered via `spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog`) constructs a Hadoop `Path` from the parent database's `LocationUri` before placing the external table definition. Even though the table has an explicit `LOCATION`, Spark validates that the database's `LocationUri` is a well-formed URI first.

Without `location_uri`, the Glue catalog database returns an empty string for `LocationUri`. Spark's Hadoop `Path` constructor raises:

```
IllegalArgumentException: Can not create a Path from an empty string
```

The `update_catalog_table()` call fails, the table is never registered, and the `AthenaValidation` state at the end of the Step Functions execution fails with `TABLE_NOT_FOUND: ecom_lakehouse_db.orders`.

Setting `location_uri` to the processed-data prefix resolves this. The path does not need to be the table's exact location — it just needs to be a valid S3 URI that the database can use as its base. The table-level `LOCATION` overrides it for actual data reads.

### `create_table_default_permission`

```hcl
create_table_default_permission {
  permissions = ["ALL"]
  principal {
    data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
  }
}
```

`IAM_ALLOWED_PRINCIPALS` is a special Lake Formation principal meaning "any principal that has IAM permission to perform this action." Setting this as the default table permission makes new tables readable by any IAM principal that has the appropriate IAM policy — without requiring an explicit Lake Formation grant for every new table.

This default is the Lake Formation "IAM passthrough" mode for tables. It is appropriate here because the explicit LF grants below cover the specific roles that need access (the Glue job role and the Step Functions role). Any other principal that might need to query tables (e.g. a data analyst's IAM user) must have IAM permissions and will be covered by this default without requiring a Terraform change.

---

## Lake Formation — The Permission Layer

AWS Lake Formation adds a second permission check on top of IAM for Glue Data Catalog resources. When Athena queries `ecom_lakehouse_db.orders`, it evaluates:

1. **Lake Formation**: Does the calling principal have `DESCRIBE` on the database and `SELECT` on the `orders` table?
2. **IAM**: Does the calling principal have `glue:GetDatabase`, `glue:GetTable`, and `s3:GetObject` on the relevant resources?

Both checks must pass. A principal with full IAM permissions but no LF grant receives `AccessDeniedException`. A principal with LF grants but no IAM permissions also fails. They are independent and additive.

### Setting the Lake Formation Admin

```hcl
data "aws_iam_session_context" "current" {
  arn = data.aws_caller_identity.current.arn
}

resource "aws_lakeformation_data_lake_settings" "account" {
  catalog_id = local.account_id
  admins     = [data.aws_iam_session_context.current.issuer_arn]

  create_database_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }

  create_table_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
}
```

`aws_lakeformation_permissions` resources can only be applied by a Lake Formation admin. Without this resource, every `aws_lakeformation_permissions` grant in the Terraform state fails with `AccessDeniedException` regardless of the IAM permissions held by the Terraform caller.

**`aws_iam_session_context`** — the non-obvious data source: `data.aws_caller_identity.current.arn` returns the assumed-role ARN in its full session form:

```
arn:aws:sts::123456789012:assumed-role/MyRole/session-name
```

Lake Formation rejects assumed-role ARNs with session suffixes as admin principals. `aws_iam_session_context` strips the session portion and returns the stable role ARN:

```
arn:aws:iam::123456789012:role/MyRole
```

This is the form Lake Formation accepts. Without the `aws_iam_session_context` data source, the `admins` list contains the session ARN and the `terraform apply` fails on every `aws_lakeformation_permissions` resource.

**`create_database_default_permissions` and `create_table_default_permissions`** set `IAM_ALLOWED_PRINCIPALS / ALL` as the default for all new databases and tables. This mirrors the catalog-level defaults, ensuring that the LF control plane does not block IAM-authorised access to resources that don't have explicit LF grants.

### Glue Job Role Grants

The Glue job role needs two Lake Formation grants to run `update_catalog_table()`:

**Database — CREATE_TABLE and DESCRIBE:**

```hcl
resource "aws_lakeformation_permissions" "glue_role_database" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["CREATE_TABLE", "DESCRIBE"]

  database {
    name = aws_glue_catalog_database.lakehouse.name
  }
}
```

`CREATE_TABLE` allows the Glue job to call `glue:CreateTable` through the Lake Formation check. Without it, `spark.sql("CREATE TABLE IF NOT EXISTS ...")` fails with LF `AccessDeniedException` even if the IAM policy on `glue_role` includes `glue:CreateTable`.

`DESCRIBE` allows the role to read the database metadata — its `LocationUri`, default permissions, and table list. The DeltaCatalog connector reads the database record at catalog registration time.

**Tables — ALL (wildcard):**

```hcl
resource "aws_lakeformation_permissions" "glue_role_alter_tables" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.lakehouse.name
    wildcard      = true
  }
}
```

`ALL` on all tables in the database covers: `SELECT` (read the data), `INSERT` (write partitions), `DELETE` (remove superseded partitions), `DESCRIBE` (read schema), `ALTER` (modify the table definition on schema evolution). The Glue MERGE operation adds new partition metadata to the catalog — without `ALTER`, partition discovery after a MERGE fails silently.

`wildcard = true` applies to all current and future tables in the database. Adding a new dataset (e.g. `returns`) does not require a new Terraform `aws_lakeformation_permissions` resource.

### Step Functions Role Grants

The SFN role runs the `AthenaValidation` query. Athena resolves the query through the catalog using the **caller's** permissions — not the Glue job role's permissions. The SFN role therefore needs its own catalog grants.

**Database — DESCRIBE:**

```hcl
resource "aws_lakeformation_permissions" "sfn_describe_database" {
  principal   = aws_iam_role.sfn_role.arn
  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.lakehouse.name
  }
}
```

Allows Athena to look up `ecom_lakehouse_db` and confirm it exists. Without this, Athena's catalog resolution fails before it evaluates any table-level permissions.

**Tables — SELECT and DESCRIBE (wildcard):**

```hcl
resource "aws_lakeformation_permissions" "sfn_select_tables" {
  principal   = aws_iam_role.sfn_role.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = aws_glue_catalog_database.lakehouse.name
    wildcard      = true
  }
}
```

`SELECT` allows Athena to read rows from all three tables. `DESCRIBE` allows Athena to read each table's schema to plan the query execution. Without `DESCRIBE`, Athena cannot determine the column types and rejects the query at the planning stage.

All four `aws_lakeformation_permissions` resources depend on `aws_lakeformation_data_lake_settings.account`:

```hcl
depends_on = [aws_lakeformation_data_lake_settings.account]
```

This ensures the LF admin is registered before any grant is applied. Without the dependency, Terraform may attempt to create grants before the admin is set, and those grants fail with `AccessDeniedException`.

---

## Glue Data Catalog IAM Policy

Separately from Lake Formation, the Glue job role holds an IAM policy that permits the API calls the DeltaCatalog connector makes:

```hcl
resource "aws_iam_role_policy" "glue_catalog" {
  policy = jsonencode({
    Statement = [{
      Action = [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:CreateDatabase",
        "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:CreatePartition", "glue:UpdatePartition", "glue:BatchCreatePartition"
      ]
      Resource = [
        "arn:aws:glue:<region>:<account>:catalog",
        "arn:aws:glue:<region>:<account>:database/${var.glue_database_name}",
        "arn:aws:glue:<region>:<account>:table/${var.glue_database_name}/*"
      ]
    }]
  })
}
```

These IAM permissions and the Lake Formation grants above both apply simultaneously. The same `glue:CreateTable` call must pass both the IAM policy check and the Lake Formation `CREATE_TABLE` grant check. Tightening either one blocks catalog registration.

The resource ARNs are scoped to the specific database and its tables — not `"*"`. A compromised Glue job role cannot create tables in other Glue databases or read metadata from unrelated projects in the same account.

---

## Spark SQL Table Registration — `update_catalog_table()`

The primary catalog registration mechanism in this pipeline is `update_catalog_table()` in `glue_jobs/utils/common.py`, called at the end of each Glue job run after the Delta MERGE commits:

```python
def update_catalog_table(args, table_name, table_path, spark=None):
    database = args["DATABASE_NAME"]
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    full_table = f"`{database}`.`{table_name}`"

    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table}
            USING DELTA
            LOCATION '{table_path}'
        """)
        logger.info("Catalog table registered: %s", full_table)
    except Exception:
        logger.exception("Catalog registration failed for %s", full_table)
        raise
```

### How `USING DELTA LOCATION` Works

`CREATE TABLE ... USING DELTA LOCATION` is processed by the DeltaCatalog connector (`spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog`). It does the following:

1. Reads the Delta transaction log at `<table_path>/_delta_log/` to determine the current schema — column names, data types, nullability, and partition columns.
2. Translates the Delta schema into a Glue table definition with the appropriate SerDe (`SymlinkTextInputFormat` for Hive-compatible tools, or native Delta format for Athena engine version 3).
3. Calls `glue:CreateTable` (or no-ops if the table already exists, because of `IF NOT EXISTS`).
4. The Glue table entry points to `<table_path>` and carries the schema read from the Delta log.

The schema in the catalog entry is always consistent with the actual data because it is derived from the Delta log — the same log that Athena engine version 3 reads when scanning the table. There is no manual schema definition step, no JSON schema file to maintain, and no risk of the catalog schema drifting from the data schema.

### Idempotency

`CREATE TABLE IF NOT EXISTS` is a no-op when the table already exists. Every subsequent pipeline run calls `update_catalog_table()` after the MERGE — the first call creates the table, every subsequent call silently does nothing. There is no `DROP TABLE` step before the `CREATE`, which means:

- A re-run never removes the existing table entry and re-registers it.
- If the Glue job crashes between the Delta MERGE commit and the catalog registration, the next run re-registers cleanly.
- Multiple pipeline runs on different batches all call the same `update_catalog_table()` — the catalog stays registered across all batches without any special handling.

The absence of `DROP TABLE` also means the Glue job role does not need `glue:DeleteTable` for normal operations — that action is only needed for the one-time cleanup described below.

### Why This Approach Over Crawlers as the Primary Path

Glue crawlers are the conventional way to register Delta tables in the Glue catalog. This pipeline uses Spark SQL registration instead for three reasons:

1. **Immediacy.** `update_catalog_table()` runs synchronously at the end of the Glue job. By the time the Step Functions state transitions to `AthenaValidation`, the catalog entry is already present. A crawler, by contrast, must be started separately and runs as an independent background process. If the crawler is started from Step Functions (not done in this pipeline), it adds 1–3 minutes of crawler startup and crawl time before the table is queryable.

2. **Dependency simplicity.** If crawlers were in the Step Functions state machine (they are not — see below), each crawler state would need its own retry policy, timeout, and failure branch. The Spark SQL approach folds registration into the job itself — one less state, one less failure mode.

3. **No schema drift risk.** A crawler re-reads the S3 location and infers the schema from the Parquet files. If a Parquet file has an unexpected column order or a subtle type difference, the crawler may infer a schema that differs from what the previous run registered. The Spark SQL approach always reads from the Delta log, which is the authoritative schema source and does not vary between runs.

---

## The Three Glue Crawlers

Three crawlers are provisioned as on-demand fallbacks — one per dataset:

```hcl
resource "aws_glue_crawler" "orders" {
  name          = "${local.name_prefix}-crawler-orders"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name

  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.data.id}/${var.processed_data_prefix}orders/"]
    write_manifest            = false
    create_native_delta_table = true
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  schedule = var.crawler_schedule != "" ? var.crawler_schedule : null
}
```

### `delta_target` Configuration

**`delta_tables`**: The S3 path to the Delta table root. The crawler reads `_delta_log/` at this path to discover the current snapshot schema and partition layout — the same read path that Athena uses. The path must include the trailing slash.

**`create_native_delta_table = true`**: Instructs the crawler to register the table in a format that Athena engine version 3 can read natively (as a Delta table, not a Parquet table). Without this, the crawler registers the table as a generic Parquet table pointing to the `_delta_log/` symlinks, and Athena reads all historical Parquet files rather than the current snapshot — producing duplicates.

**`write_manifest = false`**: Delta Lake can write a `_symlink_format_manifest/` alongside the Delta log for compatibility with tools that cannot read `_delta_log/` directly (older versions of Presto, Hive). This pipeline does not need symlink manifests — Athena engine version 3 reads the Delta log natively. Writing manifests is a write-amplification cost with no benefit here.

### `schema_change_policy`

```hcl
schema_change_policy {
  update_behavior = "UPDATE_IN_DATABASE"
  delete_behavior = "LOG"
}
```

**`UPDATE_IN_DATABASE`**: When the crawler detects that a new column has been added to the Delta table (schema evolution), it updates the Glue catalog table definition to include the new column. Without this, the catalog schema falls behind the data schema after any `ALTER TABLE ADD COLUMN` equivalent in Delta.

**`LOG`**: When the crawler detects that a partition or table appears to have been deleted from S3, it logs the event rather than deleting the catalog entry. This is the conservative choice — accidentally deleting a catalog table because a lifecycle rule moved some Parquet files to Glacier would be a disruptive false-positive. An operator reviews the log and decides whether the deletion is intentional.

### Partition Configuration

```hcl
configuration = jsonencode({
  Version = 1.0
  CrawlerOutput = {
    Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
  }
})
```

`InheritFromTable` means new partitions discovered by the crawler inherit the table's existing schema and format settings rather than being inferred independently from the partition's files. For the `orders` and `order_items` tables partitioned by `date`, this ensures all `date=` partitions have a consistent schema definition in the catalog even if individual partition Parquet files have slightly different row group sizes or statistics.

The `products` crawler adds `CombineCompatibleSchemas`:

```hcl
configuration = jsonencode({
  Version = 1.0
  CrawlerOutput = {
    Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
  }
  Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
})
```

`CombineCompatibleSchemas` tells the crawler to merge schemas from multiple S3 locations into a single table definition rather than creating separate table entries for each compatible-schema location. This matters for `products` because the `department` partition has 10 sub-directories — without combining, the crawler might create 10 separate table entries (one per department partition directory). Combining produces one catalog table with all 10 department partitions.

### Schedule

```hcl
schedule = var.crawler_schedule != "" ? var.crawler_schedule : null
```

`var.crawler_schedule` is an optional cron expression (e.g. `"cron(0 2 * * ? *)"` for 2am UTC daily). If set, crawlers run on that schedule independently of the pipeline. If the variable is empty (the default), the crawlers have no schedule and run only on explicit `StartCrawler` API calls.

In the current Terraform configuration, `var.crawler_schedule` defaults to `""` — crawlers run on demand only.

### Crawlers Are Not in the Step Functions State Machine

The Step Functions state machine does not include crawler states. The README diagram shows `RunCrawlers (×3)` as a step between `RunOrderItemsJob` and `AthenaValidation`, but the actual `step_functions.tf` goes directly from `RunOrderItemsJob` to `AthenaValidation`. The crawlers exist in Terraform and can be started manually; they are not part of the automated execution flow.

Catalog registration is handled by `update_catalog_table()` inside each Glue job, making the crawlers redundant for the automated path. They remain provisioned for two use cases:

1. **Recovery from catalog corruption.** If a manual `glue:DeleteTable` or a failed Terraform resource leaves the catalog in a bad state, a manual `aws glue start-crawler` resets the table definition from the actual Delta data on S3.

2. **Schema discovery after external changes.** If the Delta table schema changes outside the pipeline (e.g. a one-off Spark script adds a column), the crawler discovers and registers the new column without requiring a pipeline re-run.

---

## One-Time Catalog Cleanup Block

```hcl
resource "terraform_data" "drop_stale_catalog_tables" {
  triggers_replace = [timestamp()]

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = <<-EOT
      aws glue delete-table --database-name ${var.glue_database_name} --name products ...
      aws glue delete-table --database-name ${var.glue_database_name} --name orders ...
      aws glue delete-table --database-name ${var.glue_database_name} --name order_items ...
    EOT
  }

  depends_on = [aws_glue_catalog_database.lakehouse]
}
```

This block exists to handle a specific recovery scenario: when a previous failed pipeline run left corrupted catalog table entries — wrong SerDe configuration, missing `LOCATION` parameter, or Lake Formation contamination from a different permission setup — that prevent `CREATE TABLE IF NOT EXISTS` from registering the correct entry (because the table already "exists" in the catalog with a broken definition, so `IF NOT EXISTS` is a no-op and the bad entry persists).

`triggers_replace = [timestamp()]` causes this resource to be replaced on every `terraform apply`, running the `delete-table` commands each time. This is intentional for the cleanup window — the operator applies once to clear the stale entries, then `update_catalog_table()` in the next pipeline run creates correct entries.

**This block should be removed from `main.tf` after the first successful pipeline run confirms Athena can query all three tables.** Leaving it in place means every `terraform apply` deletes and recreates the catalog tables, which momentarily makes Athena queries fail and adds unnecessary API calls. The comment in the Terraform file marks it explicitly:

```hcl
# REMOVE THIS BLOCK after the pipeline has run successfully once and Athena
# queries confirm the tables are clean.
```

The `2>&1 | Out-Null` and `exit 0` at the end of each command ensure the provisioner does not fail when the table does not exist (e.g. on a fresh environment where the table was never created). `delete-table` returns an error for a non-existent table, which would cause the `local-exec` provisioner to fail and block `terraform apply`. The `2>&1 | Out-Null` swallows the error output and `exit 0` forces a success exit code regardless.

---

## Full Registration Flow Per Pipeline Run

Putting all layers together, here is what happens for the `orders` table on each pipeline run:

1. **`terraform apply`** (one-time setup): Creates `aws_glue_catalog_database.lakehouse` with `location_uri` pointing to the processed prefix. Applies all LF grants. Creates the three crawlers. The tables do not exist yet.

2. **`ingest.py`**: Uploads `orders_apr_2025.csv` to `raw/`. Starts the Step Functions execution.

3. **`RunOrdersJob` (Glue)**: Reads `raw/orders_apr_2025.csv`. Validates rows. Calls `ensure_delta_table()` — first run seeds an empty Delta table at `lakehouse-dwh/orders/`, writing `_delta_log/00000000000000000000.json`. Runs `DeltaTable.merge()` — commits `_delta_log/00000000000000000001.json` with the first batch's rows. Calls `update_catalog_table()`:
   - Spark SQL `CREATE TABLE IF NOT EXISTS ecom_lakehouse_db.orders USING DELTA LOCATION 's3://.../lakehouse-dwh/orders/'`
   - DeltaCatalog reads `_delta_log/` to get the schema
   - Calls `glue:CreateTable` — LF checks CREATE_TABLE grant (passes), IAM checks `glue:CreateTable` (passes)
   - Catalog entry created: table `orders` in database `ecom_lakehouse_db`, location `lakehouse-dwh/orders/`, schema from Delta log

4. **`AthenaValidation` (Step Functions)**: Athena receives `SELECT ... FROM ecom_lakehouse_db.orders`.
   - LF checks DESCRIBE on `ecom_lakehouse_db` for SFN role (passes)
   - LF checks SELECT on `orders` for SFN role (passes)
   - IAM checks `glue:GetTable` for SFN role (passes)
   - Athena reads `_delta_log/` snapshot → identifies current Parquet files → scans them
   - Returns row count

5. **Second run (`ingest_may_2025.py`)**: Same flow. `ensure_delta_table()` finds the existing Delta table and skips init. `DeltaTable.merge()` commits `_delta_log/00000000000000000002.json`. `update_catalog_table()` calls `CREATE TABLE IF NOT EXISTS` — table already exists in catalog, no-op. Athena queries continue working with the updated snapshot.
