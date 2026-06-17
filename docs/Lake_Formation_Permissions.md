# AWS Lake Formation Permissions

## Overview

AWS Lake Formation adds a second, independent authorization layer on top of IAM for the Glue Data Catalog. When Lake Formation governance is enabled, every catalog operation — reading a table schema, creating a table, running an Athena query — must pass both an IAM check and a Lake Formation check. Passing one but not the other is an `AccessDeniedException`. This document explains why Lake Formation intercepts catalog calls in this pipeline, how the admin registration works, the database-level versus table-level grant distinction, and the specific `CREATE_TABLE` permission problem that blocked catalog registration during development and how it was solved.

---

## Why Lake Formation Intercepts Catalog Calls

The Glue Data Catalog is the central metadata store for this pipeline — it holds the schema, location, and partition layout for the `products`, `orders`, and `order_items` Delta tables. By default, access to the catalog is controlled by IAM alone: if your IAM policy allows `glue:GetTable`, you can read any table in the account.

Lake Formation changes this. When LF governance is active, the catalog enforces LF grants regardless of what the IAM policy says. A principal with a wildcard IAM policy (`glue:*` on `*`) but no LF grant receives `AccessDeniedException` when calling `glue:GetTable`.

**Why LF is active in this project:**

The `aws_lakeformation_data_lake_settings` resource registers an LF admin:

```hcl
resource "aws_lakeformation_data_lake_settings" "account" {
  catalog_id = local.account_id
  admins     = [data.aws_iam_session_context.current.issuer_arn]
  ...
}
```

Once this resource exists in the Terraform state and has been applied, Lake Formation governance is turned on for the account's Glue catalog. From that point forward, all catalog calls go through LF authorization. This cannot be selectively applied to individual databases — it is account-wide.

**The consequence:** Every principal that touches the catalog — the Glue job role registering tables, the Step Functions role running Athena queries, the Athena service resolving table schemas — needs explicit LF grants in addition to their IAM policies. This is why the `aws_lakeformation_permissions` resources exist alongside the IAM policies.

**Why enable LF at all:** Without LF, access control on the catalog is entirely through IAM, which operates at the API level (allow/deny `glue:GetTable`). LF adds data-level access control — it can restrict access to specific columns or rows in a table, enforce tag-based policies, and provide a central audit trail of who accessed which table. For a production data pipeline, this is the correct security posture. The setup cost (explicit grants for each role) is a one-time Terraform operation.

---

## How the LF Admin is Registered

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

### The Session Context Problem

`data.aws_caller_identity.current.arn` returns the full identity ARN of the Terraform caller. When Terraform runs in a CI environment or under an assumed role, this ARN takes the session form:

```
arn:aws:sts::123456789012:assumed-role/TerraformRole/session-name-12345
```

Lake Formation rejects this form as an admin principal. It only accepts stable IAM principal ARNs:

```
arn:aws:iam::123456789012:role/TerraformRole
```

`data.aws_iam_session_context.current` strips the session suffix and returns `issuer_arn` — the stable IAM role ARN. This is the value passed to `admins`. Without this data source, every `terraform apply` on an assumed-role session would fail on all `aws_lakeformation_permissions` resources with:

```
AccessDeniedException: Insufficient Lake Formation permission(s) on ...
```

### Default Permissions — IAM_ALLOWED_PRINCIPALS

```hcl
create_database_default_permissions {
  permissions = ["ALL"]
  principal   = "IAM_ALLOWED_PRINCIPALS"
}

create_table_default_permissions {
  permissions = ["ALL"]
  principal   = "IAM_ALLOWED_PRINCIPALS"
}
```

`IAM_ALLOWED_PRINCIPALS` is Lake Formation's way of saying "defer to IAM for this resource." Any principal with the appropriate IAM permission is allowed — no explicit LF grant required for that resource.

These defaults apply to new databases and tables created in the future. They set LF's "passthrough" mode as the default, so resources that do not have explicit LF grants are still accessible to IAM-authorised principals. Without these defaults, newly created resources would have no LF grants and block all access until explicit grants were added.

This is important for the `rejected/` and `flagged/` prefixes: Athena can query those S3 paths directly (by specifying the S3 URI in the `FROM` clause rather than a catalog table name) without going through the Glue catalog, so no LF grants are needed for those paths.

---

## Database-Level vs Table-Level Grants

LF permissions follow a two-tier hierarchy. A database-level grant governs access to the database record itself. A table-level grant governs access to individual tables within that database. They are independent — having a table-level `SELECT` grant does not automatically give you database-level `DESCRIBE`. You need both.

### Why Both Levels Are Required

When Athena receives a query for `ecom_lakehouse_db.orders`, it does the following before running any SQL:

1. Calls `glue:GetDatabase` to resolve `ecom_lakehouse_db` → needs **database-level `DESCRIBE`**
2. Calls `glue:GetTable` to resolve `orders` within that database → needs **table-level `DESCRIBE`**
3. Calls `glue:GetPartitions` to enumerate the date partitions → needs **table-level `DESCRIBE`**
4. Calls `s3:GetObject` to read the Parquet files → needs **table-level `SELECT`** (LF checks) + **IAM `s3:GetObject`** (IAM checks)

If the database-level `DESCRIBE` is missing, step 1 fails. Athena cannot even confirm the database exists. The error is:
```
Database 'ecom_lakehouse_db' does not exist
```
This is misleading — the database exists, but LF denied the lookup.

If the database grant exists but the table-level `DESCRIBE` is missing, step 1 passes but step 2 fails:
```
Table not found ecom_lakehouse_db.orders
```
Again misleading — same root cause, different error surface.

---

## The Four LF Permission Grants

### Grant 1 — Glue Role: Database CREATE_TABLE + DESCRIBE

```hcl
resource "aws_lakeformation_permissions" "glue_role_database" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["CREATE_TABLE", "DESCRIBE"]

  database {
    name = aws_glue_catalog_database.lakehouse.name
  }

  depends_on = [aws_lakeformation_data_lake_settings.account]
}
```

`DESCRIBE` — allows the Glue job role to read the database record. The DeltaCatalog connector calls `glue:GetDatabase` to retrieve the `LocationUri` when executing `CREATE TABLE IF NOT EXISTS`. Without `DESCRIBE` on the database, this call is denied by LF even if the IAM policy includes `glue:GetDatabase`.

`CREATE_TABLE` — this is the permission that caused a significant development problem, documented in the next section. It allows the Glue role to call `glue:CreateTable` through the LF check. Without it, `spark.sql("CREATE TABLE IF NOT EXISTS ecom_lakehouse_db.orders USING DELTA LOCATION ...")` fails with `AccessDeniedException` from Lake Formation — not from IAM.

`depends_on = [aws_lakeformation_data_lake_settings.account]` — ensures the LF admin is registered before Terraform attempts to create grants. If this dependency is missing, Terraform may create the grant resource before the admin is set, and the grant fails.

### Grant 2 — Glue Role: Table-Level ALL (Wildcard)

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

`ALL` on tables covers every LF table permission: `SELECT`, `INSERT`, `DELETE`, `DESCRIBE`, `ALTER`, `DROP`. The Glue job role needs all of these:

- `SELECT` + `DESCRIBE`: Read existing table schema and data location during `ensure_delta_table()` and `DeltaTable.forPath()`.
- `INSERT`: Write partition metadata when new `date=` partitions are committed by the MERGE.
- `DELETE`: Remove superseded partition metadata when Delta compaction replaces old Parquet files.
- `ALTER`: Update the table schema when `update_catalog_table()` runs on a table that already exists and whose Delta schema has evolved.

`wildcard = true` covers all current and future tables in `ecom_lakehouse_db`. Adding a new dataset (`returns`, `reviews`) does not require a new Terraform `aws_lakeformation_permissions` resource for the Glue job role.

### Grant 3 — SFN Role: Database DESCRIBE

```hcl
resource "aws_lakeformation_permissions" "sfn_describe_database" {
  principal   = aws_iam_role.sfn_role.arn
  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.lakehouse.name
  }
}
```

The `AthenaValidation` state runs queries using the Step Functions execution role. Athena calls `glue:GetDatabase` using `sfn_role`'s credentials. Without the database-level `DESCRIBE` grant on `sfn_role`, this call is denied by LF and the Athena query fails before it reaches the table resolution step.

Note what is absent: `CREATE_TABLE` is not granted to `sfn_role` at the database level. The Step Functions role should never create catalog tables — that is the Glue job's responsibility. Granting `CREATE_TABLE` would be an over-grant with no operational purpose.

### Grant 4 — SFN Role: Table SELECT + DESCRIBE (Wildcard)

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

`DESCRIBE` — allows Athena to retrieve the table schema (column names, types, partition columns) for query planning.

`SELECT` — allows Athena to read rows from the table. In LF terms, `SELECT` on a table translates to permission to read the underlying S3 data at the `LOCATION` registered in the catalog. Without `SELECT`, Athena can plan the query but cannot read any Parquet files and fails with `Access Denied` on the first S3 read.

No `INSERT`, no `DELETE`, no `ALTER`. The Step Functions role is read-only at the catalog level. It cannot modify table definitions or add partitions.

---

## The CREATE_TABLE Permission Problem

During development, the `update_catalog_table()` function in `common.py` was failing with a confusing error:

```
AccessDeniedException: User: arn:aws:sts::123456789012:assumed-role/ecom-lakehouse-dev-glue-role/...
is not authorized to perform: glue:CreateTable
on resource: arn:aws:glue:eu-west-1:123456789012:table/ecom_lakehouse_db/orders
```

The IAM policy on `glue_role` explicitly included `glue:CreateTable` on `arn:aws:glue:...:table/ecom_lakehouse_db/*`. IAM was satisfied. Yet the call was denied.

**The root cause:** Lake Formation was intercepting the `glue:CreateTable` API call and checking LF permissions before the IAM policy was evaluated. The LF check requires a `CREATE_TABLE` grant at the **database level** — not the table level. The `CREATE_TABLE` action is a database-level privilege in LF because creating a table is a modification to the database (it adds a new entry to the database's table list). It cannot be granted at the table level because the table does not yet exist when you are creating it.

At the time of the error, only the table-level `ALL` grant existed:

```hcl
# This existed — but CREATE_TABLE is a database-level privilege
resource "aws_lakeformation_permissions" "glue_role_alter_tables" {
  permissions = ["ALL"]
  table { database_name = ..., wildcard = true }
}
```

Table-level `ALL` includes `SELECT`, `INSERT`, `DELETE`, `DESCRIBE`, `ALTER`, `DROP` on existing tables. It does not include `CREATE_TABLE`, which belongs to the database permission namespace.

**The fix:** Adding the database-level grant:

```hcl
resource "aws_lakeformation_permissions" "glue_role_database" {
  permissions = ["CREATE_TABLE", "DESCRIBE"]
  database { name = aws_glue_catalog_database.lakehouse.name }
}
```

After this grant was applied, `spark.sql("CREATE TABLE IF NOT EXISTS ...")` passed both the LF check (database-level `CREATE_TABLE`) and the IAM check (`glue:CreateTable` on the table ARN), and the catalog entry was successfully created.

**Why this is non-obvious:** The error message says `glue:CreateTable` is denied, which looks like an IAM problem. An engineer's natural response is to check the IAM policy — but the IAM policy is correct. The denial is coming from Lake Formation, which runs before IAM for catalog operations when LF governance is enabled. The distinction between database-level and table-level LF privileges is not surfaced in the error message and is not intuitive from IAM reasoning.

---

## LF vs IAM — The Two-Check Model in Practice

For every catalog operation, both checks must pass independently:

```
Principal calls glue:GetTable on ecom_lakehouse_db.orders
            │
            ▼
    Lake Formation Check
    Does this principal have DESCRIBE on ecom_lakehouse_db.orders?
            │
     ┌──────┴──────┐
    PASS           FAIL → AccessDeniedException (LF)
     │
     ▼
    IAM Check
    Does this principal's IAM policy allow glue:GetTable on this ARN?
     │
     ┌──────┴──────┐
    PASS           FAIL → AccessDeniedException (IAM)
     │
     ▼
    glue:GetTable succeeds — table metadata returned
```

Passing LF but failing IAM → denied. Passing IAM but failing LF → denied. Both must pass.

This is why the Terraform configuration has both `aws_iam_role_policy.glue_catalog` (IAM) and `aws_lakeformation_permissions.glue_role_database` / `glue_role_alter_tables` (LF). Neither alone is sufficient.
