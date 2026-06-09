# E-Commerce Lakehouse on AWS

A production-grade Lakehouse for e-commerce transactions. Raw CSVs land in S3,
AWS Glue + Spark clean, validate, deduplicate and **MERGE** them into ACID Delta
Lake tables, the Glue Data Catalog is refreshed, and Amazon Athena serves the
data for analytics. The whole lifecycle is orchestrated by AWS Step Functions,
provisioned with Terraform, and shipped through GitHub Actions.

---

## 1. Architecture

```
                ingestion/ingest.py  (uploads 3 CSVs, starts ONE execution)
                          │
                          ▼
   ┌──────────────┐   raw/    ┌─────────────────────── AWS Step Functions ───────────────────────┐
   │  Amazon S3   │ ────────▶ │  RunProductsJob → RunOrdersJob → RunOrderItemsJob                  │
   │  (data bkt)  │           │        (Glue + Delta, strictly ordered for referential integrity) │
   │              │           │            → RunCrawlers (×3) → AthenaValidation → Notify          │
   │ lakehouse-dwh│ ◀──────── │                                                                    │
   │ archived/    │   Delta   └────────────────────────────────────────────────────────────────────┘
   │ rejected/    │                          │                         │
   │ flagged/     │                          ▼                         ▼
   └──────────────┘                  Glue Data Catalog  ───────▶  Amazon Athena
                                                                  (downstream analytics)
       failures ──▶ SNS topic ──▶ email + (optional) Slack Lambda
```

**Why a single ordered execution and not per-file S3 triggers?** The three
datasets are one relational batch — `order_items` carries foreign keys into both
`products` (`product_id`) and `orders` (`order_id`). Three independent
file-triggered runs would race the referential-integrity checks. One Step
Functions execution running `products → orders → order_items` makes the
dependency a structural guarantee. See `terraform/step_functions.tf`.

### Storage zones (one S3 bucket, prefix-separated)

| Prefix           | Purpose                                              |
|------------------|------------------------------------------------------|
| `raw/`           | Incoming source CSVs                                 |
| `lakehouse-dwh/` | Cleaned Delta tables (`products`, `orders`, `order_items`) |
| `archived/`      | Source files moved here after a successful merge     |
| `rejected/`      | Rows that failed validation, with a `rejection_reason` |
| `flagged/`       | Rows that pass but need analyst review (e.g. huge amounts) |

---

## 2. Datasets, schema & partitioning

| Table         | Merge key (upsert)   | Partition | Notes                                  |
|---------------|----------------------|-----------|----------------------------------------|
| `products`    | `product_id`         | `department` | Dimension. Last load wins on match. |
| `orders`      | `order_id`           | `date`    | Fact. Timestamp guard: newer wins.     |
| `order_items` | `id, order_id`       | `date`    | Fact. FK → products & orders. Timestamp guard. |

### Validation rules enforced (rejected rows are logged to `rejected/`)
- No null primary identifiers (`product_id` / `order_id` / composite `id,order_id`).
- Valid, parseable timestamps; future timestamps (> 1h ahead) rejected.
- Type-safe casts (amounts, ids) — bad formats rejected, not silently nulled.
- `date` must be consistent with `order_timestamp`.
- Referential integrity: `order_items.product_id` and `order_items.order_id`
  must exist in the parent Delta tables.
- Intra-batch deduplication (last-write-wins by timestamp; stable choice for
  the products dimension).

---

## 3. Repository layout

```
glue_jobs/
  products_job.py        orders_job.py        order_items_job.py
  utils/
    common.py            # Spark/Delta session, arg parsing, rejected writer, archiver, catalog
    monitor.py           # per-stage timing + FAILURE-ONLY alerting
    notifier.py          # SNS publisher
ingestion/ingest.py      # uploads the batch + starts the Step Functions execution
terraform/               # all infrastructure (S3, IAM, Glue, Step Functions, Athena, SNS, Lambda)
tests/                   # pytest unit tests (validation logic, utils)
.github/workflows/       # ci.yml (lint + test + tf validate), deploy.yml (scripts → S3 on main)
Dockerfile, docker-compose.yml
```

---

## 4. Prerequisites

| Tool        | Version            | Notes                                             |
|-------------|--------------------|---------------------------------------------------|
| AWS account | —                  | Credentials with permission to create the stack   |
| Terraform   | ≥ 1.5              | IaC                                               |
| AWS CLI     | v2                 | `aws configure` or SSO                            |
| Python      | **3.10** (for tests) | Glue 4.0 runs Spark 3.3.x; tests pin pyspark 3.3.2 |
| Java        | **11 or 17**       | Required by Spark 3.3.x for local test runs       |
| Docker      | optional           | Easiest way to run tests with the right versions  |

> **Heads-up on local test runs:** Spark 3.3.x does **not** support Java 21+/Python 3.12.
> If your machine has a newer JDK/Python (this one runs Java 25 + Python 3.12),
> use the Docker path below — it pins compatible versions for you.

---

## 5. Run the tests & linters

### Option A — Docker (recommended, version-matched)

```bash
docker compose run --rm test     # pytest + coverage (fails under 70%)
docker compose run --rm lint     # black --check + flake8
```

### Option B — local virtualenv (needs Python 3.10 + Java 11/17)

```bash
python -m venv .venv
# Windows PowerShell:  .\.venv\Scripts\Activate.ps1
# macOS/Linux:         source .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/ -v --cov=glue_jobs --cov-report=term-missing --cov-fail-under=70
black --check --line-length 120 glue_jobs/ ingestion/ tests/
flake8 glue_jobs/ ingestion/ tests/
```

Unit tests mock the Glue runtime and Delta, so no AWS account is needed to run them.

---

## 6. Deploy the infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # then edit it

terraform init
terraform plan
terraform apply
```

Set these in `terraform.tfvars` (or via `TF_VAR_*` env vars — preferred in CI):

```hcl
alert_email       = "you@example.com"   # empty = no email subscription
# slack_webhook_url is OPTIONAL — leave empty and the Slack Lambda is not created.
# Inject at apply time instead of committing it:
#   export TF_VAR_slack_webhook_url="https://hooks.slack.com/services/..."
```

After `apply`, the Glue `.py` scripts are uploaded to the scripts bucket by
Terraform itself. Useful outputs:

```bash
terraform output data_bucket_name
terraform output scripts_bucket_name
terraform output sfn_state_machine_arn
terraform output glue_database_name
```

---

## 7. Run the pipeline

### One command (uploads the sample batch in `Data/` and starts the execution)

```bash
python ingestion/ingest.py
```

This reads the Terraform outputs, converts the `.xlsx` sources to CSV, uploads
all three files to `raw/`, then starts **one** Step Functions execution with the
batch input and prints the execution ARN to track.

### Or trigger manually

```bash
terraform -chdir=terraform output -raw manual_sfn_trigger_command   # prints a ready-to-run command
```

Track progress:

```bash
aws stepfunctions describe-execution --execution-arn <ARN>
# or watch it in the Step Functions console
```

---

## 8. Query in Athena

Use the workgroup `ecom-lakehouse-wg` and database `ecom_lakehouse_db`:

```sql
SELECT COUNT(*) FROM ecom_lakehouse_db.orders;

SELECT department, COUNT(*) AS products
FROM ecom_lakehouse_db.products
GROUP BY department
ORDER BY products DESC;



-- Verify data landed
SELECT COUNT(*) FROM ecom_lakehouse_db.products;
SELECT COUNT(*) FROM ecom_lakehouse_db.orders;
SELECT COUNT(*) FROM ecom_lakehouse_db.order_items;

-- Basic analytical query showing the data is usable
SELECT p.product_name, SUM(oi.reordered) AS reorders
FROM ecom_lakehouse_db.order_items oi
JOIN ecom_lakehouse_db.products p ON oi.product_id = p.product_id
GROUP BY p.product_name
ORDER BY reorders DESC
LIMIT 20;

-- top products by reorder volume (joins all three tables)
SELECT p.product_name, SUM(oi.reordered) AS reorders
FROM ecom_lakehouse_db.order_items oi
JOIN ecom_lakehouse_db.products p ON oi.product_id = p.product_id
GROUP BY p.product_name
ORDER BY reorders DESC
LIMIT 20;
```

---

## 9. CI/CD (GitHub Actions, `main` branch only)

- **`ci.yml`** (push + PR to `main`): black + flake8 → pytest with coverage →
  `terraform fmt -check` + `validate`.
- **`deploy.yml`** (push to `main`): packages the utilities zip and uploads the
  Glue scripts to the scripts bucket so the next execution picks up the latest
  code. Gated on AWS secrets being present.

Required repository secrets (Settings → Secrets and variables → Actions):
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `SCRIPTS_BUCKET`
(= `terraform output scripts_bucket_name`).

> Prefer GitHub OIDC (`role-to-assume`) over long-lived access keys when you can —
> it removes static secrets from the repo entirely.

---

## 10. Teardown

```bash
cd terraform
terraform destroy
```

Buckets use `force_destroy` in non-prod so they empty on destroy. In `prod` they
are retained by design.
