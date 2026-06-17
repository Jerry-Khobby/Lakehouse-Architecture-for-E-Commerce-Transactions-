# GitHub Actions CI/CD — Workflow Structure, Triggers, and Deployment

## Overview

The pipeline has two GitHub Actions workflows: `ci.yml` runs on every push and pull request to `main` and executes tests, linting, and type checks; `deploy.yml` runs only on pushes to `main` after CI passes and applies Terraform, which handles script uploads to S3 via `etag`-tracked `aws_s3_object` resources. AWS credentials are configured via OIDC federation — no long-lived access keys are stored in GitHub secrets.

---

## Workflow Files

```
.github/
└── workflows/
    ├── ci.yml      — test, lint, type-check on push + PR to main
    └── deploy.yml  — terraform apply on push to main (after ci.yml passes)
```

---

## `ci.yml` — Continuous Integration

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    name: Test and Lint
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install dependencies
        run: pip install -r requirements-dev.txt

      - name: Lint — ruff
        run: ruff check glue_jobs/ ingestion/ tests/

      - name: Format check — ruff format
        run: ruff format --check glue_jobs/ ingestion/ tests/

      - name: Type check — mypy
        run: mypy glue_jobs/ ingestion/ --ignore-missing-imports

      - name: Run unit tests
        run: pytest tests/ -v --tb=short --no-header
        env:
          PYTHONPATH: ${{ github.workspace }}
```

### Trigger Scope

`on.push.branches: [main]` and `on.pull_request.branches: [main]` scope CI runs to changes targeting `main`. Feature branches only trigger CI when a PR is opened against `main` or when pushed directly to `main` — not on every push to every branch. This prevents CI credits from being consumed on every developer commit to a personal branch.

`pull_request` events run the workflow against the merge commit (the result of merging the PR branch into `main`) not the PR branch itself, so the test environment reflects exactly what will land on `main`.

### Linting and Formatting — `ruff`

`ruff` replaces `flake8` and `black` with a single tool. `ruff check` enforces the AmaliTech naming and style rules (function names lowercase, no magic numbers, etc.) as linting rules. `ruff format --check` verifies code formatting without making changes — a format violation fails CI without modifying files. Developers run `ruff format .` locally to fix formatting before pushing.

### Type Checking — `mypy`

`mypy` runs over `glue_jobs/` and `ingestion/` with `--ignore-missing-imports`. AWS Glue SDK types (`awsglue.*`) are not distributed as PySpark stubs — `--ignore-missing-imports` prevents `mypy` from failing on the Glue-specific imports (`from awsglue.utils import getResolvedOptions`) that are only available in the Glue runtime environment, not in the CI runner.

`PYTHONPATH: ${{ github.workspace }}` ensures `from glue_jobs.utils.common import ...` resolves correctly when running tests from the repo root.

### Test Runner — `pytest`

`pytest tests/ -v --tb=short` runs all test files under `tests/`. See [Unit_Tests.md](Unit_Tests.md) for the full test structure and PySpark local session configuration. `--tb=short` produces compact tracebacks — sufficient for CI output without the full multi-screen traces that `--tb=long` produces.

---

## `deploy.yml` — Deployment via Terraform

```yaml
name: Deploy

on:
  push:
    branches: [main]

jobs:
  deploy:
    name: Terraform Apply
    runs-on: ubuntu-latest
    needs: []   # No explicit needs — deploy.yml is separate from ci.yml;
                # branch protection rules enforce ci.yml passing before merge to main

    permissions:
      id-token: write    # Required for OIDC token request
      contents: read

    environment: dev     # GitHub environment for deployment approval gate (optional)

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Configure AWS credentials via OIDC
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_DEPLOY_ROLE_ARN }}
          aws-region: eu-west-1

      - name: Set up Terraform
        uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "~> 1.7"

      - name: Set up Python (for utils zip packaging)
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Package Glue utils zip
        run: |
          cd glue_jobs
          zip -r utils/utils.zip utils/__init__.py utils/common.py utils/monitor.py utils/notifier.py
          cd ..

      - name: Terraform Init
        run: terraform -chdir=terraform init

      - name: Terraform Plan
        run: terraform -chdir=terraform plan -out=tfplan
        env:
          TF_VAR_environment: dev
          TF_VAR_slack_webhook_url: ${{ secrets.SLACK_WEBHOOK_URL }}

      - name: Terraform Apply
        run: terraform -chdir=terraform apply tfplan
```

### OIDC Federation — No Stored Access Keys

`permissions: id-token: write` enables the workflow to request an OIDC token from GitHub's token endpoint. `aws-actions/configure-aws-credentials@v4` exchanges this token for temporary AWS credentials by assuming `secrets.AWS_DEPLOY_ROLE_ARN` via AWS STS.

The benefit over stored `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` secrets:
- No long-lived credentials exist in GitHub secrets that could be exfiltrated if the repository is compromised
- Credentials are scoped to the workflow run (15-minute default STS session)
- The IAM role's trust policy constrains which repository, which branch, and which workflow can assume it:

```json
{
  "Effect": "Allow",
  "Principal": { "Federated": "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com" },
  "Action": "sts:AssumeRoleWithWebIdentity",
  "Condition": {
    "StringEquals": {
      "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
      "token.actions.githubusercontent.com:sub": "repo:org/lakehouse-repo:ref:refs/heads/main"
    }
  }
}
```

The `sub` condition pins the trust to the specific repository and `main` branch. A PR workflow (which runs on a branch, not `main`) cannot assume this role.

### Packaging the Utils Zip Before Terraform Apply

The `glue_jobs/utils/` Python package must be zipped before `terraform apply` so that the `aws_s3_object.glue_utils_zip` resource's `filemd5()` call computes the correct hash of the freshly-built zip. The packaging step runs:

```bash
zip -r utils/utils.zip utils/__init__.py utils/common.py utils/monitor.py utils/notifier.py
```

This rebuilds the zip from the current source files. If `common.py` changed in this commit, the new zip has a different MD5 than what is stored in Terraform state. Terraform detects the change via `etag = filemd5(...)` and re-uploads the zip to S3. The Glue jobs that reference `--extra-py-files` will use the new utils on their next run.

The zip is committed to the repository as a build artifact. The CI step re-creates it from source on every deploy to ensure it is always current — the committed zip is a fallback for local development, not the deploy source.

### How Scripts Reach S3

Script uploads to S3 are entirely managed by Terraform `aws_s3_object` resources with `etag`:

```hcl
resource "aws_s3_object" "orders_job_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/orders_job.py"
  source = "${path.module}/../glue_jobs/orders_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/orders_job.py")
}
```

`terraform apply` re-uploads any script whose local MD5 differs from the stored state etag. A deploy that changes only `orders_job.py` will upload only `orders_job.py` — the other scripts are unchanged and not re-uploaded. See [Infrastructure_as_Code_Terraform.md](Infrastructure_as_Code_Terraform.md) for the full `etag` mechanism.

There is no separate `aws s3 cp` step in the deploy workflow. All infrastructure changes — script uploads, IAM policy updates, new Glue job configurations — flow through `terraform apply`.

### Branch Protection Requirements

`deploy.yml` is triggered by push to `main`. The GitHub repository's branch protection settings on `main` enforce:
- `ci.yml` status check must pass before a PR can be merged
- Direct pushes to `main` are disallowed (changes must arrive via PR)

This means `deploy.yml` only runs on commits that passed CI on the PR and were merged by a reviewer. The `deploy.yml` workflow does not need an explicit `needs:` dependency on CI because the branch protection rules provide that guarantee at the merge gate.

### `TF_VAR_*` Environment Variables

Terraform variables can be set via environment variables prefixed `TF_VAR_`:

```yaml
env:
  TF_VAR_environment: dev
  TF_VAR_slack_webhook_url: ${{ secrets.SLACK_WEBHOOK_URL }}
```

`TF_VAR_slack_webhook_url` reads from the `SLACK_WEBHOOK_URL` GitHub secret. If the secret is not set, `${{ secrets.SLACK_WEBHOOK_URL }}` resolves to an empty string — which matches `variable.slack_webhook_url.default = ""` and correctly disables the Lambda notifier without failing the plan. No explicit handling is needed for the "no Slack" case.

---

## Secrets Required

| Secret Name | Purpose | Required |
|---|---|---|
| `AWS_DEPLOY_ROLE_ARN` | IAM role ARN for OIDC federation | Yes |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL | No (empty = Lambda disabled) |

All other configuration (region, environment name, bucket names) is declared in `variables.tf` with defaults and overridden via `TF_VAR_*` environment variables in the workflow.
