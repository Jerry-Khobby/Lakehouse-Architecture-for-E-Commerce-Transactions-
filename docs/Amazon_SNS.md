# Amazon SNS — Pipeline Alerts Topic and Notification Design

## Overview

Amazon Simple Notification Service (SNS) is the messaging backbone for all pipeline alerts in this project. A single SNS topic receives notifications from two sources — AWS Step Functions (pipeline-level outcomes) and the Glue jobs themselves (stage-level progress) — and fans them out to two subscriber types: an email address and a Slack Lambda function. This document covers the topic configuration, the two notification sources, the `SnsNotifier` and `PipelineMonitor` classes that produce stage-level events, the subscriber chain, and the design decisions behind this two-level alerting architecture.

---

## The SNS Topic

```hcl
resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name_prefix}-pipeline-alerts"
  # → "ecom-lakehouse-dev-pipeline-alerts"
}
```

A single topic receives all pipeline alert messages regardless of source or severity. There is no separate topic for failures vs successes, and no separate topic per dataset. Every subscriber receives every message and applies its own filtering logic (the Slack Lambda uses the message subject to choose the alert color and icon).

**Why one topic:** Multiple topics would require each publisher (Step Functions, each Glue job) to hold references to multiple ARNs and decide at publish time which topic applies. A single topic keeps the publisher interface simple — publish everything — and pushes the routing concern to subscribers, which is the correct SNS design pattern. If a future subscriber needs to receive only failures, an SNS filter policy on its subscription handles that without any change to the publisher code.

### Email Subscription

```hcl
resource "aws_sns_topic_subscription" "email_alert" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
```

The `count = var.alert_email != "" ? 1 : 0` guard means this subscription is only created if the operator has set an email address in `var.alert_email`. On a fresh environment without an email configured, the topic still exists and the Lambda subscription still works — only the email delivery is absent. This prevents the Terraform apply from failing or creating a broken subscription to an empty string endpoint.

After `terraform apply`, AWS sends a confirmation email to the address. The subscription remains `PendingConfirmation` until the link in that email is clicked. Messages published before confirmation are not delivered to email.

---

## Notification Source 1 — Step Functions (Pipeline-Level)

Step Functions publishes directly to SNS using the `arn:aws:states:::sns:publish` integration, with no Lambda intermediary. There are two states that publish:

**NotifySuccess** — fires after `AthenaValidation` succeeds:
```
Subject:  "[dev] Lakehouse ETL — SUCCESS"
Message:  "✅ Lakehouse ETL batch completed successfully.
           Batch: may_2025
           Execution: may_2025-20260615T134313"
```

**NotifyFailure** — fires after any Glue job or Athena state fails all retries:
```
Subject:  "[dev] Lakehouse ETL — FAILURE"
Message:  "❌ Lakehouse ETL batch FAILED.
           Batch: may_2025
           Execution: may_2025-20260615T134313
           Check CloudWatch logs for details."
```

Both messages use the `States.Format` intrinsic function to embed `$.batch` (from the execution input) and `$$.Execution.Name` (from the Step Functions context object). The result is a pipeline-level summary — one message per execution, fired at the very end.

These messages tell the operator: **did the whole pipeline succeed or fail?** They do not say anything about which specific stage within a Glue job failed, how many rows were validated, or how long each stage took.

---

## Notification Source 2 — Glue Jobs (Stage-Level)

The Glue jobs emit granular per-stage notifications using two classes: `SnsNotifier` and `PipelineMonitor`. Together they produce a live feed of events that arrive in Slack as the pipeline runs, rather than a single outcome message at the end.

### SnsNotifier

`glue_jobs/utils/notifier.py`:

```python
class SnsNotifier:

    def __init__(self, topic_arn: str, environment: str):
        self._topic_arn  = topic_arn
        self._environment = environment
        self._client     = boto3.client("sns")

    def send_job_started(self, job_name, stage_name):
        self._publish(
            subject=f"[{self._environment}] {job_name} — STARTED: {stage_name}",
            message=f"Stage '{stage_name}' started in job '{job_name}'.",
        )

    def send_job_succeeded(self, job_name, stage_name, elapsed, detail=""):
        message = f"Stage '{stage_name}' completed in {elapsed:.1f}s."
        if detail:
            message = f"{message}\n{detail}"
        self._publish(
            subject=f"[{self._environment}] {job_name} — SUCCESS: {stage_name}",
            message=message,
        )

    def send_job_failed(self, job_name, stage_name, error):
        self._publish(
            subject=f"[{self._environment}] {job_name} — FAILED: {stage_name}",
            message=f"Stage '{stage_name}' FAILED.\nError: {error}",
        )

    def _publish(self, subject, message):
        try:
            self._client.publish(
                TopicArn=self._topic_arn,
                Subject=subject[:SNS_SUBJECT_MAX_LENGTH],   # 100 char AWS limit
                Message=message,
            )
        except ClientError:
            logger.exception("SNS publish failed")
```

**The `ClientError` swallow in `_publish()`** is a deliberate correctness decision. If the SNS API is temporarily unavailable or the IAM role loses its `sns:Publish` permission mid-run, an unhandled `ClientError` would propagate up through `PipelineMonitor.stage()` and cause the Glue job to fail — marking a perfectly successful MERGE as a pipeline failure. The alert is secondary to the data operation. So the exception is caught, logged to CloudWatch (where the full traceback is stored for diagnosis), and execution continues.

`subject[:SNS_SUBJECT_MAX_LENGTH]` (100 characters) guards against the AWS SNS subject length limit. A job name plus stage name can approach this limit for long environment prefixes.

### PipelineMonitor

`glue_jobs/utils/monitor.py`:

```python
class PipelineMonitor:

    def __init__(self, job_name, notifier=None):
        self._job_name      = job_name
        self._notifier      = notifier
        self._stage_timings = {}

    @contextmanager
    def stage(self, stage_name):
        report = StageReport()
        logger.info("[START] %s | job=%s", stage_name, self._job_name)
        self._notify_started(stage_name)

        start_time = time.time()
        try:
            yield report                               # job body runs here
            elapsed = time.time() - start_time
            self._stage_timings[stage_name] = elapsed
            logger.info("[SUCCESS] %s — %.1fs | %s", stage_name, elapsed, report.summary())
            self._notify_succeeded(stage_name, elapsed, report.summary())

        except Exception as error:
            elapsed = time.time() - start_time
            logger.exception("[FAILED] %s — %.1fs", stage_name, elapsed)
            self._notify_failed(stage_name, error)
            raise                                      # re-raise so the Glue job fails
```

`stage()` is a Python context manager (`@contextmanager`). Each stage in a Glue job is wrapped:

```python
with monitor.stage("Validate") as report:
    valid_df = validate(raw_df, args, job_run_id)
    valid_count = valid_df.count()
    report.record(read=rows_read, valid=valid_count, rejected=rows_read - valid_count)
```

**On entry to the `with` block:** `_notify_started()` fires → SNS receives `STARTED: Validate`.

**On normal exit:** `_notify_succeeded()` fires → SNS receives:
```
Subject: [dev] ecom-lakehouse-dev-orders-etl — SUCCESS: Validate
Message: Stage 'Validate' completed in 12.4s.
         read=850 | valid=849 | rejected=1
```

**On exception:** `_notify_failed()` fires → SNS receives the error message → the exception is re-raised so Step Functions marks the Glue task as `FAILED` and triggers the retry/catch chain.

### StageReport — Attaching Metrics to Notifications

```python
class StageReport:
    def __init__(self):
        self.metrics = {}

    def record(self, **metrics):
        self.metrics.update(metrics)

    def summary(self):
        return " | ".join(f"{key}={value}" for key, value in self.metrics.items())
```

`report.record()` accepts any keyword arguments. The job decides what is worth reporting without the monitor needing to know anything about the dataset:

| Stage | Metrics recorded | Appears in SNS message |
|---|---|---|
| Read | `rows=850` | `rows=850` |
| Validate | `read=850, valid=849, rejected=1` | `read=850 \| valid=849 \| rejected=1` |
| Delta Merge | `merged=849` | `merged=849` |
| Catalog Update | `table=ecom_lakehouse_db.orders` | `table=ecom_lakehouse_db.orders` |
| Archive | `source=raw/orders_may_2025.csv, dest=archived/orders/2026-06-15/orders_may_2025.csv` | `source=... \| dest=...` |

### How the Glue Job Wires Everything Together

In `orders_job.py` `main()`:

```python
def main():
    _, _, spark, job = build_spark_session(...)
    args = parse_args()
    job_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    monitor = PipelineMonitor(
        args["JOB_NAME"],
        SnsNotifier(args["SNS_TOPIC_ARN"], args["ENVIRONMENT"]),
    )

    with monitor.stage("Read") as report:
        raw_df = read_source(spark, args)
        report.record(rows=raw_df.count())

    with monitor.stage("Validate") as report:
        valid_df = validate(raw_df, args, job_run_id)
        valid_count = valid_df.count()
        report.record(read=rows_read, valid=valid_count, rejected=rows_read - valid_count)

    with monitor.stage("Delta Merge") as report:
        table_path = merge_into_delta(spark, valid_df, args)
        report.record(merged=valid_count)

    with monitor.stage("Catalog Update") as report:
        update_catalog_table(args=args, table_name=TABLE_NAME, table_path=table_path, spark=spark)
        report.record(table=f"{args['DATABASE_NAME']}.{TABLE_NAME}")

    with monitor.stage("Archive") as report:
        archive_source_file(args)

    monitor.log_summary()
    job.commit()
```

`args["SNS_TOPIC_ARN"]` is passed by Terraform as a Glue job argument (`--SNS_TOPIC_ARN`), so the job does not hardcode any AWS resource ARN. `args["ENVIRONMENT"]` is similarly passed as `--ENVIRONMENT` and appears in every SNS subject as `[dev]` or `[prod]`.

`monitor.log_summary()` writes the per-stage timing table to CloudWatch only — it does not publish to SNS. This is intentional: the Step Functions `NotifySuccess` state already sends the pipeline-level success message. If `log_summary()` also published to SNS, every successful pipeline run would produce a duplicate success alert. The job-level summary lives in CloudWatch for operators who want to review timing without receiving another SNS message.

---

## The Two-Level Notification Architecture

The combination of Step Functions (pipeline-level) and Glue SnsNotifier (stage-level) produces two distinct streams of information:

**Stream 1 — Live per-stage feed (from Glue):**
```
[dev] orders-etl — STARTED: Read
[dev] orders-etl — SUCCESS: Read          rows=850               (1.2s)
[dev] orders-etl — STARTED: Validate
[dev] orders-etl — SUCCESS: Validate      read=850|valid=849|rejected=1  (12.4s)
[dev] orders-etl — STARTED: Delta Merge
[dev] orders-etl — SUCCESS: Delta Merge   merged=849             (34.1s)
...
```

**Stream 2 — Single pipeline outcome (from Step Functions):**
```
✅ Lakehouse ETL batch completed successfully.
   Batch: may_2025
   Execution: may_2025-20260615T134313
```

Stream 1 appears in Slack as the pipeline runs. An operator watching Slack sees each stage check in without waiting for the whole job. If the `Validate` stage fails, the failure notification arrives within seconds of the error — not 15 minutes later when the whole pipeline would have timed out.

Stream 2 is the definitive outcome. It arrives once per execution and summarises the whole batch. Email subscribers receive only stream 2 messages (because per-stage notifications would be excessive for an inbox). Slack subscribers receive both streams because the Lambda routes them to the same channel.

---

## Message Volume Per Execution

An `orders` job run through 5 stages produces 10 SNS messages (2 per stage: STARTED + SUCCESS). A full three-job pipeline with no failures produces:

| Job | Stages | Messages |
|---|---|---|
| products | 5 (Read/Validate/Merge/Catalog/Archive) | 10 |
| orders | 5 | 10 |
| order_items | 5 | 10 |
| Step Functions NotifySuccess | 1 | 1 |
| **Total** | | **31** |

SNS pricing is per-publish ($0.50 per million publishes). At 31 messages per pipeline run and one run per month, SNS cost is negligible. Even at daily runs, 31 × 30 = 930 messages/month is well within the free tier.
