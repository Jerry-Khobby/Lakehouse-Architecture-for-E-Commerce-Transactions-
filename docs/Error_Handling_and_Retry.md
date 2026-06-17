# Error Handling and Retry — Catch Blocks, Retry Policies, and Failure Routing

## Overview

Every Task state that calls Glue (ETL jobs and crawlers) has both a `Retry` policy and a `Catch` block. The `Retry` policy handles transient infrastructure errors that are safe to re-attempt automatically. The `Catch` block handles terminal failures that require human intervention — it captures the error details and routes execution to `NotifyFailure` before the `PipelineFailed` Fail state. This document covers each retry configuration, how exponential backoff delays are calculated, the two Glue-specific exceptions that require special handling (`ConcurrentRunsExceededException` and `CrawlerRunningException`), and how the Catch block preserves failure context for the SNS notification.

---

## Glue Task Retry Policy

```json
"Retry": [
  {
    "ErrorEquals": ["Glue.ConcurrentRunsExceededException"],
    "IntervalSeconds": 60,
    "MaxAttempts": 3,
    "BackoffRate": 2.0,
    "MaxDelaySeconds": 300
  },
  {
    "ErrorEquals": ["States.TaskFailed"],
    "IntervalSeconds": 30,
    "MaxAttempts": 2,
    "BackoffRate": 1.5,
    "MaxDelaySeconds": 120
  }
]
```

Step Functions evaluates retry entries in declaration order. The first entry whose `ErrorEquals` matches the thrown error is used. Once the matching entry's `MaxAttempts` is exhausted, the error propagates to the `Catch` block.

---

## `ConcurrentRunsExceededException`

### What Triggers It

Glue enforces a per-job concurrency limit defined by `max_concurrent_runs` in the Glue job configuration. In `glue_jobs.tf`:

```hcl
execution_property {
  max_concurrent_runs = 1
}
```

With `max_concurrent_runs = 1`, only one execution of `ecom-lakehouse-orders-job` can be active at any time. If a second Step Functions execution attempts to start `ProcessOrders` while a previous execution's `ProcessOrders` is still running, Glue rejects the start request with `ConcurrentRunsExceededException`.

This can happen in two scenarios:
1. An operator manually starts a second execution before the first completes (uncommon but possible)
2. A Step Functions execution times out at the state machine level but the Glue job continues running — the retried execution starts a new Glue job run, but the orphaned run from the previous execution is still active

### Retry Configuration

```json
{
  "ErrorEquals": ["Glue.ConcurrentRunsExceededException"],
  "IntervalSeconds": 60,
  "MaxAttempts": 3,
  "BackoffRate": 2.0,
  "MaxDelaySeconds": 300
}
```

**Delay schedule:**

| Attempt | Formula | Delay | Capped at |
|---|---|---|---|
| 1st retry | `60 × 2.0^0` = 60s | 60 seconds | 60s |
| 2nd retry | `60 × 2.0^1` = 120s | 120 seconds | 120s |
| 3rd retry | `60 × 2.0^2` = 240s | 240 seconds | 240s |

`MaxDelaySeconds: 300` caps each individual delay. The 3rd retry delay (240s) is under the cap — if there were a 4th retry, its delay would be `60 × 2.0^3 = 480s`, which would be capped to 300s.

**Why 60-second initial interval:**

A Glue job run for this pipeline completes in 1–3 minutes under normal conditions. A 60-second wait before the first retry is appropriate: if the competing run is a short normal execution, it will likely finish within 60 seconds, and the retry will succeed. A 5-second retry would likely hit the same concurrent run immediately — the competing job has barely started.

**Why 3 maximum attempts:**

If the concurrent run is still blocking after three 60/120/240-second retries (total wait: ~7 minutes), it is not a brief transient overlap — it is a symptom of a stuck job or an operational problem that requires investigation. Retrying indefinitely would hold the Step Functions execution open and potentially queue further executions behind it.

---

## `CrawlerRunningException`

### What Triggers It

Glue crawlers have a similar concurrency constraint: only one run of a given crawler can be active at a time. `startCrawler` raises `CrawlerRunningException` if the crawler is already running when the call arrives.

In the `RunCrawlers` parallel state, the three crawler branches start simultaneously. Each branch calls `startCrawler` for its respective crawler. The `CrawlerRunningException` case arises when a previous pipeline execution's crawler run has not yet completed — perhaps the previous execution's crawlers are still running when a new execution reaches `RunCrawlers`.

### Retry Configuration

```json
{
  "ErrorEquals": ["Glue.CrawlerRunningException"],
  "IntervalSeconds": 30,
  "MaxAttempts": 5,
  "BackoffRate": 1.5,
  "MaxDelaySeconds": 180
}
```

**Delay schedule:**

| Attempt | Formula | Delay | Capped at |
|---|---|---|---|
| 1st retry | `30 × 1.5^0` = 30s | 30 seconds | 30s |
| 2nd retry | `30 × 1.5^1` = 45s | 45 seconds | 45s |
| 3rd retry | `30 × 1.5^2` = 67.5s | 67.5 seconds | 67.5s |
| 4th retry | `30 × 1.5^3` = 101.25s | 101 seconds | 101s |
| 5th retry | `30 × 1.5^4` = 151.88s | 152 seconds | 152s |

`MaxDelaySeconds: 180` caps individual delays. The 5th retry delay (152s) is under the cap.

**Why more attempts but slower backoff than the ETL retry:**

A crawler run typically takes 30–90 seconds. Five retries with gentle backoff gives approximately 7 minutes of total retry time, which covers most realistic cases where a previous crawl is finishing. The gentler `BackoffRate: 1.5` (vs `2.0` for `ConcurrentRunsExceededException`) reflects that crawlers finish more predictably than ETL jobs — linear growth in retry delay suits a predictable wait. The higher `MaxAttempts: 5` (vs `3`) accommodates slow crawlers on larger tables.

**Why retry depth calculation matters:**

The total time budget for the `RunCrawlers` Parallel state is bounded by the retry schedule. In the worst case, one crawler branch exhausts all 5 retries:

```
30 + 45 + 67.5 + 101.25 + 151.88 = ~396 seconds ≈ 6.6 minutes of wait time alone
```

This is within the `TimeoutSeconds: 7200` on the parent execution. If the crawler still cannot start after 5 retries, the branch fails, the Parallel state fails, and execution routes to `NotifyFailure`. The retry schedule is designed so that the total wait time is significant enough to absorb realistic crawler overlap without being so long that a genuinely stuck crawler keeps the execution waiting for hours.

---

## `States.TaskFailed` Retry

```json
{
  "ErrorEquals": ["States.TaskFailed"],
  "IntervalSeconds": 30,
  "MaxAttempts": 2,
  "BackoffRate": 1.5,
  "MaxDelaySeconds": 120
}
```

`States.TaskFailed` is the generic Step Functions error raised when a task (Glue job, SNS call, SDK call) returns a failure response that does not match any more specific error name. For Glue jobs, this covers transient worker provisioning failures, brief Glue service disruptions, and other non-specific failures.

Two retry attempts with a gentle backoff handle brief infrastructure blips. Unlike `ConcurrentRunsExceededException`, `States.TaskFailed` on a Glue job could indicate a real pipeline bug (bad data causing a Glue job crash). Retrying more than twice risks spending 90+ seconds only to fail again on the same data issue. Two attempts is the minimum meaningful retry for transient errors while limiting wasted execution time on deterministic failures.

---

## The `Catch` Block

After all retry attempts for a given error are exhausted, the Catch block fires:

```json
"Catch": [
  {
    "ErrorEquals": ["States.ALL"],
    "ResultPath": "$.failureDetail",
    "Next": "NotifyFailure"
  }
]
```

### `ErrorEquals: ["States.ALL"]`

`States.ALL` is a wildcard that matches any error not already handled by a Retry entry. It catches `Glue.JobRunFailed` (the job completed but with a FAILED status — a pipeline logic error, not an infrastructure error), `States.HeartbeatTimeout`, `States.Timeout`, and any other non-retried exception.

A single `States.ALL` Catch entry covers all unhandled errors. Multiple Catch entries for specific error types would be appropriate if different failures required different routing (e.g., route `States.HeartbeatTimeout` to a different notification message than `Glue.JobRunFailed`). In this pipeline, all failures route to the same `NotifyFailure` → `PipelineFailed` path, so a single wildcard is sufficient.

### `ResultPath: "$.failureDetail"`

When the Catch block fires, Step Functions writes the error details into `$.failureDetail` on the execution state:

```json
{
  "failureDetail": {
    "Error": "Glue.JobRunFailed",
    "Cause": "JobRun jr_abc123 failed. Error: AnalysisException: orders is not a Delta table at s3://..."
  }
}
```

Without `ResultPath`, the Catch block would replace the entire execution state with just `{"Error": "...", "Cause": "..."}` — all the original input (`$.bucket`, `$.batch`, `$.files`) would be lost. `ResultPath: "$.failureDetail"` appends the error under a dedicated key while preserving the original input. This is the same PreserveKey pattern used for task results throughout the state machine.

`NotifyFailure` then reads `$.failureDetail.Error` and `$.failureDetail.Cause` to construct the SNS message:

```json
"Message": "States.Format('Pipeline failed. Batch: {}. Error: {}. Cause: {}.', $.batch, $.failureDetail.Error, $.failureDetail.Cause)"
```

The result is an SNS notification that contains the batch name, the specific error type, and the full cause string from Glue — enough context for an operator to identify the failure without navigating to the Step Functions console.

---

## `NotifyFailure` Routing Logic

```
Any Task state (after retries exhausted)
    │
    └── Catch: States.ALL → $.failureDetail
          │
          ▼
    NotifyFailure (SNS publish)
    Subject: "[dev] Pipeline FAILED — batch: apr_2025"
    Message: "Pipeline failed. Batch: apr_2025. Error: Glue.JobRunFailed. Cause: ..."
          │
          ▼
    PipelineFailed (Fail state)
    Step Functions execution status: FAILED
```

Every Task state in the pipeline — `ProcessProducts`, `ProcessOrders`, `ProcessOrderItems`, and all three crawler states within `RunCrawlers` — has this Catch block. A failure at any point routes to the same `NotifyFailure` → `PipelineFailed` path. There is no partial recovery: if `ProcessProducts` fails, neither `ProcessOrders` nor `ProcessOrderItems` runs. This is correct behaviour — a failed products load means the products Delta table may be in an incomplete state; running orders with a corrupt products table would produce misleading referential integrity results.

---

## `HeartbeatTimeout` vs `Timeout` in Failure Messages

When `$.failureDetail.Error` is `States.HeartbeatTimeout`, the cause is that the Glue worker stopped sending heartbeats — typically a worker crash, OOM condition, or EC2 instance termination by AWS. The Glue job may or may not have committed data before the worker died.

When `$.failureDetail.Error` is `States.Timeout`, the cause is that the state's `TimeoutSeconds: 7200` (2 hours) was exceeded — the job is alive and sending heartbeats but has been running longer than the maximum allowed time.

Both arrive at `NotifyFailure` with different `Error` strings. An operator reading the SNS notification can distinguish them:

- `States.HeartbeatTimeout` → investigate Glue worker logs for OOM or spot instance termination; check if partial data was committed by querying Delta history
- `States.Timeout` → investigate why the job is slow; check input data volume and Glue worker configuration

The `Cause` string from Step Functions for `HeartbeatTimeout` typically reads `"No heartbeat received in 300 seconds"`. For `Timeout`, it reads `"Task timed out after 7200 seconds"`.
