# Real-Time vs Batch — Design Justification

## The Question

The project brief states the system should "detect new data dropped into S3's raw zone." A natural reading of this is an automatic, event-driven trigger — a file lands, a pipeline fires. This document explains why this architecture deliberately chose batch ingestion over event-driven real-time processing, why that decision is correct for this specific use case, and how the simulated trigger (`ingest.py`) achieves the same operational outcome without the correctness risks of per-file event triggers.

---

## What Real-Time Would Look Like Here

In a pure event-driven design, an S3 `ObjectCreated` event for each file would trigger an EventBridge rule, which would start one Step Functions execution per file:

```
products.csv    lands → EventBridge → Start execution A (products only)
orders.csv      lands → EventBridge → Start execution B (orders only)
order_items.csv lands → EventBridge → Start execution C (order_items only)
```

Or alternatively, a single file landing would trigger a pipeline that tries to pick up all three:

```
order_items.csv lands → EventBridge → Start execution
                                       └── tries to join against products and orders
                                           (which may not exist yet)
```

Both patterns fail for the same fundamental reason.

---

## Why Per-File Event Triggers Are Incompatible with This Dataset

The three datasets are not independent. `order_items` has two foreign keys:

- `order_items.product_id` → must exist in `products`
- `order_items.order_id` → must exist in `orders`

The `order_items` Glue job validates these relationships by reading the live Delta tables:

```python
# Referential integrity — order_items validation stage
products_df = spark.read.format("delta").load(products_table_path)
orphan_product = valid_df.join(products_df, "product_id", "left_anti")
# → rows with no matching product_id → rejected as "invalid_product_id"
```

If `order_items.csv` triggers its own execution before `products.csv` and `orders.csv` have been processed and committed to their Delta tables, this join reads an empty (or stale) Delta table. Every row in `order_items` that references any product or any order gets flagged as an orphan and rejected — correctly by the code's own logic, but incorrectly from a business standpoint.

This is not a race condition that can be resolved with retries or delays. The problem is structural: you cannot validate referential integrity against data that has not been written yet. The datasets must be processed in dependency order, within a single coordinated execution.

**This is documented explicitly in `terraform/main.tf`:**

> "There is intentionally NO EventBridge S3 trigger. The three datasets form one relational batch (order_items references products and orders), so they are ingested by a SINGLE Step Functions execution started explicitly by ingestion/ingest.py after all three files have landed. Per-file S3 events would fire three independent executions and race the referential-integrity checks."

---

## Why Streaming Is Also Not Appropriate

Streaming ingestion (Kinesis Data Streams, Kafka, Kinesis Firehose) processes records individually or in micro-batches as they arrive, typically with sub-minute latency. This is the right architecture when:

- Source data arrives continuously (clickstreams, IoT sensors, financial tick data).
- Business decisions depend on the data within seconds of it being generated.
- Individual records are independently valid — no record depends on another record from a different stream being present first.

None of these conditions hold for this e-commerce use case:

**1. Data arrives as monthly snapshots, not as a continuous stream.**

The source system produces complete monthly export files: `orders_apr_2025.xlsx` contains every order for April 2025, exported once at month end. There is no continuous feed of individual order events. The data structure itself is a batch — a point-in-time snapshot of a month's transactions.

Building Kinesis or Kafka infrastructure around data that arrives once per month adds ~$100–$300/month in always-on streaming costs for zero latency benefit. The business needs monthly analytics freshness; streaming would deliver second-level freshness that nobody is consuming.

**2. Records are not independently valid.**

In a streaming model, each event is self-contained. An IoT temperature reading at 14:32:01 does not depend on the reading at 14:32:00 being present in the system. Here, an `order_items` record is only valid in the presence of its parent `order` and its referenced `product`. A streaming pipeline processing `order_items` events would need to look up parent records from somewhere — which would either be a slow external lookup (defeating latency goals) or a stateful join with a windowed stream of orders and products (complex, expensive, and error-prone for monthly batch data).

**3. The business cycle is monthly.**

Analysts query last month's completed orders. There is no business scenario where a 15-minute-old order matters differently than a 20-minute-old order. The analytical workload is retrospective, not real-time. Monthly batch ingestion aligns the pipeline's cadence with the business's actual consumption cadence.

---

## What the Project Does Instead — Simulated Trigger

The project implements what the brief calls a "simulated trigger": `ingest.py` acts as the event that a real-time trigger would produce, but with the critical difference that all three files are guaranteed to be present before any processing begins.

The flow is:

```
Operator (or CI/CD job)
  │
  ├── upload products.csv    → raw/products.csv
  ├── upload orders.csv      → raw/orders_apr_2025.csv
  ├── upload order_items.csv → raw/order_items_apr_2025.csv
  │
  └── sfn.start_execution(input={
          "bucket": "...",
          "batch": "apr_2025",
          "files": {
              "products":    "raw/products.csv",
              "orders":      "raw/orders_apr_2025.csv",
              "order_items": "raw/order_items_apr_2025.csv"
          }
      })
```

This achieves the operational outcome of "the pipeline fires when data arrives" while making "data arrives" mean "all three files for the batch are present and confirmed uploaded." The trigger is not automatic, but it is deliberate and safe.

### Why This Is the Correct Trade-off

| Property | Per-file EventBridge | This batch trigger |
|---|---|---|
| Latency | Seconds (but broken) | Minutes (but correct) |
| Referential integrity | Violated — order_items sees empty parent tables | Guaranteed — parents committed before children run |
| Operational complexity | High — three independent executions to monitor | Low — one execution with full audit trail |
| Concurrent execution risk | High — three executions compete for Delta writes | None — one execution, strictly sequential |
| Cost | EventBridge + Lambda + three partial executions | One Step Functions execution |
| Idempotency | Difficult — partial runs leave partial state | Simple — one execution, one set of Delta commits |

---

## How the Simulated Trigger Would Become a Real Trigger

If the business requirement ever changes to a more frequent cadence (daily or hourly drops), the simulated trigger can be replaced without changing the Glue jobs or the state machine. Two options:

**Option 1 — Scheduled EventBridge rule on a cron.**

Instead of a human or CI job running `ingest.py`, an EventBridge scheduled rule runs a Lambda that calls `sfn.start_execution` with the same structured input. The Lambda checks that all three expected files for the period are present in `raw/` before starting. This keeps the "all three files present" guarantee while removing the human trigger.

```python
# Lambda handler — fires on cron, checks all files present before starting
def handler(event, context):
    batch = compute_current_batch()   # e.g. "jun_2025"
    expected_keys = [
        f"raw/products.csv",
        f"raw/orders_{batch}.csv",
        f"raw/order_items_{batch}.csv"
    ]
    for key in expected_keys:
        if not s3_object_exists(bucket, key):
            print(f"Missing {key} — not starting execution.")
            return
    sfn.start_execution(...)
```

**Option 2 — S3 event notification with a completeness gate.**

An S3 event fires on every `ObjectCreated` in `raw/`. A Lambda increments a counter in DynamoDB for the current batch (keyed by date or batch label). When the counter reaches 3 (all three files have arrived), the Lambda starts the Step Functions execution. This handles files arriving at different times without requiring all three to land simultaneously.

Neither of these options is currently implemented because the business cadence (monthly manual exports) does not require them. The architecture is structured to add either option without touching the Glue jobs or the state machine.

---

## How the Current Design Handles the "Simulate Trigger" Requirement

The project brief says: "Detect a new file arrival in S3 (simulate trigger)." The word "simulate" is the key word. It acknowledges that a true automatic trigger is optional for the purposes of this project. The simulation is `ingest.py`:

1. It performs the same action as an event-driven trigger: it detects that files are ready (because it just uploaded them), and it fires the pipeline.
2. It produces the same execution input that an EventBridge Lambda would produce.
3. It is the correct architecture for data that arrives as a complete relational batch, not as a stream of independent events.

The choice to not use EventBridge is not a limitation — it is a documented architectural decision based on the referential integrity requirements of the specific datasets in this project.
