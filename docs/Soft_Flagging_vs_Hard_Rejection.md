# Soft Flagging vs Hard Rejection — Business Anomalies Are Not Data Errors

## Overview

Not every suspicious row is a data error. The pipeline draws a sharp line between two categories of problematic rows: hard rejections (rows that are provably wrong and must not enter the Silver layer) and soft flags (rows that are structurally valid but exhibit characteristics that a business analyst should review). Hard-rejected rows go to `rejected/` and are excluded from the Delta MERGE. Soft-flagged rows go to `flagged/` and are still merged into the Delta table. This document explains the conceptual distinction, the concrete business rules that define the boundary, and why conflating the two would produce a less reliable pipeline.

---

## The Conceptual Distinction

### Hard Rejection — The Data Is Wrong

A hard rejection means the pipeline can assert with certainty that the row does not represent a legitimate, complete business event. The assertion is based on structural rules that have no valid exceptions:

- A null primary key cannot identify any entity — there is no business scenario where `product_id = null` represents a real product.
- `add_to_cart_order = 0` cannot be a valid cart position — the first item in a cart is position 1, and position 0 does not exist in any numbering scheme.
- `order_id = "abc123"` with no matching row in the orders Delta table means this order item references an order that has never been committed to the pipeline. No valid business event can produce a line item for an order that does not exist.

For these rows, writing them to the Delta table would introduce false or meaningless records into the Silver layer. Athena queries over the Silver layer would produce incorrect aggregations. The pipeline rejects them without hesitation, writes them to `rejected/` for diagnosis, and continues with the valid rows.

### Soft Flag — The Data Is Unusual, Not Wrong

A soft flag means the pipeline cannot assert that the row is wrong — it can only observe that the row is statistically unusual or business-rule-adjacent. There is a legitimate business scenario that produces this row. An analyst who sees it in `flagged/` should investigate, but it would be wrong to remove it from the Silver layer without that investigation.

Examples of soft flags:
- An order with `total_amount = $47,500`. This is a very large order for a typical e-commerce platform, but it is not impossible. A corporate bulk purchase, an enterprise license renewal, or a luxury goods order can legitimately have this value. Rejecting it would silently remove a real transaction from the Silver layer.
- An order with `total_amount = $0.00`. Most orders have a positive total. A zero-total order might be a bug in the source system, or it might be a fully-discounted promotional order. The pipeline cannot distinguish between them.
- An order item where `reordered = 1` but the user's history shows no prior orders. This is inconsistent internal state from the source system, but the order item itself is structurally valid.

---

## What Goes to `flagged/`

### Large Order Threshold

```python
LARGE_ORDER_THRESHOLD = Decimal("10000.00")

large_orders = valid_df.filter(F.col("total_amount") > F.lit(LARGE_ORDER_THRESHOLD))

if not large_orders.rdd.isEmpty():
    write_flagged(
        spark,
        large_orders,
        dataset="orders",
        flag_reason="large_order_amount",
        run_id=job_run_id,
        s3_bucket=bucket,
    )
```

Orders with `total_amount > $10,000` are written to `flagged/orders/` with `flag_reason = "large_order_amount"`. Critically, they are **not** removed from `valid_df` — they proceed to the MERGE and are committed to the Delta table. The `flagged/` record is a side-channel notification that these rows exist and warrant review.

The $10,000 threshold is a configurable business rule, not a technical constant. It is passed as a Glue job argument (`LARGE_ORDER_THRESHOLD`) so it can be tuned without a code deployment. The default of $10,000 was chosen as a round number above which manual review is expected for this e-commerce category.

### Zero Total Amount

```python
zero_total = valid_df.filter(F.col("total_amount") == Decimal("0.00"))

if not zero_total.rdd.isEmpty():
    write_flagged(
        spark,
        zero_total,
        dataset="orders",
        flag_reason="zero_total_amount",
        run_id=job_run_id,
        s3_bucket=bucket,
    )
```

A `total_amount` of exactly zero is not rejected because there are legitimate business cases (fully-discounted orders, internal test orders that reached production). It is flagged because zero-total orders can also indicate a missing line items join failure upstream or a pricing calculation error. An analyst reviewing `flagged/orders/` for `flag_reason = "zero_total_amount"` can determine which case applies and whether a source system correction is needed.

Note that `total_amount <= 0` has different handling across the two layers:
- `total_amount < 0` is a **hard rejection** (`"invalid_total_amount"`) — a negative order total is impossible in any business context.
- `total_amount = 0` is a **soft flag** (`"zero_total_amount"`) — possible but unusual.

This is the clearest illustration of the distinction: the sign of the total determines whether we know the row is wrong (`< 0`) or merely suspect it (`= 0`).

---

## `write_flagged()` — Mirror of `write_rejected()`

```python
def write_flagged(
    spark: SparkSession,
    flagged_df: DataFrame,
    dataset: str,
    flag_reason: str,
    run_id: str,
    s3_bucket: str,
    source_key: str = "",
) -> None:
    if flagged_df is None or flagged_df.rdd.isEmpty():
        return

    now = datetime.utcnow()
    output_path = f"s3://{s3_bucket}/flagged/{dataset}/{now.strftime('%Y-%m-%d')}/{run_id}/"

    enriched = (
        flagged_df
        .withColumn("_flagged_at",  F.lit(now.isoformat()).cast(TimestampType()))
        .withColumn("_flag_reason", F.lit(flag_reason))
        .withColumn("_job_run_id",  F.lit(run_id))
        .withColumn("_source_key",  F.lit(source_key))
    )
    enriched.write.mode("overwrite").parquet(output_path)
```

The structure mirrors `write_rejected()` exactly. The prefix is `flagged/` instead of `rejected/`. The audit column is `_flag_reason` instead of `rejection_reason`. The same three-level directory structure (`flagged/<dataset>/<date>/<run_id>/`) and the same four audit columns apply. The same 60-day lifecycle rule expires flagged records.

The key difference from `write_rejected()` is that `write_flagged()` does not require any caller to exclude the flagged rows from the main DataFrame — the caller decides independently whether to flag-only (keep in valid_df) or flag-and-reject (also remove from valid_df). For large orders and zero totals, the call is flag-only.

---

## Why Flagged Rows Still Enter the Silver Layer

Excluding flagged rows from the MERGE would mean a $47,500 order is missing from `lakehouse-dwh/orders/`. Athena queries for total revenue would undercount. Any analyst building a report from the Silver layer would have an incomplete dataset. They might not know that large orders were silently removed — there is no missing indicator in the Delta table for excluded rows.

If a $47,500 order turns out to be fraudulent after review, the correct response is a targeted `DELETE` in the Delta table after the investigation concludes, not a blanket pre-MERGE exclusion for all large orders. Delta Lake supports `DeltaTable.delete(condition)` for exactly this use case:

```python
delta_table.delete(condition="order_id = 'ord-12345-fraud'")
```

This produces a new Delta log entry recording the deletion, which is auditable. A pre-MERGE exclusion produces no Delta history — the row was never there, and there is no log of why.

---

## The Dangerous Middle Ground

A tempting but incorrect approach is to treat anomalous rows as rejections with a soft reason: reject them (remove from valid_df), write them to `rejected/` with `rejection_reason = "large_order_amount"`, and leave the investigation to later.

This approach is wrong for two reasons:

**Reason 1 — It contaminates the `rejected/` prefix with false positives.** The `rejected/` prefix is monitored for data quality issues. An operator who sees `rejected = 15` in the CloudWatch log investigates immediately — 15 rejections suggests a source system problem. If 12 of those 15 are large orders that are actually legitimate, the operator spends time investigating something that is not a defect. Over time, if large orders are routinely "rejected," operators begin ignoring the rejection count — and real data quality problems (null keys, referential failures) get lost in the noise.

**Reason 2 — It silently removes valid business data from the Silver layer.** Revenue reporting, customer lifetime value calculations, and product demand forecasting all depend on complete order data. A systematic exclusion of orders above $10,000 — even if documented — produces a Silver layer that is wrong by design for high-value segments. This is worse than an explicit schema error because it is not obvious from the data itself that anything is missing.

The flagging pattern separates the concerns cleanly:
- `rejected/` = rows that were definitively wrong and must not be in the Silver layer
- `flagged/` = rows that are in the Silver layer but were also noted as unusual for review
- Silver layer = complete, every-valid-row committed state

---

## Flagging vs Rejection Decision Matrix

| Row Characteristic | Category | Destination | Enters Delta? | Reason |
|---|---|---|---|---|
| Null primary key | Hard rejection | `rejected/` | No | Cannot identify the entity |
| Negative ID | Hard rejection | `rejected/` | No | Impossible in source system |
| Referential failure | Hard rejection | `rejected/` | No | References non-existent entity |
| Unparseable timestamp | Hard rejection | `rejected/` | No | Cannot be cast to valid type |
| `total_amount < 0` | Hard rejection | `rejected/` | No | Negative order value is impossible |
| Intra-batch duplicate | Hard rejection | `rejected/` | No (only one representative enters) | Cardinality violation in MERGE |
| `total_amount = 0` | Soft flag | `flagged/` | **Yes** | Possible (discounts) but unusual |
| `total_amount > $10,000` | Soft flag | `flagged/` | **Yes** | Possible (bulk/luxury) but unusual |
| `reordered = 1`, no prior order history | Soft flag | `flagged/` | **Yes** | Internal state inconsistency, not a data error |
