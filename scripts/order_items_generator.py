import csv
import os
import random

from constants import OUTPUT_DIR, MAY_START, MAY_END
from helpers import random_timestamp, format_timestamp, format_date

ITEM_FIELDS = [
    "id",
    "order_id",
    "user_id",
    "days_since_prior_order",
    "product_id",
    "add_to_cart_order",
    "reordered",
    "order_timestamp",
    "date",
]

CLEAN_ITEM_TARGET = 2500
INVALID_PRODUCT_ID_BASE = 99999


def generate_clean_items(clean_orders, valid_product_ids):
    items = []
    item_id = 1
    order_pool = list(clean_orders)
    random.shuffle(order_pool)

    for order in order_pool:
        if item_id > CLEAN_ITEM_TARGET:
            break
        for cart_pos in range(1, random.randint(1, 8) + 1):
            if item_id > CLEAN_ITEM_TARGET:
                break
            days = random.choice([None] + list(range(0, 366)))
            items.append(
                {
                    "id": item_id,
                    "order_id": order["order_id"],
                    "user_id": order["user_id"],
                    "days_since_prior_order": "" if days is None else str(days),
                    "product_id": random.choice(valid_product_ids),
                    "add_to_cart_order": cart_pos,
                    "reordered": random.randint(0, 1),
                    "order_timestamp": order["order_timestamp"],
                    "date": order["date"],
                }
            )
            item_id += 1

    return items, item_id


def generate_dirty_items(next_id, clean_orders, valid_product_ids):
    dirty = []
    valid_order_ids = [o["order_id"] for o in clean_orders]
    user_by_order_id = {o["order_id"]: o["user_id"] for o in clean_orders}
    current_id = next_id

    for _ in range(10):
        oid = random.choice(valid_order_ids)
        dirty.append(
            {
                "id": "",
                "order_id": oid,
                "user_id": user_by_order_id[oid],
                "days_since_prior_order": str(random.randint(0, 365)),
                "product_id": random.choice(valid_product_ids),
                "add_to_cart_order": 1,
                "reordered": random.randint(0, 1),
                "order_timestamp": random.choice(clean_orders)["order_timestamp"],
                "date": random.choice(clean_orders)["date"],
            }
        )
        current_id += 1

    for i in range(10):
        oid = random.choice(valid_order_ids)
        dirty.append(
            {
                "id": current_id,
                "order_id": oid,
                "user_id": user_by_order_id[oid],
                "days_since_prior_order": str(random.randint(0, 365)),
                "product_id": INVALID_PRODUCT_ID_BASE + i,
                "add_to_cart_order": 1,
                "reordered": random.randint(0, 1),
                "order_timestamp": random.choice(clean_orders)["order_timestamp"],
                "date": random.choice(clean_orders)["date"],
            }
        )
        current_id += 1

    for i in range(10):
        ts = random_timestamp(MAY_START, MAY_END)
        dirty.append(
            {
                "id": current_id,
                "order_id": f"ghost_{i + 1:03d}",
                "user_id": f"usr_{random.randint(1, 200):03d}",
                "days_since_prior_order": str(random.randint(0, 365)),
                "product_id": random.choice(valid_product_ids),
                "add_to_cart_order": 1,
                "reordered": random.randint(0, 1),
                "order_timestamp": format_timestamp(ts),
                "date": format_date(ts),
            }
        )
        current_id += 1

    for _ in range(10):
        oid = random.choice(valid_order_ids)
        dirty.append(
            {
                "id": current_id,
                "order_id": oid,
                "user_id": user_by_order_id[oid],
                "days_since_prior_order": str(random.randint(0, 365)),
                "product_id": random.choice(valid_product_ids),
                "add_to_cart_order": 1,
                "reordered": 5,
                "order_timestamp": random.choice(clean_orders)["order_timestamp"],
                "date": random.choice(clean_orders)["date"],
            }
        )
        current_id += 1

    return dirty


def write_order_items(clean_items, dirty_items):
    all_rows = [{k: r[k] for k in ITEM_FIELDS} for r in clean_items] + [
        {k: r[k] for k in ITEM_FIELDS} for r in dirty_items
    ]
    random.shuffle(all_rows)

    path = os.path.join(OUTPUT_DIR, "order_items_may_2025.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ITEM_FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Written: {path}  " f"({len(clean_items)} clean + {len(dirty_items)} dirty = {len(all_rows)} total rows)")
