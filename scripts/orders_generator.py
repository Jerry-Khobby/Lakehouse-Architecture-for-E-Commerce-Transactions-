import csv
import os
import random
from datetime import timedelta

from constants import OUTPUT_DIR, MAY_START, MAY_END, FUTURE_START, FUTURE_END
from helpers import random_timestamp, format_timestamp, format_date

ORDER_FIELDS = ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]

CLEAN_ORDER_COUNT = 800
DIRTY_ORDER_BASE_NUM = CLEAN_ORDER_COUNT + 1
USER_POOL = [f"usr_{i:03d}" for i in range(1, 201)]


def generate_clean_orders():
    orders = []
    used_ids = set()

    for i in range(1, CLEAN_ORDER_COUNT + 1):
        while True:
            order_id = f"ord_{random.randint(10000, 99999)}"
            if order_id not in used_ids:
                used_ids.add(order_id)
                break
        user_id = random.choice(USER_POOL)
        ts = random_timestamp(MAY_START, MAY_END)
        total = round(random.uniform(5.00, 500.00), 2)
        orders.append(
            {
                "order_num": i,
                "order_id": order_id,
                "user_id": user_id,
                "order_timestamp": format_timestamp(ts),
                "total_amount": f"{total:.2f}",
                "date": format_date(ts),
            }
        )

    return orders


def generate_dirty_orders(clean_orders):
    dirty = []
    base_num = DIRTY_ORDER_BASE_NUM

    for _ in range(10):
        ts = random_timestamp(MAY_START, MAY_END)
        total = round(random.uniform(5.00, 500.00), 2)
        dirty.append(
            {
                "order_num": base_num,
                "order_id": "",
                "user_id": random.choice(USER_POOL),
                "order_timestamp": format_timestamp(ts),
                "total_amount": f"{total:.2f}",
                "date": format_date(ts),
            }
        )
        base_num += 1

    for _ in range(10):
        ts = random_timestamp(MAY_START, MAY_END)
        total = round(random.uniform(-500.00, -0.01), 2)
        dirty.append(
            {
                "order_num": base_num,
                "order_id": f"ord_{random.randint(10000, 99999)}",
                "user_id": random.choice(USER_POOL),
                "order_timestamp": format_timestamp(ts),
                "total_amount": f"{total:.2f}",
                "date": format_date(ts),
            }
        )
        base_num += 1

    for _ in range(10):
        ts = random_timestamp(FUTURE_START, FUTURE_END)
        total = round(random.uniform(5.00, 500.00), 2)
        dirty.append(
            {
                "order_num": base_num,
                "order_id": f"ord_{random.randint(10000, 99999)}",
                "user_id": random.choice(USER_POOL),
                "order_timestamp": format_timestamp(ts),
                "total_amount": f"{total:.2f}",
                "date": format_date(ts),
            }
        )
        base_num += 1

    for _ in range(10):
        ts = random_timestamp(MAY_START, MAY_END)
        wrong_date = ts + timedelta(days=random.randint(1, 5))
        total = round(random.uniform(5.00, 500.00), 2)
        dirty.append(
            {
                "order_num": base_num,
                "order_id": f"ord_{random.randint(10000, 99999)}",
                "user_id": random.choice(USER_POOL),
                "order_timestamp": format_timestamp(ts),
                "total_amount": f"{total:.2f}",
                "date": format_date(wrong_date),
            }
        )
        base_num += 1

    for row in random.sample(clean_orders, 10):
        new_total = round(float(row["total_amount"]) + random.uniform(0.50, 10.00), 2)
        ts = random_timestamp(MAY_START, MAY_END)
        dirty.append(
            {
                "order_num": base_num,
                "order_id": row["order_id"],
                "user_id": row["user_id"],
                "order_timestamp": format_timestamp(ts),
                "total_amount": f"{new_total:.2f}",
                "date": format_date(ts),
            }
        )
        base_num += 1

    return dirty


def write_orders(clean_orders, dirty_orders):
    all_rows = [{k: r[k] for k in ORDER_FIELDS} for r in clean_orders] + [
        {k: r[k] for k in ORDER_FIELDS} for r in dirty_orders
    ]
    random.shuffle(all_rows)

    path = os.path.join(OUTPUT_DIR, "orders_may_2025.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ORDER_FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Written: {path}  " f"({len(clean_orders)} clean + {len(dirty_orders)} dirty = {len(all_rows)} total rows)")
