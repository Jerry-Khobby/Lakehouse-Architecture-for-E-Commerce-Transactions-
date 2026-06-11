import csv
import os
import random

from constants import OUTPUT_DIR, DEPARTMENTS, PRODUCT_NAMES

PRODUCT_SUFFIXES = [
    "Original", "Classic", "Premium", "Value Pack",
    "Large", "Small", "Organic", "Fresh", "Natural", "",
]


def generate_products():
    products = []
    pid = 1
    dept_product_lists = [
        (dept_id, dept_name, PRODUCT_NAMES[dept_name])
        for dept_id, dept_name in DEPARTMENTS
    ]

    while pid <= 1000:
        for dept_id, dept_name, name_pool in dept_product_lists:
            if pid > 1000:
                break
            base_name = random.choice(name_pool)
            suffix = random.choice(PRODUCT_SUFFIXES)
            full_name = f"{base_name} {suffix}".strip()
            products.append((pid, dept_id, dept_name, full_name))
            pid += 1

    return products


def write_products(products):
    path = os.path.join(OUTPUT_DIR, "products.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["product_id", "department_id", "department", "product_name"])
        for product_id, dept_id, dept_name, product_name in products:
            writer.writerow([product_id, dept_id, dept_name, product_name])
    print(f"Written: {path}  ({len(products)} rows)")
