import os
import random
import sys

sys.path.insert(0, os.path.dirname(__file__))

from constants import OUTPUT_DIR
from products_generator import generate_products, load_product_ids, write_products
from orders_generator import generate_clean_orders, generate_dirty_orders, write_orders
from order_items_generator import generate_clean_items, generate_dirty_items, write_order_items

random.seed(42)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    products_path = os.path.join(OUTPUT_DIR, "products.csv")
    if os.path.exists(products_path):
        print("products.csv already exists — loading product IDs from it (original data preserved).")
        valid_product_ids = load_product_ids(products_path)
        print(f"  Loaded {len(valid_product_ids)} product IDs.\n")
    else:
        print("Generating products...")
        products = generate_products()
        write_products(products)
        valid_product_ids = [p[0] for p in products]

    print("Generating orders (clean)...")
    clean_orders = generate_clean_orders()

    print("Generating orders (dirty)...")
    dirty_orders = generate_dirty_orders(clean_orders)
    write_orders(clean_orders, dirty_orders)

    print("Generating order items (clean)...")
    clean_items, next_id = generate_clean_items(clean_orders, valid_product_ids)

    print("Generating order items (dirty)...")
    dirty_items = generate_dirty_items(next_id, clean_orders, valid_product_ids)
    write_order_items(clean_items, dirty_items)

    print("\nDone.")


if __name__ == "__main__":
    main()
