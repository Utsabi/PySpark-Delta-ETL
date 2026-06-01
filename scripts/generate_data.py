"""
Data Generator — simulates 30 days of e-commerce order CSV files
being dropped into the landing zone.

Each day:
  - New orders are created (status = 'pending')
  - Existing orders progress through statuses (pending → confirmed → shipped → delivered)
  - Some orders get cancelled
  - Same order_id can appear in multiple files (forces upsert logic in Silver)

Run: python scripts/generate_data.py
"""

import os
import csv
import random
from datetime import datetime, timedelta

# Add project root to path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import LANDING_PATH

# Config
NUM_DAYS = 30
ORDERS_PER_DAY = 50          # new orders per day
UPDATE_RATIO = 0.4            # 40% of existing orders get a status update each day
CANCEL_RATIO = 0.1            # 10% chance a non-delivered order gets cancelled
RANDOM_SEED = 42

random.seed(RANDOM_SEED)

STATUS_TRANSITIONS = {
    "pending":   "confirmed",
    "confirmed": "shipped",
    "shipped":   "delivered",
}

PRODUCT_POOL = [
    ("Laptop", 999.99),
    ("Headphones", 149.99),
    ("Keyboard", 89.99),
    ("Monitor", 399.99),
    ("Mouse", 49.99),
    ("Webcam", 79.99),
    ("Desk Chair", 299.99),
    ("USB Hub", 34.99),
    ("SSD Drive", 119.99),
    ("Microphone", 199.99),
]


def random_customer_id():
    return f"CUST_{random.randint(1000, 9999)}"


def random_amount(items):
    products = random.choices(PRODUCT_POOL, k=items)
    return round(sum(p[1] for p in products), 2)


def generate_landing_files():
    os.makedirs(LANDING_PATH, exist_ok=True)

    start_date = datetime(2026, 1, 1)
    active_orders = {}   # order_id -> {status, created_at, customer_id, items, amount}
    order_counter = 1

    for day in range(NUM_DAYS):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y_%m_%d")
        file_path = os.path.join(LANDING_PATH, f"orders_{date_str}.csv")

        rows = []

        # 1. Create new orders for today
        for _ in range(ORDERS_PER_DAY):
            order_id = f"ORD_{order_counter:06d}"
            order_counter += 1
            items = random.randint(1, 4)
            amount = random_amount(items)
            created_at = current_date.replace(
                hour=random.randint(8, 20),
                minute=random.randint(0, 59)
            )
            active_orders[order_id] = {
                "customer_id": random_customer_id(),
                "status": "pending",
                "amount": amount,
                "items": items,
                "created_at": created_at,
                "updated_at": created_at,
            }
            rows.append({
                "order_id": order_id,
                **active_orders[order_id],
            })

        # 2. Update a subset of existing orders
        eligible = [
            oid for oid, o in active_orders.items()
            if o["status"] not in ("delivered", "cancelled")
            and oid not in [r["order_id"] for r in rows]  # don't update newly created
        ]
        to_update = random.sample(eligible, min(int(len(eligible) * UPDATE_RATIO), len(eligible)))

        for order_id in to_update:
            order = active_orders[order_id]
            # Decide: cancel or progress?
            if random.random() < CANCEL_RATIO:
                new_status = "cancelled"
            else:
                new_status = STATUS_TRANSITIONS.get(order["status"], order["status"])

            updated_at = current_date.replace(
                hour=random.randint(8, 22),
                minute=random.randint(0, 59)
            )
            active_orders[order_id]["status"] = new_status
            active_orders[order_id]["updated_at"] = updated_at

            rows.append({
                "order_id": order_id,
                **active_orders[order_id],
            })

        # Write to CSV
        fieldnames = ["order_id", "customer_id", "status", "amount", "items", "created_at", "updated_at"]
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f"[Generator] Day {day + 1:02d} | {date_str} | {len(rows)} rows "
              f"({ORDERS_PER_DAY} new + {len(to_update)} updates) → {file_path}")

    print(f"\n[Generator] Done. {NUM_DAYS} files written to {LANDING_PATH}")
    print(f"[Generator] Total unique orders created: {order_counter - 1}")


if __name__ == "__main__":
    generate_landing_files()
