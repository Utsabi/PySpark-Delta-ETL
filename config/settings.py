import os

# Base project directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Landing zone - raw files dropped here
LANDING_PATH = os.path.join(BASE_DIR, "data", "landing")

# Delta Lake storage paths
DELTA_BASE_PATH = os.path.join(BASE_DIR, "data", "delta")
BRONZE_PATH = os.path.join(DELTA_BASE_PATH, "bronze", "orders")
SILVER_PATH = os.path.join(DELTA_BASE_PATH, "silver", "orders")
GOLD_PATH = os.path.join(DELTA_BASE_PATH, "gold", "orders_summary")

# File tracker Delta table path
FILE_TRACKER_PATH = os.path.join(DELTA_BASE_PATH, "meta", "file_tracker")

# Checkpoint paths
CHECKPOINT_BASE = os.path.join(BASE_DIR, "data", "checkpoints")

# Schema settings
ORDERS_SCHEMA = {
    "order_id": "string",
    "customer_id": "string",
    "status": "string",
    "amount": "double",
    "items": "int",
    "created_at": "timestamp",
    "updated_at": "timestamp",
}

# Business keys for MERGE (upsert)
SILVER_MERGE_KEY = "order_id"

# Valid order statuses
VALID_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]

# Spark config
SPARK_APP_NAME = "FlatFileDeltaETL"
SPARK_MASTER = "local[*]"
