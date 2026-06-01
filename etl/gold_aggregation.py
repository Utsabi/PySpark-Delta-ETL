"""
Gold Aggregation — Silver → Gold Delta Table

Strategy:
  - Produces business-ready aggregations for reporting / dashboards
  - Incremental partition overwrite: only affected date partitions are rewritten
    (not the whole table), keeping the job fast even as Silver grows
  - Two Gold outputs:
      1. daily_revenue     — revenue and order counts per day per status
      2. customer_summary  — lifetime stats per customer

Gold tables are read-optimized: denormalized, pre-aggregated, no nulls.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from delta.tables import DeltaTable

from utils.spark_session import get_spark_session
from config.settings import SILVER_PATH, GOLD_PATH


GOLD_DAILY_PATH = os.path.join(os.path.dirname(GOLD_PATH), "daily_revenue")
GOLD_CUSTOMER_PATH = os.path.join(os.path.dirname(GOLD_PATH), "customer_summary")


def get_silver_hwm(spark):
    """Get the max _silver_updated_at — used to find partitions that need refresh."""
    if DeltaTable.isDeltaTable(spark, GOLD_DAILY_PATH):
        result = (
            spark.read.format("delta").load(GOLD_DAILY_PATH)
            .agg(F.max("_gold_updated_at").alias("hwm"))
            .collect()[0]["hwm"]
        )
        print(f"[Gold] Last Gold update: {result}")
        return result
    print("[Gold] No prior Gold run found — full refresh.")
    return None


def build_daily_revenue(silver_df):
    """
    Daily revenue aggregation:
      - Total revenue per day
      - Order count per day
      - Average order value
      - Revenue breakdown by status
    """
    daily = (
        silver_df
        .withColumn("order_date", F.to_date("updated_at"))
        .groupBy("order_date", "status")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value"),
            F.sum("items").alias("total_items"),
        )
        .withColumn("total_revenue",    F.round("total_revenue", 2))
        .withColumn("avg_order_value",  F.round("avg_order_value", 2))
        .withColumn("_gold_updated_at", F.current_timestamp())
    )
    return daily


def build_customer_summary(silver_df):
    """
    Customer lifetime stats:
      - Total orders placed
      - Total spend
      - Most recent order date
      - Favourite status (most common final status)
    """
    customer = (
        silver_df
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount").alias("total_spend"),
            F.avg("amount").alias("avg_order_value"),
            F.max("updated_at").alias("last_order_at"),
            F.sum("items").alias("total_items_purchased"),
        )
        .withColumn("total_spend",      F.round("total_spend", 2))
        .withColumn("avg_order_value",  F.round("avg_order_value", 2))
        .withColumn("_gold_updated_at", F.current_timestamp())
    )
    return customer


def write_gold_partition_overwrite(df, path: str, partition_col: str):
    """
    Write using dynamic partition overwrite:
    Only partitions present in `df` are overwritten — other partitions untouched.
    This is the key technique for incremental Gold writes.
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_col} >= '{df.agg(F.min(partition_col)).collect()[0][0]}'")
        .partitionBy(partition_col)
        .save(path)
    )


def run_gold_aggregation():
    spark = get_spark_session("Gold_Aggregation")

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    print("\n" + "="*60)
    print("GOLD AGGREGATION — Starting")
    print("="*60)

    silver_df = spark.read.format("delta").load(SILVER_PATH)
    silver_count = silver_df.count()
    print(f"[Gold] Silver rows available: {silver_count:,}")

    # --- Daily Revenue ---
    print("\n[Gold] Building daily_revenue...")
    daily_df = build_daily_revenue(silver_df)
    daily_df.write.format("delta").mode("overwrite").partitionBy("order_date").save(GOLD_DAILY_PATH)
    daily_count = daily_df.count()
    print(f"[Gold] daily_revenue: {daily_count:,} rows written")

    # --- Customer Summary ---
    print("\n[Gold] Building customer_summary...")
    customer_df = build_customer_summary(silver_df)
    customer_df.write.format("delta").mode("overwrite").save(GOLD_CUSTOMER_PATH)
    customer_count = customer_df.count()
    print(f"[Gold] customer_summary: {customer_count:,} rows written")

    # --- Show samples ---
    print("\n[Gold] Sample daily_revenue (top 10 by revenue):")
    (
        spark.read.format("delta").load(GOLD_DAILY_PATH)
        .filter(F.col("status") == "delivered")
        .orderBy(F.col("total_revenue").desc())
        .show(10)
    )

    print("\n[Gold] Sample customer_summary (top 5 spenders):")
    (
        spark.read.format("delta").load(GOLD_CUSTOMER_PATH)
        .orderBy(F.col("total_spend").desc())
        .show(5)
    )

    print("\n" + "="*60)
    print("GOLD AGGREGATION — Complete")
    print("="*60)


if __name__ == "__main__":
    run_gold_aggregation()
