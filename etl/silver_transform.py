"""
Silver Transformation — Bronze → Silver Delta Table

Strategy:
  - Incremental: only Bronze rows ingested since last Silver run are processed
  - Deduplication: within a batch, keep the latest record per order_id (by updated_at)
  - Upsert: MERGE INTO Silver on order_id so updates overwrite stale records
  - Cleansing: null checks, status validation, amount validation
  - High-water mark: tracked via Silver table's max(_bronze_ingested_at)
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

from utils.spark_session import get_spark_session
from config.settings import BRONZE_PATH, SILVER_PATH, VALID_STATUSES, SILVER_MERGE_KEY


def get_high_water_mark(spark):
    """
    Returns the max _ingested_at from Silver — only Bronze rows
    AFTER this timestamp need to be processed.
    Returns None if Silver table doesn't exist yet (first run).
    """
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        result = (
            spark.read.format("delta").load(SILVER_PATH)
            .agg(F.max("_bronze_ingested_at").alias("hwm"))
            .collect()[0]["hwm"]
        )
        print(f"[Silver] High-water mark: {result}")
        return result
    print("[Silver] No high-water mark found — first run, processing all Bronze rows.")
    return None


def read_incremental_bronze(spark, high_water_mark):
    """Read only new Bronze rows since the last Silver run."""
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    if high_water_mark:
        bronze_df = bronze_df.filter(F.col("_ingested_at") > F.lit(high_water_mark))

    count = bronze_df.count()
    print(f"[Silver] Incremental Bronze rows to process: {count:,}")
    return bronze_df


def cleanse(df):
    """Apply data quality rules. Tag bad rows instead of dropping them."""
    valid_statuses_list = VALID_STATUSES

    df = df.withColumn(
        "_dq_issues",
        F.concat_ws(", ",
            F.when(F.col("order_id").isNull(), F.lit("null order_id")),
            F.when(F.col("customer_id").isNull(), F.lit("null customer_id")),
            F.when(F.col("amount") <= 0, F.lit("non-positive amount")),
            F.when(~F.col("status").isin(valid_statuses_list), F.lit("invalid status")),
            F.when(F.col("items") <= 0, F.lit("non-positive items")),
            F.when(F.col("updated_at") < F.col("created_at"), F.lit("updated_at before created_at")),
        )
    )

    clean = df.filter(F.col("_dq_issues") == "")
    rejected = df.filter(F.col("_dq_issues") != "")

    clean_count = clean.count()
    rejected_count = rejected.count()
    print(f"[Silver] Cleansed: {clean_count:,} clean, {rejected_count:,} rejected")

    if rejected_count > 0:
        print("[Silver] Sample rejected rows:")
        rejected.select("order_id", "status", "amount", "_dq_issues").show(5, truncate=False)

    return clean


def deduplicate(df):
    """
    Within the batch, keep only the latest record per order_id.
    This handles the case where the same order_id appears multiple times
    in the same Bronze batch (e.g., created and updated in the same day).
    """
    window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    deduped = (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    print(f"[Silver] After dedup: {deduped.count():,} rows")
    return deduped


def upsert_to_silver(spark, df):
    """
    MERGE incremental batch into Silver.
    If order_id already exists → update all fields.
    If new order_id → insert.
    """
    # Add Silver metadata
    df = df.withColumn("_bronze_ingested_at", F.col("_ingested_at")) \
           .withColumn("_silver_updated_at", F.current_timestamp()) \
           .drop("_ingested_at", "_ingestion_date", "_source_file", "_dq_issues")

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_table.alias("silver")
            .merge(df.alias("batch"), f"silver.{SILVER_MERGE_KEY} = batch.{SILVER_MERGE_KEY}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("[Silver] MERGE INTO Silver complete.")
    else:
        # First run — just write
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("status")
            .save(SILVER_PATH)
        )
        print("[Silver] Silver table created (first run).")


def run_silver_transform():
    spark = get_spark_session("Silver_Transform")
    print("\n" + "="*60)
    print("SILVER TRANSFORMATION — Starting")
    print("="*60)

    high_water_mark = get_high_water_mark(spark)
    bronze_df = read_incremental_bronze(spark, high_water_mark)

    if bronze_df.count() == 0:
        print("[Silver] No new Bronze rows. Silver is up to date.")
        return

    clean_df = cleanse(bronze_df)
    deduped_df = deduplicate(clean_df)
    upsert_to_silver(spark, deduped_df)

    # Summary
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    total = silver_df.count()
    print(f"\n[Silver] Total rows in Silver table: {total:,}")
    print("\n[Silver] Order status distribution:")
    silver_df.groupBy("status").count().orderBy("count", ascending=False).show()

    print("="*60)
    print("SILVER TRANSFORMATION — Complete")
    print("="*60)


if __name__ == "__main__":
    run_silver_transform()
