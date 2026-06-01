"""
Bronze Ingestion — Landing Zone → Bronze Delta Table

Strategy:
  - Append-only: raw data is never modified in Bronze
  - Incremental: only unprocessed files are read (via FileTracker)
  - Each row is enriched with metadata (source file, ingestion timestamp)
  - Bad files are caught and logged in the tracker; pipeline continues
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

from utils.spark_session import get_spark_session
from utils.file_tracker import get_unprocessed_files, mark_file_processed, mark_file_failed
from config.settings import LANDING_PATH, BRONZE_PATH


# Explicit schema — never infer in production
LANDING_SCHEMA = StructType([
    StructField("order_id",    StringType(),    True),
    StructField("customer_id", StringType(),    True),
    StructField("status",      StringType(),    True),
    StructField("amount",      DoubleType(),    True),
    StructField("items",       IntegerType(),   True),
    StructField("created_at",  TimestampType(), True),
    StructField("updated_at",  TimestampType(), True),
])


def ingest_file_to_bronze(spark, file_path: str) -> int:
    """
    Read a single landing file and append it to the Bronze Delta table.
    Returns the number of rows written.
    """
    file_name = os.path.basename(file_path)

    df = spark.read.schema(LANDING_SCHEMA).option("header", "true").csv(file_path)

    # Add Bronze metadata columns
    df = df.withColumn("_source_file",   F.lit(file_name)) \
           .withColumn("_ingested_at",   F.current_timestamp()) \
           .withColumn("_ingestion_date", F.to_date(F.current_timestamp()))

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingestion_date")   # partition by ingest date for pruning
        .save(BRONZE_PATH)
    )

    return row_count


def run_bronze_ingestion():
    spark = get_spark_session("Bronze_Ingestion")
    print("\n" + "="*60)
    print("BRONZE INGESTION — Starting")
    print("="*60)

    unprocessed = get_unprocessed_files(spark, LANDING_PATH)

    if not unprocessed:
        print("[Bronze] No new files to process. Exiting.")
        return

    success_count = 0
    fail_count = 0

    for file_path in unprocessed:
        file_name = os.path.basename(file_path)
        print(f"\n[Bronze] Processing: {file_name}")
        try:
            row_count = ingest_file_to_bronze(spark, file_path)
            mark_file_processed(spark, file_path, row_count)
            success_count += 1
            print(f"[Bronze] ✓ {file_name} → {row_count} rows appended to Bronze")
        except Exception as e:
            mark_file_failed(spark, file_path, str(e))
            fail_count += 1
            print(f"[Bronze] ✗ {file_name} failed: {e}")

    print("\n" + "="*60)
    print(f"BRONZE INGESTION — Complete: {success_count} succeeded, {fail_count} failed")
    print("="*60)

    # Show Bronze table stats
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    total = bronze_df.count()
    print(f"\n[Bronze] Total rows in Bronze table: {total:,}")
    bronze_df.groupBy("_ingestion_date").count().orderBy("_ingestion_date").show()


if __name__ == "__main__":
    run_bronze_ingestion()
