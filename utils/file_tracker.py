"""
File Tracker — Delta-backed table that records which landing files
have already been ingested into Bronze. This is what makes the
pipeline incremental: only unprocessed files are picked up each run.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from config.settings import FILE_TRACKER_PATH


# Schema for the file tracker table
TRACKER_SCHEMA = StructType([
    StructField("file_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("status", StringType(), False),       # 'processed' | 'failed'
    StructField("ingested_at", TimestampType(), False),
    StructField("row_count", StringType(), True),
    StructField("error_message", StringType(), True),
])


def init_tracker(spark: SparkSession) -> None:
    """Create the file tracker Delta table if it doesn't exist."""
    if not DeltaTable.isDeltaTable(spark, FILE_TRACKER_PATH):
        empty_df = spark.createDataFrame([], TRACKER_SCHEMA)
        (
            empty_df.write
            .format("delta")
            .mode("overwrite")
            .save(FILE_TRACKER_PATH)
        )
        print(f"[FileTracker] Initialized tracker table at {FILE_TRACKER_PATH}")
    else:
        print(f"[FileTracker] Tracker table already exists at {FILE_TRACKER_PATH}")


def get_processed_files(spark: SparkSession) -> set:
    """Return a set of file names that have already been successfully processed."""
    if not DeltaTable.isDeltaTable(spark, FILE_TRACKER_PATH):
        return set()

    processed = (
        spark.read.format("delta").load(FILE_TRACKER_PATH)
        .filter(F.col("status") == "processed")
        .select("file_name")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    return set(processed)


def get_unprocessed_files(spark: SparkSession, landing_path: str) -> list:
    """
    Compare all files in the landing zone against the tracker.
    Return list of full paths for files not yet processed.
    """
    init_tracker(spark)
    processed = get_processed_files(spark)

    all_files = [
        os.path.join(landing_path, f)
        for f in os.listdir(landing_path)
        if f.endswith((".csv", ".json")) and f not in processed
    ]

    unprocessed = sorted(all_files)  # sort by filename (date-ordered if named correctly)
    print(f"[FileTracker] Found {len(unprocessed)} unprocessed file(s) out of "
          f"{len(os.listdir(landing_path))} total in landing zone.")
    return unprocessed


def mark_file_processed(spark: SparkSession, file_path: str, row_count: int) -> None:
    """Mark a file as successfully processed in the tracker."""
    _upsert_tracker_record(spark, file_path, "processed", row_count, None)


def mark_file_failed(spark: SparkSession, file_path: str, error: str) -> None:
    """Mark a file as failed in the tracker."""
    _upsert_tracker_record(spark, file_path, "failed", 0, error)


def _upsert_tracker_record(
    spark: SparkSession,
    file_path: str,
    status: str,
    row_count: int,
    error_message: str
) -> None:
    """Internal: upsert a record into the file tracker Delta table."""
    file_name = os.path.basename(file_path)
    now = datetime.utcnow()

    record = spark.createDataFrame(
        [(file_name, file_path, status, now, str(row_count), error_message)],
        schema=TRACKER_SCHEMA
    )

    tracker = DeltaTable.forPath(spark, FILE_TRACKER_PATH)
    (
        tracker.alias("t")
        .merge(record.alias("s"), "t.file_name = s.file_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[FileTracker] Marked '{file_name}' as {status} ({row_count} rows)")


def show_tracker(spark: SparkSession) -> None:
    """Print the current state of the file tracker (for debugging)."""
    if DeltaTable.isDeltaTable(spark, FILE_TRACKER_PATH):
        spark.read.format("delta").load(FILE_TRACKER_PATH).show(truncate=False)
    else:
        print("[FileTracker] Tracker table not initialized yet.")
