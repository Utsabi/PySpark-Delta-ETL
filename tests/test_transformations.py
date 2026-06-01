"""
Unit Tests — Silver transformation logic

Tests use small in-memory DataFrames (no Delta/disk needed).
Run: pytest tests/test_transformations.py -v
"""

import os
import sys
import pytest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("TestSuite")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("order_id",    StringType(),    True),
    StructField("customer_id", StringType(),    True),
    StructField("status",      StringType(),    True),
    StructField("amount",      DoubleType(),    True),
    StructField("items",       IntegerType(),   True),
    StructField("created_at",  TimestampType(), True),
    StructField("updated_at",  TimestampType(), True),
    StructField("_ingested_at",TimestampType(), True),
    StructField("_source_file",StringType(),    True),
])

T1 = datetime(2024, 1, 1, 10, 0, 0)
T2 = datetime(2024, 1, 1, 12, 0, 0)
T3 = datetime(2024, 1, 1, 14, 0, 0)


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


# ── Import logic under test ────────────────────────────────────────────────────

def cleanse_logic(df):
    """Replicated from silver_transform to test independently."""
    from config.settings import VALID_STATUSES
    df = df.withColumn(
        "_dq_issues",
        F.concat_ws(", ",
            F.when(F.col("order_id").isNull(), F.lit("null order_id")),
            F.when(F.col("customer_id").isNull(), F.lit("null customer_id")),
            F.when(F.col("amount") <= 0, F.lit("non-positive amount")),
            F.when(~F.col("status").isin(VALID_STATUSES), F.lit("invalid status")),
            F.when(F.col("items") <= 0, F.lit("non-positive items")),
            F.when(F.col("updated_at") < F.col("created_at"), F.lit("updated_at before created_at")),
        )
    )
    return df.filter(F.col("_dq_issues") == "")


def deduplicate_logic(df):
    """Replicated from silver_transform."""
    from pyspark.sql import Window
    window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestCleanse:

    def test_valid_row_passes(self, spark):
        df = make_df(spark, [
            ("ORD_001", "CUST_1", "pending", 99.99, 1, T1, T2, T3, "f.csv")
        ])
        result = cleanse_logic(df)
        assert result.count() == 1

    def test_null_order_id_rejected(self, spark):
        df = make_df(spark, [
            (None, "CUST_1", "pending", 99.99, 1, T1, T2, T3, "f.csv")
        ])
        result = cleanse_logic(df)
        assert result.count() == 0

    def test_zero_amount_rejected(self, spark):
        df = make_df(spark, [
            ("ORD_001", "CUST_1", "pending", 0.0, 1, T1, T2, T3, "f.csv")
        ])
        result = cleanse_logic(df)
        assert result.count() == 0

    def test_invalid_status_rejected(self, spark):
        df = make_df(spark, [
            ("ORD_001", "CUST_1", "refunded", 50.0, 1, T1, T2, T3, "f.csv")
        ])
        result = cleanse_logic(df)
        assert result.count() == 0

    def test_updated_before_created_rejected(self, spark):
        df = make_df(spark, [
            ("ORD_001", "CUST_1", "pending", 50.0, 1, T2, T1, T3, "f.csv")  # T1 < T2
        ])
        result = cleanse_logic(df)
        assert result.count() == 0

    def test_mixed_rows(self, spark):
        df = make_df(spark, [
            ("ORD_001", "CUST_1", "pending",   99.99, 1, T1, T2, T3, "f.csv"),  # valid
            ("ORD_002", "CUST_2", "bad_status", 50.0, 1, T1, T2, T3, "f.csv"),  # invalid
            ("ORD_003", "CUST_3", "shipped",   -1.0,  1, T1, T2, T3, "f.csv"),  # invalid
        ])
        result = cleanse_logic(df)
        assert result.count() == 1
        assert result.collect()[0]["order_id"] == "ORD_001"


class TestDeduplicate:

    def test_keeps_latest_record(self, spark):
        df = make_df(spark, [
            ("ORD_001", "CUST_1", "pending",   99.99, 1, T1, T1, T3, "f.csv"),
            ("ORD_001", "CUST_1", "confirmed", 99.99, 1, T1, T2, T3, "f.csv"),  # latest
        ])
        result = deduplicate_logic(df)
        assert result.count() == 1
        assert result.collect()[0]["status"] == "confirmed"

    def test_no_duplicates_unchanged(self, spark):
        df = make_df(spark, [
            ("ORD_001", "CUST_1", "pending", 99.99, 1, T1, T2, T3, "f.csv"),
            ("ORD_002", "CUST_2", "shipped", 49.99, 2, T1, T2, T3, "f.csv"),
        ])
        result = deduplicate_logic(df)
        assert result.count() == 2

    def test_three_duplicates_keeps_latest(self, spark):
        df = make_df(spark, [
            ("ORD_001", "CUST_1", "pending",   100.0, 1, T1, T1, T3, "f.csv"),
            ("ORD_001", "CUST_1", "confirmed", 100.0, 1, T1, T2, T3, "f.csv"),
            ("ORD_001", "CUST_1", "shipped",   100.0, 1, T1, T3, T3, "f.csv"),  # latest
        ])
        result = deduplicate_logic(df)
        assert result.count() == 1
        assert result.collect()[0]["status"] == "shipped"
