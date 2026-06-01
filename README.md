# Flat File → Delta Lake Incremental ETL Pipeline

A production-grade incremental ETL pipeline built with **PySpark** and **Delta Lake**,
implementing the **Medallion Architecture** (Bronze / Silver / Gold).

## Architecture

```
data/landing/          ← CSV files dropped daily
      │
      ▼
 [File Tracker]        ← Delta table tracking processed files
      │
      ▼
  Bronze Layer         ← Raw, append-only, partitioned by ingestion date
      │
      ▼
  Silver Layer         ← Cleaned, deduped, upserted via MERGE INTO
      │
      ▼
  Gold Layer           ← Business aggregations (daily revenue, customer summary)
```

## Key Concepts Demonstrated

| Concept | Where |
|---|---|
| Incremental ingestion via file tracking | `utils/file_tracker.py` |
| Idempotent Bronze append | `etl/bronze_ingestion.py` |
| High-water mark for Silver | `etl/silver_transform.py` |
| MERGE INTO (upsert) | `etl/silver_transform.py` |
| Data quality tagging | `etl/silver_transform.py` |
| Partition overwrite for Gold | `etl/gold_aggregation.py` |
| Schema enforcement | `etl/bronze_ingestion.py` |

## Project Structure

```
flat-file-delta-etl/
├── data/
│   ├── landing/                  # Drop CSV files here
│   └── delta/                    # Delta tables (auto-created)
│       ├── bronze/orders/
│       ├── silver/orders/
│       ├── gold/daily_revenue/
│       ├── gold/customer_summary/
│       └── meta/file_tracker/
├── etl/
│   ├── bronze_ingestion.py
│   ├── silver_transform.py
│   └── gold_aggregation.py
├── utils/
│   ├── spark_session.py
│   └── file_tracker.py
├── config/
│   └── settings.py
├── scripts/
│   └── generate_data.py
├── tests/
│   └── test_transformations.py
├── run_pipeline.py
└── requirements.txt
```

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate 30 days of sample data
python scripts/generate_data.py

# 3. Run the full pipeline
python run_pipeline.py

# 4. Simulate next day (drop a new file) and re-run
# Only new files will be picked up — existing data untouched
python run_pipeline.py
```

## Running Individual Layers

```bash
python run_pipeline.py --bronze    # Ingest new landing files only
python run_pipeline.py --silver    # Transform Bronze → Silver only
python run_pipeline.py --gold      # Aggregate Silver → Gold only
```

## Running Tests

```bash
pytest tests/ -v
```

## Incremental Load Strategy

### Bronze — File Tracking
The `file_tracker` Delta table records every processed filename.
On each run, only files NOT in the tracker are ingested. Safe to re-run.

### Silver — High-Water Mark
Silver tracks the max `_bronze_ingested_at` from its last run.
Only Bronze rows with a newer timestamp are processed, then MERGEd
into Silver so updates overwrite stale records without duplicates.

### Gold — Partition Overwrite
Gold rewrites only the date partitions affected by new Silver data.
Unaffected historical partitions are never touched.

## Tech Stack

- **PySpark 3.5** — distributed processing
- **Delta Lake 3.2** — ACID transactions, MERGE, time travel
- **pytest** — unit testing transformation logic
