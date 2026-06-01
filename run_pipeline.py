"""
run_pipeline.py — Orchestrates the full ETL pipeline end to end.

Usage:
    python run_pipeline.py              # run all layers
    python run_pipeline.py --bronze     # only Bronze
    python run_pipeline.py --silver     # only Silver
    python run_pipeline.py --gold       # only Gold

Run this after dropping new CSV files into data/landing/.
"""

import argparse
import sys
import time

def run_all(args):
    run_bronze = args.bronze or args.all
    run_silver = args.silver or args.all
    run_gold   = args.gold   or args.all

    if run_bronze:
        print("\n>>> STEP 1: Bronze Ingestion")
        t = time.time()
        from etl.bronze_ingestion import run_bronze_ingestion
        run_bronze_ingestion()
        print(f"    Completed in {time.time() - t:.1f}s")

    if run_silver:
        print("\n>>> STEP 2: Silver Transformation")
        t = time.time()
        from etl.silver_transform import run_silver_transform
        run_silver_transform()
        print(f"    Completed in {time.time() - t:.1f}s")

    if run_gold:
        print("\n>>> STEP 3: Gold Aggregation")
        t = time.time()
        from etl.gold_aggregation import run_gold_aggregation
        run_gold_aggregation()
        print(f"    Completed in {time.time() - t:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the flat-file Delta ETL pipeline")
    parser.add_argument("--bronze", action="store_true", help="Run Bronze ingestion only")
    parser.add_argument("--silver", action="store_true", help="Run Silver transform only")
    parser.add_argument("--gold",   action="store_true", help="Run Gold aggregation only")

    args = parser.parse_args()
    # If no flags, run all
    args.all = not (args.bronze or args.silver or args.gold)

    total = time.time()
    run_all(args)
    print(f"\n✓ Pipeline finished in {time.time() - total:.1f}s total")
