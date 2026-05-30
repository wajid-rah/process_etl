"""
main.py
-------
ETL Pipeline Orchestrator — Incremental Load with 2-day Lookback Window.

Incremental strategy:
    1. Read last_loaded_date from checkpoint.json
    2. Roll back 2 days (lookback window) → fetch_from date
    3. Extract only records >= fetch_from from API
    4. Filter out rows already in PostgreSQL (anti-join)
    5. Transform and load only genuinely new rows (append mode)
    6. Save checkpoint with the latest date loaded

Run schedule (cron every 2 days):
    0 6 */2 * *  →  runs at 6 AM every 2 days

Checkpoint file: data/checkpoint.json
    { "last_loaded_date": "2026-05-18" }

To force a full reload:
    Delete data/checkpoint.json and run — the pipeline will
    default to 100 days of history on the first run.
"""

import os
import sys
import json
from datetime import date

# Resolve PROJECT_ROOT as the absolute path of the folder containing main.py
# Works correctly regardless of which directory docker compose exec runs from
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))   # /app
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Build absolute path to config.json using PROJECT_ROOT
# Prevents FileNotFoundError when docker compose exec runs from a different cwd
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.json")   # /app/config/config.json
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# PySpark env vars must be set before SparkSession import
# Remove these lines if running inside Docker
os.environ["PYSPARK_PYTHON"]        = config["spark"].get("pyspark_python", "python")
os.environ["PYSPARK_DRIVER_PYTHON"] = config["spark"].get("pyspark_driver_python", "python")

from pyspark.sql import SparkSession

from extractor.extract_api          import extract_from_api
from transformer.transform_data import (
    flatten_json_to_df,
    filter_new_rows,
    clean_silver,
    transform_gold,
    get_max_loaded_date,
)
from loader.load_postgres            import load_into_postgres
from utils.logger                    import get_logger
from utils.checkpoints                import get_last_loaded_date, save_checkpoint

logger = get_logger(__name__, log_file=config["path"]["log_file_path"])

# How many days to roll back from last_loaded_date
LOOKBACK_DAYS = 2


def build_spark_session(spark_cfg: dict) -> SparkSession:
    """Build SparkSession with PostgreSQL JDBC JAR registered."""
    logger.info("Initialising SparkSession...")
    return (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .config("spark.jars", spark_cfg["postgresql_jars"])
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def run_pipeline() -> None:
    """
    Execute the incremental ETL pipeline.

    Flow:
        checkpoint → extract (windowed) → bronze → anti-join filter
        → silver → gold → append to postgres → save checkpoint

    The checkpoint is only saved AFTER a successful load.
    If any step fails, the checkpoint stays at the previous date
    so the next run re-fetches the same window automatically.
    """
    logger.info("=" * 60)
    logger.info("INCREMENTAL ETL PIPELINE STARTED")
    logger.info(f"Run date: {date.today()} | Lookback: {LOOKBACK_DAYS} days")
    logger.info("=" * 60)

    # ── STEP 1: Determine fetch window from checkpoint ────────────────────────
    fetch_from = get_last_loaded_date(config["path"]['checkpoint_path'], lookback_days=LOOKBACK_DAYS)
    logger.info(f"Fetch window: {fetch_from} → {date.today()}")

    # ── STEP 2: Extract — only records within the window ─────────────────────
    merged_data = extract_from_api(
        api_cfg=config["api"],
        raw_data_path=config["path"]["raw_data_path"],
        fetch_from=fetch_from,
    )

    if not merged_data:
        logger.info("No data returned from API for this window. Pipeline complete.")
        return

    # ── SPARK SESSION ─────────────────────────────────────────────────────────
    spark = build_spark_session(config["spark"])
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ── STEP 3: BRONZE — flatten windowed JSON → DataFrame ────────────────
        logger.info("BRONZE: Flattening windowed JSON...")
        bronze_df = flatten_json_to_df(spark, config["path"]["raw_data_path"])

        if bronze_df.count() == 0:
            logger.info("Bronze DataFrame is empty — nothing to process.")
            return

        # ── STEP 4: INCREMENTAL FILTER — anti-join against existing DB rows ───
        logger.info("INCREMENTAL FILTER: Removing already-loaded rows...")
        filtered_df = filter_new_rows(bronze_df, config["postgres"], spark)

        if filtered_df.count() == 0:
            logger.info("All rows already exist in DB — nothing new to load.")
            return

        # ── STEP 5: SILVER — validate the new rows ────────────────────────────
        logger.info("SILVER: Validating new rows...")
        silver_df = clean_silver(filtered_df)

        # ── STEP 6: GOLD — apply business transformations ─────────────────────
        logger.info("GOLD: Applying transformations...")
        gold_df = transform_gold(silver_df)
        gold_df.show(10, truncate=False)

        # Get the latest date BEFORE writing (DataFrame may not be reusable after write)
        max_date = get_max_loaded_date(gold_df)

        # ── STEP 7: LOAD — append new rows to PostgreSQL ──────────────────────
        load_into_postgres(gold_df, config["postgres"], mode="append")

        # ── STEP 8: CHECKPOINT — only saved after successful load ─────────────
        # If the load fails, this line never runs, so next run re-fetches the
        # same window automatically — no data is silently skipped
        save_checkpoint(config["path"]['checkpoint_path'], max_date)

        logger.info("=" * 60)
        logger.info(f"PIPELINE COMPLETE — {gold_df.count() if gold_df else 0} rows appended")
        logger.info(f"Next run will fetch from: {max_date} - {LOOKBACK_DAYS} days")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        # Checkpoint is NOT saved — next run will retry the same window
        raise

    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    run_pipeline()
