"""
main.py
-------
ETL Pipeline Orchestrator — Alpha Vantage Stock Market Data.

Coordinates the full pipeline in order:
    1. EXTRACT  — fetch OHLCV data for all symbols from Alpha Vantage API
    2. BRONZE   — flatten nested JSON into a Spark DataFrame
    3. SILVER   — parse dates, run null and duplicate checks
    4. GOLD     — apply business transformations (range, return %, candle)
    5. LOAD     — write Gold DataFrame to MySQL via JDBC

Medallion Architecture:
    Bronze → raw data as-is
    Silver → cleaned and validated
    Gold   → transformed and analytics-ready

Configuration:
    All settings (Spark, MySQL, API, file paths) are loaded from
    config/config.json so no hardcoded values exist in any module.

Usage:
    python main.py
"""

import os
import sys
import json

# ── Add project root to sys.path ─────────────────────────────────────────────
# Ensures sub-packages (extractor, transformer, etc.) are importable
# regardless of which directory the script is run from.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ── Load config before any other imports ─────────────────────────────────────
# PySpark env vars must be set BEFORE SparkSession is imported,
# so config is loaded here at the top level.
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.json")
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# Set Python interpreter paths for PySpark workers
# Must happen before `from pyspark.sql import SparkSession`
os.environ["PYSPARK_PYTHON"]        = config["spark"]["pyspark_python"]
os.environ["PYSPARK_DRIVER_PYTHON"] = config["spark"]["pyspark_driver_python"]

# ── Imports (after env vars are set) ─────────────────────────────────────────
from pyspark.sql import SparkSession

from extractor.extract_api              import extract_from_api
from transformer.transformation_data   import flatten_json_to_df, clean_silver, transform_gold
from loader.load_mysql                  import load_into_mysql
from utils.logger                       import get_logger

logger = get_logger(__name__, log_file=config["path"]["log_file_path"])


def build_spark_session(spark_cfg: dict) -> SparkSession:
    """
    Initialise and return a SparkSession with MySQL JDBC support.

    Registers the MySQL Connector/J JAR so that df.write.format("jdbc")
    can connect to MySQL. The JAR path comes from config['spark']['mysql_jars'].

    Args:
        spark_cfg: Dict from config['spark'] containing app_name and mysql_jars.

    Returns:
        Configured SparkSession instance.
    """
    logger.info("Initialising SparkSession...")
    return (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .config("spark.jars", spark_cfg["mysql_jars"])  # register MySQL JDBC driver JAR
        .getOrCreate()
    )


def run_pipeline() -> None:
    """
    Execute the full ETL pipeline end-to-end.

    Steps:
        1. Extract  — call Alpha Vantage API for each symbol, save raw JSON
        2. Bronze   — flatten nested JSON → flat Spark DataFrame
        3. Silver   — parse date, null checks, duplicate checks
        4. Gold     — add daily_range, daily_return_pct, candle columns
        5. Load     — write Gold DataFrame to MySQL (overwrite mode)

    Error handling:
        Any exception in the Spark section is logged with a full stack
        trace and re-raised to stop the pipeline immediately.
        The SparkSession is always stopped in the finally block.

    Returns:
        None
    """
    logger.info("=" * 60)
    logger.info("ETL PIPELINE STARTED")
    logger.info("=" * 60)

    # ── STEP 1: EXTRACT ──────────────────────────────────────────────────────
    extract_from_api(config["api"], config["path"]["raw_data_path"])

    # ── SPARK SESSION ─────────────────────────────────────────────────────────
    spark = build_spark_session(config["spark"])
    spark.sparkContext.setLogLevel("WARN")  # suppress verbose Spark INFO logs

    try:
        # ── STEP 2: BRONZE — flatten nested JSON → DataFrame ─────────────────
        logger.info("BRONZE LAYER: Flattening raw JSON...")
        bronze_df = flatten_json_to_df(spark, config["path"]["raw_data_path"])
        bronze_df.show(5, truncate=False)

        # ── STEP 3: SILVER — clean and validate ──────────────────────────────
        logger.info("SILVER LAYER: Cleaning data...")
        silver_df = clean_silver(bronze_df)

        # ── STEP 4: GOLD — business transformations ───────────────────────────
        logger.info("GOLD LAYER: Applying transformations...")
        gold_df = transform_gold(silver_df)
        gold_df.show(5, truncate=False)
        gold_df.printSchema()

        # ── STEP 5: LOAD — write to MySQL ─────────────────────────────────────
        load_into_mysql(gold_df, config["mysql"], mode="overwrite")

        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise   # re-raise so the process exits with a non-zero code

    finally:
        # Always stop Spark — even if an exception was raised
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    run_pipeline()
