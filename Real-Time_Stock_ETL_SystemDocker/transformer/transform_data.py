import json
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, col, when, round, lit, max as spark_max
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType
)
from utils.logger import get_logger
import os

TIME_SERIES_KEY = "Time Series (Daily)"
logger = get_logger(__name__)

# Resolve absolute path of this file → /app/transformer/transform_data.py
_TRANSFORMER_DIR = os.path.dirname(os.path.abspath(__file__))   # /app/transformer
_APP_DIR         = os.path.dirname(_TRANSFORMER_DIR)            # /app


def _resolve_path(raw_data_path: str) -> str:
    """
    Convert a relative data path to absolute using /app as base.
    If already absolute, return as-is.

    data/raw/stock_market.json → /app/data/raw/stock_market.json
    /app/data/raw/stock_market.json → /app/data/raw/stock_market.json  (unchanged)
    """
    if os.path.isabs(raw_data_path):
        return raw_data_path
    return os.path.join(_APP_DIR, raw_data_path)   # /app/data/raw/stock_market.json


def flatten_json_to_df(spark: SparkSession, raw_data_path: str) -> DataFrame:
    """
    BRONZE LAYER — Flatten nested multi-symbol JSON into a Spark DataFrame.

    Reads the windowed JSON saved by the extractor (already filtered
    to fetch_from date) and produces a flat DataFrame.

    Args:
        spark:         Active SparkSession.
        raw_data_path: Path to the filtered raw JSON file.

    Returns:
        Flat Spark DataFrame — one row per symbol per trading day.
    """
    abs_raw_data_path = _resolve_path(raw_data_path)   # /app/data/raw/stock_market.json

    logger.info(f"Reading windowed JSON from: {abs_raw_data_path}")

    with open(abs_raw_data_path, "r") as f:
        merged_data = json.load(f)

    rows = []
    for symbol, content in merged_data.items():
        logger.info(f"Flattening symbol: {symbol}")

        if TIME_SERIES_KEY not in content:
            logger.warning(f"No '{TIME_SERIES_KEY}' for {symbol} — skipping.")
            continue

        for date_str, ohlcv in content[TIME_SERIES_KEY].items():
            rows.append((
                symbol,
                date_str,
                float(ohlcv["1. open"]),
                float(ohlcv["2. high"]),
                float(ohlcv["3. low"]),
                float(ohlcv["4. close"]),
                int(ohlcv["5. volume"]),
            ))

    logger.info(f"Total rows in window: {len(rows)}")

    if not rows:
        logger.warning("No rows to process in this window.")

    schema = StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        StructField("open", FloatType(), nullable=True),
        StructField("high", FloatType(), nullable=True),
        StructField("low", FloatType(), nullable=True),
        StructField("close", FloatType(), nullable=True),
        StructField("volume", LongType(), nullable=True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    logger.info("Bronze DataFrame created.")
    return df


def filter_new_rows(
    incoming_df: DataFrame,
    postgres_cfg: dict,
    spark: SparkSession,
) -> DataFrame:
    """
    INCREMENTAL FILTER — Remove rows already present in PostgreSQL.

    Reads the existing (symbol, date) pairs from the target table and
    performs a left anti-join to keep only rows NOT already loaded.

    left anti-join: keeps all rows from incoming_df where there is
    NO matching row in the existing table — i.e. genuinely new data.

    Why anti-join instead of just filtering by date?
        Anti-join is safer — it handles partial loads where some symbols
        loaded but others failed, leaving a mixed state in the DB.

    For instance:
        Why NOT just filter by date?
        # ❌ Naive approach — filter by date only
            incoming_df.filter(col("date") > last_loaded_date)
            This breaks in a partial load failure scenario:
                Run on May 16:
                    IBM  loaded successfully ✓
                    AAPL failed halfway      ✗   ← only some AAPL rows made it to DB

                Checkpoint saved: 2026-05-16

                Next run — date filter says:
                    "fetch everything after May 16"
                    → misses the AAPL rows from May 16 that failed!

        # ✅  Anti-join approach:
                Checks EVERY (symbol, date) pair against the DB
                → sees AAPL May 16 is missing → includes it ✓


    Args:
        incoming_df:   Bronze DataFrame with new candidate rows.
        postgres_cfg:  Dict from config['postgres'] for JDBC connection.
        spark:         Active SparkSession (needed to read from Postgres).

    Returns:
        DataFrame containing only rows not already in the target table.


    """
    logger.info("Checking existing records in PostgreSQL (incremental filter)...")

    try:
        # Read only symbol + date columns from existing table (lightweight)
        existing_df = (
            spark.read
            .format("jdbc")
            .option("url",      postgres_cfg["url"])
            .option("driver",   postgres_cfg["driver"])
            .option("query",    "SELECT symbol, date FROM stock_daily")
            .option("user",     postgres_cfg["user"])
            .option("password", postgres_cfg["password"])
            .load()
        )

        existing_count = existing_df.count()
        logger.info(f"Existing records in DB: {existing_count}")

        # Parse date in incoming_df before join
        incoming_parsed = incoming_df.withColumn(
            "date", to_date(col("date"), "yyyy-MM-dd")
        )

        # Left anti-join — keep only rows where (symbol, date) not in DB
        new_rows_df = incoming_parsed.join(
            existing_df,
            on=["symbol", "date"],
            how="left_anti"    # ← only rows with NO match in existing_df
        )

        # incoming_df                    existing_df (Postgresql)  result (left_anti)
        # ──────────────────────         ───────────────────    ──────────────────────
        # IBM  │ 2026-05-14    ───────►  IBM  │ 2026-05-14  ✗  DROPPED (already in DB)
        # IBM  │ 2026-05-15    ───────►  IBM  │ 2026-05-15  ✗  DROPPED (already in DB)
        # IBM  │ 2026-05-16    ───────►  IBM  │ 2026-05-16  ✗  DROPPED (already in DB)
        # IBM  │ 2026-05-17    ──── no match ──────────────  ✓  KEPT    (new row)
        # IBM  │ 2026-05-18    ──── no match ──────────────  ✓  KEPT    (new row)
        # AAPL │ 2026-05-14    ───────►  AAPL │ 2026-05-14  ✗  DROPPED (already in DB)
        # AAPL │ 2026-05-15    ───────►  AAPL │ 2026-05-15  ✗  DROPPED (already in DB)
        # AAPL │ 2026-05-16    ───────►  AAPL │ 2026-05-16  ✗  DROPPED (already in DB)
        # AAPL │ 2026-05-17    ──── no match ──────────────  ✓  KEPT    (new row)
        # AAPL │ 2026-05-18    ──── no match ──────────────  ✓  KEPT    (new row)

        new_count = new_rows_df.count()
        logger.info(
            f"Incoming: {incoming_parsed.count()} rows | "
            f"Already in DB: {existing_count} | "
            f"Genuinely new: {new_count} rows"
        )
        return new_rows_df

    except Exception as e:
        # Table doesn't exist yet (first run) — return all incoming rows
        # just return all incoming rows — nothing to compare against
        logger.warning(
            f"Could not read existing table (first run?): {e}. "
            f"Processing all incoming rows."
        )
        return incoming_df.withColumn(
            "date", to_date(col("date"), "yyyy-MM-dd")
        )
        # -------------POSSIBLE OUTPUT ----------------------------------
        # # Incremental run (normal)
        # INFO  : Existing records in DB: 6
        # INFO  : Incoming: 10 rows | Already in DB: 6 | Genuinely new: 4 rows
        #
        # # First run (table empty/missing)
        # WARN  : Could not read existing table (first run?). Processing all incoming rows.
        #
        # # No new data
        # INFO  : Incoming: 6 rows | Already in DB: 6 | Genuinely new: 0 rows
        # INFO  : All rows already exist in DB — nothing new to load.


def clean_silver(df: DataFrame) -> DataFrame:
    """
    SILVER LAYER — Validate the filtered DataFrame.

    Note: to_date() is already applied in filter_new_rows().
    This layer runs quality checks on the already-filtered new rows.

    Args:
        df: Filtered Bronze DataFrame (only new rows, date as DateType).

    Returns:
        Validated DataFrame.
    """
    logger.info("Running Silver layer checks on new rows...")

    # NULL check
    for c in ["open", "high", "low", "close", "volume"]:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            logger.warning(f"NULL CHECK FAILED: {null_count} nulls in '{c}'")
        else:
            logger.info(f"NULL CHECK PASSED: '{c}'")

    # Duplicate check within the current batch
    dup_count = (
        df.groupBy("symbol", "date")
        .count()
        .filter("count > 1")
        .count()
    )
    if dup_count > 0:
        logger.warning(f"DUPLICATE CHECK FAILED: {dup_count} duplicate (symbol, date) in batch.")
    else:
        logger.info("DUPLICATE CHECK PASSED.")

    logger.info("Silver layer ready.")
    return df


def transform_gold(df: DataFrame) -> DataFrame:
    """
    GOLD LAYER — Apply business transformations.

    Adds daily_range, daily_return_pct, and candle direction.

    Args:
        df: Silver-layer DataFrame.

    Returns:
        Transformed DataFrame ready for loading.
    """
    logger.info("Applying Gold layer transformations...")

    df = (
        df
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .withColumn("daily_range", round(col("high") - col("low"), 2))
        .withColumn("daily_return_pct", round(((col("close") - col("open")) / col("open")) * 100, 2))
        .withColumn("candle",
                    when(col("close") > col("open"), "Bullish")
                    .otherwise("Bearish"))
    )

    logger.info("Gold layer ready.")
    return df


def get_max_loaded_date(df: DataFrame) -> date:
    max_date = df.agg(spark_max("date")).collect()[0][0]
    logger.info(f"Max date in current batch: {max_date}")
    return max_date
