

"""
transformer/transformation_data.py
-----------------------------------
TRANSFORM layer of the ETL pipeline — Medallion Architecture.

Three processing layers:

    BRONZE  flatten_json_to_df()
            Reads the raw nested JSON (symbol → date → OHLCV)
            and produces a flat Spark DataFrame with an explicit schema.
            Why flatten in Python first instead of spark.read.json()?
            spark.read.json treats top-level keys (IBM, AAPL) as column
            names, making the nested dates impossible to parse cleanly.

    SILVER  clean_silver()
            Parses the date string to DateType, runs null checks and
            duplicate checks on the composite key (symbol + date).

    GOLD    transform_gold()
            Applies business transformations: daily_range, daily_return_pct,
            and candle direction (Bullish / Bearish).
"""

import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, col, when, round
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType
)
from utils.logger import get_logger

logger = get_logger(__name__)

# Alpha Vantage key for the daily time series block
TIME_SERIES_KEY = "Time Series (Daily)"


def flatten_json_to_df(spark: SparkSession, raw_data_path: str) -> DataFrame:
    """
    BRONZE LAYER — Flatten nested multi-symbol JSON into a Spark DataFrame.

    Reads the merged JSON saved by the extractor and converts the
    3-level nested structure (symbol → date → OHLCV) into a flat
    tabular format that Spark can process.

    Why not spark.read.json()?
        spark.read.json() treats top-level JSON keys ("IBM", "AAPL")
        as column names, producing unreadable nested struct columns.
        Flattening in Python first gives full control over the schema.

    Output schema:
        symbol  StringType  e.g. "IBM"
        date    StringType  e.g. "2026-05-18"  (parsed to DateType in Silver)
        open    FloatType
        high    FloatType
        low     FloatType
        close   FloatType
        volume  LongType

    Args:
        spark:         Active SparkSession.
        raw_data_path: Path to the merged JSON file written by the extractor.

    Returns:
        Flat Spark DataFrame with one row per symbol per trading day.

    Raises:
        FileNotFoundError: If raw_data_path does not exist.
        KeyError:          If OHLCV sub-keys are missing in the JSON.
    """
    logger.info(f"Reading merged JSON from: {raw_data_path}")

    with open(raw_data_path, "r") as f:
        merged_data = json.load(f)

    rows = []

    for symbol, content in merged_data.items():
        logger.info(f"Flattening symbol: {symbol}")

        if TIME_SERIES_KEY not in content:
            logger.warning(f"No '{TIME_SERIES_KEY}' found for {symbol} — skipping.")
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

    logger.info(f"Total rows flattened: {len(rows)}")

    # Explicit schema — faster than inferSchema and prevents type mismatches
    # nullable=False on symbol/date because they are the composite primary key
    schema = StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("date",   StringType(), nullable=False),
        StructField("open",   FloatType(),  nullable=True),
        StructField("high",   FloatType(),  nullable=True),
        StructField("low",    FloatType(),  nullable=True),
        StructField("close",  FloatType(),  nullable=True),
        StructField("volume", LongType(),   nullable=True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    logger.info("Bronze DataFrame created successfully.")
    return df


def clean_silver(df: DataFrame) -> DataFrame:
    """
    SILVER LAYER — Parse, validate, and clean the Bronze DataFrame.

    Transformations applied:
        1. Parse date string "yyyy-MM-dd" → Spark DateType
           (required for correct ordering, range filtering, and MySQL DATE type)

    Quality checks performed (logged as warnings, do NOT drop rows):
        2. NULL check  — counts nulls in each OHLCV column
        3. Duplicate check — counts rows where (symbol, date) is not unique

    Why log warnings instead of dropping bad rows?
        Dropping silently hides data quality issues. Logging lets the
        data engineer investigate the root cause without losing data.

    Args:
        df: Bronze-layer DataFrame from flatten_json_to_df().

    Returns:
        Cleaned DataFrame with date column as DateType.
    """
    logger.info("Cleaning data (SILVER LAYER)...")

    # Convert "2026-05-18" string → Spark DateType for proper date semantics
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    # NULL check on all numeric columns
    logger.info("Running NULL checks...")
    for c in ["open", "high", "low", "close", "volume"]:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            logger.warning(f"NULL CHECK FAILED: {null_count} nulls found in '{c}'")
        else:
            logger.info(f"NULL CHECK PASSED: '{c}'")

    # Duplicate check — composite key is (symbol, date)
    # Each stock should have exactly one record per trading day
    dup_count = (
        df.groupBy("symbol", "date")
          .count()
          .filter("count > 1")
          .count()
    )
    if dup_count > 0:
        logger.warning(f"DUPLICATE CHECK FAILED: {dup_count} duplicate (symbol, date) pairs found.")
    else:
        logger.info("DUPLICATE CHECK PASSED: all (symbol, date) pairs are unique.")

    logger.info("Silver layer ready.")
    return df


def transform_gold(df: DataFrame) -> DataFrame:
    """
    GOLD LAYER — Apply business transformations for analytics.

    Derived columns added:
        daily_range      = round(high - low, 2)
                           Measures intraday volatility.
                           Large range = high volatility day.

        daily_return_pct = round(((close - open) / open) * 100, 2)
                           Percentage price change from open to close.
                           Positive = stock gained, Negative = stock lost.

        candle           = "Bullish" if close > open else "Bearish"
                           Standard candlestick direction used in
                           technical analysis / trading charts.

    Args:
        df: Silver-layer cleaned DataFrame.

    Returns:
        Transformed DataFrame ready for loading into MySQL Gold table.
    """
    logger.info("Applying business transformations (GOLD LAYER)...")

    df = (
        df
        # Intraday price range — proxy for volatility
        .withColumn("daily_range",
            round(col("high") - col("low"), 2))

        # Daily return as percentage — core metric for stock performance
        .withColumn("daily_return_pct",
            round(((col("close") - col("open")) / col("open")) * 100, 2))

        # Candlestick direction — Bullish (price rose) or Bearish (price fell)
        .withColumn("candle",
            when(col("close") > col("open"), "Bullish")
            .otherwise("Bearish"))
    )

    logger.info("Gold layer transformations applied.")
    return df
