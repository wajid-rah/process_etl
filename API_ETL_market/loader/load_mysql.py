"""
loader/load_mysql.py
---------------------
LOAD layer of the ETL pipeline.

Writes the Gold-layer Spark DataFrame into a MySQL table via JDBC.

Target table DDL (run once in MySQL before first pipeline execution):

    CREATE TABLE stock_daily (
        symbol           VARCHAR(10)    NOT NULL,
        date             DATE           NOT NULL,
        open             DECIMAL(10, 4),
        high             DECIMAL(10, 4),
        low              DECIMAL(10, 4),
        close            DECIMAL(10, 4),
        volume           BIGINT,
        daily_range      DECIMAL(10, 4),
        daily_return_pct DECIMAL(10, 4),
        candle           ENUM('Bullish', 'Bearish'),
        PRIMARY KEY (symbol, date)
    );

Write modes:
    overwrite — truncates and rewrites the full table each run.
                Safe for full daily reloads. Default mode.
    append    — adds new rows without touching existing data.
                Use this for incremental/delta loads.
"""

from pyspark.sql import DataFrame
from utils.logger import get_logger

logger = get_logger(__name__)


def load_into_mysql(df: DataFrame, mysql_cfg: dict, mode: str = "overwrite") -> None:
    """
    Write a Spark DataFrame to a MySQL table using JDBC.

    Requires the MySQL Connector/J JAR to be registered in the
    SparkSession via spark.jars config (set in main.py).

    Args:
        df:        Gold-layer Spark DataFrame to write.
        mysql_cfg: Dict from config['mysql'] containing:
                       url      — JDBC connection string
                                  e.g. jdbc:mysql://localhost:3306/db
                       driver   — JDBC driver class
                                  e.g. com.mysql.cj.jdbc.Driver
                       dbtable  — Target table name e.g. stock_daily
                       user     — MySQL username
                       password — MySQL password
        mode:      Spark write mode. One of:
                       "overwrite" — truncate and reload (default)
                       "append"    — add rows to existing data

    Raises:
        Exception: Any JDBC/Spark write error is logged in full
                   (with stack trace via exc_info=True) then re-raised
                   so the pipeline fails loudly rather than silently.

    Example:
        >>> load_into_mysql(gold_df, config['mysql'], mode="overwrite")
    """
    row_count = df.count()
    logger.info(
        f"Loading {row_count} rows into MySQL table "
        f"'{mysql_cfg['dbtable']}' | mode: {mode} | url: {mysql_cfg['url']}"
    )

    try:
        (
            df.write
            .format("jdbc")
            .option("url",      mysql_cfg["url"])
            .option("driver",   mysql_cfg["driver"])
            .option("dbtable",  mysql_cfg["dbtable"])
            .option("user",     mysql_cfg["user"])
            .option("password", mysql_cfg["password"])
            .mode(mode)
            .save()
        )
        logger.info(f"Successfully loaded {row_count} rows into '{mysql_cfg['dbtable']}'.")

    except Exception as e:
        # exc_info=True prints the full stack trace to the log file
        logger.error(f"Failed to load data into MySQL: {e}", exc_info=True)
        raise   # re-raise so main.py catches it and stops the pipeline
