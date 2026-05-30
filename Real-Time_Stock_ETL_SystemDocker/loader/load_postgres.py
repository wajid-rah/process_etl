"""
loader/load_postgres.py
------------------------
LOAD layer — writes Gold DataFrame to PostgreSQL via JDBC.

Incremental change:
    Mode changed from 'overwrite' → 'append'.

    overwrite: truncates the entire table and rewrites — full reload.
    append:    adds only new rows — correct for incremental loads.

    The PRIMARY KEY (symbol, date) on the table acts as the final
    deduplication guard at the database level. If somehow a duplicate
    slips through the anti-join, PostgreSQL will reject it with a
    constraint violation rather than silently storing it.

Target table DDL (run once via init.sql in Docker):

    CREATE TABLE IF NOT EXISTS stock_daily (
        symbol           VARCHAR(10)    NOT NULL,
        date             DATE           NOT NULL,
        open             DECIMAL(10, 4),
        high             DECIMAL(10, 4),
        low              DECIMAL(10, 4),
        close            DECIMAL(10, 4),
        volume           BIGINT,
        daily_range      DECIMAL(10, 4),
        daily_return_pct DECIMAL(10, 4),
        candle           VARCHAR(10),
        PRIMARY KEY (symbol, date)
    );
"""

from pyspark.sql import DataFrame
from utils.logger import get_logger

logger = get_logger(__name__)


def load_into_postgres(
    df: DataFrame,
    postgres_cfg: dict,
    mode: str = "append",       # ← changed from "overwrite" to "append"
) -> None:
    """
    Append new Gold-layer rows to the PostgreSQL stock_daily table.

    Uses JDBC append mode — only adds new rows, never touches
    existing data. The DB PRIMARY KEY provides the final safety net.

    Args:
        df:           Gold-layer Spark DataFrame (new rows only).
        postgres_cfg: Dict from config['postgres'] containing url,
                      driver, dbtable, user, password.
        mode:         Spark write mode. Default 'append' for incremental.
                      Pass 'overwrite' only for a forced full reload.

    Raises:
        Exception: JDBC errors are logged with full stack trace
                   then re-raised to stop the pipeline.
    """
    row_count = df.count()

    if row_count == 0:
        logger.info("No new rows to load — skipping DB write.")
        return   # ← early exit, don't even open a JDBC connection

    logger.info(
        f"Loading {row_count} new rows → "
        f"table: '{postgres_cfg['dbtable']}' | "
        f"mode: {mode} | url: {postgres_cfg['url']}"
    )

    try:
        (
            df.write
            .format("jdbc")
            .option("url",      postgres_cfg["url"])
            .option("driver",   postgres_cfg["driver"])
            .option("dbtable",  postgres_cfg["dbtable"])
            .option("user",     postgres_cfg["user"])
            .option("password", postgres_cfg["password"])
            .mode(mode)
            .save()
        )
        logger.info(f"Successfully appended {row_count} rows to '{postgres_cfg['dbtable']}'.")

    except Exception as e:
        logger.error(f"Failed to load into PostgreSQL: {e}", exc_info=True)
        raise
