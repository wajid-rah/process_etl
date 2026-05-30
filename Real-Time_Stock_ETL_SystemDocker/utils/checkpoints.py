"""
utils/checkpoint.py
--------------------
Tracks the last successfully loaded date for incremental ETL runs.

Stores a simple JSON file on disk:
    { "last_loaded_date": "2026-05-18" }

Why file-based checkpoint instead of querying the DB?
    - Works even if the DB is temporarily unavailable
    - No extra DB query needed at pipeline startup
    - Simple to inspect and manually reset if needed

To force a full reload, simply delete checkpoint.json or set
last_loaded_date to a past date.
"""

import json
import os
from datetime import date, datetime, timedelta
from utils.logger import get_logger

logger = get_logger(__name__)

# Resolve absolute path of this file → /app/utils/checkpoints.py
_UTILS_DIR = os.path.dirname(os.path.abspath(__file__))   # /app/utils
_APP_DIR   = os.path.dirname(_UTILS_DIR)                  # /app


def _resolve_path(checkpoint_file: str) -> str:
    """
    Convert a relative checkpoint path to absolute using /app as base.
    If already absolute, return as-is.

    data/checkpoint.json → /app/data/checkpoint.json
    /app/data/checkpoint.json → /app/data/checkpoint.json  (unchanged)
    """
    if os.path.isabs(checkpoint_file):
        return checkpoint_file
    return os.path.join(_APP_DIR, checkpoint_file)   # /app/data/checkpoint.json


def get_last_loaded_date(checkpoint_file: str, lookback_days: int = 2) -> date:
    """
    Read the last successfully loaded date from the checkpoint file.

    If no checkpoint exists (first run), returns a default start date
    of 100 days ago so the first run loads ~3 months of history.

    The lookback_days parameter rolls the date BACK by N days as a
    safety buffer — this ensures we re-fetch and re-check recent dates
    that may have been missed due to weekends, API rate limits, or
    pipeline failures.

    Args:
        checkpoint_file : The path to checkpoint_file for remembering last loaded date
        lookback_days: Number of days to subtract from last loaded date.
                       Default 2 — the 2-day rolling window.

    Returns:
        date object representing the start of the next fetch window.

    Example:
        checkpoint = 2026-05-18, lookback_days = 2
        → returns   2026-05-16  (fetch from 2 days before last load)
        :param checkpoint_file:
        :param lookback_days:

    """
    abs_checkpoint = _resolve_path(checkpoint_file)   # always use absolute path

    if not os.path.exists(abs_checkpoint):
        # First run — load last 100 days of history
        default_start = date.today() - timedelta(days=100)
        # Eg: Pipeline never run before so, default_start = 2026-05-22(today) - 100 days[3 months history] = 2026-02-11

        logger.info(f"No checkpoint found. First run — fetching from: {default_start}")
        return default_start

    with open(abs_checkpoint, "r") as f:
        data = json.load(f)
        # data = {
        #    "last_loaded_date": "2026-05-22"
        # }

    last_date = datetime.strptime(data["last_loaded_date"], "%Y-%m-%d").date()
    # Convert String to date
    # "2026-05-22" -> 2026-05-22

    # Roll back by lookback_days for the safety buffer window. With loopback window, pipeline re-fetches records so
    # missed records get recovered and is called idempotent incremental pipeline design.
    window_start = last_date - timedelta(
        days=lookback_days)  # [last_date = 2026-05-22]  - [lookback_days = 2]  = [window_start = 2026-05-20]

    logger.info(
        f"Checkpoint found: last_loaded_date={last_date} | "
        f"lookback={lookback_days} days | fetch_from={window_start}"
    )
    return window_start


def save_checkpoint(checkpoint_file: str, loaded_date: date) -> None:
    """
    Save the most recently loaded date to the checkpoint file.

    Should be called AFTER a successful load — never before —
    so a failed run doesn't advance the checkpoint and skip data.

    Args:
        checkpoint_file : The path to checkpoint_file for remembering last loaded date
        loaded_date: The latest date successfully loaded in this run.

        :param checkpoint_file:
        :param loaded_date:

    """
    abs_checkpoint = _resolve_path(checkpoint_file)   # always use absolute path

    # data/ is a Docker mounted volume — Docker creates and owns this folder
    # Do NOT call os.makedirs() here — Docker volume mount controls it
    # Removed: os.makedirs(os.path.dirname(abs_checkpoint), exist_ok=True)

    payload = {"last_loaded_date": loaded_date.strftime("%Y-%m-%d")}
    # checkpoint.json
    # {
    #    "last_loaded_date": "2026-05-22"
    #   }

    with open(abs_checkpoint, "w") as f:
        json.dump(payload, f, indent=4)

    logger.info(f"Checkpoint saved: last_loaded_date = {loaded_date}")
