"""
utils/logger.py
---------------
Centralised logging factory for the ETL pipeline.

All modules call get_logger(__name__) to obtain a named logger
that writes to both the log file and the console simultaneously.
Using __name__ as the logger name means each module appears
clearly in log output, e.g.:
    extractor.extract_api -> INFO : Fetching IBM...
    transformer.transformation_data -> INFO : Silver layer ready...
"""

import logging
import os


def get_logger(name: str, log_file: str = "logs/etl_pipeline.log") -> logging.Logger:
    """
    Create or retrieve a named logger with file and console handlers.

    Uses Python's logger hierarchy — calling get_logger(__name__)
    from different modules produces separate named loggers that all
    share the same output destinations (file + console).

    Duplicate-handler guard: if the logger already has handlers
    (e.g. called twice in the same session), it returns the existing
    logger without adding duplicate handlers, preventing doubled output.

    Args:
        name:     Logger name, typically __name__ of the calling module.
        log_file: Path to the log file. Directory is auto-created
                  if it does not exist. Defaults to logs/etl_pipeline.log.

    Returns:
        Configured logging.Logger instance.

    Example:
        >>> from utils.logger import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Pipeline started")
        2026-05-20 10:00:00 | __main__ -> INFO : Pipeline started
    """
    # Auto-create the logs directory so the pipeline never fails
    # just because the folder doesn't exist yet
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)

    # Guard: avoid adding duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # Format: timestamp | module -> level : message
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s -> %(levelname)s : %(message)s"
    )

    # File handler — persists logs across runs
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Console handler — shows live progress in the terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
