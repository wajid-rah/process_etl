"""
extractor/extract_api.py
------------------------
EXTRACT layer of the ETL pipeline.

Responsibilities:
    - Loop through each stock symbol in the config
    - Fetch daily OHLCV data from Alpha Vantage REST API
    - Validate the response (rate limit / missing keys)
    - Merge all symbol responses into one JSON file on disk

Why save raw JSON to disk?
    If transformation or loading fails later, we can reprocess
    from the saved file without hitting the API again. This is
    the standard Bronze-layer pattern in Medallion Architecture.
"""

import requests
import os
import json
from utils.logger import get_logger

logger = get_logger(__name__)


def extract_from_api(api_cfg: dict, raw_data_path: str) -> dict:
    """
    Fetch daily stock data for all configured symbols and save to disk.

    Iterates over api_cfg['symbols'], builds a URL per symbol by appending
    &symbol=XXX to the base URL, fetches JSON, validates the response,
    and merges all results into one dict keyed by symbol.

    Alpha Vantage error handling:
        - "Information" key  → free-tier rate limit reached; symbol is skipped
        - Missing "Time Series (Daily)" key → unexpected structure; symbol is skipped
        - HTTP 4xx/5xx → raises requests.HTTPError immediately

    Merged output structure saved to disk:
        {
            "IBM":  { "Meta Data": {...}, "Time Series (Daily)": {...} },
            "AAPL": { "Meta Data": {...}, "Time Series (Daily)": {...} }
        }

    Args:
        api_cfg:       Dict from config['api'] containing:
                           url     — base URL with function and apikey params
                           symbols — list of ticker symbols e.g. ["IBM", "AAPL"]
        raw_data_path: File path where the merged JSON will be saved.
                       Parent directories are auto-created if missing.

    Returns:
        merged_data dict (same structure as what is written to disk).

    Raises:
        requests.HTTPError: On any non-2xx HTTP response.

    Example:
        >>> extract_from_api(config['api'], "data/raw/stock_market.json")
    """
    merged_data = {}

    logger.info(f"Starting extraction | base_url: {api_cfg['url']}")
    logger.info(f"Symbols to fetch: {api_cfg['symbols']}")

    TIME_SERIES_KEY = "Time Series (Daily)"

    for symbol in api_cfg['symbols']:

        # Append symbol as query param — base URL already has function & apikey
        url = f"{api_cfg['url']}&symbol={symbol}"
        logger.info(f"Fetching: {url}")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()   # raises HTTPError for 4xx / 5xx
            data = response.json()

        except requests.HTTPError as e:
            logger.error(f"HTTP error for {symbol}: {e}")
            continue
        except Exception as e:
            logger.error(f"Unexpected request error for {symbol}: {e}")
            continue

        # Alpha Vantage embeds errors inside a 200 OK response body
        # "Information" appears when the free-tier daily limit is reached
        if "Information" in data:
            logger.warning(f"Rate limit hit for {symbol}: {data['Information']}")
            continue

        if "Error Message" in data:
            logger.error(f"API error for {symbol}: {data['Error Message']}")
            continue

        # Validate expected key exists before storing
        if TIME_SERIES_KEY not in data:
            logger.warning(
                f"Unexpected response structure for {symbol}. "
                f"Keys found: {list(data.keys())} — skipping."
            )
            continue

        logger.info(f"Received {len(data[TIME_SERIES_KEY])} records for {symbol}")
        merged_data[symbol] = data

    # Auto-create data/raw/ directory if it doesn't exist
    os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)

    logger.info(f"Saving {len(merged_data)} symbols to: {raw_data_path}")
    with open(raw_data_path, "w") as f:
        json.dump(merged_data, f, indent=4)

    logger.info("Raw data saved successfully.")
    return merged_data
