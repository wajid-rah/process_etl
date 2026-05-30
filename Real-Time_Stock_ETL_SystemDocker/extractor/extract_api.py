"""
extractor/extract_api.py
------------------------
EXTRACT layer — fetches daily OHLCV data from Alpha Vantage API.

Incremental change:
    Alpha Vantage TIME_SERIES_DAILY does not support date range params
    in the URL directly. Instead we use outputsize=full to get all
    available data, then filter to the window AFTER extracting.

    outputsize=compact → last 100 trading days (default)
    outputsize=full    → up to 20 years of data

    For incremental runs we always use 'compact' (last 100 days)
    since our window is only 2 days. 'full' is only needed on the
    very first run when loading all historical data.
"""

import requests
import os
import json
from datetime import date
from utils.logger import get_logger

logger = get_logger(__name__)

TIME_SERIES_KEY = "Time Series (Daily)"

# Resolve absolute path of this file → /app/extractor/extract_api.py
_EXTRACTOR_DIR = os.path.dirname(os.path.abspath(__file__))   # /app/extractor
_APP_DIR       = os.path.dirname(_EXTRACTOR_DIR)              # /app


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


def extract_from_api(
        api_cfg: dict,
        raw_data_path: str,
        fetch_from: date,
) -> dict:
    """
    Fetch daily stock data for all symbols and filter to the date window.

    Since Alpha Vantage doesn't support date range query params, we:
        1. Fetch all available recent data (outputsize=compact = last 100 days)
        2. Filter in Python to only keep dates >= fetch_from
        3. Save the filtered result to disk

    For the very first run (fetch_from is ~100 days ago), compact output
    covers the full window. For incremental runs (fetch_from = 2 days ago),
    compact is more than enough.

    Args:
        api_cfg:       Dict with 'url' and 'symbols' from config.json.
        raw_data_path: Path to save the filtered raw JSON.
        fetch_from:    Only keep records on or after this date.
                       Comes from checkpoint.get_last_loaded_date().

    Returns:
        merged_data dict filtered to the date window.
    """
    merged_data = {}

    abs_raw_data_path = _resolve_path(raw_data_path)   # /app/data/raw/stock_market.json

    logger.info(f"Incremental extraction | fetch_from: {fetch_from} | symbols: {api_cfg['symbols']}")

    for symbol in api_cfg['symbols']:

        url = f"{api_cfg['url']}&symbol={symbol}"
        logger.info(f"Fetching: {url}")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            # For each loop for eg for symbol : IBM
            # data = {
            #    "Meta Data": { .....  },
            #    "Time Series (Daily)": {
            #        "2026-05-22": { ... },
            #        "2026-05-21": { ... },
            #    },
            # }

        except requests.HTTPError as e:
            logger.error(f"HTTP error for {symbol}: {e}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error for {symbol}: {e}")
            continue

        # Alpha Vantage embeds errors inside 200 OK responses
        if "Information" in data:
            logger.warning(f"Rate limit hit for {symbol}: {data['Information']}")
            continue
        if "Error Message" in data:
            logger.error(f"API error for {symbol}: {data['Error Message']}")
            continue
        if TIME_SERIES_KEY not in data:
            logger.warning(f"Missing '{TIME_SERIES_KEY}' for {symbol} — skipping.")
            continue

        # ── Filter to window: only keep dates >= fetch_from ──────────────────
        full_series = data[TIME_SERIES_KEY]
        # full_series = {
        #    "2026-05-22": {...},
        #    "2026-05-21": {...},
        #    "2026-05-20": {...}
        # }

        total_records = len(full_series)

        filtered_series = {
            date_str: ohlcv
            for date_str, ohlcv in full_series.items()
            if date.fromisoformat(date_str) >= fetch_from  # keep only new dates
            # str: "2026-05-22" -> date(2026, 5, 22)
        }

        # full_series.items()
        # [
        #       date_str   ,                                               ohlcv
        #    ("2026-05-22", {"1. open": "424.7500",  "2. high": "426.3400",    "3. low": "415.7100",   "4. close": "419.0900",       "5. volume": "31393469"}),
        #    ("2026-05-21",  {"1. open": "419.5350",  "2. high": "424.4000",    "3. low": "416.3300",   "4. close": "418.5700",       "5. volume": "22390344"})
        # ]

        # Eg fetch_from = 2026-05-21
        # Date	            Keep?
        # 2026-05-20	    ✘ removed
        # 2026-05-21	    ✔
        # 2026-05-22	    ✔

        #   filtered_series = {
        #           "2026-05-22":{"1. open": "424.7500",  "2. high": "426.3400",    "3. low": "415.7100",   "4. close": "419.0900",       "5. volume": "31393469"},
        #           "2026-05-21":{"1. open": "419.5350",  "2. high": "424.4000",    "3. low": "416.3300",   "4. close": "418.5700",       "5. volume": "22390344"}
        #   }

        logger.info(
            f"{symbol}: {total_records} total records → "
            f"{len(filtered_series)} records in window (>= {fetch_from})"
        )

        if not filtered_series:
            logger.info(f"{symbol}: no new records in window — skipping.")
            continue

        # Rebuild the data dict with only the filtered time series
        merged_data[symbol] = {
            "Meta Data": data["Meta Data"],
            TIME_SERIES_KEY: filtered_series
        }

    # After finished loop
    # merged_data = {
    #    "IBM": {
    #        "Meta Data": { .....  },
    #        "Time Series (Daily)": {
    #            "2026-05-22": { ... },
    #            "2026-05-21": { ... },
    #        },
    #    }
    #
    #    "MSFT":  {
    #        "Meta Data": { .....  },
    #        "Time Series (Daily)": {
    #            "2026-05-22": { ... },
    #            "2026-05-21": { ... },
    #        },
    #    }
    # }

    # data/raw/ is a Docker mounted volume — Docker creates and owns this folder
    # Do NOT call os.makedirs() here — Docker volume mount controls it
    # Removed: os.makedirs(os.path.dirname(abs_raw_data_path), exist_ok=True)

    logger.info(f"Saving filtered data ({len(merged_data)} symbols) to: {abs_raw_data_path}")
    with open(abs_raw_data_path, "w") as f:
        json.dump(merged_data, f, indent=4)

    logger.info("Raw incremental data saved.")
    return merged_data
