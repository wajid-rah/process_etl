Medallion Architecture-Based Stock Market Data Pipeline.
----------------------------------------------------------
This project is an end-to-end ETL pipeline built using PySpark and MySQL. The pipeline extracts stock market OHLCV data from the Alpha Vantage REST API, stores raw API responses in a Bronze layer, performs data cleansing and validation in a Silver layer, and applies business transformations in a Gold layer before loading the final analytics-ready data into MySQL using JDBC.

The project follows Medallion Architecture and uses modular components for extraction, transformation, loading, logging, and configuration management. I also implemented centralized logging, schema validation, duplicate checks, and externalized configuration handling for maintainability and scalability.


This project demonstrates REAL engineering practices:	
------------------------------------------------------------------------------------------------------
**API Extraction Layer**

•	Built a configurable multi-symbol extractor that loops over a symbol list (IBM, AAPL, TSCO.LON, MSFT) and appends each ticker to the base URL, making one API call per symbol without modifying any source code.

•	Implemented three-level response validation to handle Alpha Vantage-specific error patterns — rate-limit messages embedded inside 200 OK responses, missing time-series keys, and HTTP errors — using continue-on-failure so one bad symbol never crashes the pipeline.

•	Persisted raw JSON to disk (Bronze landing zone) before any Spark processing, enabling reprocessing without re-hitting the API on transformation failures.


**PySpark Transformation — Medallion Architecture**

•	Bronze layer: wrote a custom Python flattener (flatten_json_to_df) to unroll the 3-level nested JSON (symbol → date → OHLCV) into a flat Spark DataFrame with an explicit schema — bypassing spark.read.json which misinterprets top-level symbol keys as column names.

•	Silver layer: parsed ISO date strings to Spark DateType, ran per-column NULL checks across all OHLCV fields, and validated composite key uniqueness on (symbol, date) — logging warnings instead of silently dropping rows to preserve auditability.

•	Gold layer: engineered three derived analytical columns — daily_range (intraday volatility), daily_return_pct (open-to-close percentage change), and candle direction (Bullish/Bearish) — using PySpark's round(), when(), and col() functions.

**Data Loading**

•	Configured JDBC write to MySQL with the mysql-connector-j driver JAR, targeting a table with DECIMAL(10,4) price columns, BIGINT volume, ENUM candle direction, and a composite PRIMARY KEY (symbol, date) to enforce uniqueness at the database level.

•	Wrapped the write operation in try/except with exc_info=True logging to capture full stack traces on JDBC failures, then re-raised to ensure the pipeline exits with a non-zero code for upstream monitoring.
Pipeline Architecture & Engineering Practices

•	Designed the project in a modular folder structure (extractor/, transformer/, loader/, utils/) with each module having a single responsibility, enabling independent testing and easy extension.

•	Centralised all configuration (Spark settings, JDBC credentials, API URL, symbol list, file paths) in a single config.json — adding a new stock symbol requires only a one-line config change with zero code edits.

•	Built a shared logging utility (get_logger(__name__)) that writes timestamped, module-named log entries to both file and console simultaneously, with a duplicate-handler guard for multi-import safety.

•	Set PySpark environment variables (PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON) before SparkSession import and added sys.path injection for PROJECT_ROOT, ensuring the pipeline runs correctly from any working directory.


## TECHNOLOGIES USED

| Category | Tools / Technologies |
|---|---|
| Languages | Python 3.10 |
| Big Data | Apache PySpark (SparkSession, DataFrame API, Window functions) |
| Data Source | Alpha Vantage REST API (TIME_SERIES_DAILY endpoint) |
| Database | MySQL 8 via JDBC (mysql-connector-j-9.3.0) |
| Data Format | JSON (raw), Parquet-compatible flat schema (processed) |
| Architecture | Medallion Architecture — Bronze / Silver / Gold layers |
| Logging | Python logging module — file + console dual handler |
| Configuration | JSON-driven config (zero hardcoded values in source code) |
| Dev Environment | Windows 10, Python venv, PyCharm |

Logging	              Python logging module — file + console dual handler

Configuration	        JSON-driven config (zero hardcoded values in source code)

Dev Environment       Windows 10, Python venv, Pycharm

