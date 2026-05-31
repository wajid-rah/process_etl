# 📈 Real-Time Stock Market ETL Pipeline

> Incremental data engineering pipeline — Alpha Vantage API → PySpark → PostgreSQL → Docker

---

## 🗂️ Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [How It Works](#how-it-works)
- [Incremental Load Strategy](#incremental-load-strategy)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Database Schema](#database-schema)
- [Sample Output](#sample-output)

---

## Overview

An end-to-end **incremental ETL pipeline** that:

- Extracts daily **OHLCV** (Open, High, Low, Close, Volume) stock data for multiple symbols from the **Alpha Vantage REST API**
- Processes data through **Medallion Architecture** (Bronze → Silver → Gold) using **Apache PySpark**
- Loads analytics-ready data into **PostgreSQL** via JDBC
- Runs fully containerised with **Docker & Docker Compose**
- Loads only **new records** on each run using a checkpoint-based incremental strategy

**Tracked symbols:** `IBM` · `AAPL` · `MSFT` · `TSCO.LON`

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     ETL Pipeline Flow                       │
│                                                             │
│  Alpha Vantage API                                          │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────────┐                                            │
│  │   EXTRACT   │  Loop symbols → filter to date window      │
│  │             │  Save raw JSON → data/raw/                 │
│  └──────┬──────┘                                            │
│         │                                                   │
│         ▼                                                   │
│  ┌─────────────┐                                            │
│  │   BRONZE    │  Flatten nested JSON → Spark DataFrame     │
│  │             │  Explicit schema (symbol, date, OHLCV)     │
│  └──────┬──────┘                                            │
│         │                                                   │
│         ▼                                                   │
│  ┌─────────────┐                                            │
│  │   SILVER    │  Parse DateType · NULL checks              │
│  │             │  Duplicate checks on (symbol, date)        │
│  └──────┬──────┘                                            │
│         │                                                   │
│         ▼                                                   │
│  ┌─────────────┐                                            │
│  │    GOLD     │  + daily_range  (high - low)               │
│  │             │  + daily_return_pct                        │
│  │             │  + candle (Bullish / Bearish)              │
│  └──────┬──────┘                                            │
│         │                                                   │
│         ▼                                                   │
│  ┌─────────────┐                                            │
│  │    LOAD     │  JDBC append → PostgreSQL stock_daily      │
│  │             │  Save checkpoint on success                │
│  └─────────────┘                                            │
└─────────────────────────────────────────────────────────────┘
```

### Docker Network

```
┌──────────────────────────────────────┐
│         pyspark_network (bridge)     │
│                                      │
│  ┌───────────────┐                   │
│  │  postgresql   │ :5432             │
│  │  container    │                   │
│  └──────┬────────┘                   │
│         │  hostname = "postgresql"   │
│  ┌──────▼────────┐                   │
│  │  pyspark_etl  │                   │
│  │  container    │                   │
│  └───────────────┘                   │
└──────────────────────────────────────┘
         │ port 5432 exposed
         ▼
    localhost:5432
  (DBeaver / psql)
```

---

## Project Structure

```
Real_Time_Stock_ETL_SystemDocker/
│
├── config/
│   └── config.json               # All settings — API, DB, paths, Spark
│
├── extractor/
│   └── extract_api.py            # EXTRACT — fetch & filter API data per symbol
│
├── transformer/
│   └── transform_data.py         # BRONZE / SILVER / GOLD transformations
│
├── loader/
│   └── load_postgres.py          # LOAD — JDBC append to PostgreSQL
│
├── utils/
│   ├── logger.py                 # Centralised dual-handler logger
│   └── checkpoints.py            # Checkpoint read/write for incremental state
│
├── init/
│   └── init.sql                  # Auto-runs on first Postgres start (CREATE TABLE)
│
├── data/
│   └── raw/
│       └── stock_market.json     # Raw windowed JSON (Bronze landing zone)
│
├── logs/
│   └── etl_pipeline.log          # Pipeline logs (mounted to host)
│
├── Dockerfile                    # PySpark container — Python 3.10 + Java + JDBC JAR
├── docker-compose.yml            # Orchestrates PostgreSQL + PySpark containers
├── requirements.txt              # Python dependencies
├── main.py                       # Pipeline orchestrator
└── README.md
```

---

## Tech Stack

| Category | Technology |
|---|---|
| Language | Python 3.10 |
| Big Data | Apache PySpark 3.5.1 |
| Database | PostgreSQL 16 |
| Containerisation | Docker · Docker Compose |
| API | Alpha Vantage REST API |
| JDBC Driver | postgresql-42.7.3.jar |
| Architecture | Medallion Architecture (Bronze / Silver / Gold) |
| Incremental Load | Checkpoint file + Spark Left Anti-Join |
| Logging | Python `logging` — file + console dual handler |

---

## How It Works

### 1. Extraction
The extractor loops over all configured symbols and appends `&symbol=XXX` to the base URL. Since Alpha Vantage doesn't support date range params, it fetches the last 100 trading days (`outputsize=compact`) and filters in Python to only keep dates `>= fetch_from`.

### 2. Bronze Layer
The raw nested JSON structure (symbol → date → OHLCV) cannot be directly read by `spark.read.json()` — it would treat symbol names as column names. Instead, a custom Python flattener unrolls the structure into flat rows before creating a Spark DataFrame with an explicit schema.

### 3. Silver Layer
- Parses date strings (`"2026-05-18"`) to Spark `DateType`
- NULL checks on all OHLCV columns
- Duplicate check on composite key `(symbol, date)`

### 4. Gold Layer
Three derived columns are added:

| Column | Formula | Meaning |
|---|---|---|
| `daily_range` | `high - low` | Intraday volatility |
| `daily_return_pct` | `(close - open) / open × 100` | Daily % price change |
| `candle` | `close > open` → Bullish | Candlestick direction |

### 5. Load
Appends only new rows to PostgreSQL using JDBC. Early-exits with no DB connection if there are zero new rows.

---

## Incremental Load Strategy

```
checkpoint.json → { "last_loaded_date": "2026-05-18" }

Step 1:  fetch_from = 2026-05-18 − 2 days = 2026-05-16  (lookback window)

Step 2:  Extract dates >= 2026-05-16 from API

Step 3:  Left Anti-Join against PostgreSQL
         incoming (symbol, date) NOT IN existing → keep only new rows

Step 4:  Append new rows → PostgreSQL

Step 5:  Save checkpoint = max(date) from loaded batch
         (only on success — failed runs retry the same window)
```

**Three layers of deduplication:**

| Layer | Where | How |
|---|---|---|
| Date window filter | `extract_api.py` | Only fetch dates `>= fetch_from` |
| Left anti-join | `transform_data.py` | Drop (symbol, date) already in DB |
| PRIMARY KEY | PostgreSQL | DB rejects duplicate inserts |

**To force a full reload:**
```bash
rm data/checkpoint.json
docker compose exec pyspark python main.py
```

---

## Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key) (free tier available)

### 1. Clone the repository
```bash
git clone https://github.com/your-username/Real_Time_Stock_ETL_SystemDocker.git
cd Real_Time_Stock_ETL_SystemDocker
```

### 2. Add your API key to config.json
```json
"api": {
    "url": "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&apikey=YOUR_API_KEY",
    "symbols": ["IBM", "AAPL", "MSFT", "TSCO.LON"]
}
```

### 3. Build and start containers
```bash
docker compose up -d --build
```

### 4. Wait for PostgreSQL to be healthy
```bash
docker compose ps
# postgresql should show: healthy
```

### 5. Run the pipeline
```bash
docker compose exec pyspark python main.py
```

---

## Configuration

All settings live in `config/config.json` — no hardcoded values anywhere in the codebase.

```json
{
  "spark": {
    "app_name": "Real-Time_Stock_ETL_DOCKER",
    "postgresql_jars": "/app/jars/postgresql-42.7.3.jar"
  },
  "postgres": {
    "url": "jdbc:postgresql://postgresql:5432/wajiddb",
    "driver": "org.postgresql.Driver",
    "dbtable": "stock_daily",
    "user": "etluser",
    "password": "etlpassword"
  },
  "api": {
    "url": "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&apikey=YOUR_KEY",
    "symbols": ["IBM", "AAPL", "MSFT", "TSCO.LON"]
  },
  "path": {
    "raw_data_path": "data/raw/stock_market.json",
    "log_file_path": "logs/etl_pipeline.log",
    "checkpoint_path": "data/checkpoint.json"
  }
}
```

> ⚠️ **Never commit your real API key.** Add `config/config.json` to `.gitignore` and use a `config.example.json` template instead.

---

## Running the Pipeline

```bash
# Start all containers
docker compose up -d

# Run the ETL pipeline
docker compose exec pyspark python main.py

# View live logs
docker compose logs -f pyspark

# Check data in PostgreSQL
docker exec -it my_postgres psql -U etluser -d wajiddb
```

**Inside psql:**
```sql
-- Row count per symbol
SELECT symbol, COUNT(*) as rows FROM stock_daily GROUP BY symbol;

-- Latest loaded date per symbol
SELECT symbol, MAX(date) as latest FROM stock_daily GROUP BY symbol;

-- Sample rows
SELECT * FROM stock_daily ORDER BY date DESC LIMIT 10;

-- Exit
\q
```

**Stop everything:**
```bash
docker compose down          # stop containers, keep data
docker compose down -v       # stop containers + delete DB data
```

---

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS stock_daily (
    symbol           VARCHAR(10)    NOT NULL,
    date             DATE           NOT NULL,
    open             DECIMAL(10, 4),
    high             DECIMAL(10, 4),
    low              DECIMAL(10, 4),
    close            DECIMAL(10, 4),
    volume           BIGINT,
    daily_range      DECIMAL(10, 4),   -- derived: high - low
    daily_return_pct DECIMAL(10, 4),   -- derived: (close-open)/open * 100
    candle           VARCHAR(10),       -- derived: Bullish / Bearish
    PRIMARY KEY (symbol, date)
);
```

---

## Sample Output

```
symbol | date       | open   | high   | low    | close  | volume   | daily_range | daily_return_pct | candle
-------+------------+--------+--------+--------+--------+----------+-------------+------------------+--------
IBM    | 2026-05-18 | 218.55 | 223.33 | 217.75 | 222.75 | 5946367  |        5.58 |             1.92 | Bullish
AAPL   | 2026-05-18 | 300.24 | 300.66 | 294.91 | 297.84 | 34482959 |        5.75 |            -0.80 | Bearish
MSFT   | 2026-05-18 | 432.10 | 438.50 | 430.20 | 436.90 | 18921000 |        8.30 |             1.11 | Bullish
```

---

# 📝 Logging

Logs are automatically written to:

```text
logs/etl_pipeline.log
```

View live logs:

```bash
docker compose logs -f pyspark_etl
```

---

# 🔧 Useful Commands

### Start Containers

```bash
docker compose up -d
```

### Rebuild Containers

```bash
docker compose up -d --build
```

### Stop Containers

```bash
docker compose down
```

### Stop & Remove Database Data

```bash
docker compose down -v
```

### View Logs

```bash
docker compose logs -f
```

### Open PostgreSQL

```bash
docker exec -it etl_postgres psql -U etluser -d stockdb
```

---

# 📷 Suggested Screenshots

Create:

```text
README/images/
```

Add:

```text
pipeline_run.png
postgres_data.png
docker_containers.png
```

Then display them:

```markdown
## Pipeline Run

![Pipeline](README/images/pipeline_run.png)

## PostgreSQL Data

![Database](README/images/postgres_data.png)

## Docker Containers

![Docker](README/images/docker_containers.png)
```

---

# 🔮 Future Improvements

* Apache Airflow Scheduling
* Kafka Streaming Ingestion
* Delta Lake Storage Layer
* Great Expectations Data Validation
* AWS Deployment (S3 + Glue + RDS)
* GitHub Actions CI/CD
* Data Quality Monitoring Dashboard

---

# 👨‍💻 Author

WAJID RAHMAN

Data Engineering | PySpark | PostgreSQL | Docker

LinkedIn: https://www.linkedin.com/in/wajid-rahman/

GitHub: https://github.com/wajid-rah/process_etl/tree/main/Real-Time_Stock_ETL_SystemDocker

---

⭐ If you found this project useful, consider giving it a star.
			
