# 📈 Real-Time Stock Market ETL Pipeline

> End-to-End Incremental Data Engineering Pipeline using **Alpha Vantage API**, **PySpark**, **PostgreSQL**, and **Docker**

![Python](https://img.shields.io/badge/Python-3.10-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-success)

---

# 📌 Project Highlights

✅ End-to-End ETL Pipeline

✅ Incremental Loading Strategy

✅ Medallion Architecture (Bronze → Silver → Gold)

✅ PySpark Data Processing

✅ PostgreSQL Data Warehouse

✅ Dockerized Deployment

✅ Checkpoint-Based Recovery

✅ Production-Style Logging

✅ JDBC Integration

---

# 📖 Overview

This project demonstrates a production-style Data Engineering pipeline that ingests stock market data from the Alpha Vantage API, processes it using Apache PySpark, and loads analytics-ready data into PostgreSQL.

The pipeline follows the Medallion Architecture pattern:

```text
Bronze  → Raw Data
Silver  → Cleaned & Validated Data
Gold    → Analytics Ready Data
```

The system is fully containerized using Docker and supports incremental loading through checkpoint tracking and Spark-based deduplication.

### Tracked Symbols

```text
IBM
AAPL
MSFT
TSCO.LON
```

---

# 🏗️ End-to-End Architecture

```text
┌─────────────────────────┐
│   Alpha Vantage API     │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│       Extraction        │
│   Fetch OHLCV Data      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│        Bronze           │
│      Raw JSON Data      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│        Silver           │
│ Data Cleaning & Checks  │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│         Gold            │
│ Analytics Ready Layer   │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│      PostgreSQL         │
│      stock_daily        │
└─────────────────────────┘
```

---

# 🐳 Docker Architecture

```text
Laptop
│
└── Docker Engine
    │
    ├── Network: etl_network
    │
    ├── Volume: postgres_data
    │
    ├── Container: etl_postgres
    │       ├── PostgreSQL
    │       ├── Tables
    │       └── Persistent DB files
    │
    └── Container: etl_pyspark
            ├── Python
            ├── Java
            ├── Spark
            ├── ETL Code
            └── JDBC Driver
```

---

# 🔄 Local Development Sync

The project source code remains on your laptop and is mounted into the PySpark container.

```text
Laptop                         Container /app/
├── main.py          ←sync→   ├── main.py
├── config/          ←sync→   ├── config/
├── extractor/       ←sync→   ├── extractor/
├── transformer/     ←sync→   ├── transformer/
├── loader/          ←sync→   ├── loader/
├── utils/           ←sync→   ├── utils/
├── data/            ←sync→   ├── data/
└── logs/            ←sync→   ├── logs/
                              └── jars/
```

The PostgreSQL JDBC Driver stays safely inside the container image and is not mounted from the host machine.

---

# 📂 Project Structure

```text
api_etl_project/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
│
├── config/
│   └── config.json
│
├── extractor/
│   └── extract_api.py
│
├── transformer/
│   └── transformation_data.py
│
├── loader/
│   └── load_postgres.py
│
├── utils/
│   ├── logger.py
│   └── checkpoints.py
│
├── data/
│   └── raw/
│
├── logs/
│
├── init.sql
│
└── main.py
```

---

# 🛠️ Technology Stack

| Category          | Technology             |
| ----------------- | ---------------------- |
| Language          | Python 3.10            |
| Processing Engine | Apache PySpark 3.5.1   |
| Database          | PostgreSQL             |
| API               | Alpha Vantage          |
| Containerization  | Docker                 |
| Orchestration     | Docker Compose         |
| JDBC Driver       | PostgreSQL JDBC        |
| Logging           | Python Logging         |
| Architecture      | Medallion Architecture |

---

# ⚡ Quick Start

## 1. Clone Repository

```bash
git clone https://github.com/your-username/api_etl_project.git

cd api_etl_project
```

---

## 2. Configure Alpha Vantage API Key

Edit:

```text
config/config.json
```

Example:

```json
{
  "api": {
    "url": "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&apikey=YOUR_API_KEY",
    "symbols": ["IBM","AAPL","MSFT","TSCO.LON"]
  }
}
```

---

## 3. Build Docker Containers

```bash
docker compose up -d --build
```

---

## 4. Verify Containers

```bash
docker compose ps
```

Expected:

```text
etl_postgres     healthy
etl_pyspark      running
```

---

## 5. Run ETL Pipeline

```bash
docker compose exec pyspark_etl python main.py
```

---

## 6. Verify Loaded Data

```bash
docker exec -it etl_postgres psql -U etluser -d stockdb
```

Inside PostgreSQL:

```sql
SELECT *
FROM stock_daily
ORDER BY date DESC
LIMIT 10;
```

---

# 🔍 Step-by-Step Pipeline Walkthrough

## Step 1 — Extract Data

The extractor loops through all configured stock symbols:

```text
IBM
AAPL
MSFT
TSCO.LON
```

For each symbol:

1. API request is sent.
2. JSON response is received.
3. Data is filtered by date window.
4. Raw data is saved locally.

Output:

```text
data/raw/stock_market.json
```

---

## Step 2 — Bronze Layer

The Alpha Vantage response contains nested JSON.

Example:

```json
{
  "IBM": {
    "2026-05-18": {
      "open": "218.55",
      "high": "223.33"
    }
  }
}
```

This structure is flattened into Spark rows.

Result:

```text
symbol | date | open | high | low | close | volume
```

Purpose:

* Preserve source data
* Standardize schema
* Create Spark DataFrame

---

## Step 3 — Silver Layer

Data quality validations occur here.

Checks include:

* Date conversion
* NULL validation
* Duplicate detection
* Schema enforcement

Primary business key:

```text
(symbol, date)
```

---

## Step 4 — Gold Layer

Derived analytics columns are generated.

### Daily Range

```python
high - low
```

Measures intraday volatility.

### Daily Return %

```python
(close - open) / open * 100
```

Measures daily price movement.

### Candle Type

```text
Bullish
Bearish
```

Determined by:

```python
close > open
```

---

## Step 5 — Incremental Deduplication

Before loading:

1. Existing PostgreSQL records are read.
2. Incoming records are compared.
3. Spark performs a Left Anti Join.

```text
Incoming Data
      │
      ▼
Left Anti Join
      │
      ▼
Only New Rows
```

This prevents duplicate inserts.

---

## Step 6 — Load into PostgreSQL

PySpark writes data through JDBC.

```python
df.write.jdbc(...)
```

Target table:

```sql
stock_daily
```

---

## Step 7 — Save Checkpoint

After successful load:

```json
{
  "last_loaded_date": "2026-05-18"
}
```

The next execution resumes from the saved checkpoint.

---

# 🔁 Incremental Loading Strategy

```text
checkpoint.json
       │
       ▼
Determine Fetch Window
       │
       ▼
Extract Recent Data
       │
       ▼
Left Anti Join
       │
       ▼
Load Only New Rows
       │
       ▼
Update Checkpoint
```

### Three Layers of Deduplication

| Layer      | Protection     |
| ---------- | -------------- |
| API Filter | Date Window    |
| Spark      | Left Anti Join |
| PostgreSQL | Primary Key    |

### Force Full Reload

```bash
rm data/checkpoint.json

docker compose exec pyspark_etl python main.py
```

---

# 🗄️ Database Schema

```sql
CREATE TABLE stock_daily (
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,4),
    high DECIMAL(10,4),
    low DECIMAL(10,4),
    close DECIMAL(10,4),
    volume BIGINT,

    daily_range DECIMAL(10,4),
    daily_return_pct DECIMAL(10,4),
    candle VARCHAR(10),

    PRIMARY KEY(symbol,date)
);
```

---

# 📊 Sample Output

```text
symbol | date       | open   | close  | daily_return_pct | candle
-------+------------+--------+--------+------------------+---------
IBM    | 2026-05-18 | 218.55 | 222.75 | 1.92             | Bullish
AAPL   | 2026-05-18 | 300.24 | 297.84 | -0.80            | Bearish
MSFT   | 2026-05-18 | 432.10 | 436.90 | 1.11             | Bullish
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

GitHub: https://github.com/wajid-rah/process_etl/edit/main/Real-Time_Stock_ETL_SystemDocker

---

⭐ If you found this project useful, consider giving it a star.
