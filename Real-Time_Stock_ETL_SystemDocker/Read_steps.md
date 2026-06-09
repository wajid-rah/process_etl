# 🐳 PySpark + PostgreSQL Docker Cheatsheet

> Goal: Run PySpark and PostgreSQL in Docker and connect through the same Docker network using JDBC.

---

# Architecture

```text
Docker Engine
│
├── Network: spark_network
│
├── Container: my_postgres
│       └── PostgreSQL
│
└── Container: my_spark
        ├── Python
        ├── Java
        ├── Spark
        └── PostgreSQL JDBC Driver
```

Both containers must be attached to the same Docker network.

---

# Method 1 — Manual Docker Setup (No Dockerfile)

## Step 1: Create Network

```bash
docker network create spark_network
```

Verify:

```bash
docker network ls
```

---

## Step 2: Start PostgreSQL

```bash
docker run -d \
--name my_postgres \
--network spark_network \
-e POSTGRES_USER=user \
-e POSTGRES_PASSWORD=pass \
-e POSTGRES_DB=mydb \
-p 5432:5432 \
postgres:16.14
```

Verify:

```bash
docker ps
```

---

## Step 3: Start Spark Container

```bash
docker run -d \
--name my_spark \
--network spark_network \
-u root \
-v ./src:/app/src \
spark:scala-java17 \
tail -f /dev/null
```

---

## Step 4: Enter Spark Container

```bash
docker exec -it my_spark bash
```

---

## Step 5: Install Python

```bash
apt-get update

apt-get install -y \
python3 \
python3-pip \
curl
```

Create shortcut:

```bash
ln -sf /usr/bin/python3 /usr/bin/python
```

Verify:

```bash
python3 --version
```

---

## Step 6: Install PostgreSQL JDBC Driver

```bash
curl -L -o /opt/spark/jars/postgresql.jar \
https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

Verify:

```bash
ls /opt/spark/jars | grep postgres
```

---

## Step 7: Find Important Paths

Spark:

```bash
find / -name spark-submit 2>/dev/null
```

Java:

```bash
find / -name java -type f 2>/dev/null
```

Python:

```bash
which python3
```

---

## Step 8: Configure Environment

```bash
export SPARK_HOME=/opt/spark

export JAVA_HOME=/opt/java/openjdk

export PYSPARK_PYTHON=/usr/bin/python3

export PATH=/opt/spark/bin:$PATH

export PYTHONPATH=/app/src
```

---

## Step 9: Verify PostgreSQL Connectivity

Install client:

```bash
apt-get install -y postgresql-client
```

Test:

```bash
psql \
-h my_postgres \
-U user \
-d mydb
```

If successful:

```sql
SELECT 1;
```

---

## Step 10: Run PySpark Job

```bash
spark-submit /app/src/app.py
```

Execution Flow:

```text
spark-submit
      │
      ▼
Find Spark
      │
      ▼
Find Java
      │
      ▼
Find Python
      │
      ▼
Load JDBC Driver
      │
      ▼
Connect PostgreSQL
```

---

# Method 2 — Dockerfile + Docker Compose (Recommended)

## Folder Structure

```text
project/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
│
├── sql/
│   └── init.sql
│
└── src/
    └── app.py
```

---

# Build Everything

```bash
docker compose up -d --build
```

Docker automatically:

```text
1. Creates network
2. Starts PostgreSQL
3. Builds Spark image
4. Installs Python
5. Installs Dependencies
6. Downloads JDBC Driver
7. Sets ENV Variables
8. Starts Spark Container
```

---

# Verify Services

Show services:

```bash
docker compose config --services
```

Show containers:

```bash
docker compose ps
```

---

# Run PySpark Job

```bash
docker compose exec spark spark-submit /app/src/app.py
```

or

```bash
docker compose exec pyspark python main.py
```

depending on service name.

---

# Debugging Checklist

## Check Containers

```bash
docker ps
```

---

## Check Networks

```bash
docker network ls
```

Inspect:

```bash
docker network inspect spark_network
```

Verify both containers appear.

---

## Check Volumes

```bash
docker volume ls
```

Inspect:

```bash
docker volume inspect volume_name
```

---

## Check Mounts

```bash
docker inspect my_spark
```

or

```bash
docker inspect my_spark --format "{{json .Mounts}}"
```

---

## Enter Container

```bash
docker exec -it my_spark bash
```

---

## Check Python

```bash
which python3
python3 --version
```

---

## Check Spark

```bash
which spark-submit

spark-submit --version
```

---

## Check Java

```bash
java -version
```

---

## Check JDBC Driver

```bash
ls /opt/spark/jars | grep postgres
```

---

## Test PostgreSQL Reachability

Ping:

```bash
ping my_postgres
```

Connection:

```bash
psql -h my_postgres -U user -d mydb
```

---

# Common Restart Commands

## Stop

```bash
docker compose down
```

---

## Stop + Remove Volumes

```bash
docker compose down -v
```

---

## Rebuild

```bash
docker compose build --no-cache
```

---

## Fresh Start

```bash
docker compose up -d --build
```

---

# PostgreSQL Verification

Open PostgreSQL:

```bash
docker exec -it my_postgres psql -U user -d mydb
```

List tables:

```sql
\dt
```

Query table:

```sql
SELECT * FROM test_table;
```

---

# Golden Rule

For PySpark to connect to PostgreSQL:

```text
✓ Same Docker Network

✓ PostgreSQL Running

✓ JDBC Driver Present

✓ Correct Hostname
    host = my_postgres

✓ Correct Credentials

✓ Spark Container Has Java

✓ Spark Container Has Python
```

If all seven are true, JDBC connectivity will work.
