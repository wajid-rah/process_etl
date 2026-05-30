# ── Base image: official Python slim (lightweight) ──────────────────────────
FROM python:3.10-slim

# ── Set working directory early so all commands run from /app ────────────────
# Same as doing "cd /app" before every command
WORKDIR /app

# ── Install Java and curl ─────────────────────────────────────────────────────
# default-jdk  → PySpark needs Java to run
# curl         → needed to download JDBC jar below
# apt-get clean + rm -rf → removes package cache to keep image size small
RUN apt-get update && \
    apt-get install -y default-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ── Create all required folders ───────────────────────────────────────────────
# /app/jars  → stores PostgreSQL JDBC driver
# /app/data  → stores raw data files
# /app/logs  → stores application logs
RUN mkdir -p /app/jars /app/data /app/logs


# ── Download PostgreSQL JDBC driver into jars folder ─────────────────────────
# -L follows redirects (required or download fails)
# -o saves file to exact path we specify
RUN curl -L -o /app/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# ── Upgrade pip then install Python dependencies ──────────────────────────────
# Copy requirements.txt first — Docker layer caching trick:
# if requirements.txt has not changed, Docker reuses this cached layer
# and skips reinstalling all packages — makes rebuilds much faster
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt



# ── Set all environment variables ─────────────────────────────────────────────
# JAVA_HOME            → where Java is installed (found with find command earlier)
# PATH                 → adds Java bin folder so "java" command works anywhere
# PYSPARK_PYTHON       → tells PySpark which Python to use for workers
# PYSPARK_DRIVER_PYTHON → tells PySpark which Python to use for the driver
# PYTHONPATH           → tells Python where to find your own code modules
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:/usr/local/bin:$PATH
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
ENV PYTHONPATH=/app/src

# ── Default command: run the ETL pipeline ────────────────────────────────────
# This runs when you do "docker run" without specifying a command
# Can be overridden with: docker run image_name spark-submit /app/src/main.py
# CMD ["python", "main.py"]