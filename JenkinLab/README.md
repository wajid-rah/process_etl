# JenkinLab — Complete Jenkins CI/CD Setup Guide

A step-by-step guide covering Docker, Docker Compose, and Jenkins CI/CD pipeline with PostgreSQL and Python.

---

## Project Description 

**Project Title:** Automated ETL Pipeline with Jenkins CI/CD, Docker, and PostgreSQL

**Tech Stack:** Python · PostgreSQL · Docker · Docker Compose · Jenkins · GitHub · psycopg2 · Linux (Ubuntu)

**Points:**
- Designed and implemented an end-to-end **CI/CD pipeline** using **Jenkins** to automate ETL workflow deployment triggered by GitHub commits
- Containerized a **Python-based ETL application** using **Docker** and **Docker Compose**, orchestrating a multi-container setup with **PostgreSQL** and a Python data reader
- Built a custom **PostgreSQL Docker image** using `Dockerfile` with pre-loaded SQL scripts (`setup.sql`) to automate database schema creation and seed data insertion on container startup
- Configured **Jenkins Poll SCM** to automatically detect GitHub repository changes every 2 minutes and trigger pipeline builds without manual intervention
- Integrated **Gmail SMTP email notifications** in the Jenkins pipeline to alert on build `SUCCESS` or `FAILURE` with build details and console log links
- Implemented pipeline stages — Checkout, Tear Down, Build & Run, Verify, and Post Actions — with proper cleanup using `docker-compose down` to prevent container conflicts
- Troubleshot and resolved real-world DevOps issues including **OOM (Out of Memory) kills**, Docker socket permission errors, nested Git repository conflicts, and SMTP authentication failures
- Pushed project code to **GitHub** as a subfolder inside a monorepo (`process_etl/JenkinLab/`), configuring Jenkins `Script Path` to point to the correct `Jenkinsfile` location

---

## Project Structure

```
process_etl/
└── JenkinLab/
    ├── Dockerfile              # Python app image
    ├── Dockerfile.postgres     # Custom PostgreSQL image with setup.sql baked in
    ├── docker-compose.yml      # Orchestrates postgres + python-app
    ├── requirements.txt        # Python dependencies
    ├── setup.sql               # Creates employees table + inserts data
    ├── read_employees.py       # Reads and prints employees from PostgreSQL
    └── Jenkinsfile             # CI/CD pipeline definition
```

---

## Step 1 — Manual Docker Run

Run containers manually to understand the basics.

### What it does
- Starts PostgreSQL manually
- Installs Python dependencies manually
- Reads data from PostgreSQL

### Commands
```bash
# Create a network so containers can talk to each other
docker network create pg-network

# Start PostgreSQL
docker run -d \
  --name my_postgres \
  --network pg-network \
  -e POSTGRES_USER=etluser \
  -e POSTGRES_PASSWORD=etlpassword \
  -e POSTGRES_DB=wajiddb \
  postgres:16.14

# Run Python app
docker run --rm \
  --network pg-network \
  -v $(pwd):/app \
  python:3.10-slim \
  bash -c "pip install psycopg2-binary && python /app/read_employees.py"
```

### Output
```
=======================================================
ID    Name            Department      Salary     Join Date
=======================================================
1     Wajid Rahman    Engineering     75000.00   2023-01-15
...
Total employees: 10
=======================================================
```

### Problem with Step 1
- Must manually create network every time
- Must manually install dependencies every time
- Too many commands to remember

---

## Step 2 — Dockerfile

Automate the Python app setup into a reusable image.

### Dockerfile
```dockerfile
# Base image
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy Python script
COPY read_employees.py .

# Run script when container starts
CMD ["python", "read_employees.py"]
```

### requirements.txt
```
psycopg2-binary
```

### Commands
```bash
# Build the image once
docker build -t postgres-python-app .

# Run it (connect to existing postgres)
docker run --rm --network pg-network postgres-python-app
```

### Build vs Run
| Command | What it does | When |
|---|---|---|
| `docker build` | Creates an image (template) | Once |
| `docker run` | Creates a container from image | Every time |

### Problem with Step 2
- Still must manually create the network
- Still must start PostgreSQL separately

---

## Step 3 — Docker Compose

One command spins up everything: PostgreSQL + Python app.

### docker-compose.yml
```yaml
services:

  postgres:
    build:
      context: .
      dockerfile: Dockerfile.postgres   # custom image with setup.sql baked in
    container_name: pg-compose
    environment:
      POSTGRES_USER: etluser
      POSTGRES_PASSWORD: etlpassword
      POSTGRES_DB: wajiddb
    networks:
      - pg-network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etluser -d wajiddb"]
      interval: 5s
      timeout: 5s
      retries: 5

  python-app:
    build: .
    container_name: python-compose
    depends_on:
      postgres:
        condition: service_healthy   # wait until postgres is ready
    networks:
      - pg-network
    environment:
      DB_HOST: postgres
      DB_PORT: 5432
      DB_NAME: wajiddb
      DB_USER: etluser
      DB_PASS: etlpassword

networks:
  pg-network:
    driver: bridge
```

### Dockerfile.postgres
```dockerfile
FROM postgres:16.14
COPY setup.sql /docker-entrypoint-initdb.d/setup.sql
```

> **Why Dockerfile.postgres?**
> Baking `setup.sql` into the image avoids volume mount issues when running
> inside Jenkins (where the host path is not the same as the container path).

### setup.sql
```sql
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(100),
    salary NUMERIC(10,2),
    join_date DATE
);

INSERT INTO employees (name, department, salary, join_date) VALUES
('Wajid Rahman',  'Engineering', 75000, '2023-01-15'),
('Sara Ahmed',    'Marketing',   55000, '2022-06-01'),
('Ali Hassan',    'Engineering', 80000, '2021-03-20'),
('Priya Sharma',  'HR',          50000, '2023-07-10'),
('John Smith',    'Finance',     65000, '2020-11-05'),
('Fatima Khan',   'Engineering', 78000, '2022-09-15'),
('David Lee',     'Marketing',   52000, '2023-03-01'),
('Aisha Malik',   'HR',          48000, '2021-08-20'),
('Carlos Rivera', 'Finance',     70000, '2020-05-12'),
('Neha Patel',    'Engineering', 82000, '2022-12-01');
```

### Commands
```bash
# Start everything
docker-compose up --build

# Tear down and clean volumes
docker-compose down -v

# Rebuild from scratch
docker-compose down -v && docker-compose up --build
```

---

## Step 4 — Jenkins CI/CD Pipeline

Automate the entire flow: GitHub push → Jenkins builds → results printed.

### Flow
```
GitHub push
    ↓
Jenkins detects change (Poll SCM every 2 minutes)
    ↓
Pull latest code
    ↓
docker-compose up --build
    ↓
PostgreSQL starts → table created → Python reads data → results printed
    ↓
Jenkins shows SUCCESS or FAILURE
    ↓
Email notification sent
```

### Prerequisites

#### 1. Jenkins running with Docker socket mounted
```bash
docker run -d --name jenkins \
  -p 8080:8080 -p 50000:50000 \
  -v jenkins_home:/var/jenkins_home \
  -v /var/run/docker.sock:/var/run/docker.sock \
  jenkins/jenkins:lts
```

#### 2. Docker and docker-compose installed inside Jenkins
```bash
# Enter Jenkins container as root
docker exec -u root -it jenkins bash

# Install Docker
apt-get update && apt-get install -y docker.io

# Install docker-compose
curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64" \
  -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Allow Jenkins to use Docker socket
chmod 666 /var/run/docker.sock

# Verify
docker --version
docker-compose --version
```

### Jenkinsfile
```groovy
pipeline {
    agent any

    triggers {
        pollSCM('H/2 * * * *')   // check GitHub every 2 minutes
    }

    stages {

        stage('Checkout') {
            steps {
                echo 'Pulling latest code from GitHub...'
                checkout scm
            }
        }

        stage('Tear Down Old Containers') {
            steps {
                dir('JenkinLab') {
                    echo 'Stopping any existing containers...'
                    sh 'docker rm -f pg-compose python-compose || true'
                    sh 'docker-compose down || true'
                }
            }
        }

        stage('Build & Run') {
            steps {
                dir('JenkinLab') {
                    echo 'Building image and starting services...'
                    sh 'docker-compose build --no-cache'
                    sh 'docker-compose up --abort-on-container-exit'
                }
            }
        }

        stage('Verify Output') {
            steps {
                echo 'Pipeline complete - check logs for employee table'
            }
        }
    }

    post {
        success {
            echo '✅ SUCCESS: PostgreSQL started, data loaded, Python read it!'
            emailext(
                to: 'your.email@gmail.com',
                subject: "✅ Jenkins SUCCESS - ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                body: """
                    <h2>Build Succeeded!</h2>
                    <p><b>Job:</b> ${env.JOB_NAME}</p>
                    <p><b>Build:</b> #${env.BUILD_NUMBER}</p>
                    <p><b>Duration:</b> ${currentBuild.durationString}</p>
                    <p><a href="${env.BUILD_URL}">View Build Logs</a></p>
                """,
                mimeType: 'text/html'
            )
        }
        failure {
            echo '❌ FAILURE: Something went wrong. Check logs above.'
            dir('JenkinLab') {
                sh 'docker-compose down -v || true'
            }
            emailext(
                to: 'your.email@gmail.com',
                subject: "❌ Jenkins FAILURE - ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                body: """
                    <h2>Build Failed!</h2>
                    <p><b>Job:</b> ${env.JOB_NAME}</p>
                    <p><b>Build:</b> #${env.BUILD_NUMBER}</p>
                    <p><b>Duration:</b> ${currentBuild.durationString}</p>
                    <p><a href="${env.BUILD_URL}console">View Console Output</a></p>
                """,
                mimeType: 'text/html'
            )
        }
        always {
            echo 'Cleaning up containers...'
            dir('JenkinLab') {
                sh 'docker-compose down -v || true'
            }
        }
    }
}
```

### Jenkins Job Configuration
1. Go to `http://localhost:8080` → **New Item**
2. Name: `JenkinLab` → choose **Pipeline** → OK
3. Under **Pipeline**:
   - Definition: `Pipeline script from SCM`
   - SCM: `Git`
   - Repository URL: `https://github.com/YOUR_USERNAME/process_etl.git`
   - Branch: `*/main`
   - Script Path: `JenkinLab/Jenkinsfile`
4. Under **Build Triggers**: check **Poll SCM** → `H/2 * * * *`
5. Click **Save**

---

## Step 5 — Poll SCM Auto Trigger

Jenkins automatically checks GitHub every 2 minutes for new commits.

### Cron Schedule
```
H/2 * * * *
│   │ │ │ │
│   │ │ │ └── day of week (any)
│   │ │ └──── month (any)
│   │ └────── day of month (any)
│   └──────── hour (any)
└──────────── every 2 minutes
```

| Schedule | Frequency |
|---|---|
| `* * * * *` | Every 1 minute |
| `H/2 * * * *` | Every 2 minutes |
| `H/5 * * * *` | Every 5 minutes |
| `H/15 * * * *` | Every 15 minutes |

### Verify Polling
Go to Jenkins job → **Git Polling Log** on left sidebar:
```
Polling SCM changes on master
Changes found    ← triggers build automatically!
```

---

## Step 6 — Gmail Email Notifications

### Prerequisites
1. Enable **2-Step Verification** on your Google account
2. Generate **App Password**: Google Account → Security → App Passwords → Mail → Generate
3. Copy the 16-character password (without spaces): `abcdefghijklmnop`

### Jenkins Plugin
Install **Email Extension Plugin**: Manage Jenkins → Manage Plugins → Available → search `Email Extension Plugin`

### SMTP Configuration
Go to **Manage Jenkins** → **Configure System**:

**E-mail Notification section** → click Advanced:
```
SMTP Server:  smtp.gmail.com
Use SSL:      ✅
SMTP Port:    465
Username:     your.email@gmail.com
Password:     abcdefghijklmnop   (16-char app password, no spaces)
```

**Extended E-mail Notification section**:
```
SMTP Server:  smtp.gmail.com
SMTP Port:    465
Credentials:  your gmail + app password
Use SSL:      ✅
```

### Test
Manage Jenkins → Configure System → E-mail Notification → **Test configuration by sending test e-mail**

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `docker-compose: not found` | Not installed in Jenkins | Install inside Jenkins container |
| `exit code 137` | Out of memory | Free up RAM, remove unused containers |
| `setup.sql: Is a directory` | Volume mount fails in Jenkins | Use `Dockerfile.postgres` with `COPY` |
| `container name already in use` | Old container not cleaned up | `docker rm -f pg-compose` |
| `Connection refused port 25` | Wrong SMTP port | Use port 465 with SSL |
| `AuthenticationFailedException` | Using regular password | Use 16-char Gmail App Password |
| `src refspec main does not match` | No commits made yet | `git add . && git commit -m "..."` |
| `embedded git repository` | Nested `.git` folders | `Remove-Item -Recurse -Force .git` inside subfolder |

---

## Key Concepts

### Why `dir('JenkinLab')` in Jenkinsfile?
Jenkins clones the entire `process_etl` repo. `dir('JenkinLab')` tells every `sh` command to run from inside the `JenkinLab` subfolder where `docker-compose.yml` lives.

### Why `--abort-on-container-exit`?
Without it, PostgreSQL keeps running forever after Python finishes, and the Jenkins pipeline never ends.

### Why `|| true` after commands?
If the command fails (e.g. container doesn't exist), `|| true` prevents the pipeline from failing on the cleanup step.

### Why `Dockerfile.postgres` instead of volume mount?
When Jenkins runs `docker-compose`, the volume path `./setup.sql` refers to a path inside the Jenkins container. Docker looks for it on the **host machine** at that path — which doesn't exist — and creates an empty directory instead. Baking `setup.sql` into the image with `COPY` avoids this entirely.

---

## Next Steps

- **Multiple Environments** — Dev/Staging/Prod branches with different pipelines per branch
- **GitHub Webhook** — Instant trigger on push using ngrok tunnel
- **Slack Notifications** — Send build results to a Slack channel
- **Parameterized Builds** — Pass variables like environment name at build time
