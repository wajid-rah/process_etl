## Project Description

Developed a containerized **Python–MySQL demo application** using **Docker** and **Docker Compose**.

## Project Structure

```text
docker-compose-demo/   (LAPTOP)
│
├── app.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
│
└── init/
    └── init.sql
```

---

## Architecture

```text
Docker Compose
│
├── MySQL Container
│      │
│      └── init.sql
│             ├── CREATE TABLE
│             └── INSERT DATA
│
└── python_container
       │
       └── app.py
              │
              └── Connects to mysql_container
```

## Key Features

### Automated MySQL Database Initialization
Used Docker volume mounting to automatically execute SQL scripts during MySQL container startup. This allowed automatic creation of database tables and insertion of sample records without manual SQL execution.

### Containerized Multi-Service Architecture
Configured separate Python and MySQL containers using Docker Compose, enabling isolated services to run and communicate within the same Docker network.

### Inter-Container Communication
Established communication between Python and MySQL containers using Docker internal networking, where the Python application connected to the MySQL service using the container hostname.

### Python-Based Database Operations
Implemented Python application logic using **Python MySQL Connector** to connect with MySQL, execute SQL queries, and retrieve records from the database.

### Volume Mapping and Persistent Storage
Configured Docker volume mappings to share SQL initialization scripts from the host machine into the MySQL container filesystem.

### Dependency and Environment Management
Created reusable Docker images using Dockerfile and managed application dependencies through `requirements.txt` for consistent environment setup.

### Technologies Used
- Python MySQL Connector
- MySQL
- Dockerfile
- Docker Compose

