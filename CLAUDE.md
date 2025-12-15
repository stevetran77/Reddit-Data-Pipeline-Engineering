# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Reddit Data Pipeline project using Apache Airflow 2.7.1 with Python 3.9. The project implements an ETL (Extract, Transform, Load) pipeline for collecting and processing Reddit data. The architecture uses Airflow's CeleryExecutor with PostgreSQL for metadata storage and Redis as the message broker.

## Architecture

### Core Components

- **Airflow Orchestration**: Manages DAG (Directed Acyclic Graphs) workflows running on a CeleryExecutor
- **Data Pipeline**: ETL processes in `pipelines/` and `etls/` directories
- **PostgreSQL Database**: Stores Airflow metadata and Reddit data (airflow_reddit database)
- **Redis**: Message broker for Celery task queue
- **Docker Containerization**: Multi-container setup (webserver, scheduler, worker, postgres, redis)

### Directory Structure

- `dags/` - Airflow DAG definitions (currently empty, will contain orchestration logic)
- `pipelines/` - Pipeline scripts and processes (currently empty)
- `etls/` - ETL modules and data transformation logic (currently empty)
- `utils/` - Utility functions and helper modules (currently empty)
- `config/` - Configuration files (currently empty)
- `data/` - Local data storage for testing/development
- `logs/` - Airflow logs (generated at runtime)
- `tests/` - Test files (currently empty)

### Key Dependencies

- **apache-airflow**: 3.1.5 (scheduler and webserver)
- **praw**: 7.8.1 (Reddit API client)
- **pandas**: 2.3.3 (data manipulation)
- **sqlalchemy**: 2.0.45 (database ORM)
- **fastapi**: 0.117.1 (API framework, if needed)
- **celery**: (implicit via Airflow) - distributed task queue

## Development Setup

### Initial Setup

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize Airflow database
airflow db init
airflow db upgrade

# Create admin user
airflow users create --username admin --firstname admin --lastname admin --role Admin --email airflow@airflow.com --password admin
```

### Running Airflow Locally

```bash
# Start scheduler (in one terminal)
airflow scheduler

# Start webserver (in another terminal)
airflow webserver --port 8080
```

Access Airflow UI at: http://localhost:8080

### Running with Docker

```bash
# Build and start all services
docker-compose up -d

# Initialize database on first run
docker-compose exec airflow-init bash

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Stop services
docker-compose down
```

Services:
- Airflow Webserver: http://localhost:8080
- PostgreSQL: localhost:5432 (credentials in airlfow.env)
- Redis: localhost:6379

## Configuration

### Environment Variables (airlfow.env)

- `AIRFLOW__CORE__EXECUTOR`: CeleryExecutor (distributed task execution)
- `AIRFLOW__CELERY__BROKER_URL`: Redis connection for message broker
- `AIRFLOW__CELERY__RESULT_BACKEND`: PostgreSQL for task result storage
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: PostgreSQL connection for Airflow metadata
- `AIRFLOW__CORE__FERNET_KEY`: Encryption key for sensitive data
- `AIRFLOW__CORE__LOGGING_LEVEL`: INFO (can change to DEBUG for troubleshooting)
- `AIRFLOW__CORE__LOAD_EXAMPLES`: False (don't load example DAGs)

**Important**: Never commit credentials or sensitive keys. The FERNET_KEY is already exposed in the repository and should be rotated for production.

### Database Configuration

- Database: `airflow_reddit` (PostgreSQL)
- Host: `postgres` (in Docker) or `localhost` (local)
- Port: 5432
- User/Password: postgres/postgres (development only)

## Common Development Tasks

### Creating a New DAG

DAGs go in the `dags/` directory. Airflow scans this directory for Python files. Basic structure:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG('dag_id', start_date=datetime(2024, 1, 1), schedule_interval='@daily') as dag:
    task = PythonOperator(task_id='task_name', python_callable=function_name)
```

### Running Tests

```bash
# No tests directory structure exists yet
# Create tests/ with pytest fixtures as needed
pytest tests/
```

### Updating Dependencies

```bash
# After adding new packages, update requirements.txt
pip freeze > requirements.txt

# Rebuild Docker images if using containers
docker-compose build
```

### Debugging

- Airflow Logs: `logs/` directory (local) or Docker container logs
- Task failures: Check Airflow UI > DAG > Task Instance > Logs tab
- Database issues: Connect to PostgreSQL and inspect `airflow_reddit` database
- Celery worker issues: Monitor `docker-compose logs airflow-worker`

## Important Notes

### Security Considerations

- **Credentials**: Currently hardcoded in airlfow.env. For production, use Airflow Secrets Backend (Vault, AWS Secrets Manager, etc.)
- **FERNET_KEY**: Currently exposed in repository. Generate new key: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
- **API Keys**: Store Reddit API credentials in Airflow Variables/Connections, not in code

### Performance

- CeleryExecutor allows parallel task execution across multiple workers
- PostgreSQL is suitable for metadata; consider separate data warehouse for analytics
- Redis memory management: Monitor for memory pressure with many queued tasks

### Docker-Compose Conventions

- Services defined in `docker-compose.yml`
- Volumes mount local directories into containers for live code editing
- All Airflow services share environment via `airlfow.env`
- Restart policies set to manage container lifecycle

