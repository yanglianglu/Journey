# Journey

This repository includes sample Airflow DAGs and plugins.

## Local Airflow with Docker Compose

To start a local Airflow 2.10 environment:

```bash
# First time initialization
AIRFLOW_UID=$(id -u) docker compose up airflow-webserver airflow-scheduler postgres -d
```

The web UI will be available at http://localhost:8080 with default credentials `airflow/airflow`.
