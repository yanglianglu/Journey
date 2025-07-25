# Journey

This repository includes sample Airflow DAGs and plugins.

## Local Airflow with Docker Compose

To start a local Airflow 2.10 environment (using Python 3.11):

```bash
# First time initialization
cp .env.example .env  # create local env file


The web UI will be available at http://localhost:8080 with default credentials `airflow/airflow`.

### Dev Container

Use the included **.devcontainer** configuration with VS Code to start a containerised
development environment. This automatically builds the Airflow services using
`docker-compose`, installs Python dependencies, and connects as the `airflow`
user so the Docker features install cleanly.

