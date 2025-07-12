# Use the official Airflow image as the base.
# Pin versions so your dev env is reproducible.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

# Install build essentials & common CLI helpers
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential git curl vim && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy project-level requirements in for frozen repeatability
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Optional: pre-install AWS & Snowflake providers so cli autocomplete works
RUN pip install --no-cache-dir \
        apache-airflow-providers-amazon==9.2.0 \
        apache-airflow-providers-snowflake==5.8.0

# Switch back to airflow (non-root) so file permissions match host
USER airflow