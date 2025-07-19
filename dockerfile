FROM apache/airflow:2.10.5-python3.11

ADD requirements.txt .

# Reinstall Airflow to ensure provider packages match the pinned version.
ARG AIRFLOW_VERSION=2.10.5
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

