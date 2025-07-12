#!/usr/bin/env bash
set -euo pipefail

if [ ! -f "/opt/airflow/airflow.db_created" ]; then
  echo ">>> Initialising Airflow metadatabase"
  airflow db init

  echo ">>> Creating Admin user"
  airflow users create \
    --username admin \
    --password admin \
    --firstname Dev \
    --lastname Admin \
    --role Admin \
  touch /opt/airflow/airflow.db_created
fi