from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from dags.plugins.friday_after_third_wednesday import FridayAfterThirdWednesday


with DAG(
    dag_id="friday_after_third_wednesday_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    timetable=FridayAfterThirdWednesday(),
    catchup=False,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")
