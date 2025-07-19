from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from friday_after_third_wednesday import FridayAfterThirdWednesday
import holidays


@task
def print_day(data_interval_start=None):
    """Log the run's logical date."""
    print(f"Run for: {data_interval_start.to_date_string()}")


@task.branch
def branch_month(data_interval_start=None):
    month = data_interval_start.month
    return "say_odd" if month % 2 else "say_even"


@task
def say_odd(data_interval_start=None):
    month = data_interval_start.month
    print(f"Month {month} is odd")


@task
def say_even(data_interval_start=None):
    month = data_interval_start.month
    print(f"Month {month} is even")


@task
def list_us_holidays(data_interval_start=None, data_interval_end=None):
    start = data_interval_start.date()
    end = data_interval_end.date()
    us_holidays = holidays.US(years=range(start.year, end.year + 1))
    for day, name in sorted(us_holidays.items()):
        if start <= day < end:
            print(f"{day}: {name}")


@dag(
    dag_id="friday_after_third_wednesday_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    timetable=FridayAfterThirdWednesday("UTC"),
    catchup=False,
    tags=["example"],
)
def friday_after_third_wednesday():
    start = EmptyOperator(task_id="start")

    date_task = print_day()

    branch = branch_month()

    odd_task = say_odd()

    even_task = say_even()

    holidays_task = list_us_holidays()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> date_task >> branch
    branch >> odd_task >> holidays_task >> end
    branch >> even_task >> holidays_task


dag = friday_after_third_wednesday()
