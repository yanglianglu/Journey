# Friday After Third Wednesday Timetable

This plugin defines the `FridayAfterThirdWednesday` timetable. It schedules runs
on the Friday immediately following the third Wednesday of each month.

The timetable implements Airflow's ``Timetable`` interface and can be used by
setting ``timetable=FridayAfterThirdWednesday("UTC")`` when defining a DAG.

An example DAG using this timetable is available at
``dags/friday_after_third_wednesday_dag.py``.
