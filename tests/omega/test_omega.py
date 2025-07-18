# tests/test_friday_after_third_wed.py
import pytest
import pendulum
from airflow.timetables.base import TimeRestriction, DagRunInfo, DataInterval
from dags.plugins.omega import FridayAfterThirdWednesday

# Use a fixed timezone
TZ = pendulum.timezone("UTC")
TT = FridayAfterThirdWednesday()


def test_target_friday():
    # Example: March 2025 — 3rd Wednesday is 19th, so Friday is 21st
    friday = TT._target_friday(2025, 3, TZ)
    assert friday == TZ.datetime(2025, 3, 21, 0, 0, 0)


def test_next_dagrun_info_first_time():
    # DAG has start_date Jan 1, 2025
    restriction = TimeRestriction(
        earliest=TZ.datetime(2025, 1, 1),
        latest=None,
        catchup=True,
        timezone=TZ
    )

    info = TT.next_dagrun_info(
        last_automated_dagrun=None,
        restriction=restriction
    )
    assert info.logical_date == TZ.datetime(2025, 1, 17, 0, 0)
    assert info.data_interval == DataInterval(
        start=TZ.datetime(2025, 1, 17, 0, 0),
        end=TZ.datetime(2025, 1, 18, 0, 0)
    )


def test_next_dagrun_info_second_run():
    restriction = TimeRestriction(
        earliest=TZ.datetime(2025, 1, 1),
        latest=None,
        catchup=True,
        timezone=TZ
    )
    last_info = DagRunInfo.interval(
        TZ.datetime(2025, 1, 17),
        TZ.datetime(2025, 1, 18)
    )

    next_info = TT.next_dagrun_info(
        last_automated_dagrun=last_info,
        restriction=restriction
    )
    assert next_info.logical_date == TZ.datetime(2025, 2, 21, 0, 0)


def test_manual_trigger_before_friday():
    # Run now on March 18, 2025 (Tuesday) — should return Friday 21st
    run_after = TZ.datetime(2025, 3, 18, 10, 0)
    interval = TT.infer_manual_data_interval(run_after=run_after)

    assert interval.start == TZ.datetime(2025, 3, 21, 0, 0)
    assert interval.end == TZ.datetime(2025, 3, 22, 0, 0)


def test_manual_trigger_after_friday():
    # Run on March 23 (after target Friday) → should go to *April* target Friday
    run_after = TZ.datetime(2025, 3, 23, 10, 0)
    interval = TT.infer_manual_data_interval(run_after=run_after)

    assert interval.start == TZ.datetime(2025, 4, 18, 0, 0)  # 3rd Wed is 16 → Friday 18
    assert interval.end == TZ.datetime(2025, 4, 19, 0, 0)


def test_respects_latest():
    restriction = TimeRestriction(
        earliest=TZ.datetime(2025, 1, 1),
        latest=TZ.datetime(2025, 2, 1),
        catchup=True,
        timezone=TZ
    )

    last_info = DagRunInfo.interval(
        TZ.datetime(2025, 1, 17),
        TZ.datetime(2025, 1, 18)
    )
    info = TT.next_dagrun_info(last_automated_dagrun=last_info, restriction=restriction)
    assert info is None  # Next run (Feb 21) is after latest (Feb 1)