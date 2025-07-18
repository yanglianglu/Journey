import pendulum
from airflow.timetables.base import DataInterval, DagRunInfo, TimeRestriction

from dags.plugins.friday_after_third_wednesday import FridayAfterThirdWednesday

TZ = pendulum.timezone("UTC")
TT = FridayAfterThirdWednesday(timezone=TZ)


def test_target_friday():
    friday = TT._target_friday(2025, 3)
    assert friday == pendulum.datetime(2025, 3, 21, 0, 0, tz=TZ)


def test_next_dagrun_info_first_time():
    restriction = TimeRestriction(
        earliest=pendulum.datetime(2025, 1, 1, tz=TZ),
        latest=None,
        catchup=True,
    )
    info = TT.next_dagrun_info(last_automated_dagrun=None, restriction=restriction)
    assert info.logical_date == pendulum.datetime(2025, 1, 17, 0, 0, tz=TZ)
    assert info.data_interval == DataInterval(
        start=pendulum.datetime(2025, 1, 17, tz=TZ),
        end=pendulum.datetime(2025, 1, 18, tz=TZ),
    )


def test_next_dagrun_info_second_run():
    restriction = TimeRestriction(
        earliest=pendulum.datetime(2025, 1, 1, tz=TZ),
        latest=None,
        catchup=True,
    )
    last_info = DagRunInfo.interval(
        pendulum.datetime(2025, 1, 17, tz=TZ),
        pendulum.datetime(2025, 1, 18, tz=TZ),
    )
    next_info = TT.next_dagrun_info(last_automated_dagrun=last_info, restriction=restriction)
    assert next_info.logical_date == pendulum.datetime(2025, 2, 21, 0, 0, tz=TZ)


def test_manual_trigger_before_friday():
    run_after = pendulum.datetime(2025, 3, 18, 10, 0, tz=TZ)
    interval = TT.infer_manual_data_interval(run_after=run_after)
    assert interval.start == pendulum.datetime(2025, 3, 21, tz=TZ)
    assert interval.end == pendulum.datetime(2025, 3, 22, tz=TZ)


def test_manual_trigger_after_friday():
    run_after = pendulum.datetime(2025, 3, 23, 10, 0, tz=TZ)
    interval = TT.infer_manual_data_interval(run_after=run_after)
    assert interval.start == pendulum.datetime(2025, 4, 18, tz=TZ)
    assert interval.end == pendulum.datetime(2025, 4, 19, tz=TZ)


def test_respects_latest():
    restriction = TimeRestriction(
        earliest=pendulum.datetime(2025, 1, 1, tz=TZ),
        latest=pendulum.datetime(2025, 2, 1, tz=TZ),
        catchup=True,
    )
    last_info = DagRunInfo.interval(
        pendulum.datetime(2025, 1, 17, tz=TZ),
        pendulum.datetime(2025, 1, 18, tz=TZ),
    )
    info = TT.next_dagrun_info(last_automated_dagrun=last_info, restriction=restriction)
    assert info is None


def test_catchup_false_skips_past(monkeypatch):
    restriction = TimeRestriction(
        earliest=pendulum.datetime(2025, 1, 1, tz=TZ),
        latest=None,
        catchup=False,
    )
    monkeypatch.setattr(pendulum, "now", lambda tz=None: pendulum.datetime(2025, 5, 1, tz=TZ))
    info = TT.next_dagrun_info(last_automated_dagrun=None, restriction=restriction)
    assert info.logical_date == pendulum.datetime(2025, 5, 23, 0, 0, tz=TZ)
