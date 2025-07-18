# my_timetables/friday_after_third_wed.py
from __future__ import annotations
from datetime import timedelta
import pendulum

from airflow.timetables.base import DagRunInfo, DataInterval, Timetable, TimeRestriction

class FridayAfterThirdWednesday(Timetable):
    """
    Emits one run per month:
        00:00 local time on the Friday that is two days
        after the 3rd Wednesday of *that* month.
    The run's data interval is that 24-hour Friday window.
    """

    description = "Friday after the 3rd Wednesday each month"

    # ---------- tiny helper -------------------------------------------------
    @staticmethod
    def _target_friday(year: int, month: int, tz) -> pendulum.DateTime:
        first = pendulum.datetime(year, month, 1, tz=tz)
        third_wed = first.next(pendulum.WEDNESDAY).add(weeks=2)    # 3rd Wed
        return third_wed.add(days=2).start_of("day")               # Friday 00:00

    # ---------- manual trigger ---------------------------------------------
    def infer_manual_data_interval(
        self, *, run_after: pendulum.DateTime
    ) -> DataInterval:
        """When operator clicks *Trigger DAG*, pick the *next* eligible Friday."""
        cursor = run_after
        target = self._target_friday(cursor.year, cursor.month, cursor.tz)

        # if we've already passed this month's target Friday ➞ jump to next month
        if run_after >= target:
            target = self._target_friday(
                year=cursor.add(months=1).year,
                month=cursor.add(months=1).month,
                tz=cursor.tz,
            )
        return DataInterval(start=target, end=target + timedelta(days=1))

    # ---------- automated scheduler ----------------------------------------
    def next_dagrun_info(
        self,
        *,
        last_automated_dagrun: DagRunInfo | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        tz = restriction.timezone

        # Figure out where to start the search
        if last_automated_dagrun:
            cursor = last_automated_dagrun.data_interval.start.add(days=1)
        elif restriction.earliest:
            cursor = restriction.earliest
        else:
            return None  # no start_date on DAG

        # Don't schedule in the past when catchup = False
        if not restriction.catchup:
            cursor = max(cursor, pendulum.now(tz).start_of("day"))

        year, month = cursor.year, cursor.month
        while True:
            target = self._target_friday(year, month, tz)
            if target >= cursor:
                if restriction.latest and target > restriction.latest:
                    return None
                return DagRunInfo.interval(
                    start=target,
                    end=target + timedelta(days=1),
                )
            # move to first day of next month
            month += 1
            if month == 13:
                month = 1
                year += 1

timetable = FridayAfterThirdWednesday()