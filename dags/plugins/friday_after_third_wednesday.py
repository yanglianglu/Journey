from __future__ import annotations

from datetime import timedelta
import pendulum

from airflow.timetables.base import DagRunInfo, DataInterval, Timetable, TimeRestriction


class FridayAfterThirdWednesday(Timetable):
    """Timetable that triggers on the Friday immediately following the
    third Wednesday of each month.
    The data interval covers that Friday exactly (00:00–00:00).
    """

    description = "Friday after the 3rd Wednesday each month"

    def __init__(self, timezone: str | pendulum.tz.Timezone | None = "UTC") -> None:
        if isinstance(timezone, str):
            self._timezone = pendulum.timezone(timezone)
        elif timezone is None:
            self._timezone = pendulum.UTC
        else:
            self._timezone = timezone

    # ------------------------------------------------------------------
    def _target_friday(self, year: int, month: int) -> pendulum.DateTime:
        first = pendulum.datetime(year, month, 1, tz=self._timezone)
        days_until_wed = (pendulum.WEDNESDAY - first.day_of_week) % 7
        first_wed = first.add(days=days_until_wed)
        third_wed = first_wed.add(weeks=2)
        return third_wed.add(days=2).start_of("day")

    # ------------------------------------------------------------------
    def infer_manual_data_interval(self, *, run_after: pendulum.DateTime) -> DataInterval:
        cursor = run_after.in_timezone(self._timezone)
        target = self._target_friday(cursor.year, cursor.month)
        if cursor >= target:
            next_month = cursor.add(months=1)
            target = self._target_friday(next_month.year, next_month.month)
        return DataInterval(start=target, end=target + timedelta(days=1))

    # ------------------------------------------------------------------
    def next_dagrun_info(
        self,
        *,
        last_automated_dagrun: DagRunInfo | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        tz = (
            restriction.earliest.tzinfo if restriction.earliest is not None else self._timezone
        )

        if last_automated_dagrun:
            cursor = last_automated_dagrun.data_interval.start.add(days=1)
        elif restriction.earliest:
            cursor = restriction.earliest
        else:
            return None

        if not restriction.catchup:
            now = pendulum.now(tz).start_of("day")
            if cursor < now:
                cursor = now

        year, month = cursor.year, cursor.month
        while True:
            target = self._target_friday(year, month)
            if target >= cursor:
                if restriction.latest and target > restriction.latest:
                    return None
                return DagRunInfo.interval(start=target, end=target + timedelta(days=1))
            month += 1
            if month > 12:
                month = 1
                year += 1


timetable = FridayAfterThirdWednesday()
