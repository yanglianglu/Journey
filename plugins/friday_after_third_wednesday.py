from __future__ import annotations
from airflow.plugins_manager import AirflowPlugin
from datetime import timedelta
import pendulum

from airflow.timetables.base import (
    DagRunInfo,
    DataInterval,
    Timetable,
    TimeRestriction,
)


class FridayAfterThirdWednesday(Timetable):
    """Timetable: first Friday after the 3rd Wednesday each month."""

    description = "Friday after 3rd Wednesday monthly"

    # ------------------------------------------------------------------ #
    # Helpers                                                             #
    # ------------------------------------------------------------------ #
    def __init__(self, timezone: str | pendulum.tz.Timezone | None = "UTC") -> None:
        # Airflow always serialises tz-aware datetimes; keep one TZ object
        self._tz = (
            pendulum.timezone(timezone)
            if isinstance(timezone, str)
            else timezone or pendulum.UTC
        )

    def _target_for_month(self, year: int, month: int) -> pendulum.DateTime:
        """Return the *target Friday* (midnight) for the given month."""
        first_day = pendulum.datetime(year, month, 1, tz=self._tz)
        first_wed = (
            first_day
            if first_day.day_of_week == pendulum.WEDNESDAY
            else first_day.next(pendulum.WEDNESDAY)
        )
        third_wed = first_wed.add(weeks=2)
        return third_wed.add(days=2).start_of("day")  # -> Friday

    # Backwards compatibility for tests expecting the old method name
    def _target_friday(self, year: int, month: int) -> pendulum.DateTime:
        """Alias for ``_target_for_month`` kept for testing purposes."""
        return self._target_for_month(year, month)

    # ------------------------------------------------------------------ #
    # Trigger a manual run from the UI / REST API                         #
    # ------------------------------------------------------------------ #
    def infer_manual_data_interval(
        self, *, run_after: pendulum.DateTime
    ) -> DataInterval:
        # Choose the *next* target Friday ≥ run_after
        cursor = run_after.in_timezone(self._tz)
        tgt = self._target_for_month(cursor.year, cursor.month)
        if cursor >= tgt:
            nxt = cursor.add(months=1)
            tgt = self._target_for_month(nxt.year, nxt.month)
        return DataInterval(tgt, tgt + timedelta(days=1))

    # ------------------------------------------------------------------ #
    # Core scheduler entry point                                          #
    # ------------------------------------------------------------------ #
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        # --- 1. establish a cursor date --------------------------------
        if last_automated_data_interval:
            cursor = last_automated_data_interval.end.in_timezone(self._tz)
        elif restriction.earliest:
            cursor = restriction.earliest.in_timezone(self._tz)
        else:
            cursor = pendulum.now(self._tz).start_of("day")

        # honour no‑catch‑up: push cursor to "today" if it lags
        if not restriction.catchup:
            today = pendulum.now(self._tz).start_of("day")
            if cursor < today:
                cursor = today

        # --- 2. iterate month‑by‑month until we find a suitable target --
        year, month = cursor.year, cursor.month
        while True:
            target = self._target_for_month(year, month)
            if target >= cursor:
                if restriction.latest and target > restriction.latest:
                    return None
                return DagRunInfo.interval(
                    start=target,
                    end=target + timedelta(days=1),
                )
            # move forward one month
            month += 1
            if month > 12:
                month = 1
                year += 1

    def serialize(self) -> dict:
        """Return JSON-serialisable state for Airflow DAG serialisation."""
        return {"timezone": self._tz.name}

    @classmethod
    def deserialize(cls, data: dict) -> "FridayAfterThirdWednesday":
        return cls(timezone=data.get("timezone", "UTC"))


class FridayAfterThirdWednesdayPlugin(AirflowPlugin):
    name = "Friday_After_Third_Wednesday"
    timetables = [FridayAfterThirdWednesday]

