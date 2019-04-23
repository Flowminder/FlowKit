# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from sqlalchemy.sql import func, and_, true

__all__ = ["HourSlice"]


class DayPeriod:
    """
    Represents a repeated "daily" time period.
    """

    def __init__(self, weekday=None):
        self.freq = "day"
        if weekday is not None:
            raise ValueError(
                "If freq='day' then the `weekday` argument must not be provided."
            )

    def filter_timestamp_column(self, ts_col):
        return true()


class DayOfWeekPeriod:
    """
    Represents a repeated "day-of-the-week" time period.
    """

    valid_weekdays = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    weekday_indices_postgres = {
        "Sunday": 0,
        "Monday": 1,
        "Tuesday": 2,
        "Wednesday": 3,
        "Thursday": 4,
        "Friday": 5,
        "Saturday": 6,
    }

    def __init__(self, weekday):
        if weekday is None:
            raise ValueError(
                "If freq='week' then the `weekday` argument must be provided."
            )
        weekday = weekday.capitalize()
        if weekday not in self.valid_weekdays:
            valid_weekdays_list = ", ".join([repr(x) for x in self.valid_weekdays])
            raise ValueError(
                f"Invalid value for `weekday`. Must be one of: {valid_weekdays_list}."
            )

        self.freq = "week"
        self.weekday = weekday

    def filter_timestamp_column(self, ts_col):
        return func.extract("dow", ts_col) == self.weekday


def make_hour_slice_period(freq, *, weekday=None):
    """
    Returns an appropriate instance of `DayPeriod` or `DayOfWeekPeriod`,
    depending on the value of `freq`.
    """
    cls_lookup = {"day": DayPeriod, "week": DayOfWeekPeriod}

    try:
        cls = cls_lookup[freq]
    except KeyError:
        allowed_freqs = ", ".join([repr(x) for x in cls_lookup.keys()])
        raise ValueError(f"Argument `freq` must be one of: {allowed_freqs}.")

    return cls(weekday=weekday)


class HourSlice:
    """
    Represents an interval of hours during the day which is repeated
    regularly, for example each day, or every Tuesday.

    Parameters
    ----------
    start_hour : str
        Start hour of this hour-slice in the format 'HH:MM' (e.g. '08:00').
    stop_hour : str
        Stop hour of this hour-slice in the format 'HH:MM' (e.g. '19:30').
    freq : str
        Frequency at which the underlying time interval is repeated. This
        must be either "day" or "week". In the latter case the `weekday`
        argument must also be provided.
    weekday : str
        The day of the week for which this hour slice is valid. This argument
        is only relevant if `freq="week"` and is ignored otherwise.
    """

    def __init__(
        self, *, start_hour: str, stop_hour: str, freq: str, weekday: str = None
    ):
        self.start_hour = start_hour
        self.stop_hour = stop_hour
        self.period = make_hour_slice_period(freq, weekday=weekday)

    def filter_timestamp_column(self, ts_col):
        """
        Filter timestamp column using this hour slice.

        Parameters
        ----------
        ts_col : sqlalchemy column
            The timestamp column to filter.

        Returns
        -------
        sqlalchemy.sql.elements.BooleanClauseList
            Sqlalchemy expression representing the filtered timestamp column.
            This can be used in WHERE clauses of other sql queries.
        """
        return and_(
            func.to_char(ts_col, "HH24:MI") >= self.start_hour,
            func.to_char(ts_col, "HH24:MI") < self.stop_hour,
            self.period.filter_timestamp_column(ts_col),
        )
