# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
from operator import lt as less_than, ge as greater_or_equal
from typing import List, Union

from sqlalchemy.sql import func, and_, or_, true

__all__ = ["HourInterval", "HourSlice"]


class DayPeriod:
    """
    Represents a repeated "daily" time period.
    """

    def __init__(self, weekday=None):
        self.freq = "day"
        self.weekday = weekday
        if self.weekday is not None:
            raise ValueError(
                "If freq='day' then the `weekday` argument must not be provided."
            )

    def filter_timestamp_column_by_day_of_week(self, ts_col):
        # no additional filtering needed to limit the day of the week
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

    def filter_timestamp_column_by_day_of_week(self, ts_col):
        return (
            func.extract("dow", ts_col) == self.weekday_indices_postgres[self.weekday]
        )


def make_hour_interval_period(freq, *, weekday=None):
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


class HourOfDay:
    """
    Represents an hour of the day and allows filtering a timestamp column accordingly.
    """

    def __init__(self, hour_str: Union[str, None]):
        self._validate_hour_string(hour_str)
        self.hour_str = hour_str

    def __eq__(self, other):
        if isinstance(other, HourOfDay):
            return self.hour_str == other.hour_str
        elif isinstance(other, str) or other is None:
            return self.hour_str == other
        else:
            raise TypeError(
                f"HourOfDay cannot be compared to object of type {type(other)}"
            )

    def _validate_hour_string(self, hour_str):
        """
        Return the input string if it is a valid hour string in the format 'HH:MM'
        and raise a ValueError otherwise.
        """
        if hour_str is None:
            return
        elif isinstance(hour_str, str):
            m = re.match("^(\d\d):(\d\d)$", hour_str)
            if not m:
                raise ValueError(
                    f"Hour string must have the format 'HH:MM'. Got: '{hour_str}'"
                )
            hour = int(m.group(1))
            minutes = int(m.group(2))
            if not (0 <= hour and hour < 24):
                raise ValueError(
                    f"Invalid hour value: {hour} (must be between 0 and 23, inclusively)"
                )
            if not (0 <= minutes and minutes < 60):
                raise ValueError(
                    f"Invalid minutes value: {minutes} (must be between 0 and 59, inclusively)"
                )
        else:
            raise ValueError(
                f"Input argument must be a string the format 'HH:MM'. Got: {hour_str}"
            )

    def filter_timestamp_column(self, ts_col, cmp_op):
        """
        Filter timestamp column by comparing to this hour-of-day using the given comparison operator.

        Parameters
        ----------
        ts_col : sqlalchemy column
            The timestamp column to filter.
        cmp_op : callable
            Comparison operator to use. For example: `operator.lt`, `operator.ge`.

        Returns
        -------
        sqlalchemy.sql.elements.BooleanClauseList
            Sqlalchemy expression representing the filtered timestamp column.
            This can be used in WHERE clauses of other sql queries.
        """
        if self.hour_str is None:
            # no filtering needed
            return true()
        else:
            return cmp_op(func.to_char(ts_col, "HH24:MI"), self.hour_str)


class HourInterval:
    """
    Represents an interval of hours during the day which is repeated
    regularly, for example each day, or every Tuesday.

    Parameters
    ----------
    start_hour : str
        Start hour of this hour interval in the format 'HH:MM' (e.g. '08:00').
    stop_hour : str (optional)
        Stop hour of this hour interval in the format 'HH:MM' (e.g. '19:30').
        The stop hour can also be `None`, indicating that the hour interval
        extends until midnight.
    freq : str
        Frequency at which the underlying time interval is repeated. This
        must be either "day" or "week". In the latter case the `weekday`
        argument must also be provided.
    weekday : str
        The day of the week for which this hour interval is valid. This argument
        is only relevant if `freq="week"` and is ignored otherwise.
    """

    def __init__(
        self, *, start_hour: str, stop_hour: str, freq: str, weekday: str = None
    ):
        self.start_hour = HourOfDay(start_hour)
        self.stop_hour = HourOfDay(stop_hour)
        self.period = make_hour_interval_period(freq, weekday=weekday)

    def __repr__(self):
        return (
            f"HourInterval(start_hour={self.start_hour!r}, stop_hour={self.stop_hour!r}, "
            f"freq={self.period.freq!r}, weekday={self.period.weekday!r})"
        )

    def filter_timestamp_column(self, ts_col):
        """
        Filter timestamp column using this hour interval.

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
            self.start_hour.filter_timestamp_column(ts_col, cmp_op=greater_or_equal),
            self.stop_hour.filter_timestamp_column(ts_col, cmp_op=less_than),
            self.period.filter_timestamp_column_by_day_of_week(ts_col),
        )


class HourSlice:
    """
    Represents a collection of multiple non-overlapping hour intervals.

    Parameters
    ----------
    hour_intervals : list of HourInterval
        List of hour interval. These are assumed to be non-overlapping
        (but note that this is not currently enforced).
    """

    def __init__(self, *, hour_intervals: List[HourInterval]):
        assert isinstance(hour_intervals, (tuple, list))
        assert all([isinstance(x, HourInterval) for x in hour_intervals])
        # TODO: would be good to check that hour slices are non-overlapping
        self.hour_intervals = list(hour_intervals)

    def __repr__(self):
        return f"<HourSlice: {self.hour_intervals}>"

    def get_subsetting_condition(self, ts_col):
        """
        Return sqlalchemy expression which represents subsetting
        the given timestamp column by hours of the day.

        Parameters
        ----------
        ts_col : sqlalchemy column
            The timestamp column to which to apply the subsetting.

        Returns
        -------
        sqlalchemy.sql.elements.BooleanClauseList
        """
        return or_(*[hs.filter_timestamp_column(ts_col) for hs in self.hour_intervals])
