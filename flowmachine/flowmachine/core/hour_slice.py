# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import datetime as dt
from dateutil.parser import parse
from operator import lt as less_than, ge as greater_or_equal
from typing import List, Union

from sqlalchemy.sql import func, and_, or_, true
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.orm.attributes import InstrumentedAttribute

__all__ = ["HourInterval", "HourSlice"]


class DayPeriod:
    """
    Represents a repeated "daily" time period.

    Parameters
    ----------
    weekday : None
        This argument must be None for `DayPeriod` (it only exists
        for compatibility with the sister class `DayOfWeekPeriod`).
        An error is raised if a different value is passed.
    """

    def __init__(self, weekday=None):
        self.freq = "day"
        self.weekday = weekday
        if self.weekday is not None:
            raise ValueError(
                "If freq='day' then the `weekday` argument must not be provided."
            )

    def filter_timestamp_column_by_day_of_week(
        self, ts_col: InstrumentedAttribute
    ) -> ColumnElement:
        """
        Returns an expression equivalent to TRUE (because no additional
        filtering is needed to limit the day of the week).

        Parameters
        ----------
        ts_col : sqlalchemy.orm.attributes.InstrumentedAttribute
            The timestamp column to filter. Note that this input argument
            is ignored for the DayPeriod class because it requires no
            additional filtering to limit the day of the week.
        """
        return true()


class DayOfWeekPeriod:
    """
    Represents a repeated "day-of-the-week" time period.

    Parameters
    ----------
    weekday : str
        The weekday during which this day period is repeated every week.
        Should be one of: 'Monday', "Tuesday", "Wednesday", etc. (or an
        equivalent name if a non-English locale is being used).
    """

    def __init__(self, weekday: str):
        if weekday is None:
            raise ValueError(
                "If freq='week' then the `weekday` argument must be provided."
            )
        try:
            dt.datetime.strptime(weekday, "%A")
        except ValueError:
            raise ValueError(f"Invalid value for `weekday`: {weekday}.")

        self.freq = "week"
        self.weekday = weekday.capitalize()
        self.weekday_idx = parse(weekday).isoweekday()

    def filter_timestamp_column_by_day_of_week(self, ts_col: InstrumentedAttribute):
        """
        Returns a sql expression which filters the timestamp column
        """
        return func.extract("isodow", ts_col) == self.weekday_idx


def make_hour_interval_period(freq, *, weekday: Union[str, None] = None):
    """
    Returns an appropriate instance of `DayPeriod` or `DayOfWeekPeriod`,
    depending on the value of `freq`.

    Parameters
    ----------
    freq : {"day", "week"}
        Indicated whether the interval period should be repeated every day
        or on a particular day of the week ("week").
    weekday : str or None
        The weekday during which this day period is repeated every week.
        If `freq="day"` then the value of `weekday` must be None. Otherwise
        it should be one of: 'Monday', "Tuesday", "Wednesday", etc.

    Returns
    -------
    DayPeriod or DayOfWeekPeriod
    """
    cls_lookup = {"day": DayPeriod, "week": DayOfWeekPeriod}

    try:
        cls = cls_lookup[freq]
    except KeyError:
        allowed_freqs = ", ".join([repr(x) for x in cls_lookup.keys()])
        raise ValueError(f"Argument `freq` must be one of: {allowed_freqs}.")

    return cls(weekday=weekday)


class MissingHourAndMinutesTimestamp:
    """
    Represents a "missing" HH:MM timestamp. This is used to represent time intervals
    during the day where the start or end is missing (i.e. extends until midnight)
    """

    def __init__(self):
        self.is_missing = True

    def filter_timestamp_column(self, ts_col, cmp_op: callable) -> ColumnElement:
        """
        Filter timestamp column by comparing to this hour-of-day using the given
        comparison operator.

        Note that for the class `MissingHourAndMinutesTimestamp` this always
        returns TRUE, since a missing timestamp imposes no constraint.

        Parameters
        ----------
        ts_col : sqlalchemy column
            The timestamp column to filter.
        cmp_op : callable
            Comparison operator to use. For example: `operator.lt`, `operator.ge`.

        Returns
        -------
        sqlalchemy.sql.elements.True_
        """
        return true()

    def __repr__(self):
        return "MissingHourAndMinutesTimestamp"


class HourAndMinutesTimestamp(str):
    """
    Represents an HH:MM period of the day and allows filtering a timestamp column accordingly.

    Parameters
    ----------
    hour_str : str
        A string in the format 'HH:MM'.
    """

    def __new__(cls, hour_str, **kwargs):
        if hour_str is None:
            return MissingHourAndMinutesTimestamp()
        else:
            obj = str.__new__(cls, hour_str)
            obj._value_ = hour_str
            return obj

    def __init__(self, hour_str: str):
        self._validate_hour_string(hour_str)
        self.hour_str = hour_str
        self.is_missing = False

    def _validate_hour_string(self, hour_str: str):
        """
        Check if the input string is a valid hour string in the format 'HH:MM'
        and raise a ValueError otherwise.

        Parameters
        ----------
        hour_str : str
            The hour string to validate.

        Raises
        ------
        ValueError
            If the input value is not a valid hour string in the format 'HH:MM'.
        """
        if isinstance(hour_str, str):
            try:
                dt.datetime.strptime(hour_str, "%H:%M")
            except ValueError:
                # re-raise error with a slightly more custom error message
                raise ValueError(
                    f"Hour string must have the format 'HH:MM'. Got: '{hour_str}'"
                )
        else:
            raise ValueError(
                f"Input argument must be a string the format 'HH:MM'. Got: {hour_str}"
            )

    def filter_timestamp_column(self, ts_col, cmp_op: callable) -> ColumnElement:
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
        sqlalchemy.sql.elements.ColumnElement
            Sqlalchemy expression representing the filtered timestamp column.
            This can be used in WHERE clauses of other sql queries.
        """
        return cmp_op(func.to_char(ts_col, "HH24:MI"), self.hour_str)


class HourInterval:
    """
    Represents an interval of hours during the day which is repeated
    regularly, for example each day, or every Tuesday. The interval is
    considered to be half-open, i.e. the left endpoint is included and
    the right endpoint is excluded. For example: 08:30 <= HH:MM < 16:00.

    Parameters
    ----------
    start_hour : str
        Start hour of this hour interval in the format 'HH:MM' (e.g. '08:00').
        The start hour can also be `None`, indicating that the hour interval
        starts at midnight at the beginning of the day.
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
        self.start_hour = HourAndMinutesTimestamp(start_hour)
        self.stop_hour = HourAndMinutesTimestamp(stop_hour)
        self.period = make_hour_interval_period(freq, weekday=weekday)

    def __repr__(self):
        return (
            f"HourInterval(start_hour={self.start_hour!r}, stop_hour={self.stop_hour!r}, "
            f"freq={self.period.freq!r}, weekday={self.period.weekday!r})"
        )

    def filter_timestamp_column(self, ts_col) -> ColumnElement:
        """
        Filter timestamp column using this hour interval.

        Parameters
        ----------
        ts_col : sqlalchemy column
            The timestamp column to filter.

        Returns
        -------
        sqlalchemy.sql.elements.ColumnElement
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

    def get_subsetting_condition(self, ts_col) -> ColumnElement:
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
