# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import datetime
import pandas as pd
import warnings
from sqlalchemy import select
from typing import List

from ...core import Query, Table
from ...core.context import get_db
from ...core.errors import MissingDateError
from ...core.sqlalchemy_utils import (
    get_sqlalchemy_table_definition,
    make_sqlalchemy_column_from_flowmachine_column_description,
    get_sql_string,
)
from flowmachine.utils import list_of_dates, standardise_date
from flowmachine.core.hour_slice import HourSlice, HourInterval
from flowmachine.core.subscriber_subsetter import make_subscriber_subsetter

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class EventTableSubset(Query):
    """
    Represent the whole of a dataset subset over certain date ranges.

    Parameters
    ----------
    start : str, default None
        iso format date range for the beginning of the time frame, e.g.
        2016-01-01 or 2016-01-01 14:03:01. If None, it will use the
        earliest date seen in the `events.calls` table.
    stop : str, default None
        As above. If None, it will use the latest date seen in the
        `events.calls` table.
    hours : tuple of ints, default 'all'
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    table : str, default 'events.calls'
        schema qualified name of the table which the analysis is
        based upon
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Notes
    -----
    * A date without a hours and mins will be interpreted as
      midnight of that day, so to get data within a single day
      pass (e.g.) '2016-01-01', '2016-01-02'.

    * Use 24 hr format!

    Examples
    --------
    >>> sd = EventTableSubset(start='2016-01-01 13:30:30', stop='2016-01-02 16:25:00')
    >>> sd.head()

    """

    def __init__(
        self,
        *,
        start=None,
        stop=None,
        hours="all",
        hour_slices=None,
        table="events.calls",
        subscriber_subset=None,
        columns=["*"],
        subscriber_identifier="msisdn",
    ):

        if hours != "all" and hour_slices is not None:
            raise ValueError(
                "The arguments `hours` and `hour_slice` are mutually exclusive."
            )
        if hours != "all":
            assert (
                isinstance(hours, tuple)
                and len(hours) == 2
                and isinstance(hours[0], int)
                and isinstance(hours[1], int)
            )  # sanity check

            start_hour = hours[0]
            stop_hour = hours[1]
            start_hour_str = f"{start_hour:02d}:00"
            stop_hour_str = f"{stop_hour:02d}:00"
            if start_hour <= stop_hour:
                hs = HourInterval(
                    start_hour=start_hour_str, stop_hour=stop_hour_str, freq="day"
                )
                self.hour_slices = HourSlice(hour_intervals=[hs])
            else:
                # If hours are backwards, then this is interpreted as spanning midnight,
                # so we split it into two time slices for the beginning/end of the day.
                hs1 = HourInterval(start_hour=None, stop_hour=stop_hour_str, freq="day")
                hs2 = HourInterval(
                    start_hour=start_hour_str, stop_hour=None, freq="day"
                )
                self.hour_slices = HourSlice(hour_intervals=[hs1, hs2])
        else:
            self.hour_slices = HourSlice(hour_intervals=[])

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.subscriber_subsetter = make_subscriber_subsetter(subscriber_subset)
        self.subscriber_identifier = subscriber_identifier.lower()
        if columns == ["*"]:
            self.table_ORIG = Table(table)
            columns = self.table_ORIG.column_names
        else:
            self.table_ORIG = Table(table, columns=columns)
        self.columns = set(columns)
        try:
            self.columns.remove(subscriber_identifier)
            self.columns.add(f"{subscriber_identifier} AS subscriber")
        except KeyError:
            if self.subscriber_subsetter.is_proper_subset:
                warnings.warn(
                    f"No subscriber column requested, did you mean to include {subscriber_identifier} in columns? "
                    "Since you passed a subscriber_subset the data will still be subset by your subscriber subset, "
                    "but the subscriber column will not be present in the output.",
                    stacklevel=2,
                )
        self.columns = sorted(self.columns)

        self.sqlalchemy_table = get_sqlalchemy_table_definition(
            self.table_ORIG.fully_qualified_table_name,
            engine=get_db().engine,
        )

        if self.start == self.stop:
            raise ValueError("Start and stop are the same.")

        super().__init__()

        # This needs to happen after the parent classes init method has been
        # called as it relies upon the connection object existing
        self._check_dates()

    @property
    def column_names(self) -> List[str]:
        return [c.split(" AS ")[-1] for c in self.columns]

    def _check_dates(self):

        # Handle the logic for dealing with missing dates.
        # If there are no dates present, then we raise an error
        # if some are present, but some are missing we raise a
        # warning.
        # If the subscriber does not pass a start or stop date, then we take
        # the min/max date in the events.calls table
        if self.start is None:
            d1 = (
                get_db()
                .min_date(self.table_ORIG.fully_qualified_table_name.split(".")[1])
                .strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            d1 = self.start.split()[0]

        if self.stop is None:
            d2 = (
                get_db()
                .max_date(self.table_ORIG.fully_qualified_table_name.split(".")[1])
                .strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            d2 = self.stop.split()[0]

        all_dates = list_of_dates(d1, d2)
        # Slightly annoying feature, but if the subscriber passes a date such as '2016-01-02'
        # this will be interpreted as midnight, so we don't want to include this in our
        # calculations. Check for this here, an if this is the case pop the final element
        # of the list
        if (self.stop is not None) and (
            len(self.stop) == 10 or self.stop.endswith("00:00:00")
        ):
            all_dates.pop(-1)
        # This will be a true false list for whether each of the dates
        # is present in the database
        try:
            db_dates = [
                d.strftime("%Y-%m-%d")
                for d in get_db().available_dates[self.table_ORIG.name]
            ]
        except KeyError:  # No dates at all for this table
            raise MissingDateError
        dates_present = [d in db_dates for d in all_dates]
        logger.debug(
            f"Data for {sum(dates_present)}/{len(dates_present)} calendar dates."
        )
        # All dates are missing
        if not any(dates_present):
            raise MissingDateError
        # Some dates are missing, others are present
        elif not all(dates_present):
            present_dates = [d for p, d in zip(dates_present, all_dates) if p]
            warnings.warn(
                f"{len(dates_present) - sum(dates_present)} of {len(dates_present)} calendar dates missing. Earliest date is {present_dates[0]}, latest is {present_dates[-1]}.",
                stacklevel=2,
            )

    def _make_query_with_sqlalchemy(self):
        sqlalchemy_columns = [
            make_sqlalchemy_column_from_flowmachine_column_description(
                self.sqlalchemy_table, column_str
            )
            for column_str in self.columns
        ]
        select_stmt = select(sqlalchemy_columns)

        if self.start is not None:
            select_stmt = select_stmt.where(
                self.sqlalchemy_table.c.datetime >= self.start
            )
        if self.stop is not None:
            select_stmt = select_stmt.where(
                self.sqlalchemy_table.c.datetime < self.stop
            )

        select_stmt = select_stmt.where(
            self.hour_slices.get_subsetting_condition(self.sqlalchemy_table.c.datetime)
        )
        select_stmt = self.subscriber_subsetter.apply_subset_if_needed(
            select_stmt, subscriber_identifier=self.subscriber_identifier
        )

        return get_sql_string(select_stmt)

    _make_query = _make_query_with_sqlalchemy

    @property
    def fully_qualified_table_name(self):
        # EventTableSubset are a simple select from events, and should not be cached
        raise NotImplementedError
