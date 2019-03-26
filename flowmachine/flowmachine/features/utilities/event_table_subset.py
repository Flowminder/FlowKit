# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import datetime
import pandas as pd
import warnings
from sqlalchemy import select, between, extract, or_
from typing import List

from ...core import Query, Table
from ...core.errors import MissingDateError
from flowmachine.utils import _makesafe
from ...core.sqlalchemy_utils import (
    get_sqlalchemy_table_definition,
    make_sqlalchemy_column_from_flowmachine_column_description,
    get_sql_string,
)
from flowmachine.utils import list_of_dates
from flowmachine.core.subscriber_subsetter import make_subscriber_subsetter

import structlog

logger = structlog.get_logger(__name__)


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
    >>> sd = EventTableSubset('2016-01-01 13:30:30',
                         '2016-01-02 16:25:00')
    >>> sd.head()

    """

    def __init__(
        self,
        start=None,
        stop=None,
        *,
        hours="all",
        table="events.calls",
        subscriber_subset=None,
        columns=["*"],
        subscriber_identifier="msisdn",
    ):

        # Temporary band-aid; marshmallow deserialises date strings
        # to date objects, so we convert it back here because the
        # lower-level classes still assume we are passing date strings.
        if isinstance(start, datetime.date):
            start = start.strftime("%Y-%m-%d")
        if isinstance(stop, datetime.date):
            stop = stop.strftime("%Y-%m-%d")

        self.start = start
        self.stop = stop
        self.hours = hours
        self.subscriber_subset_ORIG = subscriber_subset
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
            self.table_ORIG.fully_qualified_table_name, engine=Query.connection.engine
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
            d1 = self.connection.min_date(
                self.table_ORIG.fully_qualified_table_name.split(".")[1]
            ).strftime("%Y-%m-%d")
        else:
            d1 = self.start.split()[0]

        if self.stop is None:
            d2 = self.connection.max_date(
                self.table_ORIG.fully_qualified_table_name.split(".")[1]
            ).strftime("%Y-%m-%d")
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
                for d in self.connection.available_dates(
                    table=self.table_ORIG.name,
                    strictness=1,
                    schema=self.table_ORIG.schema,
                )[self.table_ORIG.name]
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
            ts_start = pd.Timestamp(self.start).strftime("%Y-%m-%d %H:%M:%S")
            select_stmt = select_stmt.where(
                self.sqlalchemy_table.c.datetime >= ts_start
            )
        if self.stop is not None:
            ts_stop = pd.Timestamp(self.stop).strftime("%Y-%m-%d %H:%M:%S")
            select_stmt = select_stmt.where(self.sqlalchemy_table.c.datetime <= ts_stop)

        if self.hours != "all":
            hour_start, hour_end = self.hours
            if hour_start < hour_end:
                select_stmt = select_stmt.where(
                    between(
                        extract("hour", self.sqlalchemy_table.c.datetime),
                        hour_start,
                        hour_end - 1,
                    )
                )
            else:
                # If dates are backwards, then this will be interpreted as spanning midnight
                select_stmt = select_stmt.where(
                    or_(
                        extract("hour", self.sqlalchemy_table.c.datetime) >= hour_start,
                        extract("hour", self.sqlalchemy_table.c.datetime) < hour_end,
                    )
                )

        select_stmt = self.subscriber_subsetter.apply_subset_if_needed(
            select_stmt, subscriber_identifier=self.subscriber_identifier
        )

        return get_sql_string(select_stmt)

    def _make_query_ORIG(self):  # pragma: no cover
        # Note: this is the original implementation of _make_query. It is kept for
        # reference for the time being but will likely be removed in the near future.
        # The one currently being used is _make_query_with_sqlalchemy above.

        where_clause = ""
        if self.start is not None:
            where_clause += f"WHERE (datetime >= '{self.start}'::timestamptz)"
        if self.stop is not None:
            where_clause += "WHERE " if where_clause == "" else " AND "
            where_clause += f"(datetime <= '{self.stop}'::timestamptz)"

        sql = f"""
        SELECT {", ".join(self.columns)}
        FROM {self.table_ORIG.fully_qualified_table_name}
        {where_clause}
        """

        if self.hours != "all":
            if self.hours[0] < self.hours[1]:
                sql += f" AND EXTRACT(hour FROM datetime) BETWEEN {self.hours[0]} and {self.hours[1] - 1}"
            # If dates are backwards, then this will be interpreted as
            # spanning midnight
            else:
                sql += f" AND (   EXTRACT(hour FROM datetime)  >= {self.hours[0]}"
                sql += f"      OR EXTRACT(hour FROM datetime)  < {self.hours[1]})"

        if self.subscriber_subset_ORIG is not None:
            try:
                subs_table = self.subscriber_subset_ORIG.get_query()
                cols = ", ".join(
                    c if "AS subscriber" not in c else "subscriber"
                    for c in self.columns
                )
                sql = f"SELECT {cols} FROM ({sql}) ss INNER JOIN ({subs_table}) subs USING (subscriber)"
            except AttributeError:
                where_clause = "WHERE " if where_clause == "" else " AND "
                try:
                    assert not isinstance(self.subscriber_subset_ORIG, str)
                    ss = tuple(self.subscriber_subset_ORIG)
                except (TypeError, AssertionError):
                    ss = (self.subscriber_subset_ORIG,)
                sql = f"{sql} {where_clause} {self.subscriber_identifier} IN {_makesafe(ss)}"

        return sql

    # _make_query = _make_query_ORIG
    _make_query = _make_query_with_sqlalchemy

    @property
    def fully_qualified_table_name(self):
        # EventTableSubset are a simple select from events, and should not be cached
        raise NotImplementedError
