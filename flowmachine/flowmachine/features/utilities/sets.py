# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility classes for subsetting CDRs.

"""
import logging
import warnings
from typing import List

from ...core import Query, Table
from ...utils.utils import list_of_dates, get_columns_for_level
from ...core.utils import _makesafe
from ...core.errors import MissingDateError

from numpy import inf

logger = logging.getLogger("flowmachine").getChild(__name__)
valid_subscriber_identifiers = ("msisdn", "imei", "imsi")


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

        self.start = start
        self.stop = stop
        self.hours = hours
        self.subscriber_subset = subscriber_subset
        self.subscriber_identifier = subscriber_identifier.lower()
        if columns == ["*"]:
            self.table = Table(table)
            columns = self.table.column_names
        else:
            self.table = Table(table, columns=columns)
        self.columns = set(columns)
        try:
            self.columns.remove(subscriber_identifier)
            self.columns.add(f"{subscriber_identifier} AS subscriber")
        except KeyError:
            if subscriber_subset is not None:
                warnings.warn(
                    f"No subscriber column requested, did you mean to include {subscriber_identifier} in columns? "
                    "Since you passed a subscriber_subset the data will still be subset by your subscriber subset, "
                    "but the subscriber column will not be present in the output.",
                    stacklevel=2,
                )
        self.columns = sorted(self.columns)

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
            d1 = self.connection.min_date(self.table.table_name.split(".")[1]).strftime(
                "%Y-%m-%d"
            )
        else:
            d1 = self.start.split()[0]

        if self.stop is None:
            d2 = self.connection.max_date(self.table.table_name.split(".")[1]).strftime(
                "%Y-%m-%d"
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
                for d in self.connection.available_dates(
                    table=self.table.name, strictness=1, schema=self.table.schema
                )[self.table.name]
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

    def _make_query(self):

        where_clause = ""
        if self.start is not None:
            where_clause += f"WHERE (datetime >= '{self.start}'::timestamptz)"
        if self.stop is not None:
            where_clause += "WHERE " if where_clause == "" else " AND "
            where_clause += f"(datetime <= '{self.stop}'::timestamptz)"

        sql = f"""
        SELECT {", ".join(self.columns)}
        FROM {self.table.table_name}
        {where_clause}
        """

        if self.hours != "all":
            if self.hours[0] < self.hours[1]:
                sql += f" AND EXTRACT(hour FROM datetime) BETWEEN {self.hours[0]} and {self.hours[1] - 1}"
            # If dates are backwards, then this will be interpreted as
            # spanning midnight
            else:
                sql += f" AND EXTRACT(hour FROM datetime)  >= {self.hours[0]}"
                sql += f" OR EXTRACT(hour FROM datetime)  < {self.hours[1]}"

        if self.subscriber_subset is not None:
            try:
                subs_table = self.subscriber_subset.get_query()
                cols = ", ".join(
                    c if "AS subscriber" not in c else "subscriber"
                    for c in self.columns
                )
                sql = f"SELECT {cols} FROM ({sql}) ss INNER JOIN ({subs_table}) subs USING (subscriber)"
            except AttributeError:
                where_clause = "WHERE " if where_clause == "" else " AND "
                try:
                    assert not isinstance(self.subscriber_subset, str)
                    ss = tuple(self.subscriber_subset)
                except (TypeError, AssertionError):
                    ss = (self.subscriber_subset,)
                sql = f"{sql} {where_clause} {self.subscriber_identifier} IN {_makesafe(ss)}"

        return sql

    @property
    def table_name(self):
        # EventTableSubset are a simple select from events, and should not be cached
        raise NotImplementedError


class EventsTablesUnion(Query):
    """
    Takes a list of subtables, subsets each of them
    by date and selects a specified list of columns
    from the result and unions (i.e. appends) all
    of these tables. This class is mostly used as an
    intermediate for other classes.

    Parameters
    ----------
    start, stop : str
        ISO-format date
    columns :
        list of columns to select
    tables : str or list of strings, default 'all'
        Can be a sting of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    """

    def __init__(self, start, stop, *, columns, tables="all", hours="all", subscriber_subset=None, subscriber_identifier="msisdn"):
        """

        """

        self.start = start
        self.stop = stop
        if "*" in columns and len(tables) != 1:
            raise ValueError(
                "Must give named tables when combining multiple event type tables."
            )
        self.columns = columns
        self.tables = self._parse_tables(tables)
        self.date_subsets = self._make_table_list(hours=hours, subscriber_subset=subscriber_subset, subscriber_identifier=subscriber_identifier)

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.date_subsets[
            0
        ].column_names  # Use in preference to self.columns which might be ["*"]

    def _parse_tables(self, tables):

        if isinstance(tables, str) and tables.lower() == "all":
            return [f"events.{t}" for t in self.connection.subscriber_tables]
        elif type(tables) is str:
            return [tables]
        else:
            return tables

    def _make_table_list(self, *, hours, subscriber_subset, subscriber_identifier):
        """
        Makes a list of EventTableSubset queries.
        """

        date_subsets = []
        for table in self.tables:
            try:
                sql = EventTableSubset(
                    self.start, self.stop, table=table, columns=self.columns, hours=hours, subscriber_subset=subscriber_subset, subscriber_identifier=subscriber_identifier
                )
                date_subsets.append(sql)
            except MissingDateError:
                warnings.warn(
                    f"No data in {table} for {self.start}–{self.stop}", stacklevel=2
                )
        if not date_subsets:
            raise MissingDateError(self.start, self.stop)
        return date_subsets

    def _make_query(self):

        # Get the list of tables, select the relevant columns and union
        # them all
        sql = "\nUNION ALL\n".join(sd.get_query() for sd in self.date_subsets)

        return sql

    @property
    def table_name(self):
        # EventTableSubset are a simple select from events, and should not be cached
        raise NotImplementedError


class UniqueSubscribers(Query):
    """
    Class representing the set of all unique subscribers in our interactions
    table.

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    hours : tuple of ints, default 'all'
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    table : str, default 'all'
        Table on which to perform the query. By default it will look
        at ALL tables, which are any tables with subscriber information
        in them, specified via subscriber_tables in flowmachine.yml. Otherwise
        you need to specify a full table (with a schema) such as
        'events.calls'.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Notes
    -----
    * A date without a hours and mins will be interpreted as midnight of
      that day, so to get data within a single day pass '2016-01-01',
      '2016-01-02'.

    * Use 24 hr format!

    * Will collect only onnet callers.

    Examples
    --------
    >>> UU = UniqueSubscribers('2016-01-01 13:30:30',
                         '2016-01-02 16:25:00')
    >>> UU.as_set()
    {'038OVABN11Ak4W5P',
     '09NrjaNNvDanD8pk',
     '0DB8zw67E9mZAPK2',
     ...}

    """

    def __init__(
        self,
        start,
        stop,
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        **kwargs,
    ):
        """


        """

        self.start = start
        self.stop = stop
        self.hours = hours
        self.tables = table
        self.subscriber_identifier = subscriber_identifier
        cols = [self.subscriber_identifier]
        self.unioned = EventsTablesUnion(
            self.start,
            self.stop,
            columns=cols,
            tables=self.tables,
            subscriber_identifier=self.subscriber_identifier,
            **kwargs,
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.unioned.column_names

    def _make_query(self):
        return f"SELECT DISTINCT unioned.subscriber FROM ({self.unioned.get_query()}) unioned"

    def as_set(self):
        """
        Returns all unique subscribers as a set.
        """
        return {u[0] for u in self.connection.fetch(self.get_query())}


SubsetDates = EventTableSubset  # Backwards compatibility for unpicking queries from db


class SubscriberLocationSubset(Query):

    """
    Query to get a subset of users who have made min_calls
    number of calls within a given region during
    period of time from start to stop.

    Parameters
    ----------
    start : datetime
        Start time to filter query.
    stop : datetime
        Stop time to filter query.
    geoms : flowmachine.Query
        An object of type
    level : str, default 'admin3'
        Levels can be one of:
            'cell':
                The identifier as it is found in the CDR itself
            'versioned-cell':
                The identifier as found in the CDR combined with the version from
                the cells table.
            'versioned-site':
                The ID found in the sites table, coupled with the version
                number.
            'polygon':
                A custom set of polygons that live in the database. In which
                case you can pass the parameters column_name, which is the column
                you want to return after the join, and table_name, the table where
                the polygons reside (with the schema), and additionally geom_col
                which is the column with the geometry information (will default to
                'geom')
            'admin*':
                An admin region of interest, such as admin3. Must live in the
                database in the standard location.
            'grid':
                A square in a regular grid, in addition pass size to
                determine the size of the polygon.
    min_calls : int
        minimum number of calls a user must have made within a
    name_col : str
        Name of column with name associated to geometry
    geom_col : str
        Name of column containing geometry
    direction : {'in', 'out', 'both'}, default 'both'
        Whether to consider calls made, received, or both. Defaults to 'both'.

    Examples
    --------
    >>> sls = SubscriberLocationSubset("2016-01-01", "2016-01-07", min_calls=3,
        direction="both", level="admin3")

    >>> sls.head()
          subscriber     name
    038OVABN11Ak4W5P    Dolpa
    038OVABN11Ak4W5P     Mugu
    09NrjaNNvDanD8pk    Banke
    0ayZGYEQrqYlKw6g    Dolpa
    0DB8zw67E9mZAPK2  Baglung
    ...
    """

    def __init__(
        self,
        start,
        stop,
        min_calls,
        subscriber_identifier="msisdn",
        direction="both",
        level="admin3",
        column_name=None,
        **kwargs,
    ):

        from ...features import PerLocationSubscriberCallDurations

        self.start = start
        self.stop = stop
        self.min_calls = min_calls
        self.subscriber_identifier = subscriber_identifier
        self.direction = direction
        self.level = level
        self.column_name = column_name

        self.pslds = PerLocationSubscriberCallDurations(
            start=self.start,
            stop=self.stop,
            subscriber_identifier=self.subscriber_identifier,
            direction=self.direction,
            level=self.level,
            statistic="count",
            column_name=self.column_name,
            **kwargs,
        )

        self.pslds_subset = self.pslds.numeric_subset(
            "duration_count", low=self.min_calls, high=inf
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + get_columns_for_level(self.level, self.column_name)

    def _make_query(self):

        loc_cols = ", ".join(get_columns_for_level(self.level, self.column_name))

        sql = f"""
        SELECT
            subscriber,
            {loc_cols}
        FROM
            ({self.pslds_subset.get_query()}) _
        """

        return sql
