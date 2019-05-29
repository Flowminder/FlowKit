# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility classes for subsetting CDRs.

"""

from typing import List

from .event_table_subset import EventTableSubset
from .events_tables_union import EventsTablesUnion
from ...core import Query
from flowmachine.utils import get_columns_for_level

from numpy import inf

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)
valid_subscriber_identifiers = ("msisdn", "imei", "imsi")


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
        *,
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        subscriber_subset=None,
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
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=self.subscriber_identifier,
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
        *,
        min_calls,
        subscriber_identifier="msisdn",
        direction="both",
        level="admin3",
        column_name=None,
        hours="all",
        subscriber_subset=None,
        size=None,
        polygon_table=None,
        geom_col="geom",
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
            hours=hours,
            subscriber_subset=subscriber_subset,
            size=size,
            polygon_table=polygon_table,
            geom_col=geom_col,
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
