# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility classes for subsetting CDRs.

"""

from typing import List, Optional, Union, Tuple

from .direction_enum import Direction
from .event_table_subset import EventTableSubset
from .events_tables_union import EventsTablesUnion
from ...core import Query, make_spatial_unit
from ...core.context import get_db
from ...core.spatial_unit import AnySpatialUnit

from numpy import inf

import structlog

from flowmachine.utils import standardise_date

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
        start: str,
        stop: str,
        *,
        hours: Union[str, Tuple[int, int]] = "all",
        table: Union[str, List[str]] = "all",
        subscriber_identifier: str = "msisdn",
        subscriber_subset: Optional[Query] = None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
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
        return {u[0] for u in get_db().fetch(self.get_query())}


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
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    min_calls : int
        minimum number of calls a user must have made within a
    direction : {'in', 'out', 'both'} or Direction, default Direction.BOTH
        Whether to consider calls made, received, or both. Defaults to 'both'.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Examples
    --------
    >>> sls = SubscriberLocationSubset("2016-01-01", "2016-01-07", min_calls=3,
        direction="both", spatial_unit=make_spatial_unit("admin", level=3))

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
        direction: Union[str, Direction] = Direction.BOTH,
        spatial_unit: Optional[AnySpatialUnit] = None,
        hours="all",
        subscriber_subset=None,
    ):

        from ...features import PerLocationSubscriberCallDurations

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.min_calls = min_calls
        self.subscriber_identifier = subscriber_identifier
        self.direction = Direction(direction)
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=3)
        else:
            self.spatial_unit = spatial_unit

        self.pslds = PerLocationSubscriberCallDurations(
            start=self.start,
            stop=self.stop,
            subscriber_identifier=self.subscriber_identifier,
            direction=self.direction,
            spatial_unit=self.spatial_unit,
            statistic="count",
            hours=hours,
            subscriber_subset=subscriber_subset,
        )

        self.pslds_subset = self.pslds.numeric_subset(
            "value", low=self.min_calls, high=inf
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns

    def _make_query(self):

        loc_cols = ", ".join(self.spatial_unit.location_id_columns)

        sql = f"""
        SELECT
            subscriber,
            {loc_cols}
        FROM
            ({self.pslds_subset.get_query()}) _
        """

        return sql
