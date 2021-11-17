# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List, Optional, Tuple

from flowmachine.utils import standardise_date

"""
Classes for determining elementary location elements.
These are used for creating base objects that are
later used for computing subscriber features.

"""


from .events_tables_union import EventsTablesUnion
from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from flowmachine.features.location.spatial_aggregate import SpatialAggregate

from flowmachine.core.query import Query
from flowmachine.core import location_joined_query, make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from flowmachine.core.errors import MissingColumnsError

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class SubscriberLocations(Query):
    """
    Class representing all the locations for which a subscriber has been found.
    Can be at the level of a tower, lon-lat, or an admin unit.

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default cell
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    hours : tuple of ints, default None
        subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon. If 'all' it will pull together all of the tables
        specified as flowmachine.yml under 'location_tables'
    subscriber_identifier : str, default 'msisdn'
        whether to identify a subscriber by either msisdn, imei or imsi.
    ignore_nulls : bool, default True
        ignores those values that are null. Sometime data appears for which
        the cell is null. If set to true this will ignore those lines. If false
        these lines with null cells should still be present, although they contain
        no information on the subscribers location, they still tell us that the subscriber made
        a call at that time.

    Notes
    -----
    * A date without a hours and mins will be interpreted as
      midnight of that day, so to get data within a single day
      pass (e.g.) '2016-01-01', '2016-01-02'.

    * Use 24 hr format!

    Examples
    --------
    >>> subscriber_locs = SubscriberLocations('2016-01-01 13:30:30',
                               '2016-01-02 16:25:00')
    >>> subscriber_locs.head()
     subscriber                 time    cell
    subscriberA  2016-01-01 12:42:11  233241
    subscriberA  2016-01-01 12:52:11  234111
    subscriberB  2016-01-01 12:52:16  234111
    ...

    """

    def __init__(
        self,
        start,
        stop,
        *,
        spatial_unit: AnySpatialUnit = make_spatial_unit("cell"),
        hours: Optional[Tuple[int, int]] = None,
        table="all",
        subscriber_identifier="msisdn",
        ignore_nulls=True,
        subscriber_subset=None,
    ):

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.spatial_unit = spatial_unit
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.ignore_nulls = ignore_nulls

        self.tables = table
        cols = [self.subscriber_identifier, "datetime", "location_id"]
        self.unioned = location_joined_query(
            EventsTablesUnion(
                self.start,
                self.stop,
                columns=cols,
                tables=self.table,
                hours=self.hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=self.subscriber_identifier,
            ),
            spatial_unit=self.spatial_unit,
            time_col="datetime",
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "time"] + self.spatial_unit.location_id_columns

    def _make_query(self):

        if self.ignore_nulls:
            where_clause = "WHERE location_id IS NOT NULL AND location_id !=''"
        else:
            where_clause = ""

        location_cols = ", ".join(self.spatial_unit.location_id_columns)

        sql = f"""
                SELECT
                    subscriber, datetime as time, {location_cols}
                FROM
                    ({self.unioned.get_query()}) AS foo
                {where_clause}
                """
        return sql

    @property
    def fully_qualified_table_name(self):
        # Cost of cache creation for subscriber locations outweighs benefits
        raise NotImplementedError


class BaseLocation:
    """
    Mixin for all daily and home location methods.
    Provides the method to aggregate spatially.
    """

    def aggregate(self):
        """
        Aggregate to the spatial level returning a query object
        that represents the location, and the total counts of
        subscribers.
        """
        return SpatialAggregate(locations=self)

    def join_aggregate(self, metric, method="avg"):
        """
        Join with a metric representing object and aggregate
        spatially.

        Parameters
        ----------
        metric : Query
        method : {"avg", "max", "min", "median", "mode", "stddev", "variance"}

        Returns
        -------
        JoinedSpatialAggregate
        """

        return JoinedSpatialAggregate(metric=metric, locations=self, method=method)

    def __getitem__(self, item):

        return self.subset(col="subscriber", subset=item)
