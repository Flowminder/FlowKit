# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List, Optional, Tuple
from flowmachine.core import Query, make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from ..utilities.subscriber_locations import BaseLocation, SubscriberLocations
from flowmachine.utils import standardise_date


class VisitedMostDays(BaseLocation, Query):
    """
    Another type of reference location which localises subscribers to the
    location they visited on most days from a span of days, and breaks ties
    according to the number of events they had at each location.

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    hours : tuple of int, default 'all'
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon. If None it will use all tables that contain
        location data.
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
    """

    def __init__(
        self,
        start_date,
        end_date,
        spatial_unit: Optional[AnySpatialUnit] = None,
        hours: Optional[Tuple[int, int]] = None,
        table: Optional[List[str]] = None,
        subscriber_identifier: str = "msisdn",
        *,
        ignore_nulls: bool = True,
        subscriber_subset: Optional["Query"] = None,
    ):
        self.start_date = standardise_date(start_date)
        self.end_date = standardise_date(end_date)
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=1)
        else:
            self.spatial_unit = spatial_unit
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.subscriber_locs = SubscriberLocations(
            start=self.start_date,
            stop=self.end_date,
            spatial_unit=self.spatial_unit,
            hours=self.hours,
            table=self.table,
            subscriber_identifier=self.subscriber_identifier,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns

    def _make_query(self):
        """
        Default query method implemented in the metaclass Query().
        """

        relevant_columns = ", ".join(self.spatial_unit.location_id_columns)

        # Create a table which has the total times each subscriber visited
        # each location
        times_visited = f"""
        SELECT 
            subscriber_locs.subscriber, 
            {relevant_columns}, 
            time::date AS date_visited, 
            count(*) AS total
        FROM ({self.subscriber_locs.get_query()}) AS subscriber_locs
        GROUP BY subscriber_locs.subscriber, {relevant_columns}, time::date
        """

        sql = f"""
        SELECT 
            ranked.subscriber, 
            {relevant_columns}
        FROM
            (
                SELECT
                    agg.subscriber, {relevant_columns},
                    row_number() OVER (
                        PARTITION BY agg.subscriber
                        ORDER BY
                            agg.num_days_visited DESC,
                            agg.num_events DESC,
                            RANDOM()
                    ) AS rank
                FROM (
                    SELECT
                        subscriber, {relevant_columns},
                        COUNT(date_visited) AS num_days_visited,
                        SUM(total) AS num_events
                    FROM ({times_visited}) AS times_visited
                    GROUP BY subscriber, {relevant_columns}
                ) AS agg
            ) AS ranked
        WHERE rank = 1
        """

        return sql
