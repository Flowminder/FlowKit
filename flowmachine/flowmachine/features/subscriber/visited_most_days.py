# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List
from flowmachine.core import Query
from ..utilities.subscriber_locations import BaseLocation, SubscriberLocations


class VisitedMostDays(BaseLocation, Query):
    """
    Another type of reference location which localises subscribers to the
    location they visited on most days from a span of days, and breaks ties
    according to the number of events they had at each location.

    Parameters
    ----------
    subscriber_locations : a SubscriberLocations object, default None
    """

    def __init__(self, *, subscriber_locations: SubscriberLocations = None):
        self.subscriber_locations = subscriber_locations
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return [
            "subscriber"
        ] + self.subscriber_locations.spatial_unit.location_id_columns

    def _make_query(self):
        """
        Default query method implemented in the metaclass Query().
        """

        relevant_columns = ", ".join(
            self.subscriber_locations.spatial_unit.location_id_columns
        )

        # Create a table which has the total times each subscriber visited
        # each location
        times_visited = f"""
        SELECT 
            subscriber_locations.subscriber, 
            {relevant_columns}, 
            time::date AS date_visited, 
            count(*) AS total
        FROM ({self.subscriber_locations.get_query()}) AS subscriber_locations
        GROUP BY subscriber_locations.subscriber, {relevant_columns}, time::date
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
