# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.subscriber_locations import BaseLocation
from flowmachine.core.errors import InvalidSpatialUnitError

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}


class SubscriberStayLengths(SubscriberFeature):
    """
    Calculate stats on the lengths of a subscriber's stays at the same
    location. From the provided sequence of reference locations, stay lengths
    are calculated as the number of consecutive locations that are the same
    (e.g. a subscriber with locations [A, A, B, C, C, C, A, D, D] would have
    stay lengths [2, 1, 3, 1, 2]), and the specified statistic is calculated
    over all of a subscriber's stay lengths.

    Parameters
    ----------
    locations : list of BaseLocation
        List of reference location queries, each returning a single location
        per subscriber (or NULL location for subscribers that are active but
        unlocatable). The list is assumed to be sorted into ascending
        chronological order.
    statistic :  {'count', 'sum', 'avg', 'max', 'min', 'median', 'stddev', 'variance'}, default 'max'
        Aggregation statistic over the stay lengths. Defaults to max.
    """

    def __init__(self, *, locations: List[BaseLocation], statistic: str = "max"):
        self.locations = locations
        if len(set(l.spatial_unit for l in self.locations)) > 1:
            raise InvalidSpatialUnitError(
                "SubscriberStayLengths requires all input locations to have the same spatial unit"
            )
        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                f"'{self.statistic}' is not a valid statistic. Use one of {valid_stats}"
            )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        loc_cols_string = ", ".join(self.locations[0].spatial_unit.location_id_columns)

        # Note: in some ways it would be nicer to use a DayTrajectories query instead of a list of location queries,
        # but DayTrajectories only works with queries that have 'start' and 'stop' attributes
        locations_union = " UNION ALL ".join(
            f"SELECT subscriber, {loc_cols_string}, {i} AS ordinal FROM ({loc.get_query()}) _"
            for i, loc in enumerate(self.locations)
        )

        # Find stay lengths using gaps-and-islands approach
        sql = f"""
        SELECT subscriber, {self.statistic}(stay_length) AS value
        FROM (
            SELECT subscriber, count(*) AS stay_length
            FROM (
                SELECT
                    subscriber,
                    {loc_cols_string},
                    ordinal - dense_rank() OVER (
                        PARTITION BY subscriber, {loc_cols_string}
                        ORDER BY ordinal
                    ) AS stay_id
                FROM ({locations_union}) AS locations_union
                WHERE NOT (({loc_cols_string}) IS NULL)
            ) locations_with_stay_id
            GROUP BY subscriber, {loc_cols_string}, stay_id
        ) stay_lengths
        GROUP BY subscriber
        """

        return sql
