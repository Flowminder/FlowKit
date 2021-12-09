# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.subscriber_locations import BaseLocation
from flowmachine.features.subscriber.subscriber_stay_lengths import (
    SubscriberStayLengths,
)
from flowmachine.core.errors import InvalidSpatialUnitError


class MobilityClassification(SubscriberFeature):
    """
    Based on subscribers' reference locations in a sequence of reference
    periods, classify each subscriber as having one of the following mobility
    types (the assigned label corresponds to the first of these criteria that
    is true for a given subscriber):

    - 'unlocated': Subscriber has a NULL location in the most recent period
    - 'irregular': Subscriber is not active in at least one of the reference
      periods
    - 'not_always_locatable': Subscriber has a NULL location in at least one of
      the reference periods
    - 'mobile': Subscriber spent fewer than 'stay_length_threshold' consecutive
      periods at any single location
    - 'stable': Subscriber spent at least 'stay_length_threshold' consecutive
      periods at the same location

    Only subscribers appearing in the result of the reference location query
    for the most recent period are included in the result of this query (i.e.
    subscribers absent from the query result can be assumed to fall into a
    sixth category: "not active in the most recent period").

    Parameters
    ----------
    locations : list of BaseLocation
        List of reference location queries, each returning a single location
        per subscriber (or NULL location for subscribers that are active but
        unlocatable). The list is assumed to be sorted into ascending
        chronological order.
    stay_length_threshold : int, default 3
        Minimum number of consecutive periods over which a subscriber's
        location must remain the same for that subscriber to be classified as
        'stable'.
    """

    def __init__(
        self, *, locations: List[BaseLocation], stay_length_threshold: int = 3
    ):
        self.locations = locations
        if len(set(l.spatial_unit for l in self.locations)) > 1:
            raise InvalidSpatialUnitError(
                "MobilityClassification requires all input locations to have the same spatial unit"
            )
        self.stay_length_threshold = int(stay_length_threshold)
        self.max_stay_lengths = SubscriberStayLengths(
            locations=self.locations,
            statistic="max",
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self) -> str:
        loc_cols_string = ", ".join(self.locations[0].spatial_unit.location_id_columns)

        # Note: in some ways it would be nicer to use a DayTrajectories query instead of a list of location queries,
        # but DayTrajectories only works with queries that have 'start' and 'stop' attributes
        locations_union = " UNION ALL ".join(
            f"SELECT subscriber, {loc_cols_string}, {i} AS ordinal FROM ({loc.get_query()}) _"
            for i, loc in enumerate(self.locations)
        )

        long_term_activity = f"""
        SELECT
            subscriber,
            count(*) < {len(self.locations)} AS sometimes_inactive,
            bool_or(({loc_cols_string}) IS NULL) AS sometimes_unlocatable
        FROM ({locations_union}) AS locations_union
        GROUP BY subscriber
        """

        sql = f"""
        SELECT
            subscriber,
            CASE
                WHEN ({loc_cols_string}) IS NULL THEN 'unlocated'
                WHEN sometimes_inactive THEN 'irregular'
                WHEN sometimes_unlocatable THEN 'not_always_locatable'
                WHEN max_stay_length.value < {self.stay_length_threshold} THEN 'mobile'
                ELSE 'stable'
            END AS value
        FROM ({self.locations[-1].get_query()}) AS most_recent_period
        LEFT JOIN ({long_term_activity}) AS long_term_activity
        USING (subscriber)
        LEFT JOIN ({self.max_stay_lengths.get_query()}) AS max_stay_length
        USING (subscriber)
        """

        return sql
