# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.utilities.subscriber_locations import BaseLocation
from flowmachine.core.errors import InvalidSpatialUnitError


class MajorityLocation(BaseLocation, Query):
    """
    A class for producing a list of subscribers along with the location (derived from `spatial_unit')
    that they visited more than half the time. Takes a query that includes a 'subscribers' column,
    a 'spatial_unit' attribute and a column to be used as weighting for locations (`location_count` for example)

    Parameters
    ----------
    subscriber_location_weights: Query
        The query object containing subscribers, locations, and weights.
    weight_column: str
        The column, when summed, that will produce the count used to threshold the majority
    minimum_total_weight: int, default 0
        If the summed weight for a subscriber is less than `minimum_total_weight`,
        that subscriber will only be assigned a location with weight greater than `minimum_total_weight/2`.
        This is useful if, for example, `subscriber_location_weights` is a count of the number of days
        a location was a subscriber's daily location over one week - if a subscriber was not active every day,
        their total weight would be less than 7, which would lower the threshold for a majority.
        Setting `minimum_total_weight=7` in this case ensures that a subscriber must have the same
        daily location on a majority of _all_ days during the week, not just a majority of their _active_ days.
    include_unlocatable: bool, default False
        If `True`, returns every unique subscriber in the `subscriber_location_weights` query, with
        the location column as `NULL` if no majority is reached.
        If `False`, returns only subscribers that have achieved a majority location

    Notes
    -----
    Any rows where weight < 0 in the `subscriber_location_weights` query will be dropped.
    This is necessary to ensure the query can return at most one location per subscriber.
    """

    def __init__(
        self,
        *,
        subscriber_location_weights: Query,
        weight_column: str,
        minimum_total_weight: int = 0,
        include_unlocatable: bool = False,
    ):
        if "subscriber" not in subscriber_location_weights.column_names:
            raise ValueError("`subscriber` not in subscriber_location_weights query")
        if weight_column not in subscriber_location_weights.column_names:
            raise ValueError("weight_column must exist in subscriber_subset")
        if not hasattr(subscriber_location_weights, "spatial_unit"):
            raise InvalidSpatialUnitError(
                "subscriber_location_weights query needs a spatial_unit attribute"
            )
        if minimum_total_weight < 0:
            raise ValueError("minimum_total_weight cannot be negative")

        self.subscriber_location_weights = subscriber_location_weights
        self.weight_column = weight_column
        self.minimum_total_weight = minimum_total_weight
        self.include_unlocatable = include_unlocatable
        self.spatial_unit = subscriber_location_weights.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns

    def _make_query(self):
        loc_id_columns_string = ",".join(self.spatial_unit.location_id_columns)
        sql = f"""
WITH subscriber_subset AS (
    {self.subscriber_location_weights.get_query()}
), summed_weights AS (
    SELECT subscriber, greatest(sum({self.weight_column}), {self.minimum_total_weight}) AS total_weight
    FROM subscriber_subset
    WHERE {self.weight_column} >= 0
    GROUP BY subscriber
), seen_subs AS (
    SELECT subscriber, {loc_id_columns_string}
    FROM summed_weights JOIN subscriber_subset USING(subscriber)
    WHERE {self.weight_column} > total_weight/2.0
)
"""

        if self.include_unlocatable:
            sql += f"""
SELECT subscriber, seen_subs.{loc_id_columns_string}
FROM seen_subs RIGHT OUTER JOIN summed_weights USING(subscriber)
            """
        else:
            sql += f"""SELECT subscriber, {loc_id_columns_string} FROM seen_subs"""

        return sql
