# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List, Union

from flowmachine.core import Query
from flowmachine.features.utilities.subscriber_locations import BaseLocation
from flowmachine.core.errors import InvalidSpatialUnitError


class MajorityLocation(BaseLocation, Query):
    """
    A query for producing a list of subscribers along with the location that they visited
    more than half the time. Takes a 'subscriber location weights' query that includes a 'subscribers' column,
    location ID column(s) and a column to be used as weighting for locations (e.g. a `LocationVisits` query).

    A subscriber will only be assigned a location if that location represents more than half
    of the total weight for that subscriber. This means that each subscriber can be assigned at most one location.

    Parameters
    ----------
    subscriber_location_weights : Query
        The query object containing subscribers, locations, and weights.
    weight_column : str
        The column, when summed, that will produce the count used to threshold the majority
    minimum_total_weight : float, default 0
        If the summed weight for a subscriber is less than `minimum_total_weight`,
        that subscriber will only be assigned a location with weight greater than `minimum_total_weight/2`.
        This is useful if, for example, `subscriber_location_weights` is a count of the number of days
        a location was a subscriber's daily location over one week - if a subscriber was not active every day,
        their total weight would be less than 7, which would lower the threshold for a majority.
        Setting `minimum_total_weight=7` in this case ensures that a subscriber must have the same
        daily location on a majority of _all_ days during the week, not just a majority of their _active_ days.

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
        minimum_total_weight: float = 0.0,
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
        self.spatial_unit = subscriber_location_weights.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns

    def _make_query(self):
        loc_id_columns_string = ",".join(self.spatial_unit.location_id_columns)
        sql = f"""
        WITH summed_weights AS (
            SELECT subscriber,
                   greatest(sum({self.weight_column}), {self.minimum_total_weight}) AS total_weight
            FROM ({self.subscriber_location_weights.get_query()}) subscriber_location_weights
            WHERE {self.weight_column} >= 0
            GROUP BY subscriber
        )
        SELECT subscriber, {loc_id_columns_string}
        FROM summed_weights
        INNER JOIN ({self.subscriber_location_weights.get_query()}) subscriber_location_weights
        USING (subscriber)
        WHERE {self.weight_column} > total_weight/2.0
        """

        return sql


class MajorityLocationWithUnlocatable(BaseLocation, Query):
    """
    A query for producing a list of subscribers along with the location that they visited
    more than half the time. Similar to MajorityLocation, except that subscribers with
    no majority location will be included in the query result (with `NULL` location).

    Parameters
    ----------
    majority_location : MajorityLocation
        MajorityLocation query whose result will be augmented with unlocatable subscribers
    """

    def __init__(self, *, majority_location: MajorityLocation):
        self.majority_location = majority_location
        self.spatial_unit = majority_location.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.majority_location.column_names

    def _make_query(self):
        columns_string = ",".join(self.majority_location.column_names)
        sql = f"""
        WITH all_subscribers AS (
            SELECT subscriber
            FROM ({self.majority_location.subscriber_location_weights.get_query()}) subscriber_location_weights
            GROUP BY subscriber
        )
        SELECT {columns_string}
        FROM ({self.majority_location.get_query()}) majority_locations
        RIGHT JOIN all_subscribers
        USING (subscriber)
        """
        return sql


def majority_location(
    *,
    subscriber_location_weights: Query,
    weight_column: str,
    minimum_total_weight: float = 0.0,
    include_unlocatable: bool = False,
) -> Union[MajorityLocation, MajorityLocationWithUnlocatable]:
    """
    A query for producing a list of subscribers along with the location that they visited
    more than half the time. Takes a 'subscriber location weights' query that includes a 'subscribers' column,
    location ID column(s) and a column to be used as weighting for locations (e.g. a `LocationVisits` query).

    A subscriber will only be assigned a location if that location represents more than half
    of the total weight for that subscriber. This means that each subscriber can be assigned at most one location.
    Subscribers for whom there is no single location with an outright majority will either be excluded from the
    query result (if `include_unlocatable==False`), or included in the result with `NULL` value in the location ID
    column(s) (if `include_unlocatable==True`).

    Parameters
    ----------
    subscriber_location_weights : Query
        The query object containing subscribers, locations, and weights.
    weight_column : str
        The column in `subscriber_location_weights`, when summed, that will produce the count used to threshold the majority
    minimum_total_weight : float, default 0
        If the summed weight for a subscriber is less than `minimum_total_weight`,
        that subscriber will only be assigned a location with weight greater than `minimum_total_weight/2`.
        This is useful if, for example, `subscriber_location_weights` is a count of the number of days
        a location was a subscriber's daily location over one week - if a subscriber was not active every day,
        their total weight would be less than 7, which would lower the threshold for a majority.
        Setting `minimum_total_weight=7` in this case ensures that a subscriber must have the same
        daily location on a majority of _all_ days during the week, not just a majority of their _active_ days.
    include_unlocatable : bool, default False
        If `True`, returns every unique subscriber in the `subscriber_location_weights` query, with
        the location column(s) as `NULL` if no majority is reached.
        If `False`, returns only subscribers that have achieved a majority location

    Returns
    -------
    MajorityLocation or MajorityLocationWithUnlocatable
        Majority location query object

    Notes
    -----
    Any rows where weight < 0 in the `subscriber_location_weights` query will be dropped.
    This is necessary to ensure the query can return at most one location per subscriber.
    """
    ml = MajorityLocation(
        subscriber_location_weights=subscriber_location_weights,
        weight_column=weight_column,
        minimum_total_weight=minimum_total_weight,
    )
    if include_unlocatable:
        return MajorityLocationWithUnlocatable(majority_location=ml)
    else:
        return ml
