# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Per subscriber time series of distances from some reference location.
"""
from typing import List, Optional

from flowmachine.features.spatial import DistanceMatrix
from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import SubscriberLocations, BaseLocation


valid_stats = {"sum", "avg", "max", "min", "median", "stddev", "variance"}
valid_time_buckets = ["second", "minute", "hour", "day", "month", "year", "century"]


class DistanceSeries(SubscriberFeature):
    """
    Per subscriber time series of distance in meters from some reference location.

    Parameters
    ----------
    subscriber_locations : SubscriberLocations
        A subscriber locations query with a lon-lat spatial unit to build the distance series against.
    reference_location : BaseLocation or None, default None
        The set of home locations from which to calculate distance at each sighting.
        If not given then distances will be from (0, 0).
    statistic : str
        the statistic to calculate one of 'sum', 'avg', 'max', 'min', 
        'median', 'stddev' or 'variance'
    time_bucket : {"second", "minute", "hour", "day", "month", "year", "century"}, default "day"
        Time bucket to calculate the statistic over.

    Examples
    --------
    >>> d = DistanceSeries(subscriber_locations=SubscriberLocations("2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")))
    >>> d.head()
             subscriber    datetime         value
    0  038OVABN11Ak4W5P  2016-01-01  9.384215e+06
    1  038OVABN11Ak4W5P  2016-01-02  9.233302e+06
    2  038OVABN11Ak4W5P  2016-01-03  9.376996e+06
    3  038OVABN11Ak4W5P  2016-01-04  9.401404e+06
    4  038OVABN11Ak4W5P  2016-01-05  9.357210e+06
    """

    def __init__(
        self,
        *,
        subscriber_locations: SubscriberLocations,
        reference_location: Optional[BaseLocation] = None,
        statistic: str = "avg",
        time_bucket: str = "day",
    ):
        subscriber_locations.spatial_unit.verify_criterion("has_geography")
        subscriber_locations.spatial_unit.verify_criterion("has_lon_lat_columns")
        self.spatial_unit = subscriber_locations.spatial_unit
        if time_bucket.lower() in valid_time_buckets:
            self.aggregate_by = time_bucket.lower()
        else:
            raise ValueError(
                f"'{time_bucket}' is not a valid value for time_bucket. Use one of {valid_time_buckets}"
            )

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )
        self.start = subscriber_locations.start
        self.stop = subscriber_locations.stop
        if reference_location is None:
            # Using 0, 0
            self.reference_location = None
            self.joined = subscriber_locations
        elif isinstance(reference_location, BaseLocation):
            if reference_location.spatial_unit != subscriber_locations.spatial_unit:
                raise ValueError(
                    "reference_location must have the same spatial unit as subscriber_locations."
                )
            self.reference_location = reference_location
            self.joined = reference_location.join(
                other=subscriber_locations,
                on_left=["subscriber"],
                left_append="_from",
                right_append="_to",
            ).join(
                DistanceMatrix(spatial_unit=self.spatial_unit),
                on_left=[
                    f"{col}_{direction}"
                    for direction in ("from", "to")
                    for col in self.spatial_unit.location_id_columns
                ],
                right_append="_dist",
                how="left outer",
            )
        else:
            raise ValueError(
                "Argument 'reference_location' should be an instance of BaseLocation class or None. "
                f"Got: {type(reference_location)}"
            )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "datetime", "value"]

    def _make_query(self):
        if self.reference_location is None:
            joined = f"""
            SELECT subscriber, time as time_to, ST_Distance(ST_Point(0, 0)::geography, ST_Point(lon, lat)::geography) as value_dist
            FROM ({self.joined.get_query()}) _
            """
        else:
            joined = self.joined.get_query()

        if valid_time_buckets.index(self.aggregate_by) > valid_time_buckets.index(
            "hour"
        ):  # Slightly nicer to cast things which aren't timestamps to a date
            date_cast = "::date"
        else:
            date_cast = ""

        sql = f"""
        SELECT 
            subscriber,
            date_trunc('{self.aggregate_by}', time_to){date_cast} as datetime,
            {self.statistic}(COALESCE(value_dist, 0) as value
        FROM 
            ({joined}) _
        GROUP BY 
            subscriber, date_trunc('{self.aggregate_by}', time_to)
        """

        return sql
