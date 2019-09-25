# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Per subscriber time series of distances from some reference location.
"""
from typing import List, Optional

from flowmachine.features import DistanceSeries
from flowmachine.features.spatial import DistanceMatrix
from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import SubscriberLocations, BaseLocation


valid_stats = {"sum", "avg", "max", "min", "median", "stddev", "variance"}
valid_time_buckets = ["second", "minute", "hour", "day", "month", "year", "century"]


class ImputedDistanceSeries(DistanceSeries):
    """
    Per subscriber time series of distances from some reference location, with gaps
    filled using a rolling median.

    Parameters
    ----------
    distance_series : DistanceSeries
        A subscriber distance series which may contain gaps.
    window_size : int, default 3
        Number of observations to use for imputation.


    """

    def __init__(self, *, distance_series: DistanceSeries, window_size: int = 3):
        self.distance_series = distance_series
        self.window_size = window_size

        super().__init__()

    def _make_query(self):
        # First we need to make the series sparse
        return f"""
        SELECT subscriber, datetime, rolling_median_impute(value, {self.window_size}) as value FROM
        (SELECT subscriber, 
        generate_series(timestamptz '{self.distance_series.start}', '{self.distance_series.stop}'::timestamptz + interval '1' day, '1 {self.distance_series.aggregate_by}') 
            FROM ({self.distance_series.get_query()}) _ GROUP BY subscriber) full
        LEFT JOIN
            ({self.distance_series.get_query()}) series
        """
