# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Per subscriber time series of distances from some reference location.
"""
from typing import List

from flowmachine.core import Query
from flowmachine.features.subscriber.distance_series import DistanceSeries
from flowmachine.features.subscriber.distance_series import valid_time_buckets
from flowmachine.features.utilities.validators import valid_median_window


class ImputedDistanceSeries(Query):
    """
    Per subscriber time series of distances from some reference location, with gaps
    filled using a rolling median as used in[1]_.

    Parameters
    ----------
    distance_series : DistanceSeries
        A subscriber distance series which may contain gaps.
    window_size : int, default 3
        Number of observations to use for imputation. Must be odd, positive and greater than 1.


    References
    ----------
    .. [1] T. Li et al. https://arxiv.org/pdf/1908.02377.pdf
    """

    def __init__(self, *, distance_series: DistanceSeries, window_size: int = 3):
        self.distance_series = distance_series
        self.window_size = valid_median_window("window_size", window_size)

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.distance_series.column_names

    def _make_query(self):
        # First we need to make the series sparse
        if valid_time_buckets.index(
            self.distance_series.aggregate_by
        ) > valid_time_buckets.index(
            "hour"
        ):  # Slightly nicer to cast things which aren't timestamps to a date
            date_cast = "::date"
        else:
            date_cast = ""
        return f"""
        SELECT subscriber, datetime{date_cast}, rolling_median_impute(value, {self.window_size}) OVER(partition by subscriber order by datetime) as value FROM
        (SELECT subscriber, 
        generate_series(timestamptz '{self.distance_series.start}', '{self.distance_series.stop}'::timestamptz - interval '1' second, '1 {self.distance_series.aggregate_by}') as datetime 
            FROM ({self.distance_series.get_query()}) _ GROUP BY subscriber HAVING COUNT(value) > {self.window_size}) sparse
        LEFT JOIN
            ({self.distance_series.get_query()}) series
        USING (subscriber, datetime)
        """
