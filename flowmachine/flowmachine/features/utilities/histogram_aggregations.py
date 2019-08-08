# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from ...core.query import Query
from typing import List


class HistogramAggregation(Query):
    """ 

    """

    def __init__(self, *, locations, bins, ranges: tuple = None):

        self.locations = locations
        self.bins = bins
        self.ranges = ranges
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["value", "bin_edges"]

    def _make_query(self):
        if isinstance(self.ranges, tuple) and not None:
            max_range = max(self.ranges)
            min_range = min(self.ranges)
        elif self.ranges is None:
            max_range = (
                f"""SELECT MAX(value) FROM ({self.locations.get_query()}) AS to_agg """
            )
            min_range = (
                f""" SELECT MIN(value) FROM ({self.locations.get_query()}) AS to_agg """
            )
        else:
            raise ValueError("Range should be tuple of two values")

        filter_values = f""" SELECT value FROM ({self.locations.get_query()}) AS to_agg
                        WHERE value BETWEEN ({min_range}) AND ({max_range})"""
        if isinstance(self.bins, int):
            sql = f""" 
                SELECT
                    count(value) AS value,
                    width_bucket(foo.value::numeric,({max_range}),({min_range}),{self.bins}) AS bin_edges
                FROM ({filter_values}) AS foo
                GROUP BY bin_edges
                """
        elif isinstance(self.bins, list):

            sql = f"""
                SELECT 
                    count(foo.value) AS value, 
                    width_bucket(foo.value::numeric,Array{self.bins}::numeric[]) AS bin_edges 
                FROM ({filter_values}) AS foo
                GROUP BY bin_edges
                """
        else:
            raise ValueError("Bins should be integer or list of integer values.")

        return sql
