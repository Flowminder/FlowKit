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

        if isinstance(self.bins, int):
            if isinstance(self.ranges, tuple) and not None:
                max_range = max(self.ranges)
                min_range = min(self.ranges)
                sql = f""" 
                    SELECT
                        count(value) as value,
                        width_bucket(foo.value::numeric,{max_range},{min_range},{self.bins}) as bin_edges
                    FROM(
                        select value from ({self.locations.get_query()}) AS to_agg
                        where value between {min_range} and {max_range}
                        ) as foo
                    group by bin_edges
                    """
            elif self.ranges is None:
                max_range = f"""select max(value) from ({self.locations.get_query()}) as to_agg """
                min_range = f""" select min(value) from ({self.locations.get_query()}) as to_agg """
                sql = f""" 
                    SELECT
                        count(value) as value,
                        width_bucket(foo.value::numeric,({max_range}),({min_range}),{self.bins}) as bin_edges
                    FROM(
                        select value from ({self.locations.get_query()}) AS to_agg
                        where value between ({min_range}) and ({max_range})
                        ) as foo
                    group by bin_edges
                    """
            else:
                raise ValueError("Range should be tuple of two values")
        elif isinstance(self.bins, list):

            sql = f"""
                select count(value) as value, width_bucket(value::numeric,Array{self.bins}::numeric[]) as bin_edges 
                from ({self.locations.get_query()}) AS to_agg
                group by bin_edges
                """
        else:
            raise ValueError("Bins should be integer or list of integer values.")

        return sql
