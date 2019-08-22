# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import singledispatch

from ...core.query import Query
from itertools import count
from typing import List, Tuple, Optional, Union


@singledispatch
def _get_bins_clause(bins: int) -> Tuple[int, str]:
    return (
        bins,
        f"""
        SELECT v, 
            (SELECT lower FROM bounds)+((v-1)*(SELECT (upper-lower)/({bins}) FROM bounds)) as lower, 
            (SELECT lower FROM bounds)+((v)*(SELECT (upper-lower)/({bins}) FROM bounds)) as upper
                        FROM generate_series(
                            1, {bins}) as v
        """,
    )


@_get_bins_clause.register
def _(bins: list) -> Tuple[int, str]:
    return (
        len(bins),
        f"""
                SELECT * FROM (VALUES {",".join(f"({i}, {low}, {high})" for i, low, high in zip(count(), bins[:-1], bins[1:]))}) as t(v, lower, upper)
                """,
    )


@singledispatch
def _get_bounds_clause(bounds, value_column: str, metric: Query) -> str:
    return f"""
        SELECT max({value_column})::numeric as upper, 
                min({value_column})::numeric as lower 
            FROM ({metric.get_query()}) AS to_agg
        """


@_get_bounds_clause.register
def _(bounds: tuple, value_column: str, metric: Query) -> str:
    return f"""
            SELECT {max(bounds)}::numeric as upper, 
                   {min(bounds)}::numeric  as lower 
             """


class HistogramAggregation(Query):
    """
    Compute the histogram of another query.

    Parameters
    ----------
    metric : Query
        Query to build histogram over
    bins : int, or list of float
        Either an integer number of equally spaced bins, or a list of bin edges
    range : tuple of float, default None
        Optionally supply inclusive lower and upper bounds to build the histogram over. By default, the
        histogram will cover the whole range of the data.
    """

    def __init__(
        self,
        *,
        metric: "Query",
        bins: Union[List[float], int],
        range: Optional[Tuple[float, float]] = None,
        value_column: str = "value",
    ) -> None:

        self.metric = metric
        self.bins = bins
        self.range = range
        self.value_column = value_column
        if self.value_column not in self.metric.column_names:
            raise ValueError(
                f"'{self.value_column}' is not a column in this query. Must be one of '{self.metric.column_names}'"
            )
        if not isinstance(self.bins, (int, list)):
            raise ValueError("Bins should be an integer or list of numeric values.")
        if not isinstance(self.range, tuple) and None:
            raise ValueError("Range should be tuple of two values")

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["value", "lower_edge", "upper_edge"]

    def _make_query(self):

        num_bins, bins_sql = _get_bins_clause(self.bins)
        bounds_sql = _get_bounds_clause(self.range, self.value_column, self.metric)

        return f"""
        WITH bounds AS ({bounds_sql}),
            
            breaks AS (
            SELECT
                numrange(lower, upper, CASE WHEN v={num_bins} THEN '[]' ELSE '[)' END) as bin
            FROM ({bins_sql}) as b
            )
             
        SELECT count(*) as value, 
            lower(bin) as lower_edge,
            upper(bin) as upper_edge 
        FROM breaks
         LEFT JOIN ({self.metric.get_query()}) as to_agg
         ON bin @> to_agg.{self.value_column}::numeric
        GROUP BY bin 
        ORDER BY bin ASC
        """
