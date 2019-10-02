# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import List

from flowmachine.core import Query


class IterativeMedianFilter(Query):
    """
    Applies an iterative median filter

    Parameters
    ----------
    query_to_filter : Query
        Query to apply iterated median filter to.
    filter_window_size : int
        Size of filter window - must be odd.
    column_to_filter : str, default 'value'
        Column to apply the filter to.
    partition_column : str, default 'subscriber'
        Column to use for partitioning. May be none, in which case no partitioning is applied.
    order_column : str, default datetime
        Column to use for ordering within partitions. May be none, in which case no ordering is applied.
    """

    def __init__(
        self,
        *,
        query_to_filter: Query,
        filter_window_size: int,
        column_to_filter: str = "value",
        partition_column: str = "subscriber",
        order_column: str = "datetime",
    ):

        self.query_to_filter = query_to_filter
        self.filter_window_size = filter_window_size
        if (filter_window_size % 2) == 0:
            raise ValueError("filter_window_size must be odd.")
        self.column_to_filter = column_to_filter
        if column_to_filter not in query_to_filter.column_names:
            raise ValueError(
                f"Invalid column_to_filter: '{column_to_filter}' not in query's columns. Must be one of {query_to_filter.column_names}"
            )
        self.partition_column = partition_column
        if (
            partition_column is not None
            and partition_column not in query_to_filter.column_names
        ):
            raise ValueError(
                f"Invalid partition_column: '{partition_column}' not in query's columns. Must be one of {query_to_filter.column_names}"
            )
        self.order_column = order_column
        if (
            order_column is not None
            and order_column not in query_to_filter.column_names
        ):
            raise ValueError(
                f"Invalid order_column: '{order_column}' not in query's columns. Must be one of {query_to_filter.column_names}"
            )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return [
            name
            for name in self.query_to_filter.column_names
            if name != self.column_to_filter
        ] + [self.column_to_filter]

    def _make_query(self):
        column_names = [
            name for name in self.column_names if name != self.column_to_filter
        ]
        partition_statement = (
            f"partition by {self.partition_column}"
            if self.partition_column is not None
            else ""
        )
        cols = f"{', '.join(column_names)}," if len(column_names) > 0 else ""
        order_statement = (
            f"order by {self.order_column}" if self.order_column is not None else ""
        )
        return f"""
        SELECT {cols} iterated_median_filter({self.column_to_filter}, 
        {self.filter_window_size}) over({partition_statement} {order_statement}) as {self.column_to_filter}
        FROM
        ({self.query_to_filter.get_query()}) as to_filter
        """
