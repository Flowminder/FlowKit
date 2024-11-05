# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import List

from flowmachine.core import Query
from flowmachine.features.utilities.validators import (
    valid_median_window,
    validate_column_present,
)


class IterativeMedianFilter(Query):
    """
    Applies an iterative median filter

    Parameters
    ----------
    query_to_filter : Query
        Query to apply iterated median filter to.
    filter_window_size : int
        Size of filter window - must be odd, positive and more than 1.
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
        self.filter_window_size = valid_median_window(
            "filter_window_size", filter_window_size
        )
        self.column_to_filter = validate_column_present(
            "column_to_filter", column_to_filter, query_to_filter
        )
        self.partition_column = validate_column_present(
            "partition_column", partition_column, query_to_filter
        )

        self.order_column = validate_column_present(
            "order_column", order_column, query_to_filter
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
