# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.core.statistic_types import Statistic


class PerSubscriberAggregate(SubscriberFeature):
    """
        Query that performs per-subscriber aggregation of a column. Returns a column
        'subscriber' containing unique subscribers and a column 'value' containing the
        aggregration.

        Parameters
        ----------
        subscriber_query: SubscriberFeature
            A query with a `subscriber` column
        agg_column: str
            The name of the column in `subscriber_query` to aggregate. Cannot be 'subscriber'.
        agg_method: Statistic, default Statistic.AVG
            The method of aggregation to perform

        Examples
        --------
        Gets the maximum call duration of each subscriber on 2016-01-01.
        >>>     per_location_query = PerLocationSubscriberCallDurations("2016-01-01", "2016-01-02")
        >>>     max_psa = PerSubscriberAggregate(
        ...         subscriber_query=per_location_query, agg_column="value", agg_method="max"
        ...     )

                   subscriber   value
        0    038OVABN11Ak4W5P  4641.0
        1    0Gl95NRLjW2aw8pW   876.0
        2    0gmvwzMAYbz5We1E  2214.0
        3    0MQ4RYeKn7lryxGa  3964.0
        4    0Ze1l70j0LNgyY4w  3368.0
        ..                ...     ...
        350  ZmPRjkQ74Xeql71V  2385.0
        351  ZQG8glazmxYa1K62  4238.0
        352  Zv4W9eak2QN1M5A7   337.0
        353  zvaOknzKbEVD2eME  2171.0
        354  ZYPxqVGLzlQy6l7n  4602.0

    [355 rows x 2 columns]

    """

    def __init__(
        self,
        *,
        subscriber_query: SubscriberFeature,
        agg_column: str,
        agg_method: Statistic = Statistic.AVG,
    ):
        if "subscriber" not in subscriber_query.column_names:
            raise ValueError("'subscriber' column not in subscriber_query")
        if agg_column not in subscriber_query.column_names:
            raise ValueError(f"'{agg_column}' column not in subscriber_query")
        if agg_column == "subscriber":
            raise ValueError(f"'agg_column' cannot be 'subscriber'")

        self.subscriber_query = subscriber_query
        self.agg_column = agg_column
        self.agg_method = Statistic(agg_method.lower())
        super(PerSubscriberAggregate, self).__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        return f"""
            SELECT subscriber, {self.agg_method:{self.agg_column}} AS value
            FROM ({self.subscriber_query.get_query()}) AS sub_table
            GROUP BY subscriber
            """
