# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import List

from flowmachine.core import Query
from flowmachine.core.statistic_types import StringAggregate


class PerValueAggregate(Query):
    """
    Class representing an aggregate over values, for example a count of subscribers who
    have each value.

    Parameters
    ----------
    query : Query
        Any query with a subscriber and value column
    aggregation : StringAggregate, default StringAggregate.COUNT
        The aggregation to apply

    """

    def __init__(
        self, query: Query, aggregation: StringAggregate = StringAggregate.COUNT
    ):
        if "subscriber" not in query.column_names:
            raise ValueError(f"Query must have a subscriber column.")
        if "value" not in query.column_names:
            raise ValueError(f"Query must have a value column.")

        self.query_to_agg = query
        self.aggregation = StringAggregate(aggregation.lower())

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["grp", "value"]

    def _make_query(self):
        return f"""
            SELECT
                value as grp, {self.aggregation:subscriber} AS value
            FROM 
                ({self.query_to_agg.get_query()}) AS agg
            GROUP BY
                value
            """
