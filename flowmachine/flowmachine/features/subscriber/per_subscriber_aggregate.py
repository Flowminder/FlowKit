from typing import List
from flowmachine.features.subscriber.metaclasses import SubscriberFeature

agg_methods = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}


class PerSubscriberAggregate(SubscriberFeature):
    def __init__(
        self,
        *,
        subscriber_query: SubscriberFeature,
        agg_column: str,
        agg_method: str = "avg",
    ):
        if "subscriber" not in subscriber_query.column_names:
            raise ValueError("'subscriber' column not in subscriber_query")
        if agg_column not in subscriber_query.column_names:
            raise ValueError(f"{agg_column} not in subscriber_query")
        if agg_method not in agg_methods:
            raise ValueError(f"{agg_method} not in {agg_methods}")

        self.subscriber_query = subscriber_query
        self.agg_column = agg_column
        self.agg_method = agg_method

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        sql = f"""
SELECT subscriber, {self.agg_method}({self.agg_column}) AS value
FROM ({self.subscriber_query.get_query()}) AS sub_table
GROUP BY subscriber
"""
        return sql
