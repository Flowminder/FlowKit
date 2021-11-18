from typing import List
from flowmachine.features.subscriber.metaclasses import SubscriberFeature


class PerSubscriberAggregate(SubscriberFeature):
    def __init__(
        self,
        *,
        subscriber_query: SubscriberFeature,
        agg_column: str,
        agg_method: str = "avg",
    ):
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
