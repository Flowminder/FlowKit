from typing import List

from flowmachine.features.subscriber.active_subscribers import ActiveSubscribers


class UniqueActiveSubscribers(Query):
    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]

    def _make_query(self):

        return sql
