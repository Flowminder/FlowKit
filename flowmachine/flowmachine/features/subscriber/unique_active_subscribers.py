from typing import List

from flowmachine.features.subscriber.active_subscribers import ActiveSubscribers


class UniqueActiveSubscribers(ActiveSubscribers):
    @property
    def column_names(self) -> List[str]:
        return ["msisdn"]

    def _make_query(self):
        list = super()._make_query()
        sql = f"""
WITH active_subs AS (
    {list}
    )
SELECT DISTINCT msisdn
FROM active_subs
"""
        return sql
