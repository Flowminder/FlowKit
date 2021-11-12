from typing import List

from flowmachine.core import Query
from flowmachine.features.utilities.subscriber_locations import BaseLocation


# One of the spatial mixins here?
class MajorityLocation(BaseLocation, Query):
    def __init__(self, subscriber_subset: Query):
        self.subscriber_subset = subscriber_subset
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        # This should be a reduction, so no new columns
        return ["subscriber", "location_id"]

    def _make_query(self):
        sql = f"""
WITH subscriber_subset AS (
    {self.subscriber_subset.get_query()}
), counts AS (
	SELECT 
		subscriber,
		location_id, 
		row_number() OVER subs - rank() OVER subs + 1 AS accum_loc_counts
	FROM subscriber_subset
	WINDOW subs AS (PARTITION BY subscriber ORDER BY location_id)
), maximums AS (
	SELECT 
		subscriber, 
		max(accum_loc_counts) as highest_loc_count,
		count(subscriber) as event_count
	FROM counts
	GROUP BY subscriber
)
SELECT subscriber, location_id
FROM 
	maximums JOIN counts USING(subscriber)
WHERE maximums.highest_loc_count > (maximums.event_count/2)
AND counts.accum_loc_counts = maximums.highest_loc_count
"""
        return sql
