from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.utilities.subscriber_locations import BaseLocation


class MajorityLocation(BaseLocation, Query):
    def __init__(self, subscriber_subset: Query, weight_column):
        if weight_column not in subscriber_subset.column_names:
            raise ValueError("weight_column must exist in subscriber_subset")
        self.subscriber_subset = subscriber_subset
        self.weight_column = weight_column
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        # This should be a reduction, so no new columns
        return ["subscriber"] + self.subscriber_subset.spatial_unit.location_id_columns

    def _make_query(self):
        loc_id = ",".join(self.subscriber_subset.spatial_unit.location_id_columns)
        sql = f"""
WITH subscriber_subset AS (
    {self.subscriber_subset.get_query()}
), summed_weights AS (
    SELECT subscriber, sum({self.weight_column}) AS total_weight
    FROM subscriber_subset
    GROUP BY subscriber
)
SELECT subscriber, {loc_id}
FROM summed_weights JOIN subscriber_subset USING(subscriber)
WHERE {self.weight_column} > total_weight/2.0
"""
        return sql
