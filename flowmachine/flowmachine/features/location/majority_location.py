from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.utilities.subscriber_locations import BaseLocation


class MajorityLocation(BaseLocation, Query):
    def __init__(
        self,
        subscriber_location_weights: Query,
        weight_column,
        include_unlocatable=False,
    ):
        if weight_column not in subscriber_location_weights.column_names:
            raise ValueError("weight_column must exist in subscriber_subset")
        self.subscriber_location_weights = subscriber_location_weights
        self.weight_column = weight_column
        self.include_unlocatable = include_unlocatable
        self.spatial_unit = subscriber_location_weights.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        # This should be a reduction, so no new columns
        return [
            "subscriber"
        ] + self.subscriber_location_weights.spatial_unit.location_id_columns

    def _make_query(self):
        loc_id = ",".join(
            self.subscriber_location_weights.spatial_unit.location_id_columns
        )
        sql = f"""
WITH subscriber_subset AS (
    {self.subscriber_location_weights.get_query()}
), summed_weights AS (
    SELECT subscriber, sum({self.weight_column}) AS total_weight
    FROM subscriber_subset
    GROUP BY subscriber
), seen_subs AS (
    SELECT subscriber, {loc_id}
    FROM summed_weights JOIN subscriber_subset USING(subscriber)
    WHERE {self.weight_column} > total_weight/2.0
)
"""

        if self.include_unlocatable:
            sql += f"""
SELECT subscriber, seen_subs.{loc_id}
FROM seen_subs RIGHT OUTER JOIN summed_weights USING(subscriber)
            """
        else:
            sql += """SELECT * FROM seen_subs"""

        return sql
