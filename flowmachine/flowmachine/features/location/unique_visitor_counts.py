# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.unique_subscriber_counts import (
    UniqueSubscriberCounts,
)
from flowmachine.features.location.active_at_reference_location_counts import (
    ActiveAtReferenceLocationCounts,
)


class UniqueVisitorCounts(GeoDataMixin, Query):
    def __init__(
        self,
        active_at_reference_location_counts: ActiveAtReferenceLocationCounts,
        unique_subscriber_counts: UniqueSubscriberCounts,
    ):
        """
        A count by location of how many unique visitors each location had.

        Parameters
        ----------
        active_at_reference_location_counts : ActiveAtReferenceLocationCounts
            Subscribers who were active at their reference location
        unique_subscriber_counts : UniqueSubscriberCounts
            A count of all unique subscribers active in the location
        """
        self.active_at_reference_location_counts = active_at_reference_location_counts
        self.unique_subscriber_counts = unique_subscriber_counts
        self.spatial_unit = unique_subscriber_counts.spatial_unit
        if self.spatial_unit != self.active_at_reference_location_counts.spatial_unit:
            raise ValueError("Spatial unit mismatch.")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return [
            *self.spatial_unit.location_id_columns,
            "value",
        ]

    def _make_query(self):
        location_columns = ",".join(self.spatial_unit.location_id_columns)
        sql = f"""
        SELECT {location_columns}, us.value - ref.value as value
        FROM
        ({self.active_at_reference_location_counts.get_query()}) ref
        LEFT JOIN
         ({self.unique_subscriber_counts.get_query()}) us
        USING ({location_columns})
        """
        return sql
