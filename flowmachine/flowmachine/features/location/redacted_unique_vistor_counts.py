# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.redacted_active_at_reference_location_counts import (
    RedactedActiveAtReferenceLocationCounts,
)
from flowmachine.features.location.redacted_unique_subscriber_counts import (
    RedactedUniqueSubscriberCounts,
)
from flowmachine.features.location.unique_visitor_counts import UniqueVisitorCounts


class RedactedUniqueVisitorCounts(GeoDataMixin, Query):
    def __init__(
        self,
        unique_visitor_counts: UniqueVisitorCounts,
    ):
        """
        A count by location of how many unique visitors each location had.
        This is the redacted variation - locations with 15 or fewer subscribers for
        either the active or unique subqueries are redacted.

        Parameters
        ----------
        unique_visitor_counts : UniqueVisitorCounts
            Visitor counts to redact
        """
        self.active_at_reference_location_counts = RedactedActiveAtReferenceLocationCounts(
            active_at_reference_location_counts=unique_visitor_counts.active_at_reference_location_counts
        )
        self.unique_subscriber_counts = RedactedUniqueSubscriberCounts(
            unique_subscriber_counts=unique_visitor_counts.unique_subscriber_counts
        )
        self.spatial_unit = unique_visitor_counts.spatial_unit
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
        INNER JOIN
         ({self.unique_subscriber_counts.get_query()}) us
        USING ({location_columns})
        WHERE us.value - ref.value > 15
        """
        return sql
