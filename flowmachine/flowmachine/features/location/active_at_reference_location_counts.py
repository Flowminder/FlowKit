# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.subscriber.active_at_reference_location import (
    ActiveAtReferenceLocation,
)


class ActiveAtReferenceLocationCounts(GeoDataMixin, Query):
    def __init__(self, active_at_reference_location: ActiveAtReferenceLocation):
        """
        A count by location of subscribers who where active in their reference location.

        Parameters
        ----------
        active_at_reference_location : ActiveAtReferenceLocation
            Subscriber level active at reference location query
        """
        self.active_at_reference_location = active_at_reference_location
        self.spatial_unit = active_at_reference_location.spatial_unit
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
        SELECT {location_columns}, sum(value::int) as value
        FROM
        ({self.active_at_reference_location.reference_location.get_query()}) ref
        LEFT JOIN
         ({self.active_at_reference_location.get_query()}) active
        USING (subscriber)
        GROUP BY {location_columns}
        """
        return sql
