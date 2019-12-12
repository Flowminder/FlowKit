# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin


class SpatialAggregate(GeoDataMixin, Query):
    """
    Class representing the result of spatially aggregating
    a locations object. A locations object represents the
    location of multiple subscribers. This class represents the output
    of aggregating that data spatially.

    Parameters
    ----------
    locations : subscriber location query
    """

    def __init__(self, *, locations):

        self.locations = locations
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = locations.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):

        aggregate_cols = ",".join(self.spatial_unit.location_id_columns)

        sql = f"""
        SELECT
            {aggregate_cols},
            count(*) AS value
        FROM
            ({self.locations.get_query()}) AS to_agg
        GROUP BY
            {aggregate_cols}
        """

        return sql
