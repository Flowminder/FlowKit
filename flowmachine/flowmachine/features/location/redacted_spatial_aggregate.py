# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.spatial_aggregate import SpatialAggregate


class RedactedSpatialAggregate(GeoDataMixin, Query):
    """
    Class representing the result of spatially aggregating
    a locations object, redacted so that results are not returned if counts are 15 or less..
    A locations object represents the
    location of multiple subscribers. This class represents the output
    of aggregating that data spatially.

    Parameters
    ----------
    locations : subscriber location query
    """

    def __init__(self, *, spatial_aggregate: SpatialAggregate):

        self.spatial_aggregate = spatial_aggregate
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = spatial_aggregate.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.spatial_aggregate.column_names

    def _make_query(self):

        aggregate_cols = ",".join(self.spatial_aggregate.columns)

        sql = f"""
        SELECT
            {aggregate_cols}
        FROM
            ({self.spatial_aggregate.get_query()}) AS agged
        WHERE agged.total > 15
        """

        return sql
