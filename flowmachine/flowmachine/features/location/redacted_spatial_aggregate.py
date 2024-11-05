# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)
from flowmachine.features.location.spatial_aggregate import SpatialAggregate


class RedactedSpatialAggregate(RedactedLocationMetric, GeoDataMixin, Query):
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
        self.redaction_target = spatial_aggregate
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = spatial_aggregate.spatial_unit
        super().__init__()
