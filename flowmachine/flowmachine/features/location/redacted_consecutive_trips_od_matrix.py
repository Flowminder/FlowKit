# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core import Query
from flowmachine.features.location.consecutive_trips_od_matrix import (
    ConsecutiveTripsODMatrix,
)
from flowmachine.features.location.flows import FlowLike
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedConsecutiveTripsODMatrix(RedactedLocationMetric, FlowLike, Query):
    """
    An object which represents a count of consecutive visits between locations.

    Parameters
    ----------
    consecutive_trips_od_matrix : ConsecutiveTripsODMatrix
        An unredacted consecutive trips object
    """

    def __init__(self, *, consecutive_trips_od_matrix: ConsecutiveTripsODMatrix):

        self.redaction_target = consecutive_trips_od_matrix
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = consecutive_trips_od_matrix.spatial_unit
        super().__init__()
