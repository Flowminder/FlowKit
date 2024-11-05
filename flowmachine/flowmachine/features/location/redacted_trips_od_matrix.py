# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core import Query
from flowmachine.features.location.trips_od_matrix import TripsODMatrix
from flowmachine.features.location.flows import FlowLike
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedTripsODMatrix(RedactedLocationMetric, FlowLike, Query):
    """
    An object which represents a count of  visits between locations.

    Parameters
    ----------
    trips : TripsODMatrix
        An unredacted trips object
    """

    def __init__(self, *, trips: TripsODMatrix):
        self.redaction_target = trips
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = trips.spatial_unit
        super().__init__()
