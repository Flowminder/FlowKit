# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.core import Query
from flowmachine.features.location.flows import FlowLike
from flowmachine.features.location.meaningful_locations_od import MeaningfulLocationsOD
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedMeaningfulLocationsOD(RedactedLocationMetric, FlowLike, Query):
    """
    Calculates an OD matrix aggregated to a spatial unit between two individual
    level meaningful locations. For subscribers with more than one cluster of either
    label, counts are weight to `1/(n_clusters_label_a*n_clusters_label_b)`.

    Returns results only for region pairs with 16 or more subscribers.

    Parameters
    ----------
    meaningful_locations_od : MeaningfulLocationsOD
    """

    def __init__(self, *, meaningful_locations_od: MeaningfulLocationsOD):

        self.redaction_target = meaningful_locations_od
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = meaningful_locations_od.spatial_unit
        super().__init__()
