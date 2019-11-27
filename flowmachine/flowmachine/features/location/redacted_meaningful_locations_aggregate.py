# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.core import Query
from flowmachine.features.location.meaningful_locations_aggregate import (
    MeaningfulLocationsAggregate,
)
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedMeaningfulLocationsAggregate(RedactedLocationMetric, Query):
    """
    Aggregates an individual-level meaningful location to a spatial unit by assigning
    subscribers with clusters in that unit to it. For subscribers with more than one cluster,
    assigns `1/n_clusters` to each spatial unit that the cluster lies in. Returns results only
    for regions with 16 or more subscribers.

    Parameters
    ----------
    meaningful_locations_aggregate : MeaningfulLocationsAggregate
    """

    def __init__(self, *, meaningful_locations_aggregate: MeaningfulLocationsAggregate):

        self.redaction_target = meaningful_locations_aggregate
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = meaningful_locations_aggregate.spatial_unit
        super().__init__()
