from flowmachine.core import Query
from flowmachine.features.location.flows import BaseInOutFlow
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedInOutFlow(RedactedLocationMetric, BaseInOutFlow, Query):
    """
    An object representing the summation
    """

    def __init__(self, *, flows: BaseInOutFlow):
        self.redaction_target = flows
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = flows.spatial_unit
        super().__init__(flows.flow)
