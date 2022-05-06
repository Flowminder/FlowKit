from flowmachine.core import Query
from flowmachine.features.location.flows import BaseInOutFlow
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedInOutFlow(RedactedLocationMetric, BaseInOutFlow, Query):
    """
    An object representing the redacted summation of all movement into or out of
    a set of locations

    Params
    -----
    in_out_flow: BaseInOutFlow
        The underlying Inflow or Outflow to redact; normally created from `flows.inflow` or `flows.outflow`
    """

    def __init__(self, *, flows: BaseInOutFlow):
        self.redaction_target = flows
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = flows.spatial_unit
        super().__init__(flows.flow)
