from flowmachine.core import Query
from flowmachine.features.location.flows import FlowLike
from flowmachine.features.location.redacted_labelled_aggregate import (
    RedactedLabelledAggregate,
)


class RedactedLabelledFlows(FlowLike, RedactedLabelledAggregate):
    def __init__(self, *, labelled_flows, redaction_threshold=15):

        if not hasattr(labelled_flows, "spatial_unit"):
            raise ValueError("labelled_query must have a spatial unit")

        self.spatial_unit = labelled_flows.spatial_unit

        super().__init__(
            labelled_query=labelled_flows, redaction_threshold=redaction_threshold
        )
