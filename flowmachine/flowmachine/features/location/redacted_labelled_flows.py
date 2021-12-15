from flowmachine.core import Query
from flowmachine.features.location.flows import FlowLike
from flowmachine.features.location.redacted_labelled_mixin import RedactedLabelledMixin


class RedactedLabelledFlows(RedactedLabelledMixin, FlowLike, Query):
    def __init__(self, *, labelled_flows, redaction_threshold=15):
        super().__init__(
            labelled_query=labelled_flows, redaction_threshold=redaction_threshold
        )
