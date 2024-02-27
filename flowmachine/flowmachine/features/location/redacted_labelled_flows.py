# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.core import Query
from flowmachine.features.location.flows import FlowLike
from flowmachine.features.location.labelled_flows import LabelledFlows
from flowmachine.features.location.redacted_labelled_aggregate import (
    RedactedLabelledAggregate,
)


class RedactedLabelledFlows(FlowLike, RedactedLabelledAggregate):
    """
    Query that drops any flows that, when disaggregated by label, reveal a number of subscribers
    less than redaction_threshold
    Parameters
    ----------
    labelled_flows: LabelledFlows
        The LabelledFlows query to redact
    redaction_threshold: int default 15
        If any labels within a flow (from - to) reveal this number of subscribers or fewer, that flow is dropped
    """

    def __init__(self, *, labelled_flows: LabelledFlows, redaction_threshold: int = 15):
        if not hasattr(labelled_flows, "spatial_unit"):
            raise ValueError("labelled_flows must have a spatial unit")

        self.spatial_unit = labelled_flows.spatial_unit

        super().__init__(
            labelled_query=labelled_flows, redaction_threshold=redaction_threshold
        )
