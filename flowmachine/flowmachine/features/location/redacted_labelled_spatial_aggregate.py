# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.query import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.redacted_labelled_aggregate import (
    RedactedLabelledAggregate,
)
from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)


class RedactedLabelledSpatialAggregate(GeoDataMixin, RedactedLabelledAggregate):
    """
    Query that drops any locations aggregates that, when disaggregated by label, reveal a number of subscribers
    less than redaction_threshold
    Parameters
    ----------
    labelled_spatial_aggregate: LabelledSpatialAggregate
        The LabelledSpatialAggregate query to redact
    redaction_threshold: int default 15
        If any labels within a location reveal this number of subscribers or fewer, that location is dropped
    """

    def __init__(
        self,
        *,
        labelled_spatial_aggregate: LabelledSpatialAggregate,
        redaction_threshold: int = 15
    ):

        if not hasattr(labelled_spatial_aggregate, "spatial_unit"):
            raise ValueError("labelled_spatial_aggregate must have a spatial unit")

        self.spatial_unit = labelled_spatial_aggregate.spatial_unit

        super().__init__(
            labelled_query=labelled_spatial_aggregate,
            redaction_threshold=redaction_threshold,
        )
