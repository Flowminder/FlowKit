from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.redacted_labelled_aggregate import (
    RedactedLabelledAggregate,
)


class RedactedLabelledSpatialAggregate(GeoDataMixin, RedactedLabelledAggregate):
    def __init__(self, *, labelled_spatial_aggregate, redaction_threshold=15):

        if not hasattr(labelled_spatial_aggregate, "spatial_unit"):
            raise ValueError("labelled_query must have a spatial unit")

        super().__init__(
            labelled_query=labelled_spatial_aggregate,
            redaction_threshold=redaction_threshold,
        )
