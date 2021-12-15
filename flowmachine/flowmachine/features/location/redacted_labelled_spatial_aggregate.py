from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.redacted_labelled_mixin import RedactedLabelledMixin


class RedactedLabelledSpatialAggregate(RedactedLabelledMixin, GeoDataMixin, Query):
    def __init__(self, *, labelled_spatial_aggregate, redaction_threshold=15):
        super().__init__(
            labelled_query=labelled_spatial_aggregate,
            redaction_threshold=redaction_threshold,
        )
