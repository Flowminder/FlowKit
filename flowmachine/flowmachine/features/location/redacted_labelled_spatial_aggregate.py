from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.redacted_labelled_aggregate import (
    RedactedLabelledAggregate,
)


class RedactedLabelledSpatialAggregate(GeoDataMixin, RedactedLabelledAggregate):
    """
    Query that drops any locations aggregates that, when disaggregated by label, reveal a number of subscribers
    less than redaction_threshold
    Parameters
    ----------
    labelled_spatial_aggregate: LabelledSpatialAggregate or LabelledFlows
        The LabelledSpatialAggregate query to redact
    redaction_threshold: int default 15
        If any labels within a location reveal less than this number of subscribers, that location is dropped
    """

    def __init__(self, *, labelled_spatial_aggregate, redaction_threshold=15):

        if not hasattr(labelled_spatial_aggregate, "spatial_unit"):
            raise ValueError("labelled_query must have a spatial unit")

        self.spatial_unit = labelled_spatial_aggregate.spatial_unit

        super().__init__(
            labelled_query=labelled_spatial_aggregate,
            redaction_threshold=redaction_threshold,
        )
