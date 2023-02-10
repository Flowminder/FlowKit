# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.active_at_reference_location_counts import (
    ActiveAtReferenceLocationCounts,
)
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedActiveAtReferenceLocationCounts(
    RedactedLocationMetric, GeoDataMixin, Query
):
    """
    Class representing an active at reference location count, with counts of 15
    or less redacted.

    Parameters
    ----------
    active_at_reference_location_counts : ActiveAtReferenceLocationCounts
        Unredacted metric

    See Also
    --------
    ActiveAtReferenceLocationCounts
    """

    def __init__(
        self, *, active_at_reference_location_counts: ActiveAtReferenceLocationCounts
    ):
        self.redaction_target = active_at_reference_location_counts
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = active_at_reference_location_counts.spatial_unit
        super().__init__()
