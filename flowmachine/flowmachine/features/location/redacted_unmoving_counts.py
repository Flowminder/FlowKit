# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core import Query
from flowmachine.features.location.unmoving_counts import UnmovingCounts
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedUnmovingCounts(RedactedLocationMetric, Query):
    """
    An object which represents a count of consecutive visits between locations.

    Parameters
    ----------
    unmoving_counts : UnmovingCounts
        An unredacted unmoving counts object
    """

    def __init__(self, *, unmoving_counts: UnmovingCounts):

        self.redaction_target = unmoving_counts
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = unmoving_counts.spatial_unit
        super().__init__()
