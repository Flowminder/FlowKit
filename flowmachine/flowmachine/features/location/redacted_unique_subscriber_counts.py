# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.unique_subscriber_counts import (
    UniqueSubscriberCounts,
)
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedUniqueSubscriberCounts(RedactedLocationMetric, GeoDataMixin, Query):
    """
    Class that defines redacted counts of unique subscribers for each location.
    Each location for the given spatial unit is accompanied by the count of unique subscribers.

    Parameters
    ----------
    locations : subscriber location query
    """

    def __init__(self, *, unique_subscriber_counts: UniqueSubscriberCounts):

        self.redaction_target = unique_subscriber_counts
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = unique_subscriber_counts.spatial_unit
        super().__init__()
