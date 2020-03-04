# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine_core.query_bases.query import Query
from flowmachine_core.core.mixins import GeoDataMixin
from flowmachine_queries.features import UniqueSubscriberCounts
from flowmachine_core.utility_queries.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedUniqueSubscriberCounts(RedactedLocationMetric, GeoDataMixin, Query):
    """
    Class representing the result of spatially aggregating
    a locations object, redacted so that results are not returned if counts are 15 or less..
    A locations object represents the
    location of multiple subscribers. This class represents the output
    of aggregating that data spatially.

    Parameters
    ----------
    locations : subscriber location query
    """

    def __init__(self, *, unique_subscriber_counts: UniqueSubscriberCounts):

        self.redaction_target = unique_subscriber_counts
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = unique_subscriber_counts.spatial_unit
        super().__init__()
