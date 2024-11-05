# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.core import Query
from flowmachine.features import Flows
from flowmachine.features.location.flows import FlowLike
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)


class RedactedFlows(RedactedLocationMetric, FlowLike, Query):
    """
    An object representing the difference in locations between two location
    type objects, redacted.

    Parameters
    ----------
    flows : Flows
        An unredacted flows object
    """

    def __init__(self, *, flows: FlowLike):
        self.redaction_target = flows
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = flows.spatial_unit
        super().__init__()
