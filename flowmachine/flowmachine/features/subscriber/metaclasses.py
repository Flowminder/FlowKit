# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Here you find metaclasses for building subscriber features.

"""

from ...core.query import Query
from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class SubscriberFeature(Query):
    """
    Abstract base class for metrics about our subscriber, which
    excludes their home locations, as this is a special
    case
    """

    def join_aggregate(self, locations, method="avg"):
        """
        Join with a location representing object and aggregate
        spatially.

        Parameters
        ----------
        method : {"avg", "max", "min", "median", "mode", "stddev", "variance"}
        locations :
            Subscriber locating type query

        Returns
        -------
        JoinedSpatialAggregate
            Query object representing a version of this metric aggregated to
            the spatial unit.
        """
        return JoinedSpatialAggregate(metric=self, locations=locations, method=method)

    def __getitem__(self, item):
        return self.subset(col="subscriber", subset=item)
