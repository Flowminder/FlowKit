# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Here you find metaclasses for building subscriber features.

"""

import warnings

from ...core.query import Query
from ..utilities.spatial_aggregates import JoinedSpatialAggregate


import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class SubscriberFeature(Query):
    """
    Abstract base class for metrics about our subscriber, which
    excludes their home locations, as this is a special
    case
    """

    def join_aggregate(self, locations, method="mean"):
        """
        Join with a location representing object and aggregate
        spatially.

        Parameters
        ----------
        method : {'mean', 'mode', 'median'}
        locations :
            Subscriber locating type query

        Returns
        -------
        JoinedSpatialAggregate
            Query object representing a version of this metric aggregated to
            the location level.
        """
        return JoinedSpatialAggregate(self, locations, method=method)

    def __getitem__(self, item):

        return self.subset(col="subscriber", subset=item)
