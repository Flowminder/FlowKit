# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import warnings
from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from flowmachine.features.location.redacted_spatial_aggregate import (
    RedactedSpatialAggregate,
)
from flowmachine.features.location.spatial_aggregate import SpatialAggregate
from flowmachine.utils import parse_datestring


class RedactedJoinedSpatialAggregate(GeoDataMixin, Query):
    """
    Creates spatially aggregated data from two objects, one of which is
    a metric of subscribers, and the other of which represents the subscribers'
    locations.

    A general class that join metric information about a subscriber with location
     information about a subscriber and aggregates to the geometric level.

    Parameters
    ----------
    metric : Query
        A query object that represents a subscriber level metric such
        as radius of gyration. The underlying data must have a 'subscriber'
        column. All other columns must be numeric and will be aggregated.
    locations : Query
        A query object that represents the locations of subscribers.
        Must have a 'subscriber' column, and a 'spatial_unit' attribute.
    method : {"avg", "max", "min", "median", "mode", "stddev", "variance", "distr"}
            Method of aggregation.

    Examples
    --------
        >>>  mfl = subscribers.MostFrequentLocation('2016-01-01',
                                              '2016-01-04',
                                              spatial_unit=AdminSpatialUnit(level=3))
        >>> rog = subscribers.RadiusOfGyration('2016-01-01',
                                         '2016-01-04')
        >>> sm = JoinedSpatialAggregate(metric=rog, locations=mfl)
        >>> sm.head()
                name     rog
            0   Rasuwa   157.200039
            1   Sindhuli 192.194037
            2   Humla    123.676914
            3   Gulmi    163.980299
            4   Jumla    144.432886
            ...
    """

    def __init__(self, *, joined_spatial_aggregate: JoinedSpatialAggregate):
        self.joined_spatial_aggregate = joined_spatial_aggregate
        self.redacted_spatial_agg = RedactedSpatialAggregate(
            spatial_aggregate=SpatialAggregate(
                locations=self.joined_spatial_aggregate.locations
            )
        )
        self.spatial_unit = self.joined_spatial_aggregate.locations.spatial_unit
        super().__init__()

    def _make_query(self):
        return f"""
        SELECT jsa.* FROM
        ({self.joined_spatial_aggregate.get_query()}) as jsa
        INNER JOIN
        ({self.redacted_spatial_agg.get_query()}) as redact
        USING ({','.join(self.spatial_unit.location_id_columns)})
        """

    @property
    def column_names(self) -> List[str]:
        return self.joined_spatial_aggregate.column_names
