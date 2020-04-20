# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core.mixins.geodata_mixin import GeoDataMixin
from flowmachine.core.query import Query
from flowmachine.features.subscriber.unmoving import Unmoving
from flowmachine.features.subscriber.unmoving_at_reference_location import (
    UnmovingAtReferenceLocation,
)


class UnmovingAtReferenceLocationCounts(GeoDataMixin, Query):
    """
    Per location counts of subscribers who were only seen at that location and did not move.

    Originally added as the FlowKit implementation of COVID-19 aggregate[1]_[2]_.

    Parameters
    ----------
    unmoving_at_reference_location : UnmovingAtReferenceLocation

    Examples
    --------
    >>> UnmovingAtReferenceLocationCounts(UnmovingAtReferenceLocation(locations=UniqueLocations(SubscriberLocations("2016-01-01", "2016-01-02",spatial_unit=make_spatial_unit("admin", level=3))))).head()
              pcod  value
    0  524 1 02 09      1
    1  524 1 03 13      2
    2  524 2 04 20      1
    3  524 2 05 29      1
    4  524 3 07 37      1

    References
    ----------
    .. [1] https://github.com/Flowminder/COVID-19/issues/8
    .. [2] https://covid19.flowminder.org
    """

    def __init__(self, unmoving_at_reference_location: UnmovingAtReferenceLocation):
        self.unmoving_subscribers = unmoving_at_reference_location.subset("value", True)
        self.spatial_unit = unmoving_at_reference_location.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return [*self.spatial_unit.location_id_columns, "value"]

    def _make_query(self):
        location_cols = ", ".join(self.spatial_unit.location_id_columns)
        return f"""
        SELECT {location_cols},
        count(*) as value
        FROM
            ({self.unmoving_subscribers.get_query()}) um
            INNER JOIN
            ({self.unmoving_subscribers.reference_locations.get_query()}) locs
            USING (subscriber)
        GROUP BY {location_cols}
        """
