# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Number of call days over a certain period
of time that a given subscriber makes. This feature
represent the number of days that a subscriber
is connected to a given tower, or within a given location in a
specified time period.
"""
from typing import List

from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import SubscriberLocations


class CallDays(SubscriberFeature):
    """
    Class representing the number of call days over a certain
    period of time.  Call days represent the number of days that a
    subscriber was connected to a tower in the given period of time.

    Parameters
    ----------
    subscriber_locations : SubscriberLocations
        Locations of subscribers' interactions

    See Also
    --------
    flowmachine.features.subscriber_locations
    """

    def __init__(self, subscriber_locations: SubscriberLocations):
        self.ul = subscriber_locations
        self.spatial_unit = self.ul.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):
        location_columns = ", ".join(self.spatial_unit.location_id_columns)

        sql = f"""
        SELECT * FROM (
            SELECT
                connections.subscriber,
                {location_columns},
                COUNT(*) AS value
            FROM (
                SELECT DISTINCT locations.subscriber, {location_columns}, locations.time::date
                FROM ({self.ul.get_query()}) AS locations
            ) AS connections
            GROUP BY connections.subscriber, {location_columns}
        ) calldays
        ORDER BY calldays.subscriber ASC, calldays.value DESC
        """

        return sql
