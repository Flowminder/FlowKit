# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Number of call days over a certain period
of time that a given subscriber makes. This feature
represent the number of days that a subscriber
is connected to a given tower within a
specified time period.
"""
from typing import List, Union

from ...core import JoinToLocation
from flowmachine.utils import get_columns_for_level
from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import _SubscriberCells


class CallDays(SubscriberFeature):
    """
    Class representing the number of call days over a certain
    period of time.  Call days represent the number of days that a
    subscriber was connected to a tower in the given period of time.

    Parameters
    ----------
    subscriber_locations : JoinToLocation, _SubscriberCells
        Locations of subscribers' interactions

    See Also
    --------
    flowmachine.features.subscriber_locations
    """

    def __init__(self, subscriber_locations: Union[JoinToLocation, _SubscriberCells]):
        self.ul = subscriber_locations
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return (
            ["subscriber"]
            + get_columns_for_level(self.ul.level, self.ul.column_name)
            + ["calldays"]
        )

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        Returns a sorted calldays table.
        """
        relevant_columns = ", ".join(
            get_columns_for_level(self.ul.level, self.ul.column_name)
        )

        sql = f"""
        SELECT * FROM (
            SELECT
                connections.subscriber,
                {relevant_columns},
                COUNT(*) AS calldays
            FROM (
                SELECT DISTINCT locations.subscriber, {relevant_columns}, locations.time::date
                FROM ({self.ul.get_query()}) AS locations
            ) AS connections
            GROUP BY connections.subscriber, {relevant_columns}
        ) calldays
        ORDER BY calldays.subscriber ASC, calldays.calldays DESC
        """

        return sql
