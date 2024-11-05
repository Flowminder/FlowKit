# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""

The modal daily location of a subscriber.



"""
from typing import List


from flowmachine.core import Query
from flowmachine.features.utilities.subscriber_locations import BaseLocation
from ..utilities.multilocation import MultiLocation


class ModalLocation(MultiLocation, BaseLocation, Query):
    """
    ModalLocation is the mode of multiple DailyLocations (or other similar
    location like objects.) It can be instantiated with either a date range
    or a list of DailyLocations (the former is more common). It gives each
    subscriber only one location.
    """

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        location_columns_string = ", ".join(self.spatial_unit.location_id_columns)

        times_visited = f"""
        SELECT all_locs.subscriber, {location_columns_string}, count(*) AS total, max(all_locs.date) as date
        FROM ({self.unioned.get_query()}) AS all_locs
        GROUP BY all_locs.subscriber, {location_columns_string}
        """

        sql = f"""
        SELECT ranked.subscriber, {location_columns_string}
        FROM
             (SELECT times_visited.subscriber, {location_columns_string},
             row_number() OVER (PARTITION BY times_visited.subscriber
                 ORDER BY total DESC, times_visited.date DESC) AS rank
             FROM ({times_visited}) AS times_visited) AS ranked
        WHERE rank = 1
        """

        return sql
