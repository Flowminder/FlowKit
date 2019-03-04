# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""

The modal daily location of a subscriber.



"""
from typing import List

from functools import reduce

from flowmachine.core import Query
from flowmachine.features.utilities.subscriber_locations import BaseLocation
from flowmachine.utils import get_columns_for_level
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
        return ["subscriber"] + get_columns_for_level(self.level, self.column_name)

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        relevant_columns = self._get_relevant_columns()

        # This query represents the concatenated locations of the
        # subscribers
        all_locs = reduce(
            lambda x, y: x.union(y), (self._append_date(dl) for dl in self._all_dls)
        )

        times_visited = """
        SELECT all_locs.subscriber, {rc}, count(*) AS total, max(all_locs.date) as date
        FROM ({all_locs}) AS all_locs
        GROUP BY all_locs.subscriber, {rc}
        """.format(
            all_locs=all_locs.get_query(), rc=relevant_columns
        )

        sql = """
        SELECT ranked.subscriber, {rc}
        FROM
             (SELECT times_visited.subscriber, {rc},
             row_number() OVER (PARTITION BY times_visited.subscriber
                 ORDER BY total DESC, times_visited.date DESC) AS rank
             FROM ({times_visited}) AS times_visited) AS ranked
        WHERE rank = 1
        """.format(
            times_visited=times_visited, rc=relevant_columns
        )

        return sql
