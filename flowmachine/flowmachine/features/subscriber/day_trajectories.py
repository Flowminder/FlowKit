# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Computes daily trajectories. Trajectories are defined as
dated lists of locations. For timestamped lists 
of events see feature subscriber_locations.


"""

from functools import reduce
from typing import List

from flowmachine.core import Query
from flowmachine.features.utilities.subscriber_locations import BaseLocation
from flowmachine.utils import get_columns_for_level
from ..utilities.multilocation import MultiLocation


class DayTrajectories(MultiLocation, BaseLocation, Query):
    """
    Class that defines day-dated trajectories (list of time-sorted DailyLocations per subscriber).
    
    Examples
    --------
    >>> dt = DayTrajectories('2016-01-01', '2016-01-04',
                             level = 'admin3', method = 'last', hours = (5,17))
    >>> dt.head(4)
            subscriber                name       date
        0   038OVABN11Ak4W5P    Dolpa      2016-01-01
        1   038OVABN11Ak4W5P    Baglung    2016-01-02
        2   038OVABN11Ak4W5P    Jhapa      2016-01-03
        3   038OVABN11Ak4W5P    Dolpa      2016-01-04
    """

    @property
    def column_names(self) -> List[str]:
        return (
            ["subscriber"]
            + get_columns_for_level(self.level, self.column_name)
            + ["date"]
        )

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        relevant_columns = self._get_relevant_columns()

        # This query represents the concatenated locations of the
        # subscribers. Similar to the first step when calculating
        # ModalLocations. See modal_locations.py
        all_locs = reduce(
            lambda x, y: x.union(y), (self._append_date(dl) for dl in self._all_dls)
        )

        sql = """
        SELECT 
            all_locs.subscriber, 
            {rc},
            all_locs.date
        FROM ({all_locs}) AS all_locs
        ORDER BY all_locs.subscriber, all_locs.date
        """.format(
            all_locs=all_locs.get_query(), rc=relevant_columns
        )

        return sql
