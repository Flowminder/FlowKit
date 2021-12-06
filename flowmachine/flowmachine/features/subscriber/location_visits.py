# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Class for location visits. LocationVisits are defined as lists of
unique DailyLocations for each subscriber. Each location is accompanied
by the count of days it appears as a daily_location for that subscriber.



"""
from typing import List

from flowmachine.features.subscriber.metaclasses import SubscriberFeature


class LocationVisits(SubscriberFeature):
    """
    Class that defines lists of unique Dailylocations for each subscriber.
    Each location is accompanied by the count of times it was a daily_location.

    Examples
    --------
            >>> lv = LocationVisits('2016-01-01', '2016-01-04',
                                    spatial_unit=make_spatial_unit('admin', level=3),
                                    method='last', hours=(5,17))
            >>> lv.head(4)
                    subscriber                name       value
                0   038OVABN11Ak4W5P    Dolpa      5
                1   038OVABN11Ak4W5P    Baglung    3
                2   038OVABN11Ak4W5P    Jhapa      2
                3   038OVABN11Ak4W5P    Dolpa      1
    """

    def __init__(self, day_trajectories):
        self.day_trajectories = day_trajectories
        self.spatial_unit = day_trajectories.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        location_columns = ", ".join(self.spatial_unit.location_id_columns)

        sql = f"""
        SELECT
            day_trajectories.subscriber,
            day_trajectories.{location_columns},
            COUNT(*) AS value
        FROM
            ({self.day_trajectories.get_query()}) AS day_trajectories
        GROUP BY 
            day_trajectories.subscriber,
            day_trajectories.{location_columns}
        ORDER BY
            day_trajectories.subscriber,
            COUNT(*) DESC
        """

        return sql
