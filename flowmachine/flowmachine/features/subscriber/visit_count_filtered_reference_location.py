# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List

from flowmachine.core.query import Query
from flowmachine.features.subscriber.location_visits import LocationVisits
from flowmachine.features.utilities.subscriber_locations import BaseLocation


class VisitCountFilteredReferenceLocation(Query, BaseLocation):
    """
    A filtered reference location, which includes rows only where the subscriber
    visited the location a specified number of times.

    Parameters
    ----------
    reference_locations_query : BaseLocation
        Reference location to take locations from
    location_visits_query : LocationVisits
        Location visit counts to use for thresholding
    lower_bound : int
        Minimum number of visits to allow a location to be included
    upper_bound : int
        Maximum number of visits to allow a location to be included
    """

    def __init__(
        self,
        reference_locations_query: BaseLocation,
        location_visits_query: LocationVisits,
        lower_bound: int,
        upper_bound: int,
    ):
        self.locations = reference_locations_query.join(
            location_visits_query.numeric_subset(
                low=lower_bound, high=upper_bound, col="value"
            ),
            how="inner",
            on_left=(
                "subscriber",
                *reference_locations_query.spatial_unit.location_id_columns,
            ),
            on_right=(
                "subscriber",
                *reference_locations_query.spatial_unit.location_id_columns,
            ),
            right_append="_visit_count",
        )
        self.spatial_unit = reference_locations_query.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.locations.left.column_names

    def _make_query(self) -> str:
        return f"SELECT {self.locations.left.column_names_as_string_list} FROM ({self.locations.get_query()}) _"
