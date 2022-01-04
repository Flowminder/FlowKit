# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
import numpy as np
from typing import List, Union

from flowmachine.core.query import Query
from flowmachine.features import CallDays, PerLocationSubscriberCallDurations
from flowmachine.features.subscriber.location_visits import LocationVisits
from flowmachine.features.utilities.subscriber_locations import BaseLocation


class FilteredReferenceLocation(Query, BaseLocation):
    """
    A filtered reference location, which includes rows only where the subscriber
    filter is true, for example visited a location at least N times.

    Parameters
    ----------
    reference_locations_query : BaseLocation
        Reference location to take locations from
    filter_query : LocationVisits or CallDays or PerLocationSubscriberCallDurations
        Per subscriber per location values to use for filtering
    lower_bound : int
        Minimum number of visits to allow a location to be included
    upper_bound : int, default Inf
        Maximum number of visits to allow a location to be included. Defaults to unbounded.
    """

    def __init__(
        self,
        reference_locations_query: BaseLocation,
        filter_query: Union[
            LocationVisits, CallDays, PerLocationSubscriberCallDurations
        ],
        lower_bound: int,
        upper_bound: int = np.inf,
    ):
        if reference_locations_query.spatial_unit != filter_query.spatial_unit:
            raise ValueError(
                "reference_locations_query and filter_query must have a common spatial unit."
            )
        self.locations = reference_locations_query.join(
            filter_query.numeric_subset(low=lower_bound, high=upper_bound, col="value"),
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
