# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.query import Query
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.features.utilities.combine_first import CombineFirst
from flowmachine.features.utilities.subscriber_locations import BaseLocation


class CoalescedLocation(BaseLocation, CombineFirst):
    """
    Wrapper around CombineFirst for the special case of coalescing locations
    from two subscriber location queries. For each subscriber appearing in
    either query, this will return their location from preferred_locations if
    they appear with non-null location in the result of preferred_locations, or
    their location from fallback_locations otherwise.

    Parameters
    ----------
    preferred_locations: Query
        Subscriber locations to be chosen preferentially
    fallback_locations: Query
        Subscriber locations to be assigned for subscribers who do not have a
        location in preferred_locations

    See Also
    --------
    flowmachine.features.utilities.combine_first.CombineFirst
    """

    def __init__(self, preferred_locations: Query, fallback_locations: Query):
        if preferred_locations.spatial_unit != fallback_locations.spatial_unit:
            raise InvalidSpatialUnitError(
                "Spatial units of the preferred_locations and fallback_locations arguments to CoalescedLocation must match."
            )
        self.spatial_unit = preferred_locations.spatial_unit

        super().__init__(
            first_query=preferred_locations,
            other_query=fallback_locations,
            join_columns="subscriber",
            combine_columns=self.spatial_unit.location_id_columns,
        )
