# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List, Union

from flowmachine.core import make_spatial_unit
from flowmachine.features import ModalLocation, LastLocation, MostFrequentLocation
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.subscriber.unique_locations import UniqueLocations
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations


class UnmovingAtReferenceLocation(SubscriberFeature):
    """
    Determine whether a subscriber moved from their reference location within a time period.
    Value will be True if the subscriber did not move.

    Parameters
    ----------
    locations : UniqueLocations
        A unique locations object
    reference_locations : ModalLocation, MostFrequentLocation or LastLocation
        Per subscriber reference location check for movement from.

    Examples
    --------
    >>> UnmovingAtReferenceLocation(locations=UniqueLocations(SubscriberLocations("2016-01-01", "2016-01-01 10:00", spatial_unit=make_spatial_unit("admin", level=3))), reference_locations=LastLocation("2016-01-01", "2016-01-02")).head()
             subscriber  value
    0  038OVABN11Ak4W5P  False
    1  09NrjaNNvDanD8pk  False
    2  0ayZGYEQrqYlKw6g  False
    3  0DB8zw67E9mZAPK2  False
    4  0Gl95NRLjW2aw8pW  False
    """

    def __init__(
        self,
        locations: UniqueLocations,
        reference_locations: Union[ModalLocation, LastLocation, MostFrequentLocation],
    ):
        self.locations = locations
        self.reference_locations = reference_locations
        self.spatial_unit = locations.spatial_unit
        if reference_locations.spatial_unit != locations.spatial_unit:
            raise ValueError("Spatial unit mismatch.")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        locs_equal = " OR ".join(
            f"locs.{col} != homes.{col}"
            for col in self.spatial_unit.location_id_columns
        )

        sql = f"""
        SELECT 
            subscriber,
            sum(not_at_home::int) = 0 as value
        FROM
            (SELECT subscriber, {locs_equal} as not_at_home FROM
            ({self.reference_locations.get_query()}) homes
            INNER JOIN
            ({self.locations.get_query()}) locs            
            USING (subscriber)) at_reference
        GROUP BY subscriber
        """

        return sql
