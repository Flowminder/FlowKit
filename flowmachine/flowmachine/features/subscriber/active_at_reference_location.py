# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List, Union

from flowmachine.features.subscriber.first_location import FirstLocation
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.subscriber import (
    ModalLocation,
    MostFrequentLocation,
    LastLocation,
)
from flowmachine.features.subscriber.unique_locations import UniqueLocations
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations


class ActiveAtReferenceLocation(SubscriberFeature):
    """
    Determine whether a subscriber was active at their reference location.

    Parameters
    ----------
    subscriber_locations : SubscriberLocations
        A subscriber locations object
    reference_locations : ModalLocation, MostFrequentLocation or LastLocation
        A reference location to check for activity at.

    Examples
    --------
    >>> ActiveAtReferenceLocation(subscriber_locations=UniqueLocations(SubscriberLocations("2016-01-01", "2016-01-02",
    >>> spatial_unit=make_spatial_unit("admin", level=3))),
    >>> reference_locations=daily_location("2016-01-03")).head()
             subscriber  value
    0  038OVABN11Ak4W5P   True
    1  09NrjaNNvDanD8pk  False
    2  0ayZGYEQrqYlKw6g  False
    3  0DB8zw67E9mZAPK2  False
    4  0Gl95NRLjW2aw8pW   True
    """

    def __init__(
        self,
        subscriber_locations: UniqueLocations,
        reference_locations: Union[
            ModalLocation, MostFrequentLocation, LastLocation, FirstLocation
        ],
    ):

        self.spatial_unit = subscriber_locations.spatial_unit
        self.reference_location = reference_locations
        self.subscriber_locations = subscriber_locations
        if self.spatial_unit != reference_locations.spatial_unit:
            raise ValueError("Spatial unit mismatch.")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        location_columns = ",".join(self.spatial_unit.location_id_columns)

        sql = f"""
        SELECT 
            ref.subscriber,
            COALESCE(loc_match, FALSE) as value
        FROM
            ({self.reference_location.get_query()}) ref
            LEFT OUTER JOIN
            (
                SELECT subscriber, {location_columns}, TRUE as loc_match 
                    FROM ({self.subscriber_locations.get_query()}) _
            ) AS all_locs
            USING (subscriber, {location_columns})
        """

        return sql
