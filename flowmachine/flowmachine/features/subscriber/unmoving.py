# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List

from flowmachine.core import make_spatial_unit
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.subscriber.unique_locations import UniqueLocations
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations


class Unmoving(SubscriberFeature):
    """
    Determine whether a subscriber moved within a time period.
    Value will be True if the subscriber did not move.

    Parameters
    ----------
    locations : UniqueLocations
        A unique locations object

    Examples
    --------
    >>> Unmoving(locations=UniqueLocations(SubscriberLocations("2016-01-01", "2016-01-01 10:00",
    >>> spatial_unit=make_spatial_unit("admin", level=3)))).head()
               subscriber  value
    0   038OVABN11Ak4W5P  False
    1   09NrjaNNvDanD8pk  False
    2   0ayZGYEQrqYlKw6g  False
    3   0DB8zw67E9mZAPK2  False
    4   0Gl95NRLjW2aw8pW   True
    """

    def __init__(
        self, locations: UniqueLocations,
    ):
        self.locations = locations
        self.spatial_unit = locations.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):

        sql = f"""
        SELECT 
            subscriber,
            count(*) = 1 as value
        FROM
            ({self.locations.get_query()}) _
        GROUP BY subscriber
        """

        return sql
