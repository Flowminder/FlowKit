# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List

from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations


class UniqueLocations(SubscriberFeature):
    """
    The unique locations a subscriber has visited.
    Added to support FlowKit implementation of COVID-19 aggregates[1]_[2]_.

    Parameters
    ----------
    subscriber_locations : SubscriberLocations
        A subscriber locations object

    Examples
    --------
    >>> UniqueLocations(subscriber_locations=SubscriberLocations("2016-01-01", "2016-01-02", make_spatial_unit("admin", level=3))).head()
             subscriber         pcod
    0  038OVABN11Ak4W5P  524 2 04 20
    1  038OVABN11Ak4W5P  524 3 08 43
    2  038OVABN11Ak4W5P  524 4 12 62
    3  038OVABN11Ak4W5P  524 4 12 65
    4  09NrjaNNvDanD8pk  524 2 04 20

    References
    ----------
    .. [1] https://github.com/Flowminder/COVID-19/issues/12
    .. [2] https://covid19.flowminder.org
    """

    def __init__(
        self,
        subscriber_locations: SubscriberLocations,
    ):
        self.spatial_unit = subscriber_locations.spatial_unit
        self.subscriber_locations = subscriber_locations
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", *self.spatial_unit.location_id_columns]

    def _make_query(self):
        location_columns = ",".join(self.spatial_unit.location_id_columns)

        sql = f"""
        SELECT 
            subscriber,
            {location_columns}
        FROM
            ({self.subscriber_locations.get_query()}) _ GROUP BY subscriber, {location_columns}
        """

        return sql
