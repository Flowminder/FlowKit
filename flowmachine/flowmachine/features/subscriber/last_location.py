# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Computes the last location an subscriber was identified
at during a specified time period.



"""
from typing import List, Optional, Tuple

from flowmachine.core import Query, make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from ..utilities.subscriber_locations import BaseLocation
from ..utilities.subscriber_locations import SubscriberLocations
from flowmachine.utils import standardise_date


class LastLocation(BaseLocation, Query):
    """
    Class representing a subscribers last location within a certain time
    frame

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to which subscriber locations will be mapped. See the
        docstring ofmake_spatial_unit for more information.
    hours : tuple of ints, default None
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    method : str, default 'last'
        The method by which to calculate the location of the subscriber.
        This can be either 'most-common' or last. 'most-common' is
        simply the modal location of the subscribers, whereas 'lsat' is
        the location of the subscriber at the time of the final call in
        the data.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon. If 'ALL' it will use all tables that contain
        location data, specified in flowmachine.yml.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Notes
    -----
    * A date without a hours and mins will be interpreted as
      midnight of that day, so to get data within a single day
      pass (e.g.) '2016-01-01', '2016-01-02'.

    * Use 24 hr format!

    """

    def __init__(
        self,
        start,
        stop,
        spatial_unit: Optional[AnySpatialUnit] = None,
        hours: Optional[Tuple[int, int]] = None,
        table="all",
        subscriber_identifier="msisdn",
        *,
        ignore_nulls=True,
        subscriber_subset=None,
    ):

        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=3)
        else:
            self.spatial_unit = spatial_unit
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.subscriber_locs = SubscriberLocations(
            start=self.start,
            stop=self.stop,
            spatial_unit=self.spatial_unit,
            hours=self.hours,
            table=self.table,
            subscriber_identifier=self.subscriber_identifier,
            ignore_nulls=ignore_nulls,
            subscriber_subset=subscriber_subset,
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        relevant_columns = ",".join(self.spatial_unit.location_id_columns)

        sql = """
        SELECT final_time.subscriber, {rc}
        FROM
             (SELECT subscriber_locs.subscriber, time, {rc},
             row_number() OVER (PARTITION BY subscriber_locs.subscriber ORDER BY time DESC)
                 AS rank
             FROM ({subscriber_locs}) AS subscriber_locs) AS final_time
        WHERE rank = 1
        """.format(
            subscriber_locs=self.subscriber_locs.get_query(), rc=relevant_columns
        )

        return sql
