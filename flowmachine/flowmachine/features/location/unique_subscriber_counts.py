# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List, Union, Optional, Tuple

from ..subscriber.unique_locations import UniqueLocations
from flowmachine.utils import standardise_date

"""
Class for UniqueSubscriberCounts. UniqueSubscriberCounts counts
the total number of unique subscribers for each location. Each location
will have an integer that shows how many different subscribers
visited in a given period of time.



"""
from ...core.query import Query
from ...core.mixins import GeoDataMixin
from ...core import make_spatial_unit
from ...core.spatial_unit import AnySpatialUnit

from ..utilities.subscriber_locations import SubscriberLocations


class UniqueSubscriberCounts(GeoDataMixin, Query):
    """
    Class that defines counts of unique subscribers for each location.
    Each location for the given spatial unit is accompanied by the count of unique subscribers.

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default cell
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    hours : tuple of ints, default None
        subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon. If 'all' it will pull together all of the tables
        specified as flowmachine.yml under 'location_tables'
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    ignore_nulls : bool, default True
        ignores those values that are null. Sometime data appears for which
        the cell is null. If set to true this will ignore those lines. If false
        these lines with null cells should still be present, although they contain
        no information on the subscribers location, they still tell us that the subscriber made
        a call at that time.

    Examples
    --------
    >>> usc = UniqueSubscriberCounts('2016-01-01', '2016-01-04', spatial_unit=AdminSpatialUnit(level=3), hours=(5,17))
    >>> usc.head(4)
          name                  unique_subscriber_counts
    0     Arghakhanchi          313
    1     Baglung               415
    2     Bajhang               285
    """

    def __init__(
        self,
        start,
        stop,
        spatial_unit: AnySpatialUnit = make_spatial_unit("cell"),
        hours: Optional[Tuple[int, int]] = None,
        table="all",
        subscriber_subset=None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.spatial_unit = spatial_unit
        self.hours = hours
        self.table = table
        self.ul = UniqueLocations(
            SubscriberLocations(
                start=self.start,
                stop=self.stop,
                spatial_unit=self.spatial_unit,
                hours=self.hours,
                table=self.table,
                subscriber_subset=subscriber_subset,
            )
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """

        relevant_columns = ",".join(self.spatial_unit.location_id_columns)
        sql = """
        SELECT {rc}, COUNT(unique_subscribers) AS value FROM 
        (SELECT 
            {rc},
            all_locs.subscriber as unique_subscribers
        FROM ({all_locs}) AS all_locs) AS _
        GROUP BY {rc} 
        """.format(
            all_locs=self.ul.get_query(), rc=relevant_columns
        )
        return sql
