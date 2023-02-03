# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Class for UniqueLocationCounts. UniqueLocationCounts counts
the total number of unique locations for each subscriber. Each Subscriber
will have an integer that shows how many different locations
it visited


"""
from typing import List, Optional, Tuple

from flowmachine.core import make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from .unique_locations import UniqueLocations
from ..utilities.subscriber_locations import SubscriberLocations
from .metaclasses import SubscriberFeature


class UniqueLocationCounts(SubscriberFeature):

    """
    Class that defines counts of unique locations for each subscriber.
    Each subscriber is accompanied by the count of unique locations.

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
    tables : str, default 'ALL'
        schema qualified name of the table which the analysis is
        based upon. If 'ALL' it will pull together all of the tables
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
        >>> ulc = UniqueLocationCounts('2016-01-01', '2016-01-04',
                                spatial_unit=make_spatial_unit('admin', level=3),
                                method='last', hours=(5,17))
        >>> ulc.head(4)
                subscriber                unique_location_counts
            0   038OVABN11Ak4W5P    3
            1   0987YDNK23Da6G5K    4
            2   0679FBNM35DsTH3K    3
    """

    def __init__(
        self,
        start,
        stop,
        *,
        spatial_unit: AnySpatialUnit = make_spatial_unit("cell"),
        hours: Optional[Tuple[int, int]] = None,
        tables="all",
        subscriber_identifier="msisdn",
        ignore_nulls=True,
        subscriber_subset=None,
    ):
        self.ul = UniqueLocations(
            SubscriberLocations(
                start=start,
                stop=stop,
                spatial_unit=spatial_unit,
                hours=hours,
                table=tables,
                subscriber_identifier=subscriber_identifier,
                ignore_nulls=ignore_nulls,
                subscriber_subset=subscriber_subset,
            )
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """

        location_columns = ",".join(self.ul.spatial_unit.location_id_columns)
        sql = f"""
        SELECT 
            subscriber, 
            COUNT(*) as value  
            FROM
        ({self.ul.get_query()}) AS _
        GROUP BY subscriber  
        """

        return sql
