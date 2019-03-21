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
from typing import List

from flowmachine.utils import get_columns_for_level
from ..utilities.subscriber_locations import subscriber_locations
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
    level : str, default 'cell'
        Levels can be one of:
            'cell':
                The identifier as it is found in the CDR itself
            'versioned-cell':
                The identifier as found in the CDR combined with the version from
                the cells table.
            'versioned-site':
                The ID found in the sites table, coupled with the version
                number.
            'polygon':
                A custom set of polygons that live in the database. In which
                case you can pass the parameters column_name, which is the column
                you want to return after the join, and table_name, the table where
                the polygons reside (with the schema), and additionally geom_col
                which is the column with the geometry information (will default to
                'geom')
            'admin*':
                An admin region of interest, such as admin3. Must live in the
                database in the standard location.
            'grid':
                A square in a regular grid, in addition pass size to
                determine the size of the polygon.
    hours : tuple of ints, default 'all'
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
                                level = 'admin3', method = 'last', hours = (5,17))
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
        level="cell",
        hours="all",
        tables="all",
        subscriber_identifier="msisdn",
        ignore_nulls=True,
        column_name=None,
        subscriber_subset=None,
        polygon_table=None,
        size=None,
        radius=None,
    ):

        self.ul = subscriber_locations(
            start=start,
            stop=stop,
            level=level,
            hours=hours,
            table=tables,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            column_name=column_name,
            subscriber_subset=subscriber_subset,
            polygon_table=polygon_table,
            size=size,
            radius=radius,
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "unique_location_counts"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """

        relevant_columns = ",".join(
            get_columns_for_level(self.ul.level, self.ul.column_name)
        )
        sql = """
        SELECT 
            subscriber, 
            COUNT(*) as unique_location_counts  
            FROM
        (SELECT DISTINCT subscriber, {rc} 
          FROM ({all_locs}) AS all_locs) AS _
        GROUP BY subscriber  
        """.format(
            all_locs=self.ul.get_query(), rc=relevant_columns
        )
        return sql
