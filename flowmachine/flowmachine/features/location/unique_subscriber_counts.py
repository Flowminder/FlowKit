# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List

"""
Class for UniqueSubscriberCounts. UniqueSubscriberCounts counts
the total number of unique subscribers for each location. Each location
will have an integer that shows how many different subscribers
visited in a given period of time.



"""
from ...core.query import Query
from ...core.mixins import GeoDataMixin

from flowmachine.utils import get_columns_for_level
from ..utilities.subscriber_locations import subscriber_locations


class UniqueSubscriberCounts(GeoDataMixin, Query):

    """
    Class that defines counts of unique subscribers for each location.
    Each location for the given level is accompanied by the count of unique subscribers.

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
    column_name : str, optional
        Option, none-standard, name of the column that identifies the
        spatial level, i.e. could pass admin3pcod to use the admin 3 pcode
        as opposed to the name of the region.
    kwargs :
        Eventually passed to flowmachine.JoinToLocation.

    Examples
    --------
    >>> usc = UniqueSubscriberCounts('2016-01-01', '2016-01-04', level = 'admin3', hours = (5,17))
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
        level="cell",
        hours="all",
        table="all",
        column_name=None,
        **kwargs
    ):
        """

        """

        self.start = start
        self.stop = stop
        self.level = level
        self.hours = hours
        self.table = table
        self.column_name = column_name
        self._kwargs = kwargs
        self.ul = subscriber_locations(
            start=self.start,
            stop=self.stop,
            level=self.level,
            hours=self.hours,
            table=self.table,
            column_name=self.column_name,
            **kwargs
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return get_columns_for_level(self.level, self.column_name) + [
            "unique_subscriber_counts"
        ]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """

        relevant_columns = ",".join(get_columns_for_level(self.level, self.column_name))
        sql = """
        SELECT {rc}, COUNT(unique_subscribers) AS unique_subscriber_counts FROM 
        (SELECT 
            DISTINCT
            {rc},
            all_locs.subscriber as unique_subscribers
        FROM ({all_locs}) AS all_locs) AS _
        GROUP BY {rc} 
        """.format(
            all_locs=self.ul.get_query(), rc=relevant_columns
        )
        return sql
