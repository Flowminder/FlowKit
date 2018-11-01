# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Computes the last location an subscriber was identified
at during a specified time period.



"""
from ..utilities.subscriber_locations import BaseLocation
from ..utilities.subscriber_locations import subscriber_locations
from ...utils.utils import get_columns_for_level


class LastLocation(BaseLocation):
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
    level : str, default 'admin3'
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
    column_name : str, optional
        Option, none-standard, name of the column that identifies the
        spatial level, i.e. could pass admin3pcod to use the admin 3 pcode
        as opposed to the name of the region.
    kwargs:
        Eventually passed to flowmachine.spatial_metrics.spatial_helpers.
        JoinToLocation. Here you can specify a non standard set of polygons.
        See the doc string of JoinToLocation for more details.

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
        level="admin3",
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        column_name=None,
        **kwargs
    ):

        self.start = start
        self.stop = stop
        self.level = level
        self.hours = hours
        self.table = table
        self.subscriber_identifier = subscriber_identifier
        self.column_name = column_name
        self.subscriber_locs = subscriber_locations(
            self.start,
            self.stop,
            self.level,
            self.hours,
            table=self.table,
            subscriber_identifier=self.subscriber_identifier,
            column_name=self.column_name,
            **kwargs
        )
        super().__init__()

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        relevant_columns = ",".join(get_columns_for_level(self.level, self.column_name))

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
