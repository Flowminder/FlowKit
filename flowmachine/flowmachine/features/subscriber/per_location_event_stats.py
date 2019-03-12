# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List

from ...core import JoinToLocation
from flowmachine.utils import get_columns_for_level
from ..utilities.sets import EventsTablesUnion
from .metaclasses import SubscriberFeature

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}


class PerLocationEventStats(SubscriberFeature):
    """
    This class returns the statistics of event count per location per
    subscriber within the period, optionally limited to only incoming or
    outgoing events. For instance, it calculates the average number of events
    per cell per subscriber.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'avg'
        Defaults to avg, aggregation statistic over the durations.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'}, default 'out'
        Whether to consider calls made, received, or both. Defaults to 'out'.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables
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

    Examples
    --------

    >>> s = PerLocationEventStats("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()

          subscriber      value
    OemQ7q2DLZMWnwzB   1.388889
    By4j6PKdB4NGMpxr   1.421053
    L4V537alj321eWz6   1.130435
    4pQo67v0PWyLdYKO   1.400000
    8br1gO32xWXxjY0R   1.100000
                 ...        ...

    """

    def __init__(
        self,
        start,
        stop,
        statistic="avg",
        *,
        level="cell",
        hours="all",
        tables="all",
        subscriber_identifier="msisdn",
        direction="both",
        subscriber_subset=None,
        column_name=None,
        size=None,
        polygon_table=None,
        geom_col="geom",
    ):
        self.start = start
        self.stop = stop
        self.level = level
        self.hours = hours
        self.tables = tables
        self.subscriber_identifier = subscriber_identifier
        self.direction = direction
        self.column_name = column_name
        self.statistic = statistic

        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        if self.direction in {"both"}:
            column_list = [self.subscriber_identifier, "location_id"]
        elif self.direction in {"in", "out"}:
            column_list = [self.subscriber_identifier, "location_id", "outgoing"]
        else:
            raise ValueError("{} is not a valid direction.".format(self.direction))

        if self.level != "cell":
            column_list.append("datetime")

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.tables,
            columns=column_list,
            hours=hours,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )

        if self.level != "cell":
            self.unioned_query = JoinToLocation(
                self.unioned_query,
                level=self.level,
                column_name=self.column_name,
                time_col="datetime",
                size=size,
                polygon_table=polygon_table,
                geom_col=geom_col,
            )

        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):
        loc_cols = ", ".join(get_columns_for_level(self.level, self.column_name))

        where_clause = ""
        if self.direction != "both":
            where_clause = (
                f"WHERE outgoing IS {'TRUE' if self.direction == 'out' else 'FALSE'}"
            )

        return f"""
        SELECT subscriber, {self.statistic}(events) AS value
        FROM (
            SELECT subscriber, {loc_cols}, COUNT(*) AS events
            FROM ({self.unioned_query.get_query()}) U
            {where_clause}
            GROUP BY subscriber, {loc_cols}
        ) U
        GROUP BY subscriber
        """
