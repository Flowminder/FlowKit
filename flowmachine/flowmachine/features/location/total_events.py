# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List, Union

"""
Calculates the number of events at a location
during a specified time period.



"""
from ...core import JoinToLocation
from ..utilities import EventsTablesUnion

from ...core import Query
from ...core.mixins import GeoDataMixin

from flowmachine.utils import get_columns_for_level


class _TotalCellEvents(Query):
    def __init__(
        self,
        start: str,
        stop: str,
        *,
        table: Union[None, List[str]] = None,
        interval: str = "hour",
        direction: str = "both",
        hours="all",
        subscriber_subset=None,
        subscriber_identifier="msisdn",
    ):
        self.start = start
        self.stop = stop
        self.table = table
        self.interval = interval
        self.direction = direction

        if self.interval not in TotalLocationEvents.allowed_intervals:
            raise ValueError(
                "'Interval must be one of: {} got: {}".format(
                    TotalLocationEvents.allowed_intervals, self.interval
                )
            )

        self.time_cols = ["(datetime::date)::text AS date"]
        if self.interval == "hour" or self.interval == "min":
            self.time_cols.append("extract(hour FROM datetime) AS hour")
        if self.interval == "min":
            self.time_cols.append("extract(minute FROM datetime) AS min")

        self.cols = ["location_id", "datetime"]
        # if we need to filter on outgoing/incoming calls, we will also fetch this
        # column. Don't fetch it if it is not needed for both efficiency and the
        # possibility that we might want to do pass another data type which does not
        # have this information.
        if self.direction != "both":
            self.cols += ["outgoing"]
        if self.direction not in ["in", "out", "both"]:
            raise ValueError("Unrecognised direction: {}".format(self.direction))

        # list of columns that we want to group by, these are all the time
        # columns, plus the cell column
        self.groups = [x.split(" AS ")[0] for x in self.time_cols + ["location_id"]]
        self.unioned = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.table,
            columns=self.cols,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=subscriber_identifier,
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return (
            ["location_id"] + [x.split(" AS ")[1] for x in self.time_cols] + ["total"]
        )

    def _make_query(self):
        # Firstly get the result at the level of the cell,
        # we'll do some further aggregation if necessary later

        # Set a filter clause based on the direction of the event
        if self.direction == "both":
            filter_clause = ""
        elif self.direction == "in":
            filter_clause = "WHERE NOT outgoing"
        elif self.direction == "out":
            filter_clause = "WHERE outgoing"
        else:
            raise ValueError("Unrecognised direction: {}".format(self.direction))

        # We now need to group this table by the relevant columns in order to
        # get a count per region
        sql = """

            SELECT
                location_id,
                {time_cols},
                count(*) AS total
            FROM
                ({union}) unioned
            {filter}
            GROUP BY
                {groups}

        """.format(
            time_cols=", ".join(self.time_cols),
            union=self.unioned.get_query(),
            groups=", ".join(self.groups),
            filter=filter_clause,
        )

        return sql


class TotalLocationEvents(GeoDataMixin, Query):
    """
    Calculates the total number of events on an hourly basis
    per location (such as a tower or admin region),
    and per interaction type.

    Parameters
    ----------
    start : str
        ISO format date string to at which to start the analysis
    stop : str
        As above for the end of the analysis
    table : str, default 'all'
        Specifies a table of cdr data on which to base the analysis. Table must
        exist in events schema. If 'all' then we use all tables specified in
        flowmachine.yml.
    level : str
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
    interval : ['hour', 'day', 'min']
        Records activity on an hourly, daily, or by minute level.
    direction : str, default 'both'
        Look only at incoming or outgoing events. Can be either
        'out', 'in' or 'both'.
    column_name : str, optional
        Option, none-standard, name of the column that identifies the
        spatial level, i.e. could pass admin3pcod to use the admin 3 pcode
        as opposed to the name of the region.

    """

    allowed_intervals = {"day", "hour", "min"}

    def __init__(
        self,
        start: str,
        stop: str,
        *,
        table: Union[None, List[str]] = None,
        level: str = "cell",
        interval: str = "hour",
        direction: str = "both",
        column_name: Union[str, None] = None,
        hours="all",
        subscriber_subset=None,
        subscriber_identifier="msisdn",
        size=None,
        polygon_table=None,
        geom_col="geom",
    ):
        self.start = start
        self.stop = stop
        self.table = table
        self.level = level
        self.interval = interval
        self.direction = direction
        self.column_name = column_name

        if self.interval not in self.allowed_intervals:
            raise ValueError(
                "'Interval must be one of: {} got: {}".format(
                    self.allowed_intervals, self.interval
                )
            )
        self._obj = _TotalCellEvents(
            start,
            stop,
            table=table,
            interval=interval,
            direction=direction,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=subscriber_identifier,
        )
        if level != "cell":
            self._obj = JoinToLocation(
                self._obj,
                level=self.level,
                time_col="date",
                column_name=column_name,
                size=size,
                polygon_table=polygon_table,
                geom_col=geom_col,
            )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        cols = get_columns_for_level(self.level, self.column_name) + ["date"]
        if self.interval == "hour" or self.interval == "min":
            cols += ["hour"]
        if self.interval == "min":
            cols += ["min"]
        cols += ["total"]
        return cols

    def _make_query(self):
        # Grouped now represents a query with activities on the level of the cell,
        # if that is what the user has asked for then we are done, otherwise
        # we need to do the appropriate join and do a further group by.
        if self.level == "cell":
            sql = self._obj.get_query()
            cols = self._obj.groups
        # Otherwise we're after lat-lon, or an admin region.
        # in either case we need to join with the cell data
        else:
            cols = ", ".join(get_columns_for_level(self.level, self.column_name))
            cols += ", " + self._obj.time_col
            if self.interval == "hour" or self.interval == "min":
                cols += ",hour"
            if self.interval == "min":
                cols += ",min"

            sql = """

                SELECT
                    {cols},
                    sum(total) AS total
                FROM
                    ({j}) AS j
                GROUP BY
                    {cols}
                ORDER BY {cols}
            """.format(
                cols=cols, j=self._obj.get_query()
            )

        return sql
