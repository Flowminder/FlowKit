# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates the total number of unique sites or cells
at the network level.



"""
from typing import List

from ...core.mixins import GeoDataMixin
from ...core import JoinToLocation
from flowmachine.utils import get_columns_for_level
from ...core.query import Query
from ..utilities import EventsTablesUnion

valid_stats = {"avg", "max", "min", "median", "mode", "stddev", "variance"}
valid_periods = ["second", "minute", "hour", "day", "month", "year"]


class TotalNetworkObjects(GeoDataMixin, Query):
    """
    Class for calculating unique cells/sites per location
    and aggregate it by period.

    Parameters
    ----------
    start : datetime
        Start time to filter query.
    stop : datetime
        Stop time to filter query.
    period : {'second', 'minute', 'hour', 'day', 'month', 'year'}
        A period definition to group data by.
    table : str
        Either 'calls', 'sms', or other table under `events.*`. If
        no specific table is provided this will collect
        statistics from all tables.
    network_object : {'cell', 'versioned-cell', 'versioned-site'}
        Objects to track, defaults to 'cells', the unversioned lowest
        level of infrastructure available.
    level : {'adminN', 'grid', 'polygon'}
        Level to facet on, defaults to 'admin0'

    Other Parameters
    ----------------
    Passed to JoinToLocation

    Examples
    --------
    >>> t = TotalNetworkObjects()
    >>> t.get_dataframe()
       total                  datetime
    0     55 2016-01-01 00:00:00+00:00
    1     55 2016-01-02 00:00:00+00:00
    ...

    """

    def __init__(
        self,
        start=None,
        stop=None,
        *,
        table="all",
        period="day",
        network_object="cell",
        level="admin0",
        column_name=None,
        size=None,
        polygon_table=None,
        geom_col="geom",
        hours="all",
        subscriber_subset=None,
        subscriber_identifier="msisdn",
    ):
        self.table = table.lower()
        self.start = (
            self.connection.min_date(table=table).strftime("%Y-%m-%d")
            if start is None
            else start
        )
        self.stop = (
            self.connection.max_date(table=table).strftime("%Y-%m-%d")
            if stop is None
            else stop
        )
        if self.table != "all" and not self.table.startswith("events"):
            self.table = "events.{}".format(self.table)
        self.network_object = network_object.lower()
        if self.network_object == "cell":
            events = EventsTablesUnion(
                self.start,
                self.stop,
                tables=self.table,
                columns=["location_id", "datetime"],
                hours=hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=subscriber_identifier,
            )
        elif self.network_object in {"versioned-cell", "versioned-site"}:
            events = EventsTablesUnion(
                self.start,
                self.stop,
                tables=self.table,
                columns=["location_id", "datetime"],
                hours=hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=subscriber_identifier,
            )
            events = JoinToLocation(
                events,
                level=self.network_object,
                time_col="datetime",
                column_name=column_name,
                size=size,
                polygon_table=polygon_table,
                geom_col=geom_col,
            )
        else:
            raise ValueError("{} is not a valid network object.".format(network_object))
        self.level = level.lower()
        if self.level in {
            "cell",
            "versioned-cell",
            "versioned-site",
        }:  # No sense in aggregating network object to
            raise ValueError("{} is not a valid level".format(level))  # network object
        self.joined = JoinToLocation(
            events,
            level=level,
            time_col="datetime",
            column_name=column_name,
            size=size,
            polygon_table=polygon_table,
            geom_col=geom_col,
        )
        self.period = period.lower()
        if self.period not in valid_periods:
            raise ValueError("{} is not a valid period.".format(self.period))

        # FIXME: we are only storing these here so that they can be accessed by
        #        AggregateNetworkObjects.from_total_network_objects() below. This
        #        should be refactored soon.
        self.column_name = column_name
        self.size = size
        self.polygon_table = polygon_table
        self.geom_col = geom_col
        self.hours = hours
        self.subscriber_subset = subscriber_subset
        self.subscriber_identifier = subscriber_identifier

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return get_columns_for_level(self.level, self.joined.column_name) + [
            "total",
            "datetime",
        ]

    def _make_query(self):
        cols = ",".join(get_columns_for_level(self.network_object))
        group_cols = ",".join(
            get_columns_for_level(self.level, self.joined.column_name)
        )
        sql = """
        SELECT {group_cols}, COUNT(*) as total,
             datetime FROM
              (SELECT DISTINCT {group_cols}, {cols}, datetime FROM           
                (SELECT {group_cols}, {cols}, date_trunc('{period}', x.datetime) AS datetime
                FROM ({etu}) x) y) _
            GROUP BY {group_cols}, datetime
            ORDER BY {group_cols}, datetime
        """.format(
            period=self.period,
            etu=self.joined.get_query(),
            cols=cols,
            group_cols=group_cols,
        )

        return sql

    def aggregate(self, by=None, statistic="avg"):
        """

        Parameters
        ----------
        by : {'second', 'minute', 'hour', 'day', 'month', 'year'}
            Defaults to one level higher than the period
        statistic : {'avg', 'median'}
            Statistic to calculate, defaults to 'avg'.

        Returns
        -------
        AggregateNetworkObjects
        """
        return AggregateNetworkObjects.from_total_network_objects(
            self, by=by, statistic=statistic
        )


class AggregateNetworkObjects(GeoDataMixin, Query):
    """
    Class for calculating statistics about unique cells/sites
    and aggregate it by period.

    Parameters
    ----------
    start : datetime
        Start time to filter query.
    stop : datetime
        Stop time to filter query.
    period : {'second', 'minute', 'hour', 'day', 'month', 'year'}
        A period definition to group data by.
    by : {'second', 'minute', 'hour', 'day', 'month', 'year', 'century'}
        A period definition to calculate statistics over, defaults to the one
        greater than period.
    table : str
        Either 'calls', 'sms', or other table under `events.*`. If
        no specific table is provided this will collect
        statistics from all tables.
    statistic : {'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}
        Statistic to calculate, defaults to 'avg'.
    object : {'cell', 'versioned-cell', 'versioned-site'}
        Objects to track, defaults to 'cells', the unversioned lowest
        level of infrastructure available.
    level : {'adminN', 'grid', 'polygon'}
        Level to facet at, defaults to 'admin0'

    Other Parameters
    ----------------
    Passed to JoinToLocation

    Examples
    --------
    >>> t = AggregateNetworkObjects()
    >>> t.get_dataframe()
          name  total                  datetime
    0  Nepal     55 2016-01-01 00:00:00+00:00
    1  Nepal     55 2016-01-02 00:00:00+00:00
    2  Nepal     55 2016-01-03 00:00:00+00:00
    3  Nepal     55 2016-01-04 00:00:00+00:00
    4  Nepal     55 2016-01-05 00:00:00+00:00
    ...

    """

    def __init__(
        self,
        start=None,
        stop=None,
        statistic="avg",
        table="all",
        period="day",
        by=None,
        network_object="cell",
        level="admin0",
        column_name=None,
        size=None,
        polygon_table=None,
        geom_col="geom",
        hours="all",
        subscriber_subset=None,
        subscriber_identifier="msisdn",
    ):
        self.total_objs = TotalNetworkObjects(
            start=start,
            stop=stop,
            table=table,
            period=period,
            network_object=network_object,
            level=level,
            column_name=column_name,
            size=size,
            polygon_table=polygon_table,
            geom_col=geom_col,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=subscriber_identifier,
        )
        statistic = statistic.lower()
        if statistic in valid_stats:
            self.statistic = statistic
        else:
            raise ValueError(
                "{} is not a valid statistic use one of {!r}".format(
                    statistic, valid_stats
                )
            )
        if by is None:
            if period == "second":
                self.by = "minute"
            elif period == "minute":
                self.by = "hour"
            elif period == "hour":
                self.by = "day"
            elif period == "day":
                self.by = "month"
            elif period == "month":
                self.by = "year"
            else:
                self.by = "century"
        else:
            self.by = by
        if self.by not in valid_periods + ["century"]:
            raise ValueError(
                "{} is not a valid period to aggregate by.".format(self.by)
            )

        super().__init__()

    @classmethod
    def from_total_network_objects(cls, total_objs, statistic, by):
        """

        Parameters
        ----------
        total_objs : TotalNetworkObjects
            Object to aggregate
        statistic : {'avg', 'median'}
            Stat to calculate
        by : {'second', 'minute', 'hour', 'day', 'month', 'year'}
             Time period to aggregate to

        Returns
        -------
        AggregateNetworkObjects

        """
        return cls(
            start=total_objs.start,
            stop=total_objs.stop,
            table=total_objs.table,
            period=total_objs.period,
            network_object=total_objs.network_object,
            statistic=statistic,
            by=by,
            level=total_objs.level,
            column_name=total_objs.column_name,
            size=total_objs.size,
            polygon_table=total_objs.polygon_table,
            geom_col=total_objs.geom_col,
            hours=total_objs.hours,
            subscriber_subset=total_objs.subscriber_subset,
            subscriber_identifier=total_objs.subscriber_identifier,
        )

    @property
    def column_names(self) -> List[str]:
        return get_columns_for_level(
            self.total_objs.level, self.total_objs.joined.column_name
        ) + [self.statistic, "datetime"]

    def _make_query(self):
        group_cols = ",".join(
            get_columns_for_level(
                self.total_objs.level, self.total_objs.joined.column_name
            )
        )
        sql = """
        SELECT {group_cols}, {stat}(z.total) as {stat},
        date_trunc('{by}', z.datetime) as datetime FROM 
            ({totals}) z
        GROUP BY {group_cols}, date_trunc('{by}', z.datetime)
        ORDER BY {group_cols}, date_trunc('{by}', z.datetime)
        """.format(
            by=self.by,
            stat=self.statistic,
            totals=self.total_objs.get_query(),
            group_cols=group_cols,
        )
        return sql
