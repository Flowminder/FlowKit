# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Total and per-counterpart call durations for subscribers.



"""
import warnings
from typing import List

from ...core import JoinToLocation
from flowmachine.utils import get_columns_for_level
from ..utilities import EventsTablesUnion
from .metaclasses import SubscriberFeature

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}


class SubscriberCallDurations(SubscriberFeature):
    """
    This class returns the total amount of time a subscriber spent calling
    within the period, optionally limited to only calls they made, or received.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
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
    statistic :  {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.


    Examples
    --------

    >>> s = SubscriberCallDurations("2016-01-01", "2016-01-07", direction="in")
    >>> s.get_dataframe()

                   msisdn  duration_sum
    0    jWlyLwbGdvKV35Mm          4038.0
    1    EreGoBpxJOBNl392         12210.0
    2    nvKNoAmxMvBW4kJr         10847.0
    3    VkzMxYjv7mYn53oK         48374.0
    4    BKMy1nYEZpnoEA7G          8697.0
    ...

    """

    def __init__(
        self,
        start,
        stop,
        subscriber_identifier="msisdn",
        direction="out",
        statistic="sum",
        *,
        hours="all",
        subscriber_subset=None,
        level=None,
        size=None,
        column_name=None,
        polygon_table=None,
    ):
        self.start = start
        self.stop = stop
        self.subscriber_identifier = subscriber_identifier
        self.hours = hours
        self.direction = direction
        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )
        if direction not in {"in", "out", "both"}:
            raise ValueError("{} is not a valid direction.".format(self.direction))

        column_list = [self.subscriber_identifier, "outgoing", "duration"]
        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables="events.calls",
            columns=column_list,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=subscriber_identifier,
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", f"duration_{self.statistic}"]

    def _make_query(self):
        where_clause = ""
        if self.direction != "both":
            where_clause = "WHERE {}outgoing".format(
                "" if self.direction == "out" else "NOT "
            )
        return f"""
        SELECT subscriber, {self.statistic}(duration) as duration_{self.statistic} FROM 
        ({self.unioned_query.get_query()}) u
        {where_clause}
        GROUP BY subscriber
        """


class PerLocationSubscriberCallDurations(SubscriberFeature):
    """
    This class returns the total amount of time a subscriber spent calling
    within the period, optionally limited to only calls they made, or received,
    faceted by their location at the time.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
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
    column_name : str
        Optionally specify a non-default column name. Required if level is 'polygon'.
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.


    Examples
    --------

    >>> s = PerLocationSubscriberCallDurations("2016-01-01", "2016-01-07", direction="in")
    >>> s.get_dataframe()

                subscriber            name  duration_sum
    0     038OVABN11Ak4W5P         Baglung          1979.0
    1     038OVABN11Ak4W5P           Banke          2204.0
    2     038OVABN11Ak4W5P           Dolpa          9169.0
    ...

    """

    def __init__(
        self,
        start,
        stop,
        subscriber_identifier="msisdn",
        direction="out",
        level="admin3",
        statistic="sum",
        column_name=None,
        *,
        hours="all",
        subscriber_subset=None,
        size=None,
        polygon_table=None,
        geom_col="geom",
    ):
        self.start = start
        self.stop = stop
        self.subscriber_identifier = subscriber_identifier
        self.direction = direction
        self.level = level
        self.column_name = column_name
        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )
        if direction not in {"in", "out", "both"}:
            raise ValueError("{} is not a valid direction.".format(self.direction))

        column_list = [
            self.subscriber_identifier,
            "msisdn_counterpart",
            "outgoing",
            "duration",
            "location_id",
        ]
        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables="events.calls",
            columns=column_list,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=subscriber_identifier,
        )
        if self.level != "cell":
            etu = EventsTablesUnion(
                self.start,
                self.stop,
                tables="events.calls",
                columns=column_list + ["datetime"],
                hours=hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=self.subscriber_identifier,
            )

            self.unioned_query = JoinToLocation(
                etu,
                level=self.level,
                column_name=self.column_name,
                time_col="datetime",
                size=size,
                polygon_table=polygon_table,
                geom_col=geom_col,
            )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return (
            ["subscriber"]
            + get_columns_for_level(self.level, self.column_name)
            + [f"duration_{self.statistic}"]
        )

    def _make_query(self):
        loc_cols = ", ".join(get_columns_for_level(self.level, self.column_name))
        where_clause = ""
        if self.direction != "both":
            where_clause = "WHERE {}outgoing".format(
                "" if self.direction == "out" else "NOT "
            )
        return f"""
        SELECT subscriber, {loc_cols}, {self.statistic}(duration) as duration_{self.statistic} 
        FROM ({self.unioned_query.get_query()}) u
        {where_clause}
        GROUP BY subscriber, {loc_cols}
        """


class PairedSubscriberCallDurations(SubscriberFeature):
    """
    This class returns the total amount of time a subscriber spent calling
    each other subscriber within the period.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.


    Examples
    --------

    >>> s = PairedSubscriberCallDurations("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()

               subscriber msisdn_counterpart  duration_sum
    0    038OVABN11Ak4W5P   BVYqp0ryO1oj1gRo       10833.0
    1    09NrjaNNvDanD8pk   mJ9eZYnvvr5YGW2j       17028.0
    2    0ayZGYEQrqYlKw6g   Q1jMk7qjqXBnwoDR       10465.0
    3    0DB8zw67E9mZAPK2   jpXleRQqrLR0aPwN       14344.0
    ...

    """

    def __init__(
        self,
        start,
        stop,
        *,
        level=None,
        size=None,
        column_name=None,
        polygon_table=None,
        subscriber_identifier="msisdn",
        statistic="sum",
        hours="all",
        subscriber_subset=None,
    ):
        self.start = start
        self.stop = stop
        self.subscriber_identifier = subscriber_identifier

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        column_list = [
            self.subscriber_identifier,
            "outgoing",
            "duration",
            "msisdn_counterpart",
        ]

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables="events.calls",
            columns=column_list,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=self.subscriber_identifier,
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "msisdn_counterpart", f"duration_{self.statistic}"]

    def _make_query(self):
        return f"""
        SELECT subscriber, msisdn_counterpart, {self.statistic}(duration) as duration_{self.statistic} 
        FROM ({self.unioned_query.get_query()}) u
        WHERE outgoing
        GROUP BY subscriber, msisdn_counterpart
        """


class PairedPerLocationSubscriberCallDurations(SubscriberFeature):
    """
    This class returns the total amount of time a subscriber spent calling
    each other subscriber within the period, faceted by their respective
    locations at the time.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
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
    column_name : str
        Optionally specify a non-default column name. Required if level is 'polygon'.
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to 'sum', aggregation statistic over the durations.


    Examples
    --------
    >>> s = PairedPerLocationSubscriberCallDurations("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()

                subscriber msisdn_counterpart           name name_counterpart  \
    0     038OVABN11Ak4W5P   BVYqp0ryO1oj1gRo          Dolpa          Kailali
    1     038OVABN11Ak4W5P   BVYqp0ryO1oj1gRo          Dolpa           Rasuwa
    2     038OVABN11Ak4W5P   BVYqp0ryO1oj1gRo           Mugu          Baglung
    ...
              duration_sum
    0             2756.0
    1             1885.0
    2             1027.0

    Notes
    -----
    This query is currently *very slow*.

    """

    def __init__(
        self,
        start,
        stop,
        subscriber_identifier="msisdn",
        level="admin3",
        column_name=None,
        statistic="sum",
        *,
        hours="all",
        subscriber_subset=None,
        size=None,
        polygon_table=None,
        geom_col="geom",
    ):
        self.start = start
        self.stop = stop
        self.subscriber_identifier = subscriber_identifier
        self.level = level
        self.column_name = column_name
        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        column_list = [
            "id",
            self.subscriber_identifier,
            "msisdn_counterpart",
            "outgoing",
            "duration",
            "location_id",
        ]
        unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables="events.calls",
            columns=column_list,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=self.subscriber_identifier,
        )
        if self.level != "cell":
            etu = EventsTablesUnion(
                self.start,
                self.stop,
                tables="events.calls",
                columns=column_list + ["datetime"],
                hours=hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=self.subscriber_identifier,
            )
            unioned_query = JoinToLocation(
                etu,
                level=self.level,
                column_name=self.column_name,
                time_col="datetime",
                size=size,
                polygon_table=polygon_table,
                geom_col=geom_col,
            )
        self.joined = unioned_query.subset("outgoing", "t").join(
            unioned_query.subset("outgoing", "f"),
            on_left="id",
            on_right="id",
            right_append="_counterpart",
            how="left",
        )
        warnings.warn("This query is considerably slower than the other variants.")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return (
            ["subscriber", "msisdn_counterpart"]
            + get_columns_for_level(self.level, self.column_name)
            + [
                f"{x}_counterpart"
                for x in get_columns_for_level(self.level, self.column_name)
            ]
            + [f"duration_{self.statistic}"]
        )

    def _make_query(self):
        loc_cols = get_columns_for_level(self.level, self.column_name)
        loc_cols += [
            "{}_counterpart".format(c)
            for c in get_columns_for_level(self.level, self.column_name)
        ]
        loc_cols = ", ".join(loc_cols)

        return f"""
        SELECT subscriber, msisdn_counterpart, {loc_cols}, {self.statistic}(duration) as duration_{self.statistic}
         FROM ({self.joined.get_query()}) u
        GROUP BY subscriber, msisdn_counterpart, {loc_cols}
        """
