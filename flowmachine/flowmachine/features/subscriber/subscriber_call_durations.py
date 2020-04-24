# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Total and per-counterpart call durations for subscribers.



"""
import warnings
from typing import List, Optional, Union

from flowmachine.core import location_joined_query
from flowmachine.core.spatial_unit import AnySpatialUnit, make_spatial_unit
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import make_where, standardise_date

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
    direction : {'in', 'out', 'both'} or Direction, default Direction.OUT
        Whether to consider calls made, received, or both. Defaults to 'out'.
    statistic :  {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.


    Examples
    --------

    >>> s = SubscriberCallDurations("2016-01-01", "2016-01-07", direction="in")
    >>> s.get_dataframe()

                   msisdn           value
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
        *,
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.OUT,
        statistic="sum",
        hours="all",
        subscriber_subset=None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        self.hours = hours
        self.direction = Direction(direction)
        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        column_list = [
            self.subscriber_identifier,
            "duration",
            *self.direction.required_columns,
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
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        where_clause = make_where(self.direction.get_filter_clause())

        return f"""
        SELECT subscriber, {self.statistic}(duration) as value FROM 
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
    direction : {'in', 'out', 'both'} or Direction, default Direction.OUT
        Whether to consider calls made, received, or both. Defaults to 'out'.
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.


    Examples
    --------

    >>> s = PerLocationSubscriberCallDurations("2016-01-01", "2016-01-07", direction="in")
    >>> s.get_dataframe()

                subscriber            name          value
    0     038OVABN11Ak4W5P         Baglung          1979.0
    1     038OVABN11Ak4W5P           Banke          2204.0
    2     038OVABN11Ak4W5P           Dolpa          9169.0
    ...

    """

    def __init__(
        self,
        start,
        stop,
        *,
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.OUT,
        statistic="sum",
        spatial_unit: Optional[AnySpatialUnit] = None,
        hours="all",
        subscriber_subset=None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        self.direction = Direction(direction)
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=3)
        else:
            self.spatial_unit = spatial_unit
        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        column_list = [
            self.subscriber_identifier,
            "msisdn_counterpart",
            "duration",
            "location_id",
            "datetime",
            *self.direction.required_columns,
        ]
        self.unioned_query = location_joined_query(
            EventsTablesUnion(
                self.start,
                self.stop,
                tables="events.calls",
                columns=column_list,
                hours=hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=self.subscriber_identifier,
            ),
            spatial_unit=self.spatial_unit,
            time_col="datetime",
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"] + self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):
        loc_cols = ", ".join(self.spatial_unit.location_id_columns)
        where_clause = make_where(self.direction.get_filter_clause())

        return f"""
        SELECT subscriber, {loc_cols}, {self.statistic}(duration) as value 
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

               subscriber msisdn_counterpart  value
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
        subscriber_identifier="msisdn",
        statistic="sum",
        hours="all",
        subscriber_subset=None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
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
        return ["subscriber", "msisdn_counterpart", "value"]

    def _make_query(self):
        return f"""
        SELECT subscriber, msisdn_counterpart, {self.statistic}(duration) as value 
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
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
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
              value
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
        *,
        subscriber_identifier="msisdn",
        statistic="sum",
        spatial_unit: Optional[AnySpatialUnit] = None,
        hours="all",
        subscriber_subset=None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=3)
        else:
            self.spatial_unit = spatial_unit
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
            "datetime",
        ]
        unioned_query = location_joined_query(
            EventsTablesUnion(
                self.start,
                self.stop,
                tables="events.calls",
                columns=column_list,
                hours=hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=self.subscriber_identifier,
            ),
            spatial_unit=self.spatial_unit,
            time_col="datetime",
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
            + self.spatial_unit.location_id_columns
            + [f"{x}_counterpart" for x in self.spatial_unit.location_id_columns]
            + ["value"]
        )

    def _make_query(self):
        loc_cols = self.spatial_unit.location_id_columns
        loc_cols += [
            "{}_counterpart".format(c) for c in self.spatial_unit.location_id_columns
        ]
        loc_cols = ", ".join(loc_cols)

        return f"""
        SELECT subscriber, msisdn_counterpart, {loc_cols}, {self.statistic}(duration) as value
         FROM ({self.joined.get_query()}) u
        GROUP BY subscriber, msisdn_counterpart, {loc_cols}
        """
