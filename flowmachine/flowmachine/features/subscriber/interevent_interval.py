# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Intervent period statistics, such as the average and standard deviation of the
duration between calls.
"""

from typing import Union, Tuple, List, Optional

from flowmachine.core import Query
from flowmachine.features.utilities import EventsTablesUnion
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import make_where, standardise_date, Statistic


class IntereventInterval(SubscriberFeature):
    """
    This class calculates intervent period statistics such as the average and
    standard deviation of the duration between calls and returns them as time
    intervals.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    time_resolution : str
        Temporal resolution to return results at, e.g. 'hour' for fractional hours.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'} or Direction, default Direction.OUT
        Whether to consider calls made, received, or both. Defaults to 'out'.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables
    statistic :  Statistic, default Statistic.AVG
        Defaults to avg, aggregation statistic over the durations.

    Examples
    --------

    >>> s = IntereventInterval("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()
               subscriber           value
    0    038OVABN11Ak4W5P 04:57:22.428571
    1    09NrjaNNvDanD8pk 03:52:38.454545
    2    0ayZGYEQrqYlKw6g 04:02:05.666667
    3    0DB8zw67E9mZAPK2 06:32:30.714285
    4    0Gl95NRLjW2aw8pW 05:44:20.625000
    ..                ...             ...
    495  ZQG8glazmxYa1K62 04:12:27.705882
    496  Zv4W9eak2QN1M5A7 03:41:10.323529
    497  zvaOknzKbEVD2eME 04:21:27.218750
    498  Zy3DkbY7MDd6Er7l 04:33:00.870968
    499  ZYPxqVGLzlQy6l7n 04:01:28.212121

    [500 rows x 2 columns]

    """

    def __init__(
        self,
        start: str,
        stop: str,
        statistic: Statistic = Statistic.AVG,
        *,
        hours: Union[str, Tuple[int, int]] = "all",
        tables: Union[str, List[str]] = "all",
        subscriber_identifier: str = "msisdn",
        subscriber_subset: Optional[Query] = None,
        direction: Union[str, Direction] = Direction.OUT,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.tables = tables
        self.subscriber_identifier = subscriber_identifier
        self.direction = Direction(direction)

        column_list = [
            self.subscriber_identifier,
            "datetime",
            *self.direction.required_columns,
        ]

        self.statistic = Statistic(statistic.lower())

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.tables,
            columns=column_list,
            hours=self.hours,
            subscriber_identifier=self.subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )
        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):
        where_clause = make_where(self.direction.get_filter_clause())

        # Postgres does not support the following three operations with intervals
        if self.statistic in {"median", "stddev", "variance"}:
            statistic_clause = (
                f"MAKE_INTERVAL(secs => {self.statistic:EXTRACT(EPOCH FROM delta)})"
            )
        else:
            statistic_clause = f"{self.statistic:delta}"

        sql = f"""
        SELECT
            subscriber,
            {statistic_clause} AS value
        FROM (
            SELECT subscriber, datetime - LAG(datetime, 1, NULL) OVER (PARTITION BY subscriber ORDER BY datetime) AS delta
            FROM ({self.unioned_query.get_query()}) AS U
            {where_clause}
        ) AS U
        GROUP BY subscriber
        """

        return sql
