# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Intervent period statistics, such as the average and standard deviation of the
duration between calls.
"""
from typing import List, Union, Tuple, Optional

from flowmachine.core import Query
from flowmachine.features.subscriber.interevent_interval import IntereventInterval
from .metaclasses import SubscriberFeature
from ..utilities.direction_enum import Direction
from ...core.statistic_types import Statistic

time_resolutions = dict(
    second=1, minute=60, hour=3600, day=86400, month=2592000, year=31557600
)


class IntereventPeriod(SubscriberFeature):
    """
    This class calculates intervent period statistics such as the average and
    standard deviation of the duration between calls and returns them as fractional
    time units.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    time_resolution : {'second', 'minute', 'hour', 'day', 'month', 'year'}, default 'hour'
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
    statistic :  {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'avg'
        Defaults to sum, aggregation statistic over the durations.

    Examples
    --------

    >>> s = IntereventPeriod("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()
               subscriber     value
    0    038OVABN11Ak4W5P  4.956230
    1    09NrjaNNvDanD8pk  3.877348
    2    0ayZGYEQrqYlKw6g  4.034907
    3    0DB8zw67E9mZAPK2  6.541865
    4    0Gl95NRLjW2aw8pW  5.739062
    ..                ...       ...
    495  ZQG8glazmxYa1K62  4.207696
    496  Zv4W9eak2QN1M5A7  3.686201
    497  zvaOknzKbEVD2eME  4.357561
    498  Zy3DkbY7MDd6Er7l  4.550242
    499  ZYPxqVGLzlQy6l7n  4.024503

    [500 rows x 2 columns]
    """

    def __init__(
        self,
        start: str,
        stop: str,
        statistic: Statistic = Statistic.AVG,
        *,
        time_resolution: str = "hour",
        hours: Union[str, Tuple[int, int]] = "all",
        tables: Union[str, List[str]] = "all",
        subscriber_identifier: str = "msisdn",
        subscriber_subset: Optional[Query] = None,
        direction: Union[str, Direction] = Direction.OUT,
    ):
        try:
            self.time_divisor = time_resolutions[time_resolution.lower()]
        except KeyError:
            raise ValueError(
                f"'{time_resolution}' is not a valid time resolution. Use one of {time_resolutions.keys()}"
            )

        self.event_interval = IntereventInterval(
            start=start,
            stop=stop,
            statistic=statistic,
            hours=hours,
            tables=tables,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
            direction=direction,
        )
        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):
        sql = f"""
        SELECT
            subscriber,
            FLOOR(EXTRACT(epoch FROM value)/{self.time_divisor}) AS value
        FROM ({self.event_interval.get_query()}) AS U
        """

        return sql
