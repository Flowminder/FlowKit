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
    direction : {'in', 'out', 'both'}, default 'out'
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

        subscriber       interevent_period_avg
        JZoaw2jzvK2QMKYX       03:26:47.673913
        1QBlwRo4Kd5v3Ogz       03:23:03.979167
        V4DJ7AaxLLOyWEZ3       03:39:35.977778
        woOyJvzKBALYj8za       03:14:27.780000
        1NqnrAB9bRd597x2       03:57:17.926829
                     ...                   ...

    """

    def __init__(
        self,
        start: str,
        stop: str,
        statistic: str = "avg",
        *,
        time_resolution: str = "hour",
        hours: Union[str, Tuple[int, int]] = "all",
        tables: Union[str, List[str]] = "all",
        subscriber_identifier: str = "msisdn",
        subscriber_subset: Optional[Query] = None,
        direction: str = "both",
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
            EXTRACT(epoch FROM value/{self.time_divisor}) AS value
        FROM ({self.event_interval.get_query()}) AS U
        """

        return sql
