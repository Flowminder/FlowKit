# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Intervent period statistics, such as the average and standard deviation of the
duration between calls.
"""
import warnings
from typing import List

from ..utilities import EventsTablesUnion
from .metaclasses import SubscriberFeature

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}


class IntereventPeriod(SubscriberFeature):
    """
    This class calculates intervent period statistics such as the average and
    standard deviation of the duration between calls.

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
        start,
        stop,
        statistic="avg",
        *,
        hours="all",
        tables="all",
        subscriber_identifier="msisdn",
        subscriber_subset=None,
        direction="both",
    ):

        self.start = start
        self.stop = stop
        self.hours = hours
        self.tables = tables
        self.subscriber_identifier = subscriber_identifier
        self.direction = direction

        if self.direction in {"both"}:
            column_list = [self.subscriber_identifier, "datetime"]
        elif self.direction in {"in", "out"}:
            column_list = [self.subscriber_identifier, "datetime", "outgoing"]
        else:
            raise ValueError("{} is not a valid direction.".format(self.direction))

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

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

        where_clause = ""
        if self.direction != "both":
            where_clause = (
                f"WHERE outgoing IS {'TRUE' if self.direction == 'out' else 'FALSE'}"
            )

        # Postgres does not support the following three operations with intervals
        if self.statistic in {"median", "stddev", "variance"}:
            statistic_clause = (
                f"MAKE_INTERVAL(secs => {self.statistic}(EXTRACT(EPOCH FROM delta)))"
            )
        else:
            statistic_clause = f"{self.statistic}(delta)"

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
