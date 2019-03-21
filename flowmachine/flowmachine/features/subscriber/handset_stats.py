# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}
valid_characteristics = {
    "width",
    "height",
    "depth",
    "weight",
    "display_width",
    "display_height",
}

from ...core import Table
from .metaclasses import SubscriberFeature
from .subscriber_tacs import SubscriberHandsets


class HandsetStats(SubscriberFeature):
    """
    This class calculates statistics associated with numeric fields of the TAC
    table, such as width, height, etc.

    A subscriber might use different phones for different periods of time. In
    order to take the different usage periods into account we must calculate
    weighted statistics of the desired characteristics. As such, here we
    calculate the weighted characteristic, weighted by the number of seconds a
    subscriber held that handset.  Given that we only learn about changes in
    handset when an event occurs, this average will be biased.  For instance,
    if a subscriber changes his handset straight after an event occur, we will
    be tracking the handset that the subscriber used to handle the event and
    not the handset he actually switched to. We will only learn about the new
    handset once a new event occurs.

    We further assume that the handset held before the first observed event in
    the series is equal to the handset of the first event. Likewise, we assume
    that the handset held after the last observed event in the series is the
    handset of the last event.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    characteristic: {"width", "height", "depth", "weight", "display_width", "display_height"}
        Numeric handset characteristics present in the TAC table.
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Examples
    --------

    >>> s = HandsetStats("2016-01-01", "2016-01-08", "width")
    >>> s.get_dataframe()
    """

    def __init__(
        self,
        start,
        stop,
        characteristic,
        statistic="avg",
        *,
        subscriber_identifier="msisdn",
        hours="all",
        subscriber_subset=None,
        tables="all",
    ):
        self.start = start
        self.stop = stop
        self.characteristic = characteristic.lower()
        self.statistic = statistic.lower()

        if self.statistic not in valid_stats:
            raise ValueError(
                f"{self.statistic} is not a valid statistic. Use one of {valid_stats}"
            )

        if self.characteristic not in valid_characteristics:
            raise ValueError(
                f"{self.characteristic} is not a valid characteristic. Use one of {valid_characteristics}"
            )

        self.handsets_query = SubscriberHandsets(
            start=self.start,
            stop=self.stop,
            hours=hours,
            table=tables,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=subscriber_identifier,
        )

        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):

        if self.statistic in {"count", "max", "min"}:
            sql = f"""
            SELECT subscriber, {self.statistic}({self.characteristic}) AS value
            FROM ({self.handsets_query.get_query()}) AS U
            GROUP BY subscriber
            """
            return sql

        weighted_sum = f"SUM(weight * value)"
        weighted_avg = f"{weighted_sum} / SUM(weight)"
        weighted_var = f"(SUM(weight * (value ^ 2)) - ((({weighted_sum}) ^ 2) / SUM(weight))) / (SUM(weight) - 1)"
        weighted_stddev = f"SQRT({weighted_var})"

        statistic_clause = ""
        if self.statistic in {"sum"}:
            statistic_clause = weighted_sum
        elif self.statistic in {"avg"}:
            statistic_clause = weighted_avg
        elif self.statistic in {"stddev"}:
            statistic_clause = weighted_stddev
        elif self.statistic in {"variance"}:
            statistic_clause = weighted_var

        # Weights are the number of seconds between events, which are
        # calculated using window functions.  The lag function will return NULL
        # for the first event of each series.  In that case we assume that the
        # handset used prior to the first event is the same as the one used in
        # the first event. The generated weights are the number of seconds
        # between this event and the beginning of the observed period. To
        # account for the period between the last event and the end of the
        # observed period we generate an additional row in which we assume the
        # handset used is the same as the one used in the last event. The
        # weights are the number of seconds between this event and the end of
        # the observed period.
        weight_extraction_query = f"""
        WITH W AS (
            SELECT
                subscriber,
                {self.characteristic} AS value,
                time,
                EXTRACT(EPOCH FROM
                    time -
                    LAG(
                        time,
                        1,
                        TIMESTAMPTZ '{self.start}'
                    ) OVER msisdn_by_datetime
                ) AS weight,
                CUME_DIST() OVER msisdn_by_datetime
            FROM ({self.handsets_query.get_query()}) AS U
            WINDOW msisdn_by_datetime AS (PARTITION BY subscriber ORDER BY time)
        )
        SELECT subscriber, value, weight
        FROM W
        UNION ALL
        SELECT subscriber, value, EXTRACT(EPOCH FROM (TIMESTAMPTZ '{self.stop}' - time)) AS weight
        FROM W
        WHERE cume_dist = 1
        """

        if self.statistic in {"sum", "avg", "stddev", "variance"}:
            sql = f"""
            SELECT subscriber, {statistic_clause} AS value
            FROM ({weight_extraction_query}) U
            GROUP BY subscriber
            """
            return sql

        sql = f"""
        WITH W AS ({weight_extraction_query})
        SELECT DISTINCT ON (subscriber) A.subscriber, A.value
        FROM (
            SELECT
                subscriber,
                value,
                weight,
                SUM(weight) OVER (PARTITION BY subscriber ORDER BY weight) AS cum_sum
            FROM W
        ) A
        JOIN ( SELECT subscriber, SUM(weight) AS total_weight FROM W GROUP BY subscriber) B
        ON A.subscriber = B.subscriber AND A.cum_sum >= (B.total_weight / 2)
        ORDER BY A.subscriber, A.weight
        """

        return sql
