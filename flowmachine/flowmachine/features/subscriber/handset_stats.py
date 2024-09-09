# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from typing import Union

from ...core.statistic_types import Statistic

valid_characteristics = {
    "width",
    "height",
    "depth",
    "weight",
    "display_width",
    "display_height",
}

from .metaclasses import SubscriberFeature


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
    characteristic: {"width", "height", "depth", "weight", "display_width", "display_height"}
        Numeric handset characteristics present in the TAC table.
    statistic : Statistic, default Statistic.SUM
        Defaults to sum, aggregation statistic over the durations.
    subscriber_handsets: flowmachine.features.subscriber_tacs.SubscriberHandsets
        An instance of SubscriberHandsets listing the handsets associated with
        the subscribers during the period of observation.

    Examples
    --------

    >>> s = HandsetStats("width",
        subscriber_handsets=SubscriberHandsets("2016-01-01", "2016-01-08"))
    >>> s.get_dataframe()

              subscriber      value
        2ZdMowMXoyMByY07  30.387672
        MobnrVMDK24wPRzB  28.764593
        0Ze1l70j0LNgyY4w  26.664206
        Nnlqka1oevEMvVrm  30.570331
        gPZ7jbqlnAXR3JG5  30.049870
                     ...        ...
    """

    def __init__(
        self,
        characteristic,
        statistic: Union[Statistic, str] = Statistic.SUM,
        *,
        subscriber_handsets,
    ):
        self.characteristic = characteristic.lower()
        self.statistic = Statistic(statistic.lower())
        if self.statistic == "mode":
            raise ValueError("HandsetStats does not support weighted mode.")

        if self.characteristic not in valid_characteristics:
            raise ValueError(
                f"{self.characteristic} is not a valid characteristic. Use one of {valid_characteristics}"
            )

        self.handsets_query = subscriber_handsets
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
                        TIMESTAMPTZ '{self.handsets_query.start}'
                    ) OVER msisdn_by_datetime
                ) AS weight,
                CUME_DIST() OVER msisdn_by_datetime
            FROM ({self.handsets_query.get_query()}) AS U
            WINDOW msisdn_by_datetime AS (PARTITION BY subscriber ORDER BY time)
        )
        SELECT subscriber, value, weight
        FROM W
        UNION ALL
        SELECT subscriber, value, EXTRACT(EPOCH FROM (TIMESTAMPTZ '{self.handsets_query.stop}' - time)) AS weight
        FROM W
        WHERE cume_dist = 1
        """

        # Weighted sum, mean, standard deviation and variance
        if self.statistic in {"sum", "avg", "stddev", "variance"}:
            sql = f"""
            SELECT subscriber, {statistic_clause} AS value
            FROM ({weight_extraction_query}) U
            GROUP BY subscriber
            """
            return sql

        # Weighted median
        if self.statistic == Statistic.MEDIAN:
            return f"""
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
        raise NotImplementedError(
            f"{self.statistic} is not implemented for HandsetStats"
        )
