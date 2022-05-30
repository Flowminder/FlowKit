# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Class for calculating top-up balance statistics.
"""
from typing import Optional, Tuple

import warnings

from ..utilities.sets import EventsTablesUnion
from .metaclasses import SubscriberFeature
from flowmachine.utils import standardise_date, Statistic


class TopUpBalance(SubscriberFeature):
    """
    This class calculates statistics associated with top-up balances.

    Top-up balance is a stock variable. As such, here we calculate the weighted
    balance, weighted by the number of seconds a subscriber held that balance.
    Given that we only learn about changes in balance when a top-up event
    occurs, this average will be biased upwards. Unfortunately, we do not have
    information about depletions to the balance caused by CDR events such as
    calls, SMS, MDS, etc since this information is not provided by the MNOs.
    For instance, if a subscriber with zero balance top-up a certain amount and
    spends the whole balance right away, the subscriber's effective balance
    during the whole period is 0 and so should be its average. However, because
    we do not account for topup balance depletions its average balance is
    biased upwards by the recharge amount.

    However, given the nature of the data we take the conservative approach
    that the subscriber holds between top-up events the average balance between
    the previous top-up post-balance and the following top-up pre-balance. It
    is this average balance that is then weighted by the number of seconds
    between topup events to generate the required statistcs.

    We further assume that the average balance held before the first observed
    top-up event in the series is equal to the pre-event balance. Likewise, we
    assume that the average balance held after the last observed top-up event
    in the series is equal to the post-event balance.


    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    statistic : Statistic, default Statistic.SUM
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

    >>> s = TopUpBalance("2016-01-01", "2016-01-08")
    >>> s.get_dataframe()

        subscriber          value
    AZj6MqBAryVyNRDo   410.064467
    Bn5kZrQ2WgEy14zN    78.580122
    LBlWd64rqnMGv7kY    73.702066
    8lo9EgjnyjgKO7vL   303.409108
    jwKJorl0yBrZX5N8    78.291416
                 ...          ...

    """

    def __init__(
        self,
        start,
        stop,
        statistic: Statistic = Statistic.SUM,
        *,
        subscriber_identifier="msisdn",
        hours: Optional[Tuple[int, int]] = None,
        subscriber_subset=None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        self.hours = hours
        self.statistic = Statistic(statistic.lower())
        self.tables = "events.topups"

        column_list = [
            self.subscriber_identifier,
            "datetime",
            "pre_event_balance",
            "post_event_balance",
        ]

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.tables,
            columns=column_list,
            hours=hours,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )

        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):

        if self.statistic in {"count"}:
            return f"""
            SELECT subscriber, COUNT(*) AS value
            FROM ({self.unioned_query.get_query()}) AS U
            GROUP BY subscriber
            """

        if self.statistic in {"max", "min"}:
            return f"""
            SELECT subscriber, {self.statistic:balance} AS value
            FROM (
                SELECT subscriber, {self.statistic:pre_event_balance} AS balance
                FROM ({self.unioned_query.get_query()}) AS U
                GROUP BY subscriber
                UNION ALL
                SELECT subscriber, {self.statistic:post_event_balance} AS balance
                FROM ({self.unioned_query.get_query()}) AS U
                GROUP BY subscriber
            ) U
            GROUP BY subscriber
            """

        weighted_sum = f"SUM(weight * balance)"
        weighted_avg = f"{weighted_sum} / SUM(weight)"
        weighted_var = f"(SUM(weight * (balance ^ 2)) - ((({weighted_sum}) ^ 2) / SUM(weight))) / (SUM(weight) - 1)"
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

        # We calculate simple arithmetic average balances between events using
        # the pre-event balance of the current observation and the post-event
        # balance of the previous observation. Their respective weights are the
        # number of seconds between both events. We calculate those variables
        # using window functions.  The lag function will return NULL for the
        # first event of each series.  In that case we assume that the
        # pre-event balance equals to the pre-event balance of this first
        # event. The generate weights are the number of seconds between this
        # event and the beginning of the observed period. To account for the
        # period between the last event and the end of the observed period we
        # generate an additional row in which the average balance is equal to
        # the post-event balance of the last observed top-up event. The weights
        # are the number of seconds between this event and the end of the
        # observed period.
        weight_extraction_query = f"""
        WITH W AS (
            SELECT
                subscriber,
                (
                    pre_event_balance +
                    (LAG(post_event_balance, 1, pre_event_balance) OVER msisdn_by_datetime)
                ) / 2 AS balance,
                post_event_balance,
                datetime,
                EXTRACT(EPOCH FROM
                    datetime -
                    LAG(
                        datetime,
                        1,
                        TIMESTAMPTZ '{self.start}'
                    ) OVER msisdn_by_datetime
                ) AS weight,
                CUME_DIST() OVER msisdn_by_datetime
            FROM ({self.unioned_query.get_query()}) AS U
            WINDOW msisdn_by_datetime AS (PARTITION BY subscriber ORDER BY datetime)
        )
        SELECT subscriber, balance, weight
        FROM W
        UNION ALL
        SELECT subscriber, post_event_balance AS balance, EXTRACT(EPOCH FROM (TIMESTAMPTZ '{self.stop}' - datetime)) AS weight
        FROM W
        WHERE cume_dist = 1
        """

        if self.statistic in {"sum", "avg", "stddev", "variance"}:
            return f"""
            SELECT subscriber, {statistic_clause} AS value
            FROM ({weight_extraction_query}) U
            GROUP BY subscriber
            """

        if self.statistic in {"mode"}:
            return f"""
            SELECT DISTINCT ON (subscriber) subscriber, balance AS value
            FROM (
                SELECT subscriber, balance, SUM(weight * balance) AS total_weight
                FROM ({weight_extraction_query}) U
                GROUP BY subscriber, balance
                ORDER BY subscriber DESC, total_weight
            ) U
            """

        # Weighted median
        if self.statistic == Statistic.MEDIAN:
            return f"""
            WITH W AS ({weight_extraction_query})
            SELECT DISTINCT ON (subscriber) A.subscriber, A.balance AS value
            FROM (
                SELECT
                    subscriber,
                    balance,
                    weight,
                    SUM(weight) OVER (PARTITION BY subscriber ORDER BY weight) AS cum_sum
                FROM W
            ) A
            JOIN ( SELECT subscriber, SUM(weight) AS total_weight FROM W GROUP BY subscriber) B
            ON A.subscriber = B.subscriber AND A.cum_sum >= (B.total_weight / 2)
            ORDER BY A.subscriber, A.weight
            """

        raise NotImplementedError(
            f"{self.statistic} is not implemented for TopupBalance"
        )
