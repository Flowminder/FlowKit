# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Definition of the TotalActivePeriodsSubscriber class,
breaks a time span into many smaller time spans and counts
the number of distinct periods in which each subscriber is
present in the data.



Notes
-----
The implementation of this algorithm was principally done in order to form an
input to detection bias models (see [1]). However, it is not limited to that use-case.

References
----------
[1] Veronique Lefebvre, https://docs.google.com/document/d/1BVOAM8bVacen0U0wXbxRmEhxdRbW8J_lyaOcUtDGhx8/edit
"""
from typing import List, Tuple, Union as UnionType, Optional

from flowmachine.core import Query
from .metaclasses import SubscriberFeature
from flowmachine.utils import time_period_add, standardise_date
from ..utilities.sets import UniqueSubscribers
from ...core.union import Union


class TotalActivePeriodsSubscriber(SubscriberFeature):
    """
    Breaks a time span into distinct time periods (currently integer number
    of days). For each subscriber counts the total number of time periods in
    which each subscriber was seen.

    For instance we might ask for a month worth of data, break down our
    month into 10 3 day chunks, and ask for each subscriber how many of these
    three day chunks each subscriber was present in the data in.

    Parameters
    ----------
    start : str
        iso-format date, start of the analysis.
    total_periods : int
        Total number of periods to break your time span into
    period_length : int, default 1
        Total number of days per period.
    period_unit : {'days', 'hours', 'minutes'} default 'days'
        Split this time frame into hours or days etc.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    kwargs
        passed to flowmachine.UniqueSubscribers

    Examples
    --------

    >>> TotalActivePeriods('2016-01-01', 10, 3).get_dataframe()
        subscriber     total_periods
        subscriberA       10
        subscriberB       3
        subscriberC       7
              .
              .
              .

    """

    allowed_units = ["days", "hours", "minutes"]

    def __init__(
        self,
        start: str,
        total_periods: int,
        period_length: int = 1,
        period_unit: str = "days",
        hours: Optional[Tuple[int, int]] = None,
        table: UnionType[str, List[str]] = "all",
        subscriber_identifier: str = "msisdn",
        subscriber_subset: Optional[Query] = None,
    ):
        self.start = standardise_date(start)
        self.total_periods = total_periods
        self.period_length = period_length
        if period_unit not in self.allowed_units:
            raise ValueError(
                "`period_unit` must be one of {}".format(self.allowed_units)
            )
        self.period_unit = period_unit
        self.starts, self.stops = self._get_start_stops()
        # For convenience also store when the whole thing ends
        self.stop_date = time_period_add(
            self.start, self.total_periods * self.period_length
        )
        # This will be a long form table of unique subscribers in each time period
        # i.e. a subscriber can appear more than once in this list, up to a maximum
        # of the total time periods.
        self.unique_subscribers_table = self._get_unioned_subscribers_list(
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )

        super().__init__()

    def _get_start_stops(self):
        """
        Gets two lists, one for the start dates and one for the
        stop dates.
        """

        starts = [
            time_period_add(self.start, i * self.period_length, self.period_unit)
            for i in range(self.total_periods)
        ]
        stops = [
            time_period_add(self.start, (i + 1) * self.period_length, self.period_unit)
            for i in range(self.total_periods)
        ]
        return starts, stops

    def _get_unioned_subscribers_list(
        self,
        hours: UnionType[str, Tuple[int, int]] = "all",
        table: UnionType[str, List[str]] = "all",
        subscriber_identifier: str = "msisdn",
        subscriber_subset: Optional[Query] = None,
    ):
        """
        Constructs a list of UniqueSubscribers for each time period.
        They will be unique within each time period. Joins
        these lists into one long list and returns the result
        (as a query)
        """

        return Union(
            *[
                UniqueSubscribers(
                    start,
                    stop,
                    hours=hours,
                    table=table,
                    subscriber_identifier=subscriber_identifier,
                    subscriber_subset=subscriber_subset,
                )
                for start, stop in zip(self.starts, self.stops)
            ]
        )

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value", "inactive_periods"]

    def _make_query(self):

        sql = """
            SELECT
                ul.subscriber,
                count(*) AS value,
                {total_periods} - count(*) AS inactive_periods
            FROM
                ({unique_subscribers_table}) AS ul
            GROUP BY
                ul.subscriber
            ORDER BY value DESC
              """.format(
            unique_subscribers_table=self.unique_subscribers_table.get_query(),
            total_periods=self.total_periods,
        )

        return sql
