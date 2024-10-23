# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
A subscriber's activity calendar - an array of datetimes they had some activity in.
"""

from typing import List, Union as UnionType, Optional, Tuple

from .. import TotalActivePeriodsSubscriber, UniqueSubscribers
from ...core import Query
from ...core.union_with_fixed_values import UnionWithFixedValues
from ...utils import standardise_date_to_datetime


class CalendarActivity(TotalActivePeriodsSubscriber):
    """
    Class representing the calendar days that a subscriber was active
    in some period.

    Parameters
    ----------
    start : str
         iso-format start datetime
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    table : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables
    total_periods : int
        Total number of periods to break your time span into
    period_length : int, default 1
        Total number of days per period.
    period_unit : {'days', 'hours', 'minutes'} default 'days'
        Split this time frame into hours or days etc.

    See Also
    --------
    flowmachine.features.subscriber.call_days
    flowmachine.features.subscriber.total_active_periods
    """

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

        return UnionWithFixedValues(
            queries=[
                UniqueSubscribers(
                    start,
                    stop,
                    hours=hours,
                    table=table,
                    subscriber_identifier=subscriber_identifier,
                    subscriber_subset=subscriber_subset,
                )
                for start, stop in zip(self.starts, self.stops)
            ],
            fixed_value=[standardise_date_to_datetime(dt) for dt in self.starts],
            fixed_value_column_name="dt",
        )

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        # In theory one could do this by doing date_trunc on the events tables union, but
        # the plan ends up being pretty agressively bad, so we're going to presuppose
        # that we want to visit every day and get the unique subscribers
        sql = f"""
            SELECT
                ul.subscriber,
                array_agg(dt ORDER BY dt ASC) as value
            FROM
                ({self.unique_subscribers_table.get_query()}) AS ul
            GROUP BY
                ul.subscriber
            ORDER BY value DESC
              """

        return sql
