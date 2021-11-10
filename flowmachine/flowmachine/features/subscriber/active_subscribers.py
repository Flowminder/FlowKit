# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import date, datetime
from typing import List, Union, Optional, Literal, NewType, Tuple

from flowmachine.core.mixins.exposed_datetime_mixin import ExposedDatetimeMixin
from flowmachine.core.query import Query
from flowmachine.features.subscriber.total_active_periods import (
    TotalActivePeriodsSubscriber,
)
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
import dateutil.rrule as rr
from dateutil.relativedelta import relativedelta

SubscriberSubsetType = NewType("SubscriberSubsetType", Union[str, list, Query, "Table"])


class ActiveSubscribers(ExposedDatetimeMixin, Query):
    """
    Class that represents subscribers seen to be active across a search range

    The search range is split into major periods. Each major period is split into minor periods.

    A single minor period is `minor_period_length * period_unit` long

    A single major period is `minor_periods_per_major_period` minor periods long

    This makes the search range between `start_date` and `start_date +
    (period_unit * minor_period_length * minor_periods_per_major_period * total_major_periods)`

    A subscriber is considered to be active in a major period if they are seen in at least `minor_period_
    threshold` minor periods within that period

    A subscriber is considered active over the entire search range if they are active in at least
    `major_period_threshold` major periods.

    Parameters
    ----------
    start_date: date, datetime, str
        Beginning of the search range
    minor_period_length: int
        The number of period_units that make up a minor period
    minor_periods_per_major_period: int
        The number of minor periods to split the major period into
    total_major_periods: int
        The number of major_periods that make up the search range
    minor_period_threshold: int
        The number of minor_periods a subscriber must appear in to count as active
        in a major_period
    major_period_threshold: int
        The number of major periods a subscriber must appear active in to appear in the output
        of the query
    period_unit: {'days','hours','minutes'} default 'hours'
        The unit of time to of minor_period_length
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    hours: tuple default None
        A range of hours to restrict contributions to minor_period_counts to.

    Notes
    -----
    * if minor_period_length is equal to or less than the length of hours, there will be a set
    of major_periods that are guaranteed to be empty (as they fall outside the range set by hours).

    Examples
    --------
    Returns subscribers who were active for at least one hour a day for at least three days
    between 2016-01-01 and 2016-01-04

    >>>    active_subscribers = ActiveSubscribers(
                start_date=date(year=2016, month=1, day=1),
                minor_period_length=1,
                minor_periods_per_major_period=24,
                total_major_periods=4,
                minor_period_threshold=1,
                major_period_threshold=3,
                tables=["events.calls"],
            )

    Returns subscribers that were active in at least two ten minute intervals within half an hour,
    at least three times across the two hours between 20:00:00 and 22:00:00 on 2016-01-01

    >>>     active_subscribers = ActiveSubscribers(
                start_date=datetime(year=2016, month=1, day=1, hour=20),
                minor_period_length=10,
                minor_periods_per_major_period=3,
                total_major_periods=4,
                minor_period_threshold=2,
                major_period_threshold=3,
                period_unit="minutes",
                tables=["events.calls"],
            )


    """

    period_to_rrule_mapping = {
        "days": rr.DAILY,
        "hours": rr.HOURLY,
        "minutes": rr.MINUTELY,
    }

    def __init__(
        self,
        start_date: Union[date, datetime, str],
        minor_period_length: int,
        minor_periods_per_major_period: int,
        total_major_periods: int,
        minor_period_threshold: int,
        major_period_threshold: int,
        period_unit: Literal["days", "hours", "minutes"] = "hours",
        subscriber_identifier: Optional[str] = "msisdn",
        tables: Optional[List[str]] = None,
        subscriber_subset: Optional[SubscriberSubsetType] = None,
        hours: Optional[Tuple[int, int]] = None,
    ):
        self.start_date = start_date
        self.minor_period_threshold = minor_period_threshold
        self.major_period_threshold = major_period_threshold
        self.minor_period_length = minor_period_length
        self.minor_periods_per_major_period = minor_periods_per_major_period
        self.total_major_periods = total_major_periods
        self.period_unit = period_unit

        major_period_generator = rr.rrule(
            self.period_to_rrule_mapping[period_unit],
            interval=minor_periods_per_major_period * minor_period_length,
            dtstart=self._start_dt,
            count=self.total_major_periods,
        )

        rd_args = {
            period_unit: minor_period_length
            * minor_periods_per_major_period
            * total_major_periods
        }
        self.end_date = start_date + relativedelta(**rd_args)

        self.major_period_queries = [
            TotalActivePeriodsSubscriber(
                start=major_period_start,
                total_periods=minor_periods_per_major_period,
                period_length=minor_period_length,
                period_unit=period_unit,
                table=tables,
                subscriber_identifier=subscriber_identifier,
                subscriber_subset=subscriber_subset,
                hours=hours,
            ).numeric_subset(
                "value", low=minor_period_threshold, high=minor_periods_per_major_period
            )
            for major_period_start in major_period_generator
        ]
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]

    def _make_query(self):

        seen_on_days_clause = "\nUNION ALL\n".join(
            period_query.get_query() for period_query in self.major_period_queries
        )

        sql = f"""
        SELECT subscriber
        FROM ({seen_on_days_clause}) AS tbl
        GROUP BY subscriber
        HAVING count(subscriber) >= {self.major_period_threshold}
        """

        return sql
