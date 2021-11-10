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
    Class that represents subscribers seen to be active.

    To determine this, we regard the timeframe between `start_date` and `end_date` as the total period.
    We then break the total period into `minor_period_count` minor periods.

    A subscriber is considered to be active in a minor period if they are seen at least `minor_period_
    threshold` times within that period.

    A subscriber is considered active over the major period if they are active in at least `major_period_
    threshold` periods.

    Parameters
    ----------
    start_date, end_date: date, datetime, str
        Major period between which to search for subscribers
    minor_period_threshold: int
        The number of times a subscriber must appear inside minor_period to count as active
        in that period
    major_period_threshold: int
        The number of minor periods a subscriber must appear active in to appear in the output
        of the query
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    minor_periods_per_major_period: int, default 24
        The number of minor periods to split the major period into
    minor_period_length: int, default 1
        The number of period_units that make up a minor period
    period_unit: {'days','hours','minutes'} default 'hours'
        The unit of time to of minor_period_length

    Notes
    -----
    * The date range will be inclusive os `start_date` but exclusive of `end_date` (ie range = start_
    date ... (end_date - 1 second))
    * The default values for minor_period_count and minor_period_length assume you wish to seach
    for subscribers who are active at least `minor_period_threshold` hours throughout the day.

    Examples
    --------
    Returns subscribers who were active on at least three hours between 2016-01-01 and 2016-01-02

    >>>     active_subscribers = ActiveSubscribers(4,,
                start_date=date(year=2016, month=1, day=1),
                end_date=date(year=2016, month=1, day=4),
                minor_period_threshold=5,
                major_period_threshold=3,
                tables=["events.calls"],
            )

    Returns subscribers that were active in at least two ten minute intervals within half an hour,
    at least three times across the two hours between 20:00:00 and 22:00:00 on 2016-01-01


    >>> active_subscribers = ActiveSubscribers(4,,
                start_date=datetime(year=2016, month=1, day=1, hour=20),
                end_date=datetime(year=2016, month=1, day=1, hour=22),
                minor_period_threshold=2,
                major_period_threshold=3,
                tables=["events.calls"],
                minor_period_count=3,
                minor_period_length=10,
                period_unit="minutes",
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
        self.sub_id_column = subscriber_identifier
        self.major_period_threshold = major_period_threshold
        self.total_major_periods = total_major_periods

        minor_period_generator = rr.rrule(
            self.period_to_rrule_mapping[period_unit],
            interval=minor_periods_per_major_period * minor_period_length,
            dtstart=self._start_dt,
            count=self.total_major_periods,
        )

        self.period_queries = [
            TotalActivePeriodsSubscriber(
                start=minor_period_start,
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
            for minor_period_start in minor_period_generator
        ]
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]

    def _make_query(self):

        seen_on_days_clause = "\nUNION ALL\n".join(
            period_query.get_query() for period_query in self.period_queries
        )

        sql = f"""
        SELECT subscriber
        FROM ({seen_on_days_clause}) AS tbl
        GROUP BY subscriber
        HAVING count(subscriber) >= {self.major_period_threshold}
        """

        return sql
