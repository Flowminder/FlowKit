import datetime
from datetime import timedelta, date, datetime
from typing import List, Union, Optional
from functools import reduce

from flowmachine.core.query import Query
from flowmachine.features.subscriber.call_days import CallDays
from flowmachine.features.subscriber.interevent_interval import IntereventInterval
from flowmachine.features.subscriber.total_active_periods import (
    TotalActivePeriodsSubscriber,
)
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.utils import standardise_date
from dateutil.rrule import rrule, DAILY


class ActiveSubscribers(Query):
    """Returns a list of subscribers active `active_days`
    out of `interval`, with at least Z call-hours active"""

    # TODO: Parameterise which events tables to use + which ID method to use

    def __init__(
        self,
        start_date: Union[date, str],
        end_date: Union[date, str],
        active_hours: int,
        subscriber_id: str = "msisdn",
        events_tables: Optional[List[str]] = None,
        subscriber_subset=None,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.active_hours = active_hours
        self.sub_id_column = subscriber_id
        self.events_tables = events_tables

        self.events_table_query = EventsTablesUnion(
            self.start_date,
            self.end_date,
            tables=events_tables,
            subscriber_identifier=subscriber_id,
            columns=[subscriber_id, "datetime"],
            subscriber_subset=subscriber_subset,
        )

        hour_queries = [
            TotalActivePeriodsSubscriber(
                start=day,
                total_periods=24,
                period_length=1,
                period_unit="hours",
                table=self.events_tables,
                subscriber_identifier=self.sub_id_column,
                subscriber_subset=self.events_table_query,
            ).numeric_subset("value", low=active_hours, high=24)
            for day in rrule(DAILY, dtstart=self._start_dt, until=self._end_dt)
        ]
        self.bigquery = reduce(lambda x, y: x.union(y), hour_queries)
        super().__init__()

    @property
    def start_date(self):
        return self._start_dt.strftime("%Y-%m-%d")

    @start_date.setter
    def start_date(self, value):
        if type(value) is str:
            self._start_dt = datetime.strptime(value, "%Y-%m-%d")
        elif type(value) in [date, datetime]:
            self._start_dt = value
        else:
            raise TypeError("start_date must be datetime or yyyy-mm-dd")

    @property
    def end_date(self):
        return self._end_dt.strftime("%Y-%m-%d")

    @end_date.setter
    def end_date(self, value):
        if type(value) is str:
            self._end_dt = datetime.strptime(value, "%Y-%m-%d")
        elif type(value) in [date, datetime]:
            self._end_dt = value
        else:
            raise TypeError("end_date must be datetime or yyyy-mm-dd")

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]

    def _make_query(self):

        return self.bigquery.get_query()
