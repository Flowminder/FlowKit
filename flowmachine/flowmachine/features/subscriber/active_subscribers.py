from datetime import date
from typing import List, Union, Optional
from functools import reduce

from flowmachine.core.mixins.exposed_datetime_mixin import ExposedDatetimeMixin
from flowmachine.core.query import Query
from flowmachine.features.subscriber.total_active_periods import (
    TotalActivePeriodsSubscriber,
)
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
import dateutil.rrule as rr

"""Returns a list of subscribers seen at least `active_days` between `start_date` and `end_date`,
    where 'active' is at least `active_hours` call-hours active"""


class ActiveSubscribers(ExposedDatetimeMixin, Query):
    """
    Class that represents subscribers seen  within a period.
    """

    period_to_rrule_mapping = {
        "days": rr.DAILY,
        "hours": rr.HOURLY,
        "minutes": rr.MINUTELY,
    }

    def __init__(
        self,
        start_date: Union[date, str],
        end_date: Union[date, str],
        active_hours: int,
        active_days: int,
        subscriber_identifier: str = "msisdn",
        tables: Optional[List[str]] = None,
        subscriber_subset=None,
        total_periods=24,
        period_length=1,
        period_unit="hours",
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.active_hours = active_hours
        self.sub_id_column = subscriber_identifier
        self.events_tables = tables
        self.active_days = active_days

        self.events_table_query = EventsTablesUnion(
            self.start_date,
            self.end_date,
            tables=tables,
            subscriber_identifier=subscriber_identifier,
            columns=[subscriber_identifier, "datetime"],
            subscriber_subset=subscriber_subset,
        )

        date_generator = rr.rrule(
            self.period_to_rrule_mapping[period_unit],
            interval=total_periods,
            dtstart=self._start_dt,
            until=self._end_dt,
        )

        self.period_queries = [
            TotalActivePeriodsSubscriber(
                start=date,
                total_periods=total_periods,
                period_length=period_length,
                period_unit=period_unit,
                table=tables,
                subscriber_identifier=subscriber_identifier,
                subscriber_subset=subscriber_subset,
            ).numeric_subset("value", low=active_hours, high=total_periods)
            for date in date_generator
        ]
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]

    def _make_query(self):

        seen_on_days_clause = "\nUNION\n".join(
            period_query.get_query() for period_query in self.period_queries
        )

        sql = f"""
        SELECT subscriber
        FROM ({seen_on_days_clause}) AS tbl
        GROUP BY subscriber
        HAVING count(subscriber) >= {self.active_days}
        """

        return sql
