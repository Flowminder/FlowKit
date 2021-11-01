import datetime
from datetime import timedelta, date, datetime
from typing import List, Union, Optional
from flowmachine.core.query import Query
from flowmachine.features.subscriber.call_days import CallDays
from flowmachine.features.subscriber.interevent_interval import IntereventInterval
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.utils import standardise_date


class ActiveSubscribers(Query):
    """Returns a list of subscribers active `active_days`
    out of `interval`, optionally with at least Z call-hours active"""

    # TODO: Parameterise which events tables to use + which ID method to use

    def __init__(
        self,
        start_date: Union[date, str],
        end_date: Union[date, str],
        active_days: int,
        interval: int,
        active_hours: int = None,
        subscriber_id: str = "msisdn",
        events_tables: Optional[List[str]] = None,
        subscriber_subset=None,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.active_days = active_days
        self.interval = interval
        self.active_hours = active_hours
        self.sub_id_column = subscriber_id
        self._window_start = self._start_date - timedelta(days=self.interval - 1)
        self.events_table = EventsTablesUnion(
            self._window_start,
            end_date,
            tables=events_tables,
            subscriber_identifier=subscriber_id,
            columns=[subscriber_id, "datetime"],
            subscriber_subset=subscriber_subset,
        )
        super().__init__()

    @property
    def start_date(self):
        return self._start_date.strftime("%Y-%m-%d")

    @start_date.setter
    def start_date(self, value):
        if type(value) is str:
            self._start_date = datetime.strptime(value, "%Y-%m-%d")
        elif type(value) in [date, datetime]:
            self._start_date = value
        else:
            raise TypeError("start_date must be datetime or yyyy-mm-dd")

    @property
    def end_date(self):
        return self._end_date.strftime("%Y-%m-%d")

    @end_date.setter
    def end_date(self, value):
        if type(value) is str:
            self._end_date = datetime.strptime(value, "%Y-%m-%d")
        elif type(value) in [date, datetime]:
            self._end_date = value
        else:
            raise TypeError("end_date must be datetime or yyyy-mm-dd")

    @property
    def column_names(self) -> List[str]:
        return ["datetime", "subscriber"]

    # What should subscriber ID be?

    def _make_query(self):

        # Review questions:
        # How should we pass dates around internally in fm?
        # Should this return subscribers-day pairs, or just a list of subscribers?
        # Should we offer the choice?

        sql = f"""
WITH ordered_events AS(
	SELECT  subscriber, datetime
	FROM ({self.events_table.get_query()}) AS tbl
	WHERE datetime BETWEEN date('{self._window_start :%Y-%m-%d}') AND date('{self._end_date:%Y-%m-%d}')
	ORDER BY subscriber, datetime
), seen_on_days AS(
	SELECT DISTINCT ON (event_date, subscriber)
	subscriber, datetime::date as event_date
	FROM ordered_events
	ORDER BY subscriber
), dates_of_interest AS (
	SELECT i::date as dates_of_interest
	FROM generate_series('{self._window_start :%Y-%m-%d}', '{self._end_date:%Y-%m-%d}', '1 day'::interval) AS i
), active_as_of AS (
	SELECT dates_of_interest, subscriber
	FROM dates_of_interest
	LEFT OUTER JOIN seen_on_days ON dates_of_interest.dates_of_interest = seen_on_days.event_date
	ORDER BY dates_of_interest
), lookback_count_table AS(
	SELECT 
		dates_of_interest,
		subscriber,
		count(subscriber) OVER lookback_period AS lookback_count
	FROM active_as_of
	WINDOW lookback_period AS (
		PARTITION BY subscriber
		ORDER BY dates_of_interest
		RANGE BETWEEN '{self.interval - 1} days' PRECEDING AND CURRENT ROW
	)
)
SELECT 
    dates_of_interest AS datetime,
    subscriber
FROM lookback_count_table
WHERE lookback_count >= {self.active_days}
AND dates_of_interest BETWEEN date('{self._start_date:%Y-%m-%d}') AND date('{self._end_date:%Y-%m-%d}')
"""

        return sql
