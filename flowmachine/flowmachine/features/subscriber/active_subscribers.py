from datetime import timedelta, date
from typing import List
from flowmachine.core.query import Query
from flowmachine.features.subscriber.call_days import CallDays
from flowmachine.features.subscriber.interevent_interval import IntereventInterval


class ActiveSubscribers(Query):
    """Returns a list of subscribers active `active_days`
    out of `interval`, optionally with at least Z call-hours active"""

    def __init__(
        self,
        *args,
        start_date: date,
        end_date: date,
        active_days: int,
        interval: int,
        active_hours: int = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.start_date = start_date
        self.end_date = end_date
        self.active_days = active_days
        self.interval = interval
        self.active_hours = active_hours

    @property
    def column_names(self) -> List[str]:
        return ["datetime", "subscriber_id"]

    # What should subscriber ID be?

    def _make_query(self):

        # Review questions:
        # What ID should we join on? Been using msisdn so far.
        # How should we pass dates around internally in fm?
        # Should this return subscribers-day pairs, or just a list of subscribers?
        # Should we offer the choice?

        window_start = self.start_date - timedelta(days=self.interval - 1)

        sql = f"""
WITH ordered_events AS(
	SELECT msisdn, datetime
	FROM events.calls
	WHERE datetime BETWEEN date('{window_start:%Y-%m-%d}') AND date('{self.end_date:%Y-%m-%d}')
	ORDER BY msisdn, datetime
), seen_on_days AS(
	SELECT DISTINCT ON (event_date, msisdn)
	msisdn, datetime::date as event_date
	FROM ordered_events
	ORDER BY msisdn
), dates_of_interest AS (
	SELECT i::date as dates_of_interest
	FROM generate_series('{window_start:%Y-%m-%d}', '{self.end_date:%Y-%m-%d}', '1 day'::interval) AS i
), active_as_of AS (
	SELECT dates_of_interest, msisdn
	FROM dates_of_interest
	LEFT OUTER JOIN seen_on_days ON dates_of_interest.dates_of_interest = seen_on_days.event_date
	ORDER BY dates_of_interest
), lookback_count_table AS(
	SELECT 
		dates_of_interest,
		msisdn,
		count(msisdn) OVER lookback_period AS lookback_count
	FROM active_as_of
	WINDOW lookback_period AS (
		PARTITION BY msisdn
		ORDER BY dates_of_interest
		RANGE BETWEEN '{self.interval - 1} days' PRECEDING AND CURRENT ROW
	)
)
SELECT 
    dates_of_interest AS datetime,
    msisdn
FROM lookback_count_table
WHERE lookback_count >= {self.active_days}
AND dates_of_interest BETWEEN date('{self.start_date:%Y-%m-%d}') AND date('{self.end_date:%Y-%m-%d}')
"""

        return sql
