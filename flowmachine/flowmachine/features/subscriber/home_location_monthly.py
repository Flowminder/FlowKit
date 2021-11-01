from typing import List, Union
from flowmachine.core.spatial_unit import AnySpatialUnit
from datetime import datetime, timedelta
from flowmachine.core.query import Query
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY
from copy import copy

from flowmachine.features.subscriber.daily_location import daily_location
from flowmachine.features.subscriber.modal_location import ModalLocation
from flowmachine.features.subscriber.unique_active_subscribers import (
    UniqueActiveSubscribers,
    ActiveSubscribers,
)
from flowmachine.features.subscriber.last_location import LastLocation
from flowmachine.features.subscriber.location_visits import LocationVisits


class HomeLocationMonthly(Query):

    """For each subscriber, if their modal last location was their last location on more than X days this month
    (choose X=15 here), then choose this as their home location. If not, but it was their last location on more than Y
     days this month (Y<X; choose Y=10 here) AND it is the same as their “reference location” (in our case reference
     location is modal location and last location on more than X days last month), then choose this as their home
     location. Otherwise set their home location to “unlocatable” (a special value, not NULL)."""

    def __init__(
        self,
        window_start: Union[str, datetime],
        window_stop: Union[str, datetime],
        agg_unit: AnySpatialUnit,
        home_this_month: int,
        home_last_month: int,
        ref_location: Union["HomeLocationMonthly", None] = None,
        events_tables=None,
        modal_lookback=40,
        active_days=4,
        interval=7,
    ):

        self.window_start = window_start
        self.window_stop = window_stop
        self.ref_location = ref_location
        self.agg_unit = agg_unit
        self.home_this_month = home_this_month
        self.home_last_month = home_last_month
        self.modal_lookback = modal_lookback

        self.active_subs = ActiveSubscribers(
            start_date=self.window_start,
            end_date=self.window_stop,
            active_days=active_days,
            interval=interval,
            events_tables=events_tables,
        )

        if self._window_start >= self._window_stop - timedelta(days=modal_lookback):
            full_range_start = self._window_stop - timedelta(days=modal_lookback)
        else:
            full_range_start = self._window_start
        full_range_stop = self._window_stop

        all_locations = {
            day: LastLocation(
                start=day.strftime("%Y-%m-%d"),
                stop=(day + timedelta(days=1)).strftime("%Y-%m-%d"),
                spatial_unit=self.agg_unit,
                subscriber_subset=self.active_subs,
                table=events_tables,
            )
            for day in rrule(DAILY, dtstart=full_range_start, until=full_range_stop)
        }
        modal_days = [
            query
            for (day, query) in all_locations.items()
            if full_range_start <= day <= full_range_stop
        ]
        self.last_locations = [
            query
            for (day, query) in all_locations.items()
            if self._window_start <= day <= self._window_stop
        ]
        self.modal_locations = ModalLocation(*modal_days)

        # self.daily_location_frequency = LocationVisits(
        #     self.last_locations  # Ask Jono about this
        # )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        cols = ["subscriber", "location", "month"]
        return cols

    @property
    def window_start(self):
        return self._window_start.strftime("%Y-%m-%d")

    @window_start.setter
    def window_start(self, value):
        if type(value) is str:
            self._window_start = datetime.strptime(value, "%Y-%m-%d")
        elif type(value) is datetime:
            self._window_start = value
        else:
            raise TypeError("window_start must be datetime or yyyy-mm-dd")

    @property
    def window_stop(self):
        return self._window_stop.strftime("%Y-%m-%d")

    @window_stop.setter
    def window_stop(self, value):
        if type(value) is str:
            self._window_stop = datetime.strptime(value, "%Y-%m-%d")
        elif type(value) is datetime:
            self._window_stop = value
        else:
            raise TypeError("window_stop must be datetime or yyyy-mm-dd")

    def _make_query(self):

        last_locations_clause = ""

        # NOTE: last_locations seems to be dropping the final record? This is weird.
        for last_location in self.last_locations:
            last_locations_clause += f"""
SELECT subscriber, pcod, '{last_location.start}' AS day
FROM ({last_location.get_query()}) AS tbl
UNION ALL
"""

        last_locations_clause = last_locations_clause.rstrip("UNION ALL\n")

        sql = f"""
WITH last_locations AS (
    {last_locations_clause}
), modal_locations AS (
    {self.modal_locations.get_query()}
), location_histogram AS (
	SELECT subscriber, pcod, count(pcod) as y
	FROM last_locations
	GROUP BY subscriber, pcod
), this_month_known_homes AS (
	SELECT modal_locations.subscriber, modal_locations.pcod 
	FROM modal_locations
		INNER JOIN  location_histogram ON location_histogram.subscriber = modal_locations.subscriber
	WHERE modal_locations.pcod = location_histogram.pcod
	AND modal_locations.subscriber = location_histogram.subscriber
	AND location_histogram.y >= {self.home_this_month}
), """

        if self.ref_location:
            sql += f"""
reference_locations AS (
    SELECT subscriber, pcod
    FROM ({self.ref_location.get_query()}) AS tbl
), last_month_known_homes AS (
    SELECT location_histogram.subscriber, location_histogram.pcod
    FROM location_histogram 
    INNER JOIN  reference_locations USING (subscriber) 
    INNER JOIN modal_locations USING (subscriber)
    WHERE modal_locations.pcod = reference_locations.pcod
    AND location_histogram.y >= {self.home_last_month}
    AND location_histogram.subscriber NOT IN (SELECT subscriber FROM this_month_known_homes)
), """

        if self.ref_location is None:
            sql += f"""
known_homes AS (
    SELECT subscriber, pcod
    FROM this_month_known_homes
), """
        else:
            sql += f"""
known_homes AS (
    SELECT subscriber, pcod
    FROM this_month_known_homes
    UNION ALL
    SELECT subscriber, pcod
    FROM last_month_known_homes
), """

        sql += f"""
unknown_homes AS(
	SELECT DISTINCT(subscriber), 'unknown' AS pcod
	FROM location_histogram
	WHERE subscriber NOT IN (SELECT subscriber FROM known_homes)
)
SELECT subscriber, pcod
FROM known_homes
UNION ALL
SELECT subscriber, pcod
FROM unknown_homes
"""

        return sql
