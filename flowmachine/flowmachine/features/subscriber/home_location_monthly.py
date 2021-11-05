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
        modal_location: ModalLocation,
        location_visits: LocationVisits,
        subscriber_subset: Query,
        home_this_month: int,
        reference_location: Union["HomeLocationMonthly", None] = None,
        home_last_month: Union[int, None] = None,
    ):

        self.subscriber_subset = subscriber_subset
        self.location_visits = location_visits
        self.modal_locations = modal_location
        self.ref_location = reference_location
        self.home_this_month = (home_this_month,)
        self.home_last_month = home_last_month

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

        sql = f"""
WITH last_locations AS (
    {self.location_visits.get_query()}
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
