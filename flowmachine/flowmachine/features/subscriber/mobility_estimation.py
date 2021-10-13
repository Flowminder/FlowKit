from typing import List
from flowmachine.core.query import Query

import datetime
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY

from flowmachine.features.subscriber.daily_location import daily_location
from flowmachine.features.subscriber.modal_location import ModalLocation
from flowmachine.core import make_spatial_unit


def _parse_date(date_string):
    date_now = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    date_before = date_now - relativedelta(months=1)
    #    return(
    #        date_now.strftime("%Y-%m-%d"),
    #        date_before.strftime("%Y-%m-%d")
    #    )
    return date_now, date_before


class MobilityEstimation(Query):
    def __init__(
        self,
        start,
        stop,
        agg_unit,
    ):
        self.this_month_start, self.last_month_start = _parse_date(start)
        self.this_month_stop, self.last_month_stop = _parse_date(stop)
        self.agg_unit = make_spatial_unit("admin", level=3)

    @property
    def column_names(self) -> List[str]:
        return [
            "area",
            "resident_count",
            "resident_count_change",
            "resident_count_change_percent",
            "resident_count_change_median",
            "resident_count_change_median_percent",
            "high_mobility_resident_percent",
            "displaced_resident_percent",
        ]

    # Questions for Galena;
    # Is the kernel overlapping? skimage.median_filter points to no
    # Are the csv files you're iterating over daily event records?
    def _make_query(self):

        # Remember; rrule will include the date given in 'until'
        daily_locations_this_month = [
            daily_location(day.strftime("%Y-%m-%d"))
            for day in rrule(
                DAILY, dtstart=self.this_month_start, until=self.this_month_stop
            )
        ]
        modal_location_this_month = ModalLocation(*daily_locations_this_month)

        daily_locations_last_month = [
            daily_location(day.strftime("%Y-%m-%d"))
            for day in rrule(
                DAILY, dtstart=self.last_month_start, until=self.last_month_stop
            )
        ]
        modal_location_last_month = ModalLocation(*daily_locations_last_month)

        sql = f"""
WITH home_locs_this_month AS (
    {modal_location_this_month.get_query()}
), home_locs_last_month AS(
    {modal_location_last_month.get_query()}
), home_count_this_month AS(
    SELECT pcod, count(subscriber) as sub_count
    FROM home_locs_this_month
    GROUP BY pcod
), home_count_last_month AS(
    SELECT pcod, count(subscriber) as sub_count
    FROM home_locs_last_month
    GROUP BY pcod
), home_count_time_series AS(
    SELECT 
        pcod,
        home_count_this_month.sub_count AS home_locs_this_month,
        home_count_last_month.sub_count AS home_locs_last_month
    FROM
        home_count_this_month INNER JOIN home_count_last_month USING (pcod)
), stats_precursor_1 AS(
    SELECT
        pcod AS area,
        9999 as month,
        home_locs_this_month AS resident_count,
        home_locs_last_month,
        home_locs_this_month - home_locs_last_month AS resident_count_change,    
        9999 AS year_median
    FROM home_count_time_series
), stats_precursor_2 AS(
    SELECT
        area, month, resident_count, resident_count_change, home_locs_last_month,
        resident_count - year_median AS resident_count_change_median
    FROM stats_precursor_1
)
SELECT
    area, month, resident_count, resident_count_change,
    (cast(resident_count_change as decimal)/home_locs_last_month)*100 AS resident_count_change_percent,       
    resident_count_change_median,
    (cast(resident_count_change as decimal)/resident_count_change_median)*100 AS resident_count_change_median_percent,
    9999 AS high_mobility_resident_percent,
    9999 AS displaced_resident_percent
FROM stats_precursor_2
"""
        return sql
