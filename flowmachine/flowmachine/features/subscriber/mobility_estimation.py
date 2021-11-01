from typing import List, AnyStr
from flowmachine.core.query import Query

import datetime
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY

from flowmachine.features.subscriber.daily_location import daily_location
from flowmachine.features.subscriber.modal_location import ModalLocation
from flowmachine.features.subscriber.home_location_monthly import HomeLocationMonthly
from flowmachine.core import make_spatial_unit


def _parse_date(date_string):
    if type(date_string) == str:
        date_now = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    else:
        date_now = date_string
    date_before = date_now - relativedelta(months=1)
    #    return(
    #        date_now.strftime("%Y-%m-%d"),
    #        date_before.strftime("%Y-%m-%d")
    #    )
    return date_now, date_before


class MobilityEstimation(Query):
    def __init__(
        self,
        *,
        start,
        stop,
        agg_unit,
    ):
        self.this_month_start, self.last_month_start = _parse_date(start)
        self.this_month_stop, self.last_month_stop = _parse_date(stop)
        self.agg_unit = make_spatial_unit("admin", level=3)
        self.home_location_monthly = HomeLocationMonthly(
            window_start=self.this_month_start,
            window_stop=self.this_month_stop,
            spatial_unit=self.agg_unit,
            home_this_month=10,
            home_last_month=15,
        )

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

        sql = f"""
WITH home_count_time_series AS (
    {self.home_location_monthly.get_query()}
), stats_precursor_1 AS(
    SELECT
        pcod AS area,
        9999 as month,
        home_locs_0 AS resident_count,
        home_locs_1,
        home_locs_0 - home_locs_1 AS resident_count_change,    
        9999 AS year_median
    FROM home_count_time_series
), stats_precursor_2 AS(
    SELECT
        area, month, resident_count, resident_count_change, home_locs_1,
        resident_count - year_median AS resident_count_change_median
    FROM stats_precursor_1
)
SELECT
    area, month, resident_count, resident_count_change,
    (cast(resident_count_change as decimal)/home_locs_1)*100 AS resident_count_change_percent,       
    resident_count_change_median,
    (cast(resident_count_change as decimal)/resident_count_change_median)*100 AS resident_count_change_median_percent,
    9999 AS high_mobility_resident_percent,
    9999 AS displaced_resident_percent
FROM stats_precursor_2
"""

        return sql
