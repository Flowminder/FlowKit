from typing import List
from flowmachine.core.spatial_unit import AnySpatialUnit
from datetime import datetime
from flowmachine.core.query import Query
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY
from flowmachine.features.subscriber.daily_location import daily_location
from flowmachine.features.subscriber.modal_location import ModalLocation


class HomeLocationMonthly(Query):
    def __init__(
        self,
        *,
        window_start: datetime,
        window_stop: datetime,
        n_months_back: int,
        agg_unit: AnySpatialUnit,
    ):
        self.window_start = window_start
        self.window_stop = window_stop
        self.n_months_back = n_months_back
        self.agg_unit = agg_unit

    @property
    def column_names(self) -> List[str]:
        cols = ["subscriber_id"] + list(range(self.n_months_back))
        return cols

    def _make_query(self):

        # TODO: Refactor this to have sub_area, area, month as columns

        # This is a bit gross... Please feel free to refactor
        # Final query should be of the form (for two months)
        # SELECT
        #   pcod,
        #   home_count_0.sub_count AS home_locs_0,
        #   home_count_1.sub_count AS home_locs_1
        # FROM
        #   home_count_0
        #   INNER JOIN home_count_1 USING pcod

        sql = "WITH "
        for month in range(self.n_months_back):
            month_start = self.window_start - relativedelta(months=month)
            month_stop = self.window_stop - relativedelta(months=month)
            daily_locations_in_month = [
                daily_location(day.strftime("%Y-%m-%d"), spatial_unit=self.agg_unit)
                for day in rrule(DAILY, dtstart=month_start, until=month_stop)
            ]
            # This is going to be subbed in for the actual home location query
            modal_location = ModalLocation(*daily_locations_in_month)
            sql += f"""
home_locs_{month} AS (
    {modal_location.get_query()}
), home_count_{month} AS (
    SELECT pcod, count(subscriber) as sub_count, {month} as month
    FROM home_locs_{month}
    GROUP BY pcod
    UNION ALL
),
"""

        sql = sql.rstrip(",")

        sql += f"""
SELECT
    pcod,
"""
        for month in range(self.n_months_back):
            sql += f"home_count_{month}.sub_count AS home_locs_{month},\n"

        sql = sql.rstrip(",\n")
        sql += """
FROM
    home_count_0
"""

        for month in range(1, self.n_months_back):
            sql += f"INNER JOIN home_count_{month} USING (pcod)\n"

        return sql
