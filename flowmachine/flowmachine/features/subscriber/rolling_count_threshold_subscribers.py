from functools import reduce
from typing import List
from dateutil.rrule import DAILY, rrule
from dateutil.relativedelta import relativedelta

from flowmachine.core.mixins.exposed_datetime_mixin import ExposedDatetimeMixin
from flowmachine.core.query import Query
from flowmachine.features.subscriber.active_subscribers import ActiveSubscribers


class RollingCountThresholdSubscribers(ExposedDatetimeMixin, Query):
    """For anything with a subscriber and datetime column, selects each `subscriber on day` between `start_date`
    and `end_date` that appears at least `threshold` times between `day-lookback_period' and `day+lookforward_period`.
    """

    def __init__(
        self,
        start_date,
        end_date,
        threshold,
        active_hours,
        events_tables,
        lookback_period=0,
        lookforward_period=0,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.lookback_period = lookback_period
        self.lookforward_period = lookforward_period
        self.threshold = threshold
        self.events_table = events_tables
        self.active_hours = active_hours

        self.window_start = self._start_dt - relativedelta(days=self.lookback_period)
        self.window_end = self._end_dt + relativedelta(days=self.lookforward_period)

        self.rolling_queries = [
            ActiveSubscribers(
                start_date=day - relativedelta(days=self.lookback_period),
                end_date=day + relativedelta(days=self.lookforward_period),
                active_days=self.threshold,
                active_hours=self.active_hours,
                tables=self.events_table,
            )
            for day in rrule(DAILY, dtstart=self._start_dt, until=self._end_dt)
        ]
        self.final_query = reduce(lambda x, y: x.union(y), self.rolling_queries)

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]

    def _make_query(self):
        sql = f"""
        SELECT DISTINCT subscriber
        FROM ({self.final_query.get_query()}) AS tbl
        """

        return sql
