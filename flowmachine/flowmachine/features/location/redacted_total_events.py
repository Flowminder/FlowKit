# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core.mixins import GeoDataMixin
from functools import reduce

import numpy as np

from flowmachine.core.query import Query
from flowmachine.features import UniqueSubscriberCounts
from flowmachine.features.location.redacted_unique_subscriber_counts import (
    RedactedUniqueSubscriberCounts,
)
from flowmachine.features.location.total_events import TotalLocationEvents
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)
from flowmachine.utils import standardise_date
import datetime
import pandas as pd


class RedactedTotalEvents(RedactedLocationMetric, GeoDataMixin, Query):
    """
    Calculates the total number of events on an hourly basis
    per location (such as a tower or admin region),
    and per interaction type. Returns values only where there
    events were more than 15 people at the location in all buckets.

    Parameters
    ----------
    total_events : TotalLocationEvents
        Total location events query to redact
    """

    interval_to_pandas = dict(day="D", min="T", hour="H")
    interval_to_delta_arg = dict(day="days", min="minutes", hour="hours")
    interval_to_timestamp_attr = dict(day="day", min="minute", hour="hour")

    def __init__(self, total_events: TotalLocationEvents):
        self.redaction_target = total_events
        self.spatial_unit = total_events.spatial_unit
        # Construct redacted unique subscriber counts for each bucket
        self.counts = []
        self.additional_columns = []
        for d in pd.date_range(
            total_events.start,
            total_events.stop,
            freq=self.interval_to_pandas[total_events.interval],
        )[:-1]:
            delta = d + datetime.timedelta(
                **{self.interval_to_delta_arg[total_events.interval]: 1}
            )
            if total_events.hours is not None:
                if not (total_events.hours[0] >= delta.hour >= total_events.hours[1]):
                    continue
            self.additional_columns.append(
                (
                    d.date(),
                    *(
                        getattr(d, self.interval_to_timestamp_attr[t_col])
                        for t_col in total_events.time_cols[1:]
                    ),
                )
            )
            self.counts.append(
                RedactedUniqueSubscriberCounts(
                    unique_subscriber_counts=UniqueSubscriberCounts(
                        d,
                        d
                        + datetime.timedelta(
                            **{self.interval_to_delta_arg[total_events.interval]: 1}
                        ),
                        spatial_unit=total_events.spatial_unit,
                        table=total_events.table,
                        hours=total_events.hours,
                    ),
                )
            )
        super().__init__()

    def _make_query(self):
        time_cols = []
        for added in self.additional_columns:
            time_col_list = [
                f"'{added[0]}' as date",
                *(
                    f"{added} as {t_col}"
                    for added, t_col in zip(
                        added[1:], self.redaction_target.time_cols[1:]
                    )
                ),
            ]
            time_cols.append(", ".join(time_col_list))
        count_qs = [
            f"""SELECT {time_col}, 
                {", ".join(count_q.spatial_unit.location_id_columns)}
                        FROM ({count_q.get_query()}) as _
                       """
            for time_col, count_q in zip(time_cols, self.counts)
        ]

        redact_filter = reduce(lambda x, y: f"{x} UNION ALL {y}", count_qs)

        sql = f"""
        SELECT tne.* FROM
        ({self.redaction_target.get_query()}) tne
        INNER JOIN
        ({redact_filter}) redactor
        USING ({", ".join((*self.redaction_target.spatial_unit.location_id_columns, *self.redaction_target.time_cols))})
        """
        print(sql)
        return sql
