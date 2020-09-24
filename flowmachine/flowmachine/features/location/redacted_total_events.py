# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
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


class RedactedTotalEvents(RedactedLocationMetric, Query):
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

    def __init__(self, total_events: TotalLocationEvents):
        self.redaction_target = total_events
        # Construct redacted unique subscriber counts for each bucket
        self.counts = []
        self.additional_colums = []
        for d in pd.date_range(
            total_events.start,
            total_events.stop,
            freq=self.interval_to_pandas[total_events.interval],
        ):
            delta = d + datetime.timedelta(
                **{self.interval_to_delta_arg[total_events.interval]: 1}
            )
            if total_events.hours != "all":
                if not (total_events.hours[0] >= delta.hour >= total_events.hours[1]):
                    continue
            self.additional_colums.append(
                (d.date, d.getattr(self.interval_to_delta_arg[total_events.interval]))
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
        count_qs = [q.get_query() for q in self.counts]
        if self.redaction_target.interval != "day":
            count_qs = [
                f"""SELECT {standardise_date(added[0])} as date, {added[1]} as {self.redaction_target.interval}, *
                        FROM ({count_q}) as _
                       """
                for added, count_q in zip(self.additional_colums, count_qs)
            ]

        redact_filter = reduce(lambda x, y: f"{x} UNION ALL {y}", count_qs)

        return f"""
        SELECT tne.* FROM
        ({self.redaction_target.get_query()}) tne
        NATURAL INNER JOIN
        ({redact_filter}) redactor
        """
