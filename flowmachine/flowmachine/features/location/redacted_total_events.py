# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.features import TotalLocationEvents


class RedactedTotalEvents(Query):
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

    def __init__(self, total_events: TotalLocationEvents):
        self.total_events = total_events
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.total_events.column_names

    def _make_query(self):

        # running same sql as original query by with added having on the groupby
        sql = f"""
            {self.total_events._make_query()}
            HAVING
                count(distinct subscriber) > 15
        """

        return sql
