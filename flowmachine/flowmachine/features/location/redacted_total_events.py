# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.query import Query
from flowmachine.features.location.total_events import TotalLocationEvents
from flowmachine.features.location.redacted_location_metric import (
    RedactedLocationMetric,
)
from flowmachine.utils import make_where


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

    def __init__(self, total_events: TotalLocationEvents):
        self.redaction_target = total_events
        super().__init__()

    def _make_query(self):

        # list of columns that we want to group by, these are all the time
        # columns, plus the location columns
        groups = [
            x.split(" AS ")[0] for x in self.redaction_target.time_cols
        ] + self.redaction_target.spatial_unit.location_id_columns

        returning_columns = ", ".join(
            [
                x.split(" AS ")[-1]
                for x in self.redaction_target.spatial_unit.location_id_columns
            ]
        )

        returning_time_columns = ", ".join(
            [x.split(" AS ")[-1] for x in self.redaction_target.time_cols]
        )
        # We now need to group this table by the relevant columns in order to
        # get a count per region
        sql = f"""
            WITH tne AS (SELECT
                {', '.join(self.redaction_target.spatial_unit.location_id_columns)},
                {', '.join(self.redaction_target.time_cols)},
                count(*) AS value,
                count(distinct subscriber) > 15 AS safe_agg
            FROM
                ({self.redaction_target.unioned.get_query()}) unioned
            {make_where(self.redaction_target.direction.get_filter_clause())}
            GROUP BY
                {', '.join(groups)})
                
            SELECT {returning_columns}, 
                {returning_time_columns},
                value
            FROM tne NATURAL JOIN
            (SELECT {returning_columns} FROM tne 
            GROUP BY {returning_columns}
            HAVING every(safe_agg)
            ) _
        """

        return sql
