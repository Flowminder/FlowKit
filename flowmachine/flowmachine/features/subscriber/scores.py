# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# --*-- coding: utf-8 -*-
"""
Calculates an event score for each event based
on a scoring dictionary.
"""

from typing import Dict, Union, Tuple, Optional

from typing import List

from ..utilities import EventsTablesUnion
from ...core import Query, location_joined_query, make_spatial_unit
from ...core.spatial_unit import AnySpatialUnit
from flowmachine.utils import standardise_date


class EventScore(Query):
    """
    Represents an event score class.

    This class assigns a score to each event based on the hour of the day and
    the day of the week. The scores can be useful to cluster a set of events
    based on its signature. Such type of analysis reduces the dimensionality of
    the problem by projecting a given event pattern onto the real line.

    This class returns a table with scores averaged across the requested spatial unit
    per subscriber.

    Parameters
    ----------
    score_hour : list of float
      A length 24 list containing numerical scores between -1 and 1, where entry 0 is midnight.
    score_dow : dict
      A dictionary containing a key for every day of the week, and a numerical score between
      zero and 1. Keys should be the lowercase, full name of the day.
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01
    stop : str
        As above
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.
    hours : tuple of ints, default None
        Subset the result within certain hours, e.g. (4,17)
        This will subset the query only with these hours, but
        across all specified days. Or set to 'all' to include
        all hours.
    table : str, default 'all'
        schema qualified name of the table which the analysis is
        based upon.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Examples
    --------
    >>> es = EventScore(start='2016-01-01', stop='2016-01-05',
                spatial_unit=make_spatial_unit('versioned-site'))
    >>> es.head()
                 subscriber location_id  version  score_hour  score_dow
    3EgqzplqPYDyGRVK      DbWg4K        0         0.0       -1.0
    G2DQzae1qOa48jK9      EyZykQ        0         1.0       -1.0
    148ZaRZe54wPGQ9r      nWM8R3        0        -1.0       -1.0
    QrAlXqDbXDkNJe3E      pdVVV4        0         1.0        0.0
    kjGXLy9lWnZ4V6J7      r9KbQy        0         0.0        1.0
    ...

    """

    def __init__(
        self,
        *,
        start: str,
        stop: str,
        spatial_unit: Optional[AnySpatialUnit] = None,
        hours: Union[str, Tuple[int, int]] = "all",
        table: Union[str, List[str]] = "all",
        score_hour: List[float] = [
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            0,
            0,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            0,
            0,
            0,
            0,
            -1,
            -1,
            -1,
        ],
        score_dow: Dict[str, float] = {
            "monday": 1,
            "tuesday": 1,
            "wednesday": 1,
            "thursday": 0,
            "friday": -1,
            "saturday": -1,
            "sunday": -1,
        },
        subscriber_identifier: str = "msisdn",
        subscriber_subset=None,
    ):
        if set(score_dow.keys()) != {
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        }:
            raise ValueError(
                f"Day of week score dictionary must have values for all days. Only got {set(score_dow.keys())}"
            )
        if len(score_hour) != 24:
            raise ValueError(
                f"Hour of day score list must have 24 hours. Got {len(score_hour)}"
            )
        if not all([-1 <= float(x) <= 1 for x in score_hour]):
            raise ValueError(f"Hour of day scores must be floats between -1 and 1.")
        if not all([-1 <= float(x) <= 1 for x in score_dow.values()]):
            raise ValueError(f"Day of week scores must be floats between -1 and 1.")
        self.score_hour = score_hour
        self.score_dow = score_dow
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=3)
        else:
            self.spatial_unit = spatial_unit
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.subscriber_identifier = subscriber_identifier
        self.sds = location_joined_query(
            EventsTablesUnion(
                start=start,
                stop=stop,
                columns=[subscriber_identifier, "location_id", "datetime"],
                tables=table,
                hours=self.hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=self.subscriber_identifier,
            ),
            spatial_unit=self.spatial_unit,
            time_col="datetime",
        )

        super().__init__()

    def _make_query(self):

        # to_char('2016-01-01'::date, 'day');
        # select extract(hour from timestamp '2001-02-16 20:38:40');

        day_of_week_and_hour_added = f"""SELECT *, 
        trim(to_char(datetime, 'day')) as dow, extract(hour from datetime) as hour 
        FROM ({self.sds.get_query()}) _"""

        hour_case = f"""(CASE 
        {" ".join(f"WHEN hour={hour} THEN {score}" for hour, score in enumerate(self.score_hour))}
        END)"""

        day_case = f"""(CASE 
                {" ".join(f"WHEN dow='{dow}' THEN {score}" for dow, score in self.score_dow.items())}
                END)"""

        location_cols = self.spatial_unit.location_id_columns

        query = f"""
        SELECT subscriber, {", ".join(location_cols)}, datetime,
               {hour_case} AS score_hour,
               {day_case} AS score_dow
        FROM ({day_of_week_and_hour_added}) AS subset_dates
        """

        location_cols = ", ".join([f"scores.{c}" for c in location_cols])

        sql = f"""

            SELECT scores.subscriber AS subscriber,
                {location_cols},
                SUM(score_hour)::float / COUNT(*) AS score_hour,
                SUM(score_dow)::float / COUNT(*) AS score_dow
            FROM ({query}) AS scores
            GROUP BY scores.subscriber,
                    {location_cols}

        """

        return sql

    @property
    def column_names(self) -> List[str]:
        return (
            ["subscriber"]
            + self.spatial_unit.location_id_columns
            + ["score_hour", "score_dow"]
        )
