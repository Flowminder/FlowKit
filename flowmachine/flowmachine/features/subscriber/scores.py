# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# --*-- coding: utf-8 -*-
"""
Calculates an event score for each event based
on a scoring dictionary.
"""

from typing import Dict, Union, Tuple

from typing import List

from ..utilities import EventsTablesUnion
from ...core import Query
from ...core import JoinToLocation
from flowmachine.utils import get_columns_for_level


class EventScore(Query):
    """
    Represents an event score class.

    This class assigns a score to each event based on the hour of the day and
    the day of the week. The scores can be useful to cluster a set of events
    based on its signature. Such type of analysis reduces the dimensionality of
    the problem by projecting a given event pattern onto the real line.

    This class returns a table with scores averaged across the requested level
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
    level : str, default 'admin3'
        Levels can be one of:
            'cell':
                The identifier as it is found in the CDR itself
            'versioned-cell':
                The identifier as found in the CDR combined with the version from
                the cells table.
            'versioned-site':
                The ID found in the sites table, coupled with the version
                number.
            'polygon':
                A custom set of polygons that live in the database. In which
                case you can pass the parameters column_name, which is the column
                you want to return after the join, and table_name, the table where
                the polygons reside (with the schema), and additionally geom_col
                which is the column with the geometry information (will default to
                'geom')
            'admin*':
                An admin region of interest, such as admin3. Must live in the
                database in the standard location.
            'grid':
                A square in a regular grid, in addition pass size to
                determine the size of the polygon.
    hours : tuple of ints, default 'all'
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
    column_name : str, optional
        Option, none-standard, name of the column that identifies the
        spatial level, i.e. could pass admin3pcod to use the admin 3 pcode
        as opposed to the name of the region.

    Examples
    --------
    >>> es = EventScore(start='2016-01-01', stop='2016-01-05',
                level='versioned-site')
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
        level: str = "admin3",
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
        column_name: Union[str, List[str]] = None,
        subscriber_subset=None,
        polygon_table=None,
        size=None,
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
                f"Hour of day score dictionary must have 24 hours. Got {len(score_hour)}"
            )
        if not all([-1 <= float(x) <= 1 for x in score_hour]):
            raise ValueError(f"Hour of day scores must be floats between -1 and 1.")
        if not all([-1 <= float(x) <= 1 for x in score_dow.values()]):
            raise ValueError(f"Day of week scores must be floats between -1 and 1.")
        self.score_hour = score_hour
        self.score_dow = score_dow
        self.level = level
        self.start = start
        self.stop = stop
        self.hours = hours
        self.subscriber_identifier = subscriber_identifier
        self.column_name = column_name
        self.sds = JoinToLocation(
            EventsTablesUnion(
                start=start,
                stop=stop,
                columns=[subscriber_identifier, "location_id", "datetime"],
                tables=table,
                hours=self.hours,
                subscriber_subset=subscriber_subset,
                subscriber_identifier=self.subscriber_identifier,
            ),
            level=self.level,
            time_col="datetime",
            column_name=self.column_name,
            polygon_table=polygon_table,
            size=size,
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

        location_cols = get_columns_for_level(self.level, self.column_name)

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
            + get_columns_for_level(self.level, self.column_name)
            + ["score_hour", "score_dow"]
        )
