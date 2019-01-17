# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# --*-- coding: utf-8 -*-
"""
Calculates an event score for each event based
on a scoring dictionary.
"""

import re
import datetime as dt
from _md5 import md5
from typing import List, Dict, Union

from ..utilities.sets import EventTableSubset, EventsTablesUnion
from ...core import Query
from ...core import JoinToLocation
from ...utils.utils import get_columns_for_level


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
    score_hour : dict
      A dictionary containing a key for every hour of the day, and a numerical score between
      zero and 1.
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
        start: str,
        stop: str,
        level: str = "admin3",
        hours: str = "all",
        table: str = "all",
        score_hour: Dict[int, float] = {
            0: -1,
            1: -1,
            2: -1,
            3: -1,
            4: -1,
            5: -1,
            6: -1,
            7: 0,
            8: 0,
            9: 1,
            10: 1,
            11: 1,
            12: 1,
            13: 1,
            14: 1,
            15: 1,
            16: 1,
            17: 0,
            18: 0,
            19: 0,
            20: 0,
            21: -1,
            22: -1,
            23: -1,
        },
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
        **kwargs,
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
        if set(score_hour.keys()) != set(range(24)):
            raise ValueError(
                f"Hour of day score dictionary must have values for all hours. Only got {set(score_hour.keys())}"
            )
        if not all([-1 <= float(x) <= 1 for x in score_hour.values()]):
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
                start,
                stop,
                [subscriber_identifier, "location_id", "datetime"],
                tables=table,
                hours=self.hours,
                subscriber_identifier=self.subscriber_identifier,
                **kwargs,
            ),
            level=self.level,
            time_col="datetime",
            column_name=self.column_name,
            **kwargs,
        )

        super().__init__()

    def _make_query(self):

        # to_char('2016-01-01'::date, 'day');
        # select extract(hour from timestamp '2001-02-16 20:38:40');

        day_of_week_and_hour_added = f"""SELECT *, 
        trim(to_char(datetime, 'day')) as dow, extract(hour from datetime) as hour 
        FROM ({self.sds.get_query()}) _"""

        hour_score_case = f"""(CASE 
        {" ".join(f"WHEN hour={hour} THEN {score}" for hour, score in self.score_hour.items())}
        END)"""

        day_score_case = f"""(CASE 
                {" ".join(f"WHEN dow='{dow}' THEN {score}" for dow, score in self.score_dow.items())}
                END)"""

        location_cols = get_columns_for_level(self.level, self.column_name)

        query = f"""
        SELECT subscriber, {", ".join(location_cols)}, datetime,
               {hour_score_case} AS score_hour,
               {day_score_case} AS score_dow
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


class LabelEventScore(Query):
    """
    Represents a label event score class.

    This class will label a table containing scores based on a labelling
    dictionary. It allows one to specify labels which every subscriber must have.
    This class is used to label locations based on scoring signatures in the
    absence of other automated labelling mechanisms.

    Parameters
    ----------
    scores : flowmachine.Query
        A flowmachine.Query object.
        This represents a table that contains scores which are used to label
        a given location. This table must have a subscriber column (called subscriber).
    labels : dict
        A dictionary whose keys are the label names and the values are
        strings specifying which observations should be labelled with the
        given label. Those rules should be written in the same way as one
        would write a `WHERE` clause in SQL.  Observations which do not
        match any of the criteria are given the reserved label 'unknown'.
        Eg.: \`{'evening': '(score_hour > 0) AND (score_dow > 0.5 OR
        score_dow < -0.5)' , 'daytime': '(score_hour < 0) AND (score_dow <
        0.5 AND score_dow > -0.5)'}\`
    enum_type : str
        The name of the Enumerated type in the database used to represent
        the labels. It is important to ensure that this type does not yet
        exist in the database in case you want to redefine it.
    required :
        Specifies a label which every subscriber must possess independently of
        the score.  This is used in cases where, for instance, we require
        that all subscribers must have an evening/home location.
    """

    def __init__(self, scores, labels, enum_type, required=None):

        self.scores = scores
        if not isinstance(scores, Query):
            raise TypeError(
                "Scores must be of type Query, e.g. EventScores, Table, CustomQuery"
            )
        self.labels = labels
        self.enum_type = enum_type
        self.required = required

        injection_attempt = re.compile(r"(THEN|END|WHEN|ELSE)", re.I)
        for l, r in self.labels.items():
            if injection_attempt.search(r) is not None:
                raise ValueError(
                    "Rules with keywords 'THEN', 'END', 'WHEN' and 'ELSE' are not valid"
                )

        self.label_names = list(labels.keys())
        if "unknown" in self.label_names:
            raise ValueError(
                "'unknown' is a reserved label name, please use another name"
            )
        else:
            self.label_names = ["unknown"] + self.label_names

        super().__init__()

    def _create_enum_type(self):
        """
        Creates the `Enumerate` type in the database based with labels
        initialized in this class.
        """
        with self.connection.engine.begin():
            type_exists = self.connection.engine.execute(
                f"""
            SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname='{self.enum_type}')
            """
            ).fetchall()[0][0]

            if type_exists:
                label_names = self.connection.engine.execute(
                    f"""
                SELECT UNNEST(ENUM_RANGE(ENUM_FIRST(null::{self.enum_type}),null::{self.enum_type}))
                """
                ).fetchall()
                label_names = [l[0] for l in label_names]

                if not all([l in label_names for l in self.label_names]):
                    raise ValueError(
                        "`labels_set` is already defined in "
                        + "the database and does not include the labels defined in `labels`. "
                        + "Please choose another `label_sets` or remove this Enumerate type from the database."
                    )

            else:
                label_names_string = ", ".join([f"'{l}'" for l in self.label_names])
                self.connection.engine.execute(
                    f"""
                CREATE TYPE {self.enum_type} AS ENUM ({label_names_string})
                """
                )

    def _make_query(self):

        scores_cols = self.scores.column_names
        scores = f"({self.scores.get_query()}) AS scores"

        self._create_enum_type()

        rules = "CASE WHEN "
        rules += " WHEN ".join([f"{r} THEN '{l}'" for l, r in self.labels.items()])
        rules += " ELSE 'unknown' END"

        sql = f"""
        SELECT *, ({rules})::{self.enum_type} AS label FROM {scores}
        """

        if self.required is not None:
            scores_cols = ", ".join([f"labelled.{c}" for c in scores_cols])
            sql = f"""

                WITH labelled AS ({sql}),
                    filtered AS (SELECT l.subscriber AS subscriber FROM labelled l
                                GROUP BY l.subscriber, label HAVING label = '{self.required}')

                SELECT {scores_cols}, '{self.required}'::{self.enum_type} AS label
                FROM labelled
                LEFT JOIN filtered
                ON labelled.subscriber = filtered.subscriber
                WHERE filtered.subscriber IS NULL
                UNION ALL
                SELECT {scores_cols}, label
                FROM labelled
                RIGHT JOIN filtered
                ON labelled.subscriber = filtered.subscriber

            """

        return sql

    @property
    def column_names(self) -> List[str]:
        return self.scores.column_names + ["label"]
