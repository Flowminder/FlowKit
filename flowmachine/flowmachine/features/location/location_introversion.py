# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List

"""
Location introversion [1]_ calculates the proportion
of interactions made within a certain location 
in which the interaction counterpart is located
in the same location. Its opposite is
measured by location extroversion.


References
---------
.. [1] Christopher Smith, Afra Mashhadi, Licia Capra. "Ubiquitous Sensing for Mapping Poverty in Developing Countries". NetMob Conference Proceedings, 2013. http://haig.cs.ucl.ac.uk/staff/L.Capra/publicatiONs/d4d.pdf

"""
from ...core import Query
from ...core.mixins import GeoDataMixin


from ...core import JoinToLocation
from ..utilities import EventsTablesUnion


class LocationIntroversion(GeoDataMixin, Query):
    """
    Calculates the proportions of events that take place
    within a location in which all involved parties
    are located in the same location (introversion), and those
    which are between parties in different locations (extroversion).

    Parameters
    ----------
    start : str
        ISO format date string to at which to start the analysis
    stop : str
        AS above for the end of the analysis
    table : str, default 'all'
        Specifies a table of cdr data on which to base the analysis. Table must
        exist in events schema. If 'ALL' then we use all tables specified in
        flowmachine.yml.
    level : str
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
    direction : str, default 'both'.
        Determines if query should filter only outgoing
        events ('out'), incoming events ('in'), or both ('both').

    Notes
    -----

    Equation 3 of the original paper states introversion as the ratio of introverted to extroverted events
    but indicates that this will return values in the range [0, 1]. However, the preceding text indicate
    that introversion is the _proportion_ of events which are introverted. We follow the latter here.

    Examples
    --------
    >>> LocationIntroversion("2016-01-01", "2016-01-07").head()
          location_id  introversion  extroversion
    0    AUQZGMW3      0.050000      0.950000
    1    ns6vzdkC      0.049180      0.950820
    2    llTlNC7E      0.046122      0.953878
    3    WET2L101      0.045549      0.954451
    4    eAwMUT94      0.045175      0.954825
    """

    def __init__(
        self,
        start,
        stop,
        *,
        table="all",
        level="cell",
        direction="both",
        hours="all",
        subscriber_subset=None,
        subscriber_identifier="msisdn",
        size=None,
        polygon_table=None,
        geom_col="geom",
        column_name=None,
    ):

        self.query_columns = ["id", "outgoing", "location_id", "datetime"]
        self.start = start
        self.stop = stop
        self.table = table
        self.level = level
        self.direction = direction

        if self.level == "versioned-site":
            raise NotImplementedError(
                'The level "versioned-site" is currently not'
                + "supported in the `LocationIntroversion()` class."
            )

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            columns=self.query_columns,
            tables=self.table,
            hours=hours,
            subscriber_subset=subscriber_subset,
            subscriber_identifier=subscriber_identifier,
        )
        self.level_columns = ["location_id"]

        if self.level not in ("cell"):
            self.join_to_location = JoinToLocation(
                self.unioned_query,
                level=self.level,
                time_col="datetime",
                column_name=column_name,
                size=size,
                polygon_table=polygon_table,
                geom_col=geom_col,
            )
            cols = set(self.join_to_location.column_names)
            if self.level != "lat-lon":
                cols -= {"lat", "lon"}
            self.level_columns = list(cols.difference(self.query_columns))
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.level_columns + ["introversion", "extroversion"]

    def __build_query(self, location_columns, union):
        """
        Private method for building feature query. This
        is abstracted for readability. It's only called
        by _make_query()

        Parameters
        ----------
        location_columns: str
            Relevant location column to make join to.
            This 

        union: str
            SQL query representing an union table query.
            This query either comes from the EventsTablesUnion()
            or the JoinToLocation() class.

        Returns
        -------
        sql: str
            SQL string with query representation.

        """
        if self.direction == "both":
            sql_direction = ""
        elif self.direction == "in":
            sql_direction = """
                WHERE NOT A.outgoing
            """
        elif self.direction == "out":
            sql_direction = """
                WHERE A.outgoing
            """

        sql = f"""
        WITH unioned_table AS ({union})
        SELECT *, 1-introversion as extroversion FROM
        (SELECT {', '.join(location_columns)}, sum(introverted::integer)/count(*)::float as introversion FROM (
            SELECT
               {', '.join(f'A.{c} as {c}' for c in location_columns)},
               {' AND '.join(f'A.{c} = B.{c}' for c in location_columns)} as introverted
            FROM unioned_table as A
            INNER JOIN unioned_table AS B
                  ON A.id = B.id
                     AND A.outgoing != B.outgoing
                     {sql_direction}
        ) _
        GROUP BY {', '.join(location_columns)}) _
        ORDER BY introversion DESC
        """

        return sql

    def _make_query(self):

        if self.level == "cell":
            sql = self.__build_query(
                location_columns=self.level_columns,
                union=self.unioned_query.get_query(),
            )
        else:
            sql = self.__build_query(
                location_columns=self.level_columns,
                union=self.join_to_location.get_query(),
            )

        return sql
