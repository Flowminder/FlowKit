# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Core class definition JoinToLocation, which facilitates
the joining of any query with cell/site information to
another spatial level, such as a grid or an admin region.

"""
from typing import List

from flowmachine.utils import get_columns_for_level
from .query import Query
from .custom_query import CustomQuery
from .errors import BadLevelError


class JoinToLocation(Query):
    """
    Intermediate class which joins any query object, or python
    string representing a query object, to some geographical level.
    This can be simply the site with a version a lat-lon value, an
    admin region, a gridded map, or any arbitrary polygon. This
    will return everything in the original query, plus an additional 
    column or columns representing the spatial region that the infrastructure
    element lies within.

    Parameters
    ----------
    left : str or flowmachine.Query
        String to table (with the schema) or else a flowmachine.Query object.
        This represents a table that can be joined to the cell information
        table. This must have a date column (called time) and a location column
        call 'location_id'.
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
    time_col : str, default 'time':
        The name of the column that identifies the time in the source table
        e.g. 'time', 'date', 'start_time' etc.
    column_name : str, optional
        Name of the column that identifies the region. This is only relevant
        for admin region levels or polygon.

    See Also
    --------

    flowmachine/docs/source/notebooks/working_with_locations.ipynb

    for further examples of how this works.

    """

    allowed_levels = [
        "admin0",
        "admin1",
        "admin2",
        "admin3",
        "polygon",
        "grid",
        "lat-lon",
        "versioned-site",
        "versioned-cell",
    ]

    def __init__(
        self,
        left,
        *,
        level,
        time_col="time",
        column_name=None,
        size=None,
        polygon_table=None,
        geom_col="geom",
    ):
        """

        """

        if level not in self.allowed_levels:
            raise BadLevelError(level, self.allowed_levels)
        self.level = level
        if self.level == "polygon" and column_name is None:
            raise ValueError("Must pass a column_name for level=polygon")
        # If the user passes a string, rather than a flowmachine.Query object
        # then we'll simply turn this string into a flowmachine.Query object
        # and proceed as normal.
        if type(left) is str:
            self.left = CustomQuery(left, left.column_names)
        else:
            self.left = left
        self.time_col = time_col
        self.column_name = column_name
        self.location_table_fqn = self.connection.location_table
        self.right_query = self._get_site_query(
            size=size, polygon_table=polygon_table, geom_col=geom_col
        )
        super().__init__()

    def __getattr__(self, name):
        # Don't extend this to hidden variables, such as _df
        # and _len
        if name.startswith("_"):
            raise AttributeError
        try:
            return self.left.__getattribute__(name)
        except AttributeError:
            return self.right_query.__getattribute__(name)

    def _get_site_query(self, *, size, polygon_table, geom_col):
        """
        Returns the appropriate object to join on
        to the right.
        """
        from ..features.spatial.cell_mappings import (
            CellToAdmin,
            CellToPolygon,
            CellToGrid,
        )

        # The logic here finds a query that represents the mapping
        # of cells to an region of interest.
        if self.level.startswith("admin"):
            return CellToAdmin(level=self.level, column_name=self.column_name)
        elif self.level == "polygon":
            return CellToPolygon(
                column_name=self.column_name,
                polygon_table=polygon_table,
                geom_col=geom_col,
            )
        elif self.level == "grid":
            return CellToGrid(size=size)
        elif self.level == "lat-lon":
            sql = f"""
                   SELECT
                        id AS location_id,
                        date_of_first_service,
                        date_of_last_service,
                        ST_X(geom_point::geometry) AS lon,
                        ST_Y(geom_point::geometry) AS lat
                   FROM {self.location_table_fqn}"""
            return CustomQuery(
                sql,
                [
                    "location_id",
                    "date_of_first_service",
                    "date_of_last_service",
                    "lon",
                    "lat",
                ],
            )
        elif self.level == "versioned-site":
            if self.location_table_fqn == "infrastructure.sites":
                sql = """
                    SELECT
                        id AS location_id,
                        id AS site_id,
                        date_of_first_service,
                        date_of_last_service,
                        version,
                        ST_X(geom_point::geometry) AS lon,
                        ST_Y(geom_point::geometry) AS lat
                    FROM infrastructure.sites
                    """
            elif self.location_table_fqn == "infrastructure.cells":
                sql = """
                    SELECT
                        c.id AS location_id,
                        s.id AS site_id,
                        s.date_of_first_service AS date_of_first_service,
                        s.date_of_last_service AS date_of_last_service,
                        s.version as version,
                        ST_X(s.geom_point::geometry) AS lon,
                        ST_Y(s.geom_point::geometry) AS lat
                    FROM infrastructure.sites AS s
                    RIGHT JOIN
                    infrastructure.cells AS c
                    ON s.id = c.site_id
                    """
            return CustomQuery(
                sql,
                [
                    "location_id",
                    "site_id",
                    "date_of_first_service",
                    "date_of_last_service",
                    "version",
                    "lon",
                    "lat",
                ],
            )
        elif self.level == "versioned-cell":
            if self.location_table_fqn == "infrastructure.cells":
                sql = """
                    SELECT
                        id AS location_id,
                        date_of_first_service,
                        date_of_last_service,
                        version,
                        ST_X(geom_point::geometry) AS lon,
                        ST_Y(geom_point::geometry) AS lat
                    FROM infrastructure.cells
                    """
                return CustomQuery(
                    sql,
                    [
                        "location_id",
                        "version",
                        "date_of_first_service",
                        "date_of_last_service",
                        "lon",
                        "lat",
                    ],
                )
            else:
                raise ValueError("Versioned cell level is unavailable.")

    @property
    def column_names(self) -> List[str]:
        right_columns = get_columns_for_level(self.level, self.column_name)
        left_columns = self.left.column_names
        for column in right_columns:
            if column in left_columns:
                left_columns.remove(column)
        return left_columns + right_columns

    def _make_query(self):

        right_columns = get_columns_for_level(self.level, self.column_name)
        left_columns = self.left.column_names
        for column in right_columns:
            if column in left_columns:
                left_columns.remove(column)

        right_columns_str = ", ".join([f"sites.{c}" for c in right_columns])
        left_columns_str = ", ".join([f"l.{c}" for c in left_columns])

        sql = f"""
        SELECT
            {left_columns_str},
            {right_columns_str}
        FROM
            ({self.left.get_query()}) AS l
        INNER JOIN
            ({self.right_query.get_query()}) AS sites
        ON
            l.location_id = sites.location_id
          AND
            l.{self.time_col}::date BETWEEN coalesce(sites.date_of_first_service,
                                                '-infinity'::timestamptz) AND
                                       coalesce(sites.date_of_last_service,
                                                'infinity'::timestamptz)
        """

        return sql
