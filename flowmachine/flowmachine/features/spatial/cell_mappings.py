# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that deal with mapping cells (or towers or sites)
to a spatial level, mostly be performing a spatial join.
Examples of this include CellToAdmin or CellToGrid.
"""
from typing import List

from ...core import Query
from .grid import Grid


class CellToPolygon(Query):
    """
    Class that maps a cell with a lat-lon to a geographical
    region.

    Parameters
    ----------
    column_name : str, optional
        The name of the column to fetch from the geometry
        table in the database. Can also be a list of names.
    polygon_table : str, or flowmachine.Query optional
        name of the table containing the geography information.
        Can be either the name of a table, with the schema, a flowmachine.Query
        object, or a string representing a query.
    geom_col : str, default 'geom'
        column that defines the geography.
    """

    def __init__(self, *, column_name, polygon_table, geom_col="geom"):

        if type(column_name) is str:
            self.column_name = [column_name]
        else:
            self.column_name = column_name
        self.polygon_table = polygon_table
        self.geom_col = geom_col
        self.location_info_table_fqn = self.connection.location_table
        self.location_info_table = self.connection.location_table.split(".")[-1]

        super().__init__()

    def _get_subtable(self):
        """
        Private method which takes the table and returns a query
        representing the object. This is necessary as the table can
        be passed in a variety of ways.
        """

        if issubclass(self.polygon_table.__class__, Query):
            return f"({self.polygon_table.get_query()}) AS polygon"
        elif "select " in self.polygon_table.lower():
            return f"({self.polygon_table}) AS polygon"
        else:
            return f"{self.polygon_table} AS polygon"

    @property
    def column_names(self) -> List[str]:
        return [
            "location_id",
            "version",
            "date_of_first_service",
            "date_of_last_service",
        ] + self.column_name

    def _make_query(self):

        # if the subscriber wants to select a geometry from the sites table there
        # is no need to join the table with itself.
        if (
            isinstance(self.polygon_table, str)
            and self.location_info_table_fqn == self.polygon_table.lower().strip()
        ):

            columns = ", ".join(f"locinfo.{c}" for c in self.column_name)

            # Create a table
            tower_admins = f"""
            SELECT
                locinfo.id AS location_id,
                locinfo.version,
                locinfo.date_of_first_service,
                locinfo.date_of_last_service,
                {columns}
            FROM
                {self.location_info_table_fqn} AS locinfo
            """

        # otherwise performs the geometric join
        else:
            columns = ", ".join(f"polygon.{c}" for c in self.column_name)

            # Create a table
            tower_admins = f"""
            SELECT
                locinfo.id AS location_id,
                locinfo.version,
                locinfo.date_of_first_service,
                locinfo.date_of_last_service,
                {columns}
            FROM
                {self.location_info_table_fqn} AS locinfo
            INNER JOIN
                {self._get_subtable()}
            ON ST_within(
                locinfo.geom_point::geometry,
                ST_SetSRID(polygon.{self.geom_col}, 4326)::geometry
            )
            """

        return tower_admins


class CellToAdmin(Query):
    """
    Maps all cells (aka sites) to a admin region. This is a thin wrapper to
    the more general class CellToPolygon, which assumes that you have
    the standard set-up.

    Parameters
    ----------
    level : {'adminN'}
        One of admin1, admin2 etc.
    column_name : str, optional
        Pass a string of the column to use as the
        identifier of the admin region. By default
        this will be admin*pcod. But you may wish
        to use something else, such as admin3name.
    """

    def __init__(self, *, level, column_name=None):
        self.level = level
        # If there is no column_name passed then we can use
        # the default, which is of the form admin3name.
        if column_name is None:
            self.column_name = self._get_standard_name()
        else:
            self.column_name = column_name
        table = f"geography.{self.level}"
        self.mapping = CellToPolygon(column_name=self.column_name, polygon_table=table)

        super().__init__()

    def _get_standard_name(self):
        """
        Returns the standard name of the column that identifies
        the name of the region.
        """

        return f"{self.level}pcod"

    @property
    def column_names(self) -> List[str]:
        columns = self.mapping.column_names
        columns.remove(self.column_name)
        # If the user has asked for the standard column_name
        # then we will alias this column as 'pcod', otherwise
        # we'll won't alias it at all.
        if self.column_name == self._get_standard_name():
            col_name = "pcod"
        else:
            col_name = self.column_name
        return columns + [col_name]

    def _make_query(self):

        columns = self.mapping.column_names
        columns.remove(self.column_name)
        other_cols = ", ".join(columns)

        # If the user has asked for the standard column_name
        # then we will alias this column as 'pcod', otherwise
        # we'll won't alias it at all.
        if self.column_name == self._get_standard_name():
            col_name = f"{self.column_name} AS pcod"
        else:
            col_name = self.column_name

        sql = f"""
        SELECT
            {other_cols},
            {col_name}
        FROM
            ({self.mapping.get_query()}) AS map
        """

        return sql


class CellToGrid(Query):
    """
    Query representing a mapping between all the sites in the database
    and a grid of arbitrary size.

    Parameters
    ----------
    size : float or int
        Size of the grid in kilometres
    """

    def __init__(self, *, size):
        """

        """

        self.size = size
        self.grid = Grid(self.size)
        self.mapping = CellToPolygon(
            polygon_table=self.grid, column_name=["grid_id"], geom_col="geom_square"
        )
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.mapping.column_names

    def _make_query(self):
        return self.mapping.get_query()
