# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that map cells (or towers or sites) to a spatial unit
(e.g. versioned-cell, admin*, grid, ...).
"""
from typing import List
import re

from . import Query, GeoTable, Grid


def get_alias(column_name):
    """
    Given a column name string, return the alias (if there is one),
    or return the provided column name if there is no alias.

    Examples
    --------
    >>> get_alias("col AS alias")
      "alias"
    >>> get_alias("col")
      "col"
    """
    return re.split(" as ", column_name, flags=re.IGNORECASE)[-1]


class SpatialUnit(Query):
    def __init__(self, *, selected_column_names, location_column_names, location_info_table=None, join_clause=""):
        if type(selected_column_names) is str:
            self._cols = [selected_column_names]
        else:
            self._cols = selected_column_names

        if type(other_column_names) is str:
            self._loc_cols = [location_column_names]
        else:
            self._loc_cols = location_column_names

        missing_cols = [c for c in self._loc_cols if not (c in self.column_names)]
        if missing_cols:
            raise ValueError(
                f"Location columns {missing_cols} are not in returned columns"
            )

        if location_info_table:
            self.location_info_table = location_info_table
        else:
            self.location_info_table = self.connection.location_table
        
        self._join_clause = join_clause

        super().__init__()

    # TODO: Need a method to check whether the required data can be found in the DB

    @property
    def location_columns(self) -> List[str]:
        return self._loc_cols

    @property
    def column_names(self) -> List[str]:
        return [get_alias(c) for c in self._cols]

    def _make_query(self):
        columns = ", ".join(self._cols)
        sql = f"""
        SELECT
            {columns}
        FROM {self.location_info_table}
        {self._join_clause}
        """

        return sql


class LatLonSpatialUnit(SpatialUnit)
    def __init__(self):
        super().__init__(
            selected_column_names=[
                "id AS location_id",
                "date_of_first_service",
                "date_of_last_service",
                "ST_X(geom_point::geometry) AS lon",
                "ST_Y(geom_point::geometry) AS lat",
            ],
            location_column_names=["lat", "lon"],
        )


class VersionedCellSpatialUnit(SpatialUnit):
    def __init__(self):
        if self.connection.location_table != "infrastructure.cells":
            raise ValueError("Versioned cell spatial unit is unavailable.")
        
        super().__init__(
            selected_column_names=[
                "id AS location_id",
                "date_of_first_service",
                "date_of_last_service",
                "version",
                "ST_X(geom_point::geometry) AS lon",
                "ST_Y(geom_point::geometry) AS lat",
            ],
            location_column_names=["location_id", "version", "lon", "lat"],
            location_info_table="infrastructure.cells",
        )


class VersionedSiteSpatialUnit(SpatialUnit):
    def __init__(self):
        location_table = self.connection.location_table

        sites_alias = "s"
        if location_table == "infrastructure.sites":
            cells_alias = sites_alias
            join_clause = f"""
            RIGHT JOIN
            infrastructure.cells AS {cells_alias}
            ON {sites_alias}.id = {cells_alias}.site_id
            """
        elif location_table == "infrastructure.cells":
            cells_alias = "c"
            join_clause = ""
        else:
            raise ValueError(
                f"Expected location table to be 'infrastructure.cells' "
                f"or 'infrastructure.sites', not '{location_table}''"
            )
        
        super().__init__(
            selected_column_names=[
                f"{cells_alias}.id AS location_id",
                f"{sites_alias}.id AS site_id",
                f"{sites_alias}.date_of_first_service AS date_of_first_service",
                f"{sites_alias}.date_of_last_service AS date_of_last_service",
                f"{sites_alias}.version as version",
                f"ST_X({sites_alias}.geom_point::geometry) AS lon",
                f"ST_Y({sites_alias}.geom_point::geometry) AS lat",
            ],
            location_column_names=["location_id", "version", "lon", "lat"],
            location_info_table=f"infrastructure.sites AS {sites_alias}",
            join_clause=join_clause,
        )


class PolygonSpatialUnit(SpatialUnit):
    """
    Class that provides a mapping from cell/site data in the location table to
    spatial regions defined by geography information in a table.

    Parameters
    ----------
    column_name : str or list
        The name of the column to fetch from the geometry
        table in the database. Can also be a list of names.
    geom_table : str or flowmachine.Query
        name of the table containing the geography information.
        Can be either the name of a table, with the schema, or
        a flowmachine.Query object.
    geom_col : str, default 'geom'
        column that defines the geography.
    """

    _columns_from_locinfo_table = (
        "id AS location_id",
        "version",
        "date_of_first_service",
        "date_of_last_service",
    )

    def __init__(self, *, column_name, geom_table, geom_col="geom"):
        if type(column_name) is str:
            self.column_name = [column_name]
        else:
            self.column_name = column_name
        if issubclass(geom_table.__class__, Query):
            self.geom_table = geom_table
        else:
            self.geom_table = GeoTable(name=geom_table, geom_column=geom_col)
        self.geom_col = geom_col
        self.location_info_table_fqn = self.connection.location_table
        # if the subscriber wants to select a geometry from the sites table there
        # is no need to join the table with itself.
        self.requires_join = not (
            hasattr(self.geom_table, "fully_qualified_table_name")
            and (
                self.location_info_table_fqn
                == self.geom_table.fully_qualified_table_name
            )
        )

        super().__init__()

    # Need a method to check whether the required data can be found in the DB

    def _other_columns(self):
        """
        Helper function which returns the list of returned column names,
        excluding self.location_columns.
        """
        return [get_alias(c) for c in self._columns_from_locinfo_table]

    @property
    def location_columns(self) -> List[str]:
        return self.column_name

    @property
    def location_columns_string(self) -> str:
        return ", ".join(self.location_columns)

    @property
    def column_names(self) -> List[str]:
        return self._other_columns() + self.location_columns

    def _join_clause(self):
        if self.requires_join:
            joined_name = "polygon"
            join = f"""
            INNER JOIN
                ({self.geom_table.get_query()}) AS polygon
            ON ST_within(
                locinfo.geom_point::geometry,
                ST_SetSRID(polygon.{self.geom_col}, 4326)::geometry
            )
            """
        else:
            # if the subscriber wants to select a geometry from the sites table
            # there is no need to join the table with itself.
            joined_name = "locinfo"
            join = ""

        return joined_name, join

    def _make_query(self):
        joined_name, join = self._join_clause()
        other_cols = ", ".join(f"locinfo.{c}" for c in self._columns_from_locinfo_table)
        columns = ", ".join(f"{table_name}.{c}" for c in self.column_name)

        # Create a table
        sql = f"""
        SELECT
            {other_cols},
            {columns}
        FROM
            {self.location_info_table_fqn} AS locinfo
        {join}
        """

        return sql


class AdminSpatialMapping(PolygonSpatialMapping):
    """
    Maps all cells (aka sites) to an admin region. This is a thin wrapper to
    the more general class SpatialMapping, which assumes that you have
    the standard set-up.

    Parameters
    ----------
    level : int
        Admin level (e.g. 1 for admin1, 2 for admin2, etc.)
    column_name : str, optional
        Pass a string of the column to use as the
        identifier of the admin region. By default
        this will be admin*pcod. But you may wish
        to use something else, such as admin3name.
    """

    def __init__(self, *, level, column_name=None):
        self.level = level
        # If there is no column_name passed then we can use
        # the default, which is of the form admin3pcod.
        if column_name is None:
            col_name = self._get_standard_name()
        else:
            col_name = column_name
        table = f"geography.admin{self.level}"

        super().__init__(column_name=col_name, geom_table=table)

    def _get_standard_name(self):
        """
        Returns the standard name of the column that identifies
        the name of the region.
        """

        return f"admin{self.level}pcod"

    @property
    def location_columns(self) -> List[str]:
        # If the user has asked for the standard column_name
        # then we will alias this column as 'pcod', otherwise
        # we'll won't alias it at all.
        if self.column_name[0] == self._get_standard_name():
            columns = ["pcod"]
        else:
            columns = self.column_name
        return columns

    def _make_query(self):
        table_name, join = self._join_clause()
        other_cols = ", ".join(f"locinfo.{c}" for c in self._columns_from_locinfo_table)
        # If the user has asked for the standard column_name
        # then we will alias this column as 'pcod', otherwise
        # we'll won't alias it at all.
        if self.column_name[0] == self._get_standard_name():
            col_name = f"{table_name}.{self.column_name[0]} AS pcod"
        else:
            col_name = f"{table_name}.{self.column_name[0]}"

        # Create a table
        sql = f"""
        SELECT
            {other_cols},
            {col_name}
        FROM
            {self.location_info_table_fqn} AS locinfo
        {join}
        """

        return sql


class GridSpatialMapping(PolygonSpatialMapping):
    """
    Query representing a mapping between all the sites in the database
    and a grid of arbitrary size.

    Parameters
    ----------
    size : float or int
        Size of the grid in kilometres
    """

    def __init__(self, *, size):
        self.size = size
        self.grid = Grid(self.size)
        super().__init__(
            column_name=["grid_id"], geom_table=self.grid, geom_col="geom_square"
        )
