# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that map cells (or towers or sites) to a spatial unit
(e.g. versioned-cell, admin*, grid, ...).
"""
from typing import List

from flowmachine.utils import get_alias
from . import Query, GeoTable, Grid


class SpatialUnit(Query):
    """
    Base class for all spatial units. Selects columns from the location table,
    and optionally joins to data in another table.

    Parameters
    ----------
    selected_column_names : str or list
        The name(s) of the column(s) to fetch from the location
        table in the database.
    location_column_names : str or list
        Name(s) of the location-related column(s).
        Must be a subset of the column_names for this query.
    location_info_table : str, optional
        Fully qualified name of the location info table to select from.
        Defaults to self.connection.location_table
    join_clause : str, optional
        Optionally provide a SQL join clause to join data from the
        location info table to spatial regions in another table.
    """

    def __init__(
        self,
        *,
        selected_column_names,
        location_column_names,
        location_info_table=None,
        join_clause="",
    ):
        if type(selected_column_names) is str:
            self._cols = [selected_column_names]
        else:
            self._cols = selected_column_names

        if type(other_column_names) is str:
            self._loc_cols = [location_column_names]
        else:
            self._loc_cols = location_column_names

        # Check that _loc_cols is a subset of column_names
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
        """
        List of the location-related column names.
        """
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


class LatLonSpatialUnit(SpatialUnit):
    """
    Class that maps cell location_id to lat-lon coordinates. 
    """

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
    """
    Class that maps cell location_id to a cell version and lat-lon coordinates.
    """

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
    """
    Class that maps cell location_id to a site version and lat-lon coordinates.
    """

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
    polygon_column_names : str or list
        The name of the column to fetch from the geometry
        table in the database. Can also be a list of names.
    polygon_table : str or flowmachine.Query
        name of the table containing the geography information.
        Can be either the name of a table, with the schema, a flowmachine.Query
        object, or a string representing a query.
    geom_col : str, default 'geom'
        column that defines the geography.
    """

    def __init__(self, *, polygon_column_names, polygon_table, geom_col="geom"):
        self.polygon_table = polygon_table
        self.geom_col = geom_col

        location_info_table = self.connection.location_table

        locinfo_alias = "locinfo"
        if (
            isinstance(self.polygon_table, str)
            and location_info_table == self.polygon_table.lower().strip()
        ):
            # if the subscriber wants to select a geometry from the sites table
            # there is no need to join the table with itself.
            joined_alias = locinfo_alias
            join_clause = ""
        else:
            joined_alias = "polygon"
            join_clause = f"""
            INNER JOIN
                {self._get_subtable()} AS {joined_alias}
            ON ST_within(
                {locinfo_alias}.geom_point::geometry,
                ST_SetSRID({joined_alias}.{self.geom_col}, 4326)::geometry
            )
            """

        locinfo_column_names = [
            f"{locinfo_alias}.id AS location_id",
            f"{locinfo_alias}.version AS version",
            f"{locinfo_alias}.date_of_first_service AS date_of_first_service",
            f"{locinfo_alias}.date_of_last_service AS date_of_last_service",
        ]
        if type(polygon_columns) is str:
            polygon_cols = [polygon_column_names]
        else:
            polygon_cols = polygon_column_names
        all_column_names = locinfo_column_names + [
            f"{joined_alias}.{c}" for c in polygon_cols
        ]
        location_column_names = [get_alias(c) for c in polygon_cols]

        super().__init__(
            selected_column_names=all_column_names,
            location_column_names=location_column_names,
            location_info_table=f"{location_info_table} AS {locinfo_alias}",
            join_clause=join_clause,
        )

    def _get_subtable(self):
        """
        Private method which takes the table and returns a query
        representing the object. This is necessary as the table can
        be passed in a variety of ways.
        """

        if issubclass(self.polygon_table.__class__, Query):
            return f"({self.polygon_table.get_query()})"
        elif "select " in self.polygon_table.lower():
            return f"({self.polygon_table})"
        else:
            return self.polygon_table


class AdminSpatialUnit(PolygonSpatialUnit):
    """
    Class that maps all cells (aka sites) to an admin region. This is a thin
    wrapper to the more general class PolygonSpatialUnit, which assumes that
    you have the standard set-up.

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
        # If the user has asked for the standard column_name
        # then we will alias this column as 'pcod', otherwise
        # we'll won't alias it at all.
        if (column_name is None) or (column_name == self._get_standard_name()):
            col_name = f"{self._get_standard_name()} AS pcod"
        else:
            col_name = column_name
        table = f"geography.admin{self.level}"

        super().__init__(polygon_column_names=col_name, polygon_table=table)

    def _get_standard_name(self):
        """
        Returns the standard name of the column that identifies
        the name of the region.
        """

        return f"admin{self.level}pcod"


class GridSpatialUnit(PolygonSpatialUnit):
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
        super().__init__(
            polygon_column_names=["grid_id"],
            polygon_table=Grid(self.size),
            geom_col="geom_square",
        )
