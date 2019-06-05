# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that map cells (or towers or sites) to a spatial unit.

The available spatial units are:
    CellSpatialUnit:
        The identifier as found in the CDR.
    lat_lon_spatial_unit:
        Latitude and longitude of cell/site locations.
    versioned_cell_spatial_unit:
        The identifier as found in the CDR combined with the version from the
        cells table.
    versioned_site_spatial_unit:
        The ID found in the sites table, coupled with the version number.
    PolygonSpatialUnit:
        A custom set of polygons that live in the database. Takes the
        parameters polygon_column_names, which is the columns you want to
        return after the join, and polygon_table, the table where the polygons
        reside (with the schema), and additionally geom_column which is the column
        with the geometry information (will default to 'geom').
    admin_spatial_unit:
        An admin region of interest, such as admin3. Must live in the database
        in the standard location.
        Special case of PolygonSpatialUnit.
    grid_spatial_unit:
        A square in a regular grid, in addition pass size to determine the size
        of the polygon.
        Special case of PolygonSpatialUnit.
"""
from typing import List

from flowmachine.utils import get_name_and_alias
from . import Query, Table
from .grid import Grid


# class SpatialUnitMixin:


class CellSpatialUnit:
    """
    This class represents the case where no join of cell ID to other data is
    required. As such, this class does not inherit from Query, is not a valid
    parameter to JoinToLocation, and only exists to provide the
    location_columns property and for consistency with the other spatial units.
    """

    _loc_cols = ("location_id",)

    def __eq__(self, other):
        return isinstance(other, CellSpatialUnit)

    def __hash__(self):
        # We may never need CellSpatialUnits to be hashable, but I'll define
        # this just in case.
        return hash(self.__class__.__name__)

    @property
    def location_columns(self) -> List[str]:
        """
        List of the location-related column names.
        """
        return list(self._loc_cols)


class SpatialUnit(Query):
    """
    Base class for all spatial units except CellSpatialUnit. Selects columns
    from the location table, and optionally joins to data in another table.

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
    geom_column : str, default "geom"
        Name of the column that defines the geometry in location_info_table.
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
        geom_column="geom",
        join_clause="",
    ):
        if type(selected_column_names) is str:
            self._cols = (selected_column_names,)
        else:
            self._cols = tuple(selected_column_names)

        if type(location_column_names) is str:
            self._loc_cols = (location_column_names,)
        else:
            self._loc_cols = tuple(location_column_names)

        # Check that _loc_cols is a subset of column_names
        missing_cols = [c for c in self._loc_cols if not (c in self.column_names)]
        if missing_cols:
            raise ValueError(
                f"Location columns {missing_cols} are not in returned columns."
            )

        if location_info_table:
            self.location_info_table = location_info_table
        else:
            self.location_info_table = self.connection.location_table

        self._geom_column = geom_column

        self._join_clause = join_clause

        super().__init__()

    # TODO: Currently most spatial units require a FlowDB connection at init time.
    # It would be useful to remove this requirement wherever possible, and instead
    # implement a method to check whether the required data can be found in the DB.

    def __eq__(self, other):
        try:
            return self.md5 == other.md5
        except AttributeError:
            return False

    def __hash__(self):
        # Must define this because we explicitly define self.__eq__
        return hash(self.md5)

    @property
    def location_columns(self) -> List[str]:
        """
        List of names of the columns which identify the locations.
        """
        return list(self._loc_cols)

    @property
    def column_names(self) -> List[str]:
        return [get_name_and_alias(c)[1].split(".").pop() for c in self._cols]

    def get_geom_query(self):
        """
        Returns a SQL query which can be used to map locations (identified by
        the values in self.location_columns) to their geometries (in a column
        named "geom").
        """
        columns = [
            c for c in self._cols if get_name_and_alias(c)[1] in self.location_columns
        ] + [f"{self._geom_column} AS geom"]

        sql = f"SELECT {','.join(columns)} FROM {self.location_info_table}"

        return sql

    def _make_query(self):
        columns = ", ".join(self._cols)
        sql = f"""
        SELECT
            {columns}
        FROM {self.location_info_table}
        {self._join_clause}
        """

        return sql


def lat_lon_spatial_unit():
    """
    Returns a SpatialUnit that maps cell location_id to lat-lon coordinates.

    Returns
    -------
    flowmachine.core.spatial_unit.SpatialUnit
    """
    return SpatialUnit(
        selected_column_names=[
            "id AS location_id",
            "date_of_first_service",
            "date_of_last_service",
            "ST_X(geom_point::geometry) AS lon",
            "ST_Y(geom_point::geometry) AS lat",
        ],
        location_column_names=["lat", "lon"],
        geom_column="geom_point",
    )


def versioned_cell_spatial_unit():
    """
    Returns a SpatialUnit that maps cell location_id to a cell version and
    lat-lon coordinates.

    Returns
    -------
    flowmachine.core.spatial_unit.SpatialUnit
    """
    if Query.connection.location_table != "infrastructure.cells":
        raise ValueError("Versioned cell spatial unit is unavailable.")

    return SpatialUnit(
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
        geom_column="geom_point",
    )


def versioned_site_spatial_unit():
    """
    Returns a SpatialUnit that maps cell location_id to a site version and
    lat-lon coordinates.

    Returns
    -------
    flowmachine.core.spatial_unit.SpatialUnit
    """
    location_table = Query.connection.location_table

    sites_alias = "s"
    if location_table == "infrastructure.sites":
        cells_alias = sites_alias
        join_clause = ""
    elif location_table == "infrastructure.cells":
        cells_alias = "c"
        join_clause = f"""
        RIGHT JOIN
        infrastructure.cells AS {cells_alias}
        ON {sites_alias}.id = {cells_alias}.site_id
        """
    else:
        raise ValueError(
            f"Expected location table to be 'infrastructure.cells' "
            f"or 'infrastructure.sites', not '{location_table}''"
        )

    return SpatialUnit(
        selected_column_names=[
            f"{cells_alias}.id AS location_id",
            f"{sites_alias}.id AS site_id",
            f"{sites_alias}.date_of_first_service AS date_of_first_service",
            f"{sites_alias}.date_of_last_service AS date_of_last_service",
            f"{sites_alias}.version AS version",
            f"ST_X({sites_alias}.geom_point::geometry) AS lon",
            f"ST_Y({sites_alias}.geom_point::geometry) AS lat",
        ],
        location_column_names=["site_id", "version", "lon", "lat"],
        location_info_table=f"infrastructure.sites AS {sites_alias}",
        geom_column="geom_point",
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
    geom_column : str, default 'geom'
        Name of the column in polygon_table that defines the geography.
    """

    def __init__(self, *, polygon_column_names, polygon_table, geom_column="geom"):
        if isinstance(polygon_table, Query):
            self.polygon_table = polygon_table
        else:
            # Creating a Table object here means that we don't have to handle
            # admin tables and Grid objects differently in join_clause and self.get_geom_query
            self.polygon_table = Table(name=polygon_table)

        location_info_table = self.connection.location_table

        locinfo_alias = "locinfo"
        if hasattr(self.polygon_table, "fully_qualified_table_name") and (
            location_info_table == self.polygon_table.fully_qualified_table_name
        ):
            # if the subscriber wants to select a geometry from the sites table
            # there is no need to join the table with itself.
            joined_alias = locinfo_alias
            join_clause = ""
        else:
            joined_alias = "polygon"
            join_clause = f"""
            INNER JOIN
                ({self.polygon_table.get_query()}) AS {joined_alias}
            ON ST_within(
                {locinfo_alias}.geom_point::geometry,
                ST_SetSRID({joined_alias}.{geom_column}, 4326)::geometry
            )
            """

        locinfo_column_names = [
            f"{locinfo_alias}.id AS location_id",
            f"{locinfo_alias}.version AS version",
            f"{locinfo_alias}.date_of_first_service AS date_of_first_service",
            f"{locinfo_alias}.date_of_last_service AS date_of_last_service",
        ]
        if type(polygon_column_names) is str:
            self._polygon_column_names = (polygon_column_names,)
        else:
            self._polygon_column_names = tuple(polygon_column_names)
        all_column_names = locinfo_column_names + [
            f"{joined_alias}.{c}" for c in self._polygon_column_names
        ]
        location_column_names = [
            get_name_and_alias(c)[1] for c in self._polygon_column_names
        ]

        super().__init__(
            selected_column_names=all_column_names,
            location_column_names=location_column_names,
            location_info_table=f"{location_info_table} AS {locinfo_alias}",
            geom_column=geom_column,
            join_clause=join_clause,
        )

    def get_geom_query(self):
        """
        Returns a SQL query which can be used to map locations (identified by
        the values in self.location_columns) to their geometries (in a column
        named "geom").
        """
        columns = list(self._polygon_column_names) + [f"{self._geom_column} AS geom"]

        sql = f"""
        SELECT {','.join(columns)} FROM ({self.polygon_table.get_query()}) AS polygon
        """

        return sql


def admin_spatial_unit(*, level, column_name=None):
    """
    Returns a PolygonSpatialUnit object that maps all cells (aka sites) to an
    admin region. This assumes that you have geography data in the standard
    location in FlowDB.

    Parameters
    ----------
    level : int
        Admin level (e.g. 1 for admin1, 2 for admin2, etc.)
    column_name : str, optional
        Pass a string of the column to use as the
        identifier of the admin region. By default
        this will be admin*pcod. But you may wish
        to use something else, such as admin3name.
    
    Returns
    -------
    flowmachine.core.spatial_unit.PolygonSpatialUnit
        Query which maps cell/site IDs to admin regions
    """
    # If there is no column_name passed then we can use the default, which is
    # of the form admin3pcod. If the user has asked for the standard
    # column_name then we will alias this column as 'pcod', otherwise we won't
    # alias it at all.
    if (column_name is None) or (column_name == f"admin{level}pcod"):
        col_name = f"admin{level}pcod AS pcod"
    else:
        col_name = column_name
    table = f"geography.admin{level}"

    return PolygonSpatialUnit(polygon_column_names=col_name, polygon_table=table)


def grid_spatial_unit(*, size):
    """
    Returns a PolygonSpatialUnit representing a mapping
    between all the sites in the database and a grid of arbitrary size.

    Parameters
    ----------
    size : float or int
        Size of the grid in kilometres
    
    Returns
    -------
    flowmachine.core.spatial_unit.PolygonSpatialUnit
        Query which maps cell/site IDs to grid squares
    """
    return PolygonSpatialUnit(
        polygon_column_names=["grid_id"],
        polygon_table=Grid(size),
        geom_column="geom_square",
    )
