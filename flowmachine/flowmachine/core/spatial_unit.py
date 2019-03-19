# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that map cells (or towers or sites) to a spatial unit.

The available spatial units are:
    VersionedCellSpatialUnit:
        The identifier as found in the CDR combined with the version from the
        cells table.
    VersionedSiteSpatialUnit:
        The ID found in the sites table, coupled with the version number.
    PolygonSpatialUnit:
        A custom set of polygons that live in the database. Takes the
        parameters polygon_column_names, which is the columns you want to
        return after the join, and polygon_table, the table where the polygons
        reside (with the schema), and additionally geom_col which is the column
        with the geometry information (will default to 'geom').
    AdminSpatialUnit:
        An admin region of interest, such as admin3. Must live in the database
        in the standard location.
    GridSpatialUnit:
        A square in a regular grid, in addition pass size to determine the size
        of the polygon.
"""
from typing import List
from abc import ABCMeta, abstractmethod

from flowmachine.utils import get_name_and_alias
from . import Query, GeoTable
from .grid import Grid


class BaseSpatialUnit(Query, metaclass=ABCMeta):
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

        if type(location_column_names) is str:
            self._loc_cols = [location_column_names]
        else:
            self._loc_cols = location_column_names

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
        return [get_name_and_alias(c)[1].split(".").pop() for c in self._cols]

    @abstractmethod
    def geo_augment(self, query):
        """
        Given a query object (which is assumed to be a JoinToLocation object,
        joined to this spatial unit), return a version of the query augmented
        with a geom column and a gid column.

        Parameters
        ----------
        query : flowmachine.Query
            The query to augment with geom and gid columns
        
        Returns
        -------
        str
            A version of this query with geom and gid columns
        list
            The columns this query contains
        """
        raise NotImplementedError

    def distance_matrix_query(self, return_geometry):
        """
        A query that calculates the complete distance matrix between all
        elements of this spatial unit. Distance is returned in km.

        Parameters
        ----------
        return_geometry : bool
            If True, geometries are returned in query
            (represented as WKB in a dataframe)

        Returns
        -------
        str
            SQL query string

        """
        raise NotImplementedError(
            f"Spatial units of type {type(self).__name__} do not support distance_matrix_query at this time."
        )

    def distance_matrix_columns(self, return_geometry=False):
        """
        List of columns for self.distance_matrix_query
        """
        col_names = [f"{c}_from" for c in self.location_columns]
        col_names += [f"{c}_to" for c in self.location_columns]
        col_names += ["distance"]
        if return_geometry:
            col_names += ["geom_origin", "geom_destination"]
        return col_names

    def _make_query(self):
        columns = ", ".join(self._cols)
        sql = f"""
        SELECT
            {columns}
        FROM {self.location_info_table}
        {self._join_clause}
        """

        return sql


class LatLonSpatialUnit(BaseSpatialUnit):
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

    def geo_augment(self, query):
        sql = f"""
        SELECT 
            row_number() over() AS gid,
            *, 
            ST_SetSRID(ST_Point(lon, lat), 4326) AS geom
        FROM ({query.get_query()}) AS L
        """
        cols = list(set(query.column_names + ["gid", "geom"]))
        return sql, cols


class VersionedCellSpatialUnit(BaseSpatialUnit):
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

    def geo_augment(self, query):
        sql = f"""
        SELECT 
            row_number() OVER () AS gid, 
            geom_point AS geom, 
            U.*
        FROM ({query.get_query()}) AS U
        LEFT JOIN infrastructure.cells AS S
            ON U.location_id = S.id AND
                U.version = S.version
        """
        cols = list(set(query.column_names + ["gid", "geom"]))
        return sql, cols

    def distance_matrix_query(self, return_geometry=False):
        return_geometry_statement = ""
        if return_geometry:
            return_geometry_statement = """
                ,
                A.geom_point AS geom_origin,
                B.geom_point AS geom_destination
            """

        sql = f"""

            SELECT
                A.id AS location_id_from,
                A.version AS version_from,
                B.id AS location_id_to,
                B.version AS version_to,
                ST_X(A.geom_point::geometry) AS lon_from,
                ST_Y(A.geom_point::geometry) AS lat_from,
                ST_X(B.geom_point::geometry) AS lon_to,
                ST_Y(B.geom_point::geometry) AS lat_to,
                ST_Distance(
                    A.geom_point::geography, 
                    B.geom_point::geography
                ) / 1000 AS distance
                {return_geometry_statement}
            FROM infrastructure.cells AS A
            CROSS JOIN infrastructure.cells AS B
            ORDER BY distance DESC
            
        """

        return sql


class VersionedSiteSpatialUnit(BaseSpatialUnit):
    """
    Class that maps cell location_id to a site version and lat-lon coordinates.
    """

    def __init__(self):
        location_table = self.connection.location_table

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

        super().__init__(
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
            join_clause=join_clause,
        )

    def geo_augment(self, query):
        sql = f"""
        SELECT 
            row_number() OVER () AS gid, 
            geom_point AS geom, 
            U.*
        FROM ({query.get_query()}) AS U
        LEFT JOIN infrastructure.sites AS S
            ON U.site_id = S.id AND
                U.version = S.version
        """
        cols = list(set(query.column_names + ["gid", "geom"]))
        return sql, cols

    def distance_matrix_query(self, return_geometry=False):
        return_geometry_statement = ""
        if return_geometry:
            return_geometry_statement = """
                ,
                A.geom_point AS geom_origin,
                B.geom_point AS geom_destination
            """

        sql = f"""

            SELECT
                A.id AS site_id_from,
                A.version AS version_from,
                B.id AS site_id_to,
                B.version AS version_to,
                ST_X(A.geom_point::geometry) AS lon_from,
                ST_Y(A.geom_point::geometry) AS lat_from,
                ST_X(B.geom_point::geometry) AS lon_to,
                ST_Y(B.geom_point::geometry) AS lat_to,
                ST_Distance(
                    A.geom_point::geography, 
                    B.geom_point::geography
                ) / 1000 AS distance
                {return_geometry_statement}
            FROM infrastructure.sites AS A
            CROSS JOIN infrastructure.sites AS B
            ORDER BY distance DESC
            
        """

        return sql


class PolygonSpatialUnit(BaseSpatialUnit):
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
        if isinstance(polygon_table, Query):
            self.polygon_table = polygon_table
        else:
            self.polygon_table = GeoTable(name=polygon_table, geom_column=geom_col)

        self.geom_col = geom_col

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
                ST_SetSRID({joined_alias}.{self.geom_col}, 4326)::geometry
            )
            """

        locinfo_column_names = [
            f"{locinfo_alias}.id AS location_id",
            f"{locinfo_alias}.version AS version",
            f"{locinfo_alias}.date_of_first_service AS date_of_first_service",
            f"{locinfo_alias}.date_of_last_service AS date_of_last_service",
        ]
        if type(polygon_column_names) is str:
            self.polygon_column_names = [polygon_column_names]
        else:
            self.polygon_column_names = polygon_column_names
        all_column_names = locinfo_column_names + [
            f"{joined_alias}.{c}" for c in self.polygon_column_names
        ]
        location_column_names = [
            get_name_and_alias(c)[1] for c in self.polygon_column_names
        ]

        super().__init__(
            selected_column_names=all_column_names,
            location_column_names=location_column_names,
            location_info_table=f"{location_info_table} AS {locinfo_alias}",
            join_clause=join_clause,
        )

    def geo_augment(self, query):
        r_col_name, l_col_name = get_name_and_alias(self.polygon_column_names[0])
        sql = f"""
        SELECT 
            row_number() OVER () as gid, 
            {self.geom_col} AS geom, 
            U.*
        FROM ({query.get_query()}) AS U
        LEFT JOIN ({self.polygon_table.get_query()}) AS G
            ON U.{l_col_name} = G.{r_col_name}
        """
        cols = list(set(query.column_names + ["gid", "geom"]))
        return sql, cols


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
