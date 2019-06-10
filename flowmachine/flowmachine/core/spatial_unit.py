# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that map cell (or tower or site) IDs to a spatial unit.

The helper function 'make_spatial_unit' can be used to create spatial unit objects.
"""
from typing import List
from abc import ABCMeta, abstractmethod

from flowmachine.utils import get_name_and_alias
from flowmachine.core.errors import InvalidSpatialUnitError
from . import Query, Table
from .grid import Grid

# TODO: Currently most spatial units require a FlowDB connection at init time.
# It would be useful to remove this requirement wherever possible, and instead
# implement a method to check whether the required data can be found in the DB.


class SpatialUnitMixin:
    """
    Mixin for spatial unit classes, which provides a 'location_id_columns' property
    and methods for verifying whether a spatial unit meets different criteria
    (useful for checking whether a spatial unit is valid in a given query).
    """

    @property
    def location_id_columns(self) -> List[str]:
        """
        Names of the columns that identify a location.
        """
        return list(self._locid_cols)

    @property
    def has_geography(self):
        """
        True if spatial unit has geography information.
        """
        return hasattr(self, "get_geom_query")

    @property
    def has_lat_lon_columns(self):
        """
        True if spatial unit has lat/lon columns.
        """
        return "lat" in self.location_id_columns and "lon" in self.location_id_columns

    @property
    def is_network_object(self):
        """
        True if spatial unit is a network object (cell or site).
        """
        return (
            "location_id" in self.location_id_columns
            or "site_id" in self.location_id_columns
        )

    @property
    def is_polygon(self):
        """
        True if spatial unit's geographies are polygons.
        """
        return isinstance(self, PolygonSpatialUnit)

    def verify_criterion(self, criterion, negate=False):
        """
        Check whether this spatial unit meets a criterion, and raise an
        InvalidSpatialUnitError if not.

        Parameters
        ----------
        criterion : str
            One of:
                'has_geography'
                'has_lat_lon_columns'
                'is_network_object'
                'is_polygon'
        negate : bool, default False
            If True, negate the criterion check (i.e. raise an error if
            criterion is met).
        
        Raises
        ------
        InvalidSpatialUnitError
            if criterion is not met
        ValueError
            if criterion is not recognised
        """
        criteria = {
            "has_geography": {
                "property": self.has_geography,
                "message": f"{'has' if negate else 'does not have'} geography information.",
            },
            "has_lat_lon_columns": {
                "property": self.has_lat_lon_columns,
                "message": f"{'has' if negate else 'does not have'} latitude/longitude columns.",
            },
            "is_network_object": {
                "property": self.is_network_object,
                "message": f"{'is' if negate else 'is not'} a network object.",
            },
            "is_polygon": {
                "property": self.is_polygon,
                "message": f"{'is' if negate else 'is not'} a polygon spatial unit.",
            },
        }
        if criterion not in criteria.keys():
            raise ValueError(f"Unrecognised criterion '{criterion}'.")
        if criteria[criterion]["property"] == negate:
            raise InvalidSpatialUnitError(
                f"Spatial unit {self} with location columns {self.location_id_columns} "
                + criteria[criterion]["message"]
            )

    def location_subset_clause(self, locations, check_column_names=True):
        """
        Return a SQL "WHERE" clause to subset a query (joined to this spatial
        unit) to a location or set of locations.

        Parameters
        ----------
        locations : str, dict, or list/tuple thereof
            Location or list of locations to subset to.
            This should have one of the following formats:
                str, or list/tuple of str
                    Values correspond to the first column in
                    self.location_id_columns.
                dict, or list/tuple of dict
                    Dict keys correspond to the column names in
                    self.location_id_columns, and values correspond to the
                    values in those columns.
        check_column_names : bool, default True
            If True, check that all dict keys can be found in
            self.location_id_columns.
        
        Returns
        -------
        str
            SQL where clause.
        
        See also
        --------
        LatLonSpatialUnit.location_subset_clause
        """
        raise NotImplementedError(
            "location_subset_clause is not fully implemented yet."
        )
        if isinstance(locations, list) or isinstance(locations, tuple):
            if isinstance(locations[0], dict):
                # multiple locations, multiple columns
                # TODO: Check keys are subset of self.location_id_columns when check_column_names==True
                ands = [
                    " AND ".join(f"{key} = '{value}'" for key, value in loc.items())
                    for loc in locations
                ]
                return "WHERE (" + ") OR (".join(ands) + ")"
            else:
                # multiple locations, first column
                locs_list_string = ", ".join(f"'{l}'" for l in locations)
                return f"WHERE {self.location_id_columns[0]} IN ({locs_list_string})"
        elif isinstance(locations, dict):
            # one location, multiple columns
            # TODO: Check keys are subset of self.location_id_columns when check_column_names==True
            return "WHERE " + " AND ".join(
                f"{key} = '{value}'" for key, value in locations.items()
            )
        else:
            # one location, first column
            return f"WHERE {self.location_id_columns[0]} = '{locations}'"

        # From FirstLocation._get_locations_clause:
        # if len(column_name) == 1:  # polygon, admin, cell, grid
        #     if isinstance(location, tuple) or isinstance(location, list):
        #         in_list = "('" + "','".join(location) + "')"
        #         return "WHERE {} in {}".format(column_name[0], in_list)
        #     else:
        #         return "WHERE {} = '{}'".format(column_name[0], location)
        # elif self.ul.level == "lat-lon":
        #     if isinstance(location, tuple) or isinstance(location, list):
        #         in_list = (
        #             "('"
        #             + "','".join(
        #                 "ST_SetSRID(ST_Point({}, {}), 4326)".format(lon, lat)
        #                 for lon, lat in location
        #             )
        #             + "')"
        #         )
        #         return "WHERE ST_SetSRID(ST_Point(lon, lat), 4326) in {}".format(
        #             in_list
        #         )
        #     else:
        #         return "WHERE ST_SetSRID(ST_Point(lon, lat), 4326) = 'ST_SetSRID(ST_Point({}, {}), 4326)'".format(
        #             *location
        #         )
        # else:  # Versioned things
        #     if isinstance(location, str):  # Deal with single string
        #         location = (location,)
        #     elif isinstance(
        #         location, list
        #     ):  # Deal with possible single strings in list
        #         location = [l if isinstance(l, tuple) else (l,) for l in location]
        #     if isinstance(location, tuple):
        #         return "WHERE " + " AND ".join(
        #             "{} = '{}'".format(c, l) for c, l in zip(column_name, location)
        #         )
        #     else:
        #         ands = " OR ".join(
        #             "({})".format(
        #                 " AND ".join(
        #                     "{} = '{}'".format(c, l) for c, l in zip(column_name, loc)
        #                 )
        #             )
        #             for loc in location
        #         )
        #         return "WHERE " + ands


class CellSpatialUnit(SpatialUnitMixin):
    """
    This class represents the case where no join of cell ID to other data is
    required. As such, this class does not inherit from Query, is not a valid
    parameter to JoinToLocation, and only exists to provide the
    location_id_columns property and for consistency with the other spatial units.
    """

    _locid_cols = ("location_id",)

    def __eq__(self, other):
        return isinstance(other, CellSpatialUnit)

    def __hash__(self):
        # We may never need CellSpatialUnits to be hashable, but we define this
        # just in case.
        return hash(self.__class__.__name__)


class GeomSpatialUnit(SpatialUnitMixin, Query, metaclass=ABCMeta):
    """
    Base class for spatial units that map location IDs in
    connection.location_table to geographic locations.

    Derived classes must implement the _join_clause method, to determine how to
    join the location table to the table with geography data.

    Parameters
    ----------
    geom_table_column_names : str or list
        Name(s) of the column(s) to fetch from geom_table.
    location_id_column_names : str or list
        Name(s) of the column(s) which identify the locations.
        Must be a subset of the column_names for this query.
    geom_table : str or flowmachine.Query, optional
        Name of the table containing the geography information.
        Can be either the name of a table, with the schema, or a
        flowmachine.Query object.
        Defaults to connection.location_table
    geom_column : str, default "geom"
        Name of the column in geom_table that defines the geometry.
    """

    def __init__(
        self,
        *,
        geom_table_column_names,
        location_id_column_names,
        geom_table=None,
        geom_column="geom",
    ):
        if isinstance(geom_table_column_names, str):
            self._geom_table_cols = (geom_table_column_names,)
        else:
            self._geom_table_cols = tuple(geom_table_column_names)

        if isinstance(location_id_column_names, str):
            self._locid_cols = (location_id_column_names,)
        else:
            self._locid_cols = tuple(location_id_column_names)

        self._geom_col = geom_column

        # Check that _locid_cols is a subset of column_names
        missing_cols = [c for c in self._locid_cols if not (c in self.column_names)]
        if missing_cols:
            raise ValueError(
                f"Location ID columns {missing_cols} are not in returned columns."
            )

        if geom_table is None:
            # Creating a Table object here means that we don't have to handle
            # tables and Query objects differently in _make_query and get_geom_query
            self.geom_table = Table(name=self.connection.location_table)
        elif isinstance(geom_table, Query):
            self.geom_table = geom_table
        else:
            self.geom_table = Table(name=geom_table)

        super().__init__()

    def __eq__(self, other):
        try:
            return self.md5 == other.md5
        except AttributeError:
            return False

    def __hash__(self):
        # Must define this because we explicitly define self.__eq__
        return hash(self.md5)

    def _get_aliased_geom_table_cols(self, table_alias):
        return [f"{table_alias}.{c}" for c in self._geom_table_cols]

    @abstractmethod
    def _join_clause(self, loc_table_alias, geom_table_alias):
        """
        Returns a SQL join clause to join the location table to the geography
        table. The join clause is not used if self.geom_table and
        self.connection.location_table are the same table.

        Parameters
        ----------
        loc_table_alias : str
            Table alias for the location table.
        geom_table_alias : str
            Table alias for the geography table.
        
        Returns
        -------
        str
            SQL join clause
        """
        raise NotImplementedError

    def _make_query(self):
        loc_table_alias = "loc_table"

        if hasattr(self.geom_table, "fully_qualified_table_name") and (
            self.geom_table.fully_qualified_table_name == self.connection.location_table
        ):
            # No need to join location_table to itself
            geom_table_alias = loc_table_alias
            join_clause = ""
        else:
            geom_table_alias = "geom_table"
            join_clause = self._join_clause(loc_table_alias, geom_table_alias)

        geom_table_cols_string = ", ".join(
            self._get_aliased_geom_table_cols(geom_table_alias)
        )

        loc_table_cols_string = f"{loc_table_alias}.id AS location_id"

        geom_table_col_aliases = [
            get_name_and_alias(c)[1] for c in self._geom_table_cols
        ]
        if not (
            "date_of_first_service" in geom_table_col_aliases
            and "date_of_last_service" in geom_table_col_aliases
        ):
            # If we're not selecting dates from the geom table, we need to
            # select them from the location table
            loc_table_cols_string += f""",
            {loc_table_alias}.date_of_first_service,
            {loc_table_alias}.date_of_last_service
            """

        sql = f"""
        SELECT
            {loc_table_cols_string},
            {geom_table_cols_string}
        FROM {self.connection.location_table} AS {loc_table_alias}
        {join_clause}
        """

        return sql

    @property
    def column_names(self) -> List[str]:
        cols = ["location_id"]
        geom_table_cols = [
            get_name_and_alias(c)[1]
            for c in self._get_aliased_geom_table_cols("geom_table")
        ]
        if not (
            "date_of_first_service" in geom_table_cols
            and "date_of_last_service" in geom_table_cols
        ):
            cols += ["date_of_first_service", "date_of_last_service"]
        cols += geom_table_cols
        return cols

    def get_geom_query(self):
        """
        Returns a SQL query which can be used to map locations (identified by
        the values in self.location_id_columns) to their geometries (in a column
        named "geom").
        """
        geom_table_alias = "geom_table"

        # List of column names whose aliases are in self.location_id_columns
        columns = [
            c
            for c in self._get_aliased_geom_table_cols(geom_table_alias)
            if get_name_and_alias(c)[1] in self.location_id_columns
        ] + [f"{self._geom_col} AS geom"]

        # For versioned-cell spatial unit, the geometry table _is_ the location table.
        # In this case 'location_id' is one of the location ID columns but
        # isn't in self._geom_table_cols, so we specify it separately.
        if "location_id" in self.location_id_columns:
            columns = [f"{geom_table_alias}.id AS location_id"] + columns

        sql = f"SELECT {','.join(columns)} FROM ({self.geom_table.get_query()}) AS {geom_table_alias}"

        return sql


class LatLonSpatialUnit(GeomSpatialUnit):
    """
    Class that provides a mapping from cell/site IDs in the location table to
    latitude and longitude.

    In addition to the requested geom_table_column_names, this query returns
    latitude and longitude values in columns "lat" and "lon".

    Parameters
    ----------
    geom_table_column_names : str or list, default []
        Name(s) of the column(s) to fetch from geom_table.
    location_id_column_names : str or list, default []
        Name(s) of the column(s) which identify the locations.
        Must be a subset of the column_names for this query.
        "lon" and "lat" will be appended to this list of names.
    geom_table : str or flowmachine.Query, optional
        Name of the table containing the geography information.
        Can be either the name of a table, with the schema, or a
        flowmachine.Query object.
        Defaults to connection.location_table
    geom_column : str, default "geom_point"
        Name of the column in geom_table that defines the point geometry from
        which latitude and longitude will be extracted.
    geom_table_join_on : str, optional
        Name of the column from geom_table to join on.
        Required if geom_table != connection.location_table.
    location_table_join_on : str, optional
        Name of the column from connection.location_table to join on.
        Required if geom_table != connection.location_table.
    """

    def __init__(
        self,
        *,
        geom_table_column_names=(),
        location_id_column_names=(),
        geom_table=None,
        geom_column="geom_point",
        geom_table_join_on=None,
        location_table_join_on=None,
    ):
        self._geom_on = geom_table_join_on
        self._loc_on = location_table_join_on
        super().__init__(
            geom_table_column_names=geom_table_column_names,
            location_id_column_names=location_id_column_names,
            geom_table=geom_table,
            geom_column=geom_column,
        )

    def _get_aliased_geom_table_cols(self, table_alias):
        return super()._get_aliased_geom_table_cols(table_alias) + [
            f"ST_X({table_alias}.{self._geom_col}::geometry) AS lon",
            f"ST_Y({table_alias}.{self._geom_col}::geometry) AS lat",
        ]

    def _join_clause(self, loc_table_alias, geom_table_alias):
        if self._loc_on is None or self._geom_on is None:
            raise ValueError("No columns specified for join.")
        return f"""
        LEFT JOIN
            ({self.geom_table.get_query()}) AS {geom_table_alias}
        ON {loc_table_alias}.{self._loc_on} = {geom_table_alias}.{self._geom_on}
        """

    @property
    def location_id_columns(self) -> List[str]:
        """
        Names of the columns that identify a location.
        """
        return list(self._locid_cols) + ["lon", "lat"]

    def location_subset_clause(self, locations, check_column_names=True):
        """
        Return a SQL "WHERE" clause to subset a query (joined to this spatial
        unit) to a location or set of locations. This method differs from the
        default implementation in its handling of lat-lon values, i.e. it returns
            WHERE ST_SetSRID(ST_Point(lon, lat), 4326) = ST_SetSRID(ST_Point(<lon_value>, <lat_value>), 4326)'
        instead of
            WHERE lon = '<lon_value>' AND lat = '<lat_value>'

        Parameters
        ----------
        locations : tuple, str, dict, or list/tuple thereof
            Location or list of locations to subset to.
            This should have one of the following formats:
                tuple (length 2), or list/tuple of tuple
                    Values are (longitude, latitude) pairs, corresponding to
                    the 'lon' and 'lat' columns.
                str, or list/tuple of str
                    Values correspond to the first column in
                    self.location_id_columns.
                dict, or list/tuple of dict
                    Dict keys correspond to the column names in
                    self.location_id_columns, and values correspond to the
                    values in those columns.
        check_column_names : bool, default True
            If True, check that all dict keys can be found in
            self.location_id_columns.
        
        Returns
        -------
        str
            SQL where clause.
        
        See also
        --------
        SpatialUnitMixin.location_subset_clause
        """
        raise NotImplementedError(
            "LatLonSpatialUnit.location_subset_clause is not implemented yet."
        )
        # TODO: Implement this.
        # Should raise an error if locations is a string or list of string and
        # location_id_columns[0] == 'lon'.
        # Should return example in docstring (or
        # "WHERE ST_SetSRID(ST_Point(lon, lat), 4326) IN (...)") if
        # locations is a tuple or list of tuples.
        # Should return same as SpatialUnitMixin.location_subset_clause if
        # locations is a dict or list of dict and "lat" and "lon" columns are
        # not included in keys.
        # If lat" and "lon" are included in the dict keys, should be able to
        # create a new dict with "lat", "lon" keys replaced by
        # "ST_SetSRID(ST_Point(lon, lat), 4326)" (and set corresponding value),
        # and then call super().location_subset_clause with check_column_names=False


class PolygonSpatialUnit(GeomSpatialUnit):
    """
    Class that provides a mapping from cell/site data in the location table to
    spatial regions defined by geography information in a table.

    Parameters
    ----------
    geom_table_column_names : str or list
        Name(s) of the column(s) to fetch from geom_table.
        This column or columns will be used to identify the polygons.
    geom_table : str or flowmachine.Query
        Name of the table containing the geography information.
        Can be either the name of a table, with the schema, or a
        flowmachine.Query object.
    geom_column : str, default 'geom'
        Name of the column in geom_table that defines the geography.
    """

    def __init__(self, *, geom_table_column_names, geom_table, geom_column="geom"):
        if isinstance(geom_table_column_names, str):
            location_id_column_names = get_name_and_alias(geom_table_column_names)[1]
        else:
            location_id_column_names = [
                get_name_and_alias(c)[1] for c in geom_table_column_names
            ]
        super().__init__(
            geom_table_column_names=geom_table_column_names,
            location_id_column_names=location_id_column_names,
            geom_table=geom_table,
            geom_column=geom_column,
        )

    def _join_clause(self, loc_table_alias, geom_table_alias):
        return f"""
        INNER JOIN
            ({self.geom_table.get_query()}) AS {geom_table_alias}
        ON ST_within(
            {loc_table_alias}.geom_point::geometry,
            ST_SetSRID({geom_table_alias}.{self._geom_col}, 4326)::geometry
        )
        """


def versioned_cell_spatial_unit():
    """
    Returns a LatLonSpatialUnit that maps cell location_id to a cell version
    and lat-lon coordinates.

    Returns
    -------
    flowmachine.core.spatial_unit.LatLonSpatialUnit
    """
    if Query.connection.location_table != "infrastructure.cells":
        raise InvalidSpatialUnitError("Versioned cell spatial unit is unavailable.")

    return LatLonSpatialUnit(
        geom_table_column_names=["version"],
        location_id_column_names=["location_id", "version"],
        geom_table="infrastructure.cells",
    )


def versioned_site_spatial_unit():
    """
    Returns a LatLonSpatialUnit that maps cell location_id to a site version
    and lat-lon coordinates.

    Returns
    -------
    flowmachine.core.spatial_unit.LatLonSpatialUnit
    """
    return LatLonSpatialUnit(
        geom_table_column_names=[
            "date_of_first_service",
            "date_of_last_service",
            "id AS site_id",
            "version",
        ],
        location_id_column_names=["site_id", "version"],
        geom_table="infrastructure.sites",
        geom_table_join_on="id",
        location_table_join_on="site_id",
    )


def admin_spatial_unit(*, level, region_id_column_name=None):
    """
    Returns a PolygonSpatialUnit object that maps all cells (aka sites) to an
    admin region. This assumes that you have geography data in the standard
    location in FlowDB.

    Parameters
    ----------
    level : int
        Admin level (e.g. 1 for admin1, 2 for admin2, etc.)
    region_id_column_name : str, optional
        Pass a string of the column to use as the
        identifier of the admin region. By default
        this will be admin*pcod. But you may wish
        to use something else, such as admin3name.
    
    Returns
    -------
    flowmachine.core.spatial_unit.PolygonSpatialUnit
        Query which maps cell/site IDs to admin regions
    """
    # If there is no region_id_column_name passed then we can use the default,
    # which is of the form admin3pcod. If the user has asked for the standard
    # region_id_column_name then we will alias this column as 'pcod', otherwise
    # we won't alias it at all.
    if region_id_column_name is None or region_id_column_name == f"admin{level}pcod":
        col_name = f"admin{level}pcod AS pcod"
    else:
        col_name = region_id_column_name
    table = f"geography.admin{level}"

    return PolygonSpatialUnit(geom_table_column_names=col_name, geom_table=table)


def grid_spatial_unit(*, size):
    """
    Returns a PolygonSpatialUnit that maps all the sites in the database to a
    grid of arbitrary size.

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
        geom_table_column_names="grid_id",
        geom_table=Grid(size),
        geom_column="geom_square",
    )


def make_spatial_unit(
    spatial_unit_type,
    *,
    level=None,
    region_id_column_name=None,
    size=None,
    geom_table=None,
    geom_column="geom",
):
    """
    Helper function to create an object representing a spatial unit.

    Parameters
    ----------
    spatial_unit_type : str
        Can be one of:
            'cell'
                The identifier as found in the CDR.
            'lat-lon'
                Latitude and longitude of cell/site locations.
            'versioned-cell'
                The identifier as found in the CDR combined with the version
                from the cells table.
            'versioned-site'
                The ID found in the sites table, coupled with the version
                number.
            'polygon'
                A custom set of polygons that live in the database. In which
                case you can pass the parameters 'region_id_column_name', which
                is the column or columns you want to return after the join, and
                'geom_table', the table where the polygons reside (with the
                schema), and additionally geom_column which is the column with
                the geometry information (will default to 'geom').
            'admin'
                An admin region of interest, such as admin3. Must live in the
                database in the standard location. In addition pass the 'level'
                parameter, e.g. level=3 for admin3. Optionally also pass the
                parameter 'column_name' to choose the column to use as the
                identifier of the admin region (default is 'admin*pcod')
            'grid'
                A square in a regular grid, in addition pass the 'size'
                parameter to determine the size of the polygon.
    level : int
        Admin level (e.g. 1 for admin1, 2 for admin2, etc.).
        Required when spatial_unit_type='admin'.
    region_id_column_name : str or list
        Name(s) of column(s) which identifies the polygon regions.
        Required when spatial_unit_type='polygon',
        optional when spatial_unit_type='admin'.
    size : float or int
        Size of the grid in kilometres.
        Required when spatial_unit_type='grid'.
    geom_table : str or flowmachine.Query
        Name of the table containing the geography information. Can be either
        the name of a table, with the schema, or a flowmachine.Query object.
        Required when spatial_unit_type='polygon'.
    geom_column : str, default 'geom'
        Name of the column in geom_table that defines the geography.
        Required when spatial_unit_type='polygon'.
    
    Returns
    -------
    flowmachine.core.spatial_unit.*SpatialUnit
        An object representing a mapping from location identifiers to a spatial
        unit.
    """
    if spatial_unit_type == "cell":
        return CellSpatialUnit()
    elif spatial_unit_type == "versioned-cell":
        return versioned_cell_spatial_unit()
    elif spatial_unit_type == "versioned-site":
        return versioned_site_spatial_unit()
    elif spatial_unit_type == "lat-lon":
        return LatLonSpatialUnit()
    elif spatial_unit_type == "admin":
        if level is None:
            raise ValueError(
                "'level' parameter is required for spatial unit of type 'admin'."
            )
        return admin_spatial_unit(
            level=level, region_id_column_name=region_id_column_name
        )
    elif spatial_unit_type == "grid":
        if size is None:
            raise ValueError(
                "'size' parameter is required for spatial unit of type 'grid'."
            )
        return grid_spatial_unit(size=size)
    elif spatial_unit_type == "polygon":
        if geom_table is None:
            raise ValueError(
                "'geom_table' parameter is required for spatial unit of type 'polygon'."
            )
        if region_id_column_name is None:
            raise ValueError(
                "'region_id_column_name' parameter is required for spatial unit of type 'polygon'."
            )
        return PolygonSpatialUnit(
            geom_table_column_names=region_id_column_name,
            geom_table=geom_table,
            geom_column=geom_column,
        )
    else:
        raise ValueError(f"Unrecognised spatial unit type: {spatial_unit_type}.")
