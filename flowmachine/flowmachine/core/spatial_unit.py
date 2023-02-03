# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes that map cell (or tower or site) IDs to a spatial unit.

The helper function 'make_spatial_unit' can be used to create spatial unit objects.
"""
from abc import abstractmethod, ABCMeta
from typing import Union, List, Iterable, Optional

from flowmachine.utils import get_name_and_alias
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.core import Query, Table
from flowmachine.core.context import get_db
from flowmachine.core.grid import Grid

# TODO: Currently most spatial units require a FlowDB connection at init time.
# It would be useful to remove this requirement wherever possible, and instead
# implement a method to check whether the required data can be found in the DB.


def _substitute_lat_lon(location_dict):
    """
    Replace "lat" and "lon" keys in location_dict with "ST_SetSRID(ST_Point(lon, lat), 4326)"
    This function is used by `LonLatSpatialUnit.location_subset_clause()`
    """
    location_copy = location_dict.copy()
    if "lon" in location_copy and "lat" in location_copy:
        lon = location_copy.pop("lon")
        lat = location_copy.pop("lat")
        location_copy[
            "ST_SetSRID(ST_Point(lon, lat), 4326)"
        ] = f"ST_SetSRID(ST_Point({lon}, {lat}), 4326)"
    return location_copy


class SpatialUnitMixin(metaclass=ABCMeta):
    """
    Mixin for spatial unit classes, which provides a 'location_id_columns' property
    and methods for verifying whether a spatial unit meets different criteria
    (useful for checking whether a spatial unit is valid in a given query).
    """

    @property
    @abstractmethod
    def canonical_name(self) -> str:
        """
        Get the canonical name for this type of spatial unit, to allow checking access rights
        in FlowAPI.

        Returns
        -------
        str
            The name of this type of spatial unit, suitable for comparison with flowapi's geography permissions.
        """
        raise NotImplementedError

    @property
    def location_id_columns(self) -> List[str]:
        """
        Names of the columns that identify a location.
        """
        return list(self._locid_cols)

    @property
    def has_geography(self) -> bool:
        """
        True if spatial unit has geography information.
        """
        return hasattr(self, "get_geom_query")

    @property
    def has_lon_lat_columns(self) -> bool:
        """
        True if spatial unit has lon/lat columns.
        """
        return "lon" in self.location_id_columns and "lat" in self.location_id_columns

    @property
    def is_network_object(self) -> bool:
        """
        True if spatial unit is a network object (cell or site).
        """
        return (
            "location_id" in self.location_id_columns
            or "site_id" in self.location_id_columns
        )

    @property
    def is_polygon(self) -> bool:
        """
        True if spatial unit's geographies are polygons.
        """
        return isinstance(self, PolygonSpatialUnit)

    def verify_criterion(self, criterion, negate=False) -> None:
        """
        Check whether this spatial unit meets a criterion, and raise an
        InvalidSpatialUnitError if not.

        Parameters
        ----------
        criterion : str
            One of:
                'has_geography'
                'has_lon_lat_columns'
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
            "has_lon_lat_columns": {
                "property": self.has_lon_lat_columns,
                "message": f"{'has' if negate else 'does not have'} longitude/latitude columns.",
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

    def location_subset_clause(self, locations, check_column_names: bool = True) -> str:
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
        LonLatSpatialUnit.location_subset_clause
        """
        if isinstance(locations, list) or isinstance(locations, tuple):
            if isinstance(locations[0], dict):
                # Multiple locations, multiple columns
                if check_column_names:
                    unrecognised_columns = (
                        set().union(*locations).difference(self.location_id_columns)
                    )
                    if unrecognised_columns:
                        raise ValueError(
                            f"Columns {unrecognised_columns} are not in location_id_columns."
                        )
                ands = [
                    " AND ".join(f"{key} = '{value}'" for key, value in loc.items())
                    for loc in locations
                ]
                return "WHERE (" + ") OR (".join(ands) + ")"
            else:
                # Multiple locations, first column
                locs_list_string = ", ".join(f"'{l}'" for l in locations)
                return f"WHERE {self.location_id_columns[0]} IN ({locs_list_string})"
        elif isinstance(locations, dict):
            # Single location, multiple columns
            if check_column_names:
                unrecognised_columns = set(locations).difference(
                    self.location_id_columns
                )
                if unrecognised_columns:
                    raise ValueError(
                        f"Columns {unrecognised_columns} are not in location_id_columns."
                    )
            return "WHERE " + " AND ".join(
                f"{key} = '{value}'" for key, value in locations.items()
            )
        else:
            # Single location, first column
            return f"WHERE {self.location_id_columns[0]} = '{locations}'"


class CellSpatialUnit(SpatialUnitMixin):
    """
    This class represents the case where no join of cell ID to other data is
    required. As such, this class does not inherit from Query, is not a valid
    parameter to JoinToLocation, and only exists to provide the
    location_id_columns property and for consistency with the other spatial units.
    """

    _locid_cols = ("location_id",)

    def __repr__(self):
        # Define this so that str(CellSpatialUnit()) will always return the
        # same string (otherwise 2 identical queries with different instances
        # of this spatial unit will have different query_ids).
        return self.__class__.__name__ + "()"

    def __eq__(self, other):
        return isinstance(other, CellSpatialUnit)

    def __hash__(self):
        # We may never need CellSpatialUnits to be hashable, but we define this
        # just in case.
        return hash(str(self))

    @property
    def canonical_name(self) -> str:
        return "cell"


class GeomSpatialUnit(SpatialUnitMixin, Query):
    """
    Base class for spatial units that map location IDs in
    connection.location_table to geographic locations.

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
    mapping_table : str or flowmachine.Query, optional
        Name of a bridge table to geography information.
        Can be either the name of a table, with the schema, or a
        flowmachine.Query object.
    geom_column : str, default "geom"
        Name of the column in geom_table that defines the geometry.
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
        geom_table_column_names: Union[str, Iterable[str]],
        location_id_column_names: Union[str, Iterable[str]],
        geom_table: Optional[Union[Query, str]] = None,
        mapping_table: Optional[Union[Query, str]] = None,
        geom_column: str = "geom",
        geom_table_join_on: Optional[str] = None,
        location_table_join_on: Optional[str] = None,
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
            self.geom_table = Table(name=get_db().location_table)
        elif isinstance(geom_table, Query):
            self.geom_table = geom_table
        else:
            self.geom_table = Table(name=geom_table)

        if mapping_table is not None:
            # Creating a Table object here means that we don't have to handle
            # tables and Query objects differently in _make_query and get_geom_query
            if isinstance(mapping_table, Query):
                self.mapping_table = mapping_table
            else:
                self.mapping_table = Table(name=mapping_table)

            if location_table_join_on not in self.mapping_table.column_names:
                raise ValueError(
                    "location_table_join_on not in mapping table's columns."
                )
            elif geom_table_join_on not in self.mapping_table.column_names:
                raise ValueError("geom_table_join_on not in mapping table's columns.")

        self._geom_on = geom_table_join_on
        self._loc_on = location_table_join_on

        super().__init__()

    def __eq__(self, other):
        try:
            return self.query_id == other.query_id
        except AttributeError:
            return False

    def __hash__(self):
        # Must define this because we explicitly define self.__eq__
        return hash(self.query_id)

    def _get_aliased_geom_table_cols(self, table_alias: str) -> List[str]:
        return [f"{table_alias}.{c}" for c in self._geom_table_cols]

    def _join_clause(self, loc_table_alias: str, geom_table_alias: str) -> str:
        """
        Returns a SQL join clause to join the location table to the geography
        table. The join clause is not used if self.geom_table and
        get_db().location_table are the same table.

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
        if self._loc_on is None or self._geom_on is None:
            raise ValueError("No columns specified for join.")
        if hasattr(self, "mapping_table"):
            return f"""
                    INNER JOIN
                        ({self.mapping_table.get_query()}) AS _ USING ({self._loc_on})
                    LEFT JOIN
                        ({self.geom_table.get_query()}) AS {geom_table_alias}
                    USING ({self._geom_on})
                    """
        else:
            return f"""
                    INNER JOIN
                        ({self.geom_table.get_query()}) AS {geom_table_alias}
                    ON {loc_table_alias}.{self._loc_on} = {geom_table_alias}.{self._geom_on}
                    """

    def _make_query(self):
        loc_table_alias = "loc_table"

        if hasattr(self.geom_table, "fully_qualified_table_name") and (
            self.geom_table.fully_qualified_table_name == get_db().location_table
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
        FROM {get_db().location_table} AS {loc_table_alias}
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

    def get_geom_query(self) -> str:
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


class LonLatSpatialUnit(GeomSpatialUnit):
    """
    Class that provides a mapping from cell/site IDs in the location table to
    longitude and latitude.

    In addition to the requested geom_table_column_names, this query returns
    longitude and latitude values in columns "lon" and "lat".

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
    mapping_table : str or flowmachine.Query, optional
        Name of a bridge table to geography information.
        Can be either the name of a table, with the schema, or a
        flowmachine.Query object.
    geom_column : str, default "geom_point"
        Name of the column in geom_table that defines the point geometry from
        which longitude and latitude will be extracted.
    """

    def __init__(
        self,
        *,
        geom_table_column_names: Union[str, Iterable[str]] = (),
        location_id_column_names: Union[str, Iterable[str]] = (),
        geom_table: Optional[Union[Query, str]] = None,
        mapping_table: Optional[Union[Query, str]] = None,
        geom_column: str = "geom_point",
        geom_table_join_on: Optional[str] = None,
        location_table_join_on: Optional[str] = None,
    ):
        super().__init__(
            geom_table_column_names=geom_table_column_names,
            location_id_column_names=location_id_column_names,
            geom_table=geom_table,
            mapping_table=mapping_table,
            geom_column=geom_column,
            geom_table_join_on=geom_table_join_on,
            location_table_join_on="id"
            if mapping_table is not None and location_table_join_on is None
            else location_table_join_on,
        )

    def _get_aliased_geom_table_cols(self, table_alias: str) -> List[str]:
        return super()._get_aliased_geom_table_cols(table_alias) + [
            f"ST_X({table_alias}.{self._geom_col}::geometry) AS lon",
            f"ST_Y({table_alias}.{self._geom_col}::geometry) AS lat",
        ]

    @property
    def canonical_name(self) -> str:
        return "lon-lat"

    @property
    def location_id_columns(self) -> List[str]:
        """
        Names of the columns that identify a location.
        """
        return list(self._locid_cols) + ["lon", "lat"]

    def location_subset_clause(self, locations, check_column_names: bool = True) -> str:
        """
        Return a SQL "WHERE" clause to subset a query (joined to this spatial
        unit) to a location or set of locations. This method differs from the
        default implementation in its handling of lon-lat values, i.e. it returns
            WHERE ST_SetSRID(ST_Point(lon, lat), 4326) = 'ST_SetSRID(ST_Point(<lon_value>, <lat_value>), 4326)'
        instead of
            WHERE lon = '<lon_value>' AND lat = '<lat_value>'

        Parameters
        ----------
        locations : tuple, str, dict, or list/tuple thereof
            Location or list of locations to subset to.
            This should have one of the following formats:
                list/tuple of tuple
                    Values are (longitude, latitude) pairs, corresponding to
                    the 'lon' and 'lat' columns.
                    Note: cannot pass a single (lon, lat) tuple, as this would
                    be ambiguous (could be a tuple of str, see below). For a
                    single location, either pass a length-1 list [(lon, lat)]
                    or a dict {"lon": lon, "lat": lat}.
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
        # TODO: Once we have a class for representing lon-lat pairs
        # (see https://github.com/Flowminder/FlowKit/issues/915), accept these
        # instead of tuples to remove ambiguity.
        if isinstance(locations, list) or isinstance(locations, tuple):
            if isinstance(locations[0], tuple):
                if len(locations) == 1:
                    # Single location, lat-lon columns
                    lon, lat = locations[0]
                    return f"WHERE ST_SetSRID(ST_Point(lon, lat), 4326) = 'ST_SetSRID(ST_Point({lon}, {lat}), 4326)'"
                else:
                    # Multiple locations, lat-lon columns
                    locs_list_string = ", ".join(
                        f"'ST_SetSRID(ST_Point({lon}, {lat}), 4326)'"
                        for lon, lat in locations
                    )
                    return f"WHERE ST_SetSRID(ST_Point(lon, lat), 4326) IN ({locs_list_string})"
            elif isinstance(locations[0], dict):
                # Multiple locations, multiple columns
                if check_column_names:
                    unrecognised_columns = (
                        set().union(*locations).difference(self.location_id_columns)
                    )
                    if unrecognised_columns:
                        raise ValueError(
                            f"Columns {unrecognised_columns} are not in location_id_columns."
                        )
                locations_copy = [
                    _substitute_lat_lon(location) for location in locations
                ]
                return super().location_subset_clause(
                    locations_copy, check_column_names=False
                )
            else:
                # Multiple locations, first column
                return super().location_subset_clause(locations)
        elif isinstance(locations, dict):
            # Single location, multiple columns
            if check_column_names:
                unrecognised_columns = set(locations).difference(
                    self.location_id_columns
                )
                if unrecognised_columns:
                    raise ValueError(
                        f"Columns {unrecognised_columns} are not in location_id_columns."
                    )
            return super().location_subset_clause(
                _substitute_lat_lon(locations), check_column_names=False
            )
        else:
            # Single location, first column
            return super().location_subset_clause(locations)


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
    mapping_table : str or flowmachine.Query, optional
        Name of a bridge table to geography information.
        Can be either the name of a table, with the schema, or a
        flowmachine.Query object.
    geom_column : str, default 'geom'
        Name of the column in geom_table that defines the geography.
    """

    def __init__(
        self,
        *,
        geom_table_column_names: Union[str, Iterable[str]],
        geom_table: Union[Query, str],
        mapping_table: Optional[Union[Query, str]] = None,
        geom_column: str = "geom",
        geom_table_join_on: Optional[str] = None,
    ):
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
            mapping_table=mapping_table,
            geom_column=geom_column,
            geom_table_join_on=geom_table_join_on,
            location_table_join_on="id" if mapping_table is not None else None,
        )

    def _join_clause(self, loc_table_alias: str, geom_table_alias: str) -> str:
        if hasattr(self, "mapping_table"):
            return super()._join_clause(loc_table_alias, geom_table_alias)
        else:
            return f"""
            INNER JOIN
                ({self.geom_table.get_query()}) AS {geom_table_alias}
            ON ST_within(
                {loc_table_alias}.geom_point::geometry,
                ST_SetSRID({geom_table_alias}.{self._geom_col}, 4326)::geometry
            )
            """

    @property
    def canonical_name(self) -> str:
        return "polygon"


class VersionedCellSpatialUnit(LonLatSpatialUnit):
    """
    Subclass of LonLatSpatialUnit that maps cell location_id to a cell version
    and lon-lat coordinates.
    """

    def __init__(self) -> None:
        if get_db().location_table != "infrastructure.cells":
            raise InvalidSpatialUnitError("Versioned cell spatial unit is unavailable.")

        super().__init__(
            geom_table_column_names=["version"],
            location_id_column_names=["location_id", "version"],
            geom_table="infrastructure.cells",
        )

    @property
    def canonical_name(self) -> str:
        return "versioned-cell"


class VersionedSiteSpatialUnit(LonLatSpatialUnit):
    """
    Subclass of LonLatSpatialUnit that maps cell location_id to a site version
    and lon-lat coordinates.
    """

    def __init__(self) -> None:
        super().__init__(
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

    @property
    def canonical_name(self) -> str:
        return "versioned-site"


class AdminSpatialUnit(PolygonSpatialUnit):
    """
    Subclass of PolygonSpatialUnit object that maps all cells (aka sites) to an
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
    mapping_table : str or flowmachine.Query, optional
        Name of a bridge table to geography information.
        Can be either the name of a table, with the schema, or a
        flowmachine.Query object.
    """

    def __init__(
        self,
        *,
        level: int,
        region_id_column_name: Optional[str] = None,
        mapping_table: Union[str, Query] = None,
    ) -> None:
        # If there is no region_id_column_name passed then we can use the default,
        # which is of the form admin3pcod. If the user has asked for the standard
        # region_id_column_name then we will alias this column as 'pcod', otherwise
        # we won't alias it at all.
        if (
            region_id_column_name is None
            or region_id_column_name == f"admin{level}pcod"
        ):
            col_name = f"admin{level}pcod AS pcod"
        else:
            col_name = region_id_column_name
        table = f"geography.admin{level}"
        self.level = level

        super().__init__(
            geom_table_column_names=col_name,
            geom_table=table,
            mapping_table=mapping_table,
            geom_table_join_on=None if mapping_table is None else f"admin{level}pcod",
        )

    @property
    def canonical_name(self) -> str:
        return f"admin{self.level}"


class GridSpatialUnit(PolygonSpatialUnit):
    """
    Subclass of PolygonSpatialUnit that maps all the sites in the database to a
    grid of arbitrary size.

    Parameters
    ----------
    size : float or int
        Size of the grid in kilometres
    """

    def __init__(
        self,
        *,
        size: Union[float, int],
        mapping_table: Union[str, Query] = None,
        location_table_join_on: Optional[str] = None,
    ) -> None:
        self.grid = Grid(size)

        super().__init__(
            geom_table_column_names="grid_id",
            geom_table=self.grid,
            geom_column="geom_square",
            geom_table_join_on=None if mapping_table is None else "grid_id",
            mapping_table=mapping_table,
        )

    @property
    def canonical_name(self) -> str:
        return "grid"


AnySpatialUnit = Union[CellSpatialUnit, GeomSpatialUnit]


def make_spatial_unit(
    spatial_unit_type: str,
    *,
    level: Optional[int] = None,
    region_id_column_name: Optional[Union[str, Iterable[str]]] = None,
    size: Union[float, int] = None,
    geom_table: Optional[Union[Query, str]] = None,
    geom_column: str = "geom",
    mapping_table: Optional[Union[str, Query]] = None,
    geom_table_join_on: Optional[str] = None,
) -> Union[CellSpatialUnit, GeomSpatialUnit]:
    """
    Helper function to create an object representing a spatial unit.

    Parameters
    ----------
    spatial_unit_type : str
        Can be one of:
            'cell'
                The identifier as found in the CDR.
            'lon-lat'
                Longitude and latitude of cell/site locations.
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
    mapping_table : str or flowmachine.Query, optional
        Name of a bridge table to geography information.
        Can be either the name of a table, with the schema, or a
        flowmachine.Query object.
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
        return VersionedCellSpatialUnit()
    elif spatial_unit_type == "versioned-site":
        return VersionedSiteSpatialUnit()
    elif spatial_unit_type == "lon-lat":
        return LonLatSpatialUnit(
            mapping_table=mapping_table,
            geom_table=geom_table,
            geom_table_join_on=geom_table_join_on,
        )
    elif spatial_unit_type == "admin":
        if level is None:
            raise ValueError(
                "'level' parameter is required for spatial unit of type 'admin'."
            )
        return AdminSpatialUnit(
            level=level,
            region_id_column_name=region_id_column_name,
            mapping_table=mapping_table,
        )
    elif spatial_unit_type == "grid":
        if size is None:
            raise ValueError(
                "'size' parameter is required for spatial unit of type 'grid'."
            )
        return GridSpatialUnit(size=size, mapping_table=mapping_table)
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
            mapping_table=mapping_table,
        )
    else:
        raise ValueError(f"Unrecognised spatial unit type: {spatial_unit_type}.")
