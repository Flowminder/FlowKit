# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility methods for calculating a distance
matrix from a given point collection.

"""
from typing import List, Optional

from flowmachine.core import Table
from ...core.query import Query
from ...core.mixins import GraphMixin
from ...core import make_spatial_unit
from ...core.spatial_unit import LonLatSpatialUnit


class _GeomDistanceMatrix(Query):
    def __init__(self, *, geom_table):
        self.geom_table = geom_table
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["geom_origin", "geom_destination", "value"]

    def _make_query(self):
        geom_query = f"SELECT geom_point as geom FROM {self.geom_table.fully_qualified_table_name} GROUP BY geom"

        sql = f"""
            SELECT
                A.geom as geom_origin,
                B.geom as geom_destination,
                ST_Distance(
                    A.geom::geography, 
                    B.geom::geography
                ) / 1000 AS value
            FROM ({geom_query}) AS A
            CROSS JOIN ({geom_query}) AS B
        """

        return sql


class DistanceMatrix(GraphMixin, Query):
    """
    Calculates the complete distance matrix between a 
    location set. This is useful for the further 
    computation of distance travelled, area of influence, 
    and other features.

    Distance is returned in km.

    Parameters
    ----------
    spatial_unit : flowmachine.core.spatial_unit.LonLatSpatialUnit, default versioned-cell
        Locations to compute distances for.
        Note: only point locations (i.e. spatial_unit.has_lon_lat_columns) are
        supported at this time.
    return_geometry : bool
        If True, geometries are returned in query
        (represented as WKB in a dataframe). This
        is an useful option if one is computing
        other geographic properties out of the

    """

    def __init__(
        self,
        spatial_unit: Optional[LonLatSpatialUnit] = None,
        return_geometry: bool = False,
    ):
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("versioned-cell")
        else:
            self.spatial_unit = spatial_unit

        self.spatial_unit.verify_criterion("has_lon_lat_columns")
        self.geom_matrix = _GeomDistanceMatrix(geom_table=self.spatial_unit.geom_table)
        self.return_geometry = return_geometry

        super().__init__()

    @property
    def table_name(self):
        raise NotImplementedError("Store this object's geom_matrix instead.")

    @property
    def column_names(self) -> List[str]:
        col_names = [f"{c}_from" for c in self.spatial_unit.location_id_columns]
        col_names += [f"{c}_to" for c in self.spatial_unit.location_id_columns]
        col_names += ["value"]
        if self.return_geometry:
            col_names += ["geom_origin", "geom_destination"]
        return col_names

    def _make_query(self):
        if self.spatial_unit == LonLatSpatialUnit():
            if self.return_geometry:
                return_geometry_statement = """
                    ,
                    geom_origin,
                    geom_destination
                """
            else:
                return_geometry_statement = ""
            return f"""
            SELECT ST_X(geom_origin) as lon_from, ST_Y(geom_origin) as lat_from,
            ST_X(geom_destination) as lon_to, ST_Y(geom_destination) as lat_to,
            value
            {return_geometry_statement}
            FROM ({self.geom_matrix.get_query()}) G
            """
        cols_A = ",".join(
            [f"A.{c} AS {c}_from" for c in self.spatial_unit.location_id_columns]
        )
        cols_B = ",".join(
            [f"B.{c} AS {c}_to" for c in self.spatial_unit.location_id_columns]
        )

        geom_query = self.spatial_unit.get_geom_query()

        if self.return_geometry:
            return_geometry_statement = """
                ,
                A.geom AS geom_origin,
                B.geom AS geom_destination
            """
        else:
            return_geometry_statement = ""

        sql = f"""
            SELECT
                {cols_A},
                {cols_B},
                value
                {return_geometry_statement}
            FROM ({geom_query}) AS A
            CROSS JOIN ({geom_query}) AS B
            LEFT JOIN
            ({self.geom_matrix.get_query()}) G
            ON A.geom=G.geom_origin
            AND
            B.geom=G.geom_destination
        """

        return sql
