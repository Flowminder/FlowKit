# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility methods for calculating a distance
matrix from a given point collection.

"""
from typing import List

from flowmachine.utils import get_name_and_alias
from ...core.query import Query
from ...core.mixins import GraphMixin
from ...core.spatial_unit import VersionedSiteSpatialUnit, VersionedCellSpatialUnit


class DistanceMatrix(GraphMixin, Query):
    """
    Calculates the complete distance matrix between a 
    location set. This is useful for the further 
    computation of distance travelled, area of influence, 
    and other features.

    This is a wrapper around the SpatialUnit.distance_matrix_query method.

    Distance is returned in km.

    Parameters
    ----------
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default VersionedCellSpatialUnit()
        Locations to compute distances for.
        Note: only VersionedCellSpatialUnit and VersionedSiteSpatialUnit are
        supported at this time.

    return_geometry : bool
        If True, geometries are returned in query
        (represented as WKB in a dataframe). This
        is an useful option if one is computing
        other geographic properties out of the

    """

    def __init__(self, spatial_unit=None, return_geometry=False):
        if spatial_unit is None:
            self.spatial_unit = VersionedCellSpatialUnit()
        else:
            self.spatial_unit = spatial_unit

        self.location_id_cols = set(self.spatial_unit.location_columns)
        try:
            self.location_id_cols.remove("lat")
            self.location_id_cols.remove("lon")
        except KeyError:
            raise ValueError("Only point locations are supported at this time.")

        self.return_geometry = return_geometry

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        col_names = [f"{c}_from" for c in self.location_id_cols]
        col_names += [f"{c}_to" for c in self.location_id_cols]
        col_names += ["lon_from", "lat_from", "lon_to", "lat_to", "distance"]
        if self.return_geometry:
            col_names += ["geom_origin", "geom_destination"]
        return col_names

    def _make_query(self):
        # FIXME: Accessing a "private" attribute of self.spatial_unit here
        names_for_loc_id_col_aliases = [
            c
            for c in self.spatial_unit._cols
            if get_name_and_alias(c)[1] in self.location_id_cols
        ]

        cols_A = ",".join(
            [
                f"A.{get_name_and_alias(c)[0].split('.')[-1]} AS {get_name_and_alias(c)[1]}_from"
                for c in names_for_loc_id_col_aliases
            ]
        )
        if cols_A != "":
            cols_A += ","
        cols_B = ",".join(
            [
                f"B.{get_name_and_alias(c)[0].split('.')[-1]} AS {get_name_and_alias(c)[1]}_to"
                for c in names_for_loc_id_col_aliases
            ]
        )
        if cols_B != "":
            cols_B += ","

        locinfo_table = get_name_and_alias(self.spatial_unit.location_info_table)[0]

        if self.return_geometry:
            return_geometry_statement = """
                ,
                A.geom_point AS geom_origin,
                B.geom_point AS geom_destination
            """
        else:
            return_geometry_statement = ""

        sql = f"""

            SELECT
                {cols_A}
                {cols_B}
                ST_X(A.geom_point::geometry) AS lon_from,
                ST_Y(A.geom_point::geometry) AS lat_from,
                ST_X(B.geom_point::geometry) AS lon_to,
                ST_Y(B.geom_point::geometry) AS lat_to,
                ST_Distance(
                    A.geom_point::geography, 
                    B.geom_point::geography
                ) / 1000 AS distance
                {return_geometry_statement}
            FROM {locinfo_table} AS A
            CROSS JOIN {locinfo_table} AS B
            ORDER BY distance DESC
            
        """

        return sql
