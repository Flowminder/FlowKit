# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility methods for calculating a distance
matrix from a given point collection.

"""
from typing import List, Optional

from ...core.query import Query
from ...core.mixins import GraphMixin
from ...core import make_spatial_unit
from ...core.spatial_unit import LonLatSpatialUnit


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

        self.return_geometry = return_geometry

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        col_names = [f"{c}_from" for c in self.spatial_unit.location_id_columns]
        col_names += [f"{c}_to" for c in self.spatial_unit.location_id_columns]
        col_names += ["distance"]
        if self.return_geometry:
            col_names += ["geom_origin", "geom_destination"]
        return col_names

    def _make_query(self):
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
                ST_Distance(
                    A.geom::geography, 
                    B.geom::geography
                ) / 1000 AS distance
                {return_geometry_statement}
            FROM ({geom_query}) AS A
            CROSS JOIN ({geom_query}) AS B
            ORDER BY distance DESC
        """

        return sql
