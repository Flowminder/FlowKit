# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility methods for calculating a distance
matrix from a given point collection.

"""
from typing import List

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
        if type(self.spatial_unit) not in {
            VersionedSiteSpatialUnit,
            VersionedCellSpatialUnit,
        }:
            raise ValueError("Only point locations are supported at this time.")
        self.return_geometry = return_geometry

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.spatial_unit.distance_matrix_columns(
            return_geometry=self.return_geometry
        )

    def _make_query(self):
        return self.spatial_unit.distance_matrix_query(
            return_geometry=self.return_geometry
        )
