# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Definition of the Geography class, which returns a table of the
location_id_columns and geometry for a spatial unit.
"""
from typing import List

from ...core.query import Query
from ...core.mixins import GeoDataMixin
from ...core.spatial_unit import GeomSpatialUnit


class Geography(GeoDataMixin, Query):
    """
    This class is a wrapper around the SQL query returned by the
    'get_geom_query' method of spatial unit objects, adding geographic utility
    methods (e.g. 'to_geojson').

    Queries of this type are used to return GeoJSON data via the FlowAPI
    'get_geography' route.

    Parameters
    ----------
    spatial_unit : flowmachine.core.spatial_unit.GeomSpatialUnit
        Spatial unit to return geography data for. See the docstring of
        make_spatial_unit for more information.
    """

    def __init__(self, spatial_unit: GeomSpatialUnit):
        spatial_unit.verify_criterion("has_geography")
        self.spatial_unit = spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.spatial_unit.location_id_columns + ["geom"]

    def _geo_augmented_query(self):
        sql = f"""
        SELECT row_number() over() AS gid, *
        FROM ({self.get_query()}) AS Q
        """

        return sql, ["gid"] + self.column_names

    def _make_query(self):
        return self.spatial_unit.get_geom_query()
