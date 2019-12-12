# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Optional, List

from flowmachine.core import Query, make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from flowmachine.features.subscriber import MeaningfulLocations
from flowmachine.features.location.flows import FlowLike


class MeaningfulLocationsOD(FlowLike, Query):
    """
    Calculates an OD matrix aggregated to a spatial unit between two individual
    level meaningful locations. For subscribers with more than one cluster of either
    label, counts are weight to `1/(n_clusters_label_a*n_clusters_label_b)`.


    Parameters
    ----------
    meaningful_locations_a, meaningful_locations_a : MeaningfulLocations
        Per-subscriber meaningful locations objects calculate an OD between
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to aggregate to
    """

    def __init__(
        self,
        *,
        meaningful_locations_a: MeaningfulLocations,
        meaningful_locations_b: MeaningfulLocations,
        spatial_unit: Optional[AnySpatialUnit] = None,
    ) -> None:
        self.flow = meaningful_locations_a.join(
            meaningful_locations_b,
            on_left="subscriber",
            left_append="_from",
            right_append="_to",
        )
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=3)
        else:
            self.spatial_unit = spatial_unit
        self.spatial_unit.verify_criterion("is_polygon")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return [
            f"{col}_{direction}"
            for col in ["label"] + self.spatial_unit.location_id_columns
            for direction in ("from", "to")
        ] + ["value"]

    def _make_query(self):
        agg_query = self.spatial_unit.get_geom_query()
        location_cols = [
            f"{col}_{direction}"
            for col in self.spatial_unit.location_id_columns
            for direction in ("from", "to")
        ]
        location_cols_aliased = [
            f"{direction}_q.{col} as {col}_{direction}"
            for col in self.spatial_unit.location_id_columns
            for direction in ("from", "to")
        ]

        location_cols = ", ".join(location_cols)
        location_cols_aliased = ", ".join(location_cols_aliased)
        return f"""
        SELECT label_from, label_to, {location_cols_aliased}, sum(1./(n_clusters_from*n_clusters_to)) as value FROM
        ({self.flow.get_query()}) meaningful_locations
        LEFT JOIN 
        ({agg_query}) from_q
        ON ST_Contains(from_q.geom::geometry, meaningful_locations.cluster_from::geometry)
        LEFT JOIN 
        ({agg_query}) to_q
        ON ST_Contains(to_q.geom::geometry, meaningful_locations.cluster_to::geometry)
        GROUP BY label_from, label_to, {location_cols}
        ORDER BY label_from, label_to, {location_cols}
        """
