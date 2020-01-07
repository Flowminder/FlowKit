# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Optional, List

from flowmachine.core import Query, make_spatial_unit
from flowmachine.core.spatial_unit import AnySpatialUnit
from flowmachine.features.subscriber import MeaningfulLocations


class MeaningfulLocationsAggregate(Query):
    """
    Aggregates an individual-level meaningful location to a spatial unit by assigning
    subscribers with clusters in that unit to it. For subscribers with more than one cluster,
    assigns `1/n_clusters` to each spatial unit that the cluster lies in.

    Parameters
    ----------
    meaningful_locations : MeaningfulLocations
        A per-subscriber meaningful locations object to aggregate
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default admin3
        Spatial unit to aggregate to
    """

    def __init__(
        self,
        *,
        meaningful_locations: MeaningfulLocations,
        spatial_unit: Optional[AnySpatialUnit] = None,
    ) -> None:
        self.meaningful_locations = meaningful_locations
        if spatial_unit is None:
            self.spatial_unit = make_spatial_unit("admin", level=3)
        else:
            self.spatial_unit = spatial_unit
        self.spatial_unit.verify_criterion("is_polygon")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["label"] + self.spatial_unit.location_id_columns + ["value"]

    def _make_query(self):
        location_cols = ", ".join(self.spatial_unit.location_id_columns)

        return f"""
        SELECT label, {location_cols}, sum(1./n_clusters) as value FROM
        ({self.meaningful_locations.get_query()}) meaningful_locations
        LEFT JOIN 
        ({self.spatial_unit.get_geom_query()}) agg
        ON ST_Contains(agg.geom::geometry, meaningful_locations.cluster::geometry)
        GROUP BY label, {location_cols}
        ORDER BY label, {location_cols}
        """
