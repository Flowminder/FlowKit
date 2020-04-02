# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List, Optional

from flowmachine.core import Table
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.core.query import Query
from flowmachine.features.spatial.versioned_infrastructure import (
    VersionedInfrastructure,
)


class TowerCluster(GeoDataMixin, Query):
    """
    A clustering of sites or cells. Returns the convex hull of each cluster.

    Parameters
    ----------
    start_date, end_date : str
        Half-open date range where towers must have been active.
    minimum_points : int, default 1
        Minimum number of infrastructure items to form a cluster
    distance : int, default 1000
        Distance in meters to build clusters
    infrastructure_table : {'cells', 'sites'}, default 'cells'
        Infrastructure table to cluster
    """

    def __init__(
        self,
        start_date: str,
        end_date: str,
        *,
        minimum_points: int = 1,
        distance: int = 1000,
        infrastructure_table: str = "cells",
    ) -> None:
        self.infrastructure = Table(
            schema="infrastructure", name=infrastructure_table.lower()
        )
        self.start_date = start_date
        self.end_date = end_date
        self.minimum_points = minimum_points
        self.distance = distance
        super().__init__()

    def _geo_augmented_query(self):
        """
        Returns a version of this query with geom and gid columns.

        Returns
        -------
        str
            A version of this query with geom and gid columns
        """

        return (
            f"SELECT cluster_id, geom, row_number() OVER (ORDER BY longitude, latitude) as gid FROM ({self.get_query()}) as x",
            ["cluster_id", "geom", "gid"],
        )

    @property
    def column_names(self) -> List[str]:
        return ["cluster_id", "geom", "geom_point"]

    def _make_query(self):
        infra = f"{self.infrastructure.get_query()} WHERE tstzrange(date_of_first_service, date_of_last_service) && tstzrange('{self.start_date}', '{self.end_date}')"
        sql = f"""
        SELECT tower_cluster as cluster_id, ST_Centroid(ST_Collect(geom_point)) as geom_point, ST_Convexhull(ST_Collect(geom_point)) as geom FROM
        (SELECT geom_point, ST_ClusterDBScan(ST_Transform(geom_point, 3857), eps := {self.distance}, minpoints:={self.minimum_points}) over () as tower_cluster
            FROM ({infra}) infra) clustered
        GROUP BY tower_cluster
        """
        return sql
