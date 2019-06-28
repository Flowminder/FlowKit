# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Dict, Any, List, Union, Optional

from ...core import GeoTable, Query, Grid, make_spatial_unit
from ...core.spatial_unit import AnySpatialUnit
from . import LabelEventScore, HartiganCluster, EventScore


class MeaningfulLocations(Query):
    """
    Infer 'meaningful' locations for individual subscribers (for example, home and work) based on
    a clustering of the cell towers they use, and their usage patterns for those towers.

    Return a count of meaningful locations at some unit of spatial aggregation.
    Generates clusters of towers used by subscribers' over the given time period, scores the clusters based on the
    subscribers' usage patterns over hours of the day and days of the week. Each subscriber then has a number of
    clusters, each of which has a score for hourly usage, and day of week usage. These clusters are then labelled
    based on whether they overlap with the regions of that space defined in the `labels` parameter.

    This is an extension of work by Isaacman et al.[1]_ by Flowminder in collaboration with the World Bank[2]_.

    Parameters
    ----------
    clusters : HartiganCluster
        Per subscriber clusters of towers
    scores : EventScore
        Per subscriber, per tower scores based on hour of day and day of week of interactions with the tower
    labels : dict of dict
        Labels to apply to clusters given their usage pattern scoring
    label : str
        Meaningful label to extract clusters for

    References
    ----------
    .. [1] S. Isaacman et al., "Identifying Important Places in People's Lives from Cellular Network Data", International Conference on Pervasive Computing (2011), pp 133-151.
    .. [2] Zagatti, Guilherme Augusto, et al. "A trip to work: Estimation of origin and destination of commuting patterns in the main metropolitan regions of Haiti using CDR." Development Engineering 3 (2018): 133-165.
    """

    def __init__(
        self,
        *,
        clusters: HartiganCluster,
        scores: EventScore,
        labels: Dict[str, Dict[str, Any]],
        label: str,
    ) -> None:
        labelled_clusters = LabelEventScore(
            scores=clusters.join_to_cluster_components(scores), labels=labels
        )
        self.labelled_subset = labelled_clusters.subset("label", label)

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "label", "cluster", "n_clusters"]

    def _make_query(self):
        return f"""
        SELECT subscriber, label, cluster, (sum(1) OVER (PARTITION BY subscriber)) as n_clusters FROM 
            ({self.labelled_subset.get_query()}) clus 
        GROUP BY subscriber, label, cluster
        ORDER BY subscriber
        """


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
            self.spatial_unit == make_spatial_unit("admin", level=3)
        else:
            self.spatial_unit = spatial_unit
        self.spatial_unit.verify_criterion("is_polygon")
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["label"] + self.spatial_unit.location_id_columns + ["total"]

    def _make_query(self):
        location_cols = ", ".join(self.spatial_unit.location_id_columns)

        return f"""
        SELECT label, {location_cols}, sum(1./n_clusters) as total FROM
        ({self.meaningful_locations.get_query()}) meaningful_locations
        LEFT JOIN 
        ({self.spatial_unit.get_geom_query()}) agg
        ON ST_Contains(agg.geom::geometry, meaningful_locations.cluster::geometry)
        GROUP BY label, {location_cols}
        HAVING sum(1./n_clusters) > 15
        ORDER BY label, {location_cols}
        """


class MeaningfulLocationsOD(Query):
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
        ] + ["total"]

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
        SELECT label_from, label_to, {location_cols_aliased}, sum(1./(n_clusters_from*n_clusters_to)) as total FROM
        ({self.flow.get_query()}) meaningful_locations
        LEFT JOIN 
        ({agg_query}) from_q
        ON ST_Contains(from_q.geom::geometry, meaningful_locations.cluster_from::geometry)
        LEFT JOIN 
        ({agg_query}) to_q
        ON ST_Contains(to_q.geom::geometry, meaningful_locations.cluster_to::geometry)
        GROUP BY label_from, label_to, {location_cols}
        HAVING sum(1./(n_clusters_from*n_clusters_to)) > 15
        ORDER BY label_from, label_to, {location_cols}
        """
