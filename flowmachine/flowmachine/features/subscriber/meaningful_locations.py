# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Dict, Any, List

from ...core import Query
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
