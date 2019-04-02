# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
The Louvain Method is a popular community
identification algorithm for the exploration
of very large networks.

The algorithm [4]_ was designed to optimize for
the network's Modularity. [2]_ That is,
this algorithm measures the density of connections
(i.e. edges) from nodes and identifies "small"
communities. Then it uses those "small" to
identify the interactions that they have
with other communities of comparable size.
It does so iteratively until "a maximum
of modularity is attained". [1]_ [3]_



References
----------
.. [1] http://barabasi.com/f/618.pdf
.. [2] https://en.wikipedia.org/wiki/Modularity
.. [3] https://sites.google.com/site/findcommunities/
.. [4] Vincent D. Blondel, Jean-Loup Guillaume, Renaud Lambiotte, Etienne Lefebvre. "Fast unfolding of communities in large networks". https://arxiv.org/abs/0803.0476

"""

import community
import pandas as pd
import networkx as nx
import warnings

from math import inf

from ..core.model import Model, model_result

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class Louvain(Model):
    """
    Class for running the Louvain community identifying
    on a networkx Graph(). This class is useful for
    quickly identifying communities from a calculated
    `flowmachine` feature (e.g. ContactBalance()).

    Parameters
    ----------
    graph : networkx.Graph() object
        This parameter specifies a networkx.Graph()
        object to use. This can be a `flowmachine` feature
        (e.g. ContactBalance()) after calling the
        to_networkx() method.

    Examples
    --------
    The class needs to be instantiated with a
    networkx.Graph(). One can use classes that
    have the `GraphMixin` mixin as follows:

    >>> l = Louvain(
            flowmachine.features.ContactBalance('2016-01-02', '2016-01-07').to_networkx(
                directed_graph=False))

    Now let's run the algorithm:

    >>> l.run(weight_property='events', min_members=9)
                     subscriber  community
    0    bvEWVnZdwJ8Lgkm2          0
    1    YMBqRkzbbxGkX3zA          0
    2    7XebRKr35JMJnq8A          1
    3    xE8QMJxmLGMW6lZ4          1
    4    w2YgmOPlr2O5x3VJ          2
    5    NBpb54xVMEw1Yqza          2
    6    7lNP0mDOAK3xKWv4          3

    """

    def __init__(self, graph):

        self.graph = graph

        if not isinstance(graph, nx.Graph):
            raise ValueError("Graph provided is not a networkx Graph() type.")

    def __get_partitions(self, resolution, weight="weight", **kwargs):
        """
        Protected method for computing the best
        partitions using python-louvain. This is
        a simple wrapper to avoid code repetition.

        Parameters
        ----------
        weight :
            Column to use that represents edge
            weights.

        resolution :
            Resolution of Louvain graph. Passed down
            to community.best_partition().

        **kwargs :
            Eventually passed to community.best_partition().

        Returns
        -------
        A pandas dataframe with the community
        members (subscribers) and their respective
        community identifier.

        """
        self.partition = community.best_partition(
            self.graph_louvain, weight=weight, resolution=resolution, **kwargs
        )

        return pd.DataFrame(
            list(self.partition.items()), columns=["subscriber", "community"]
        )

    @model_result
    def run(
        self,
        min_contacts=1,
        max_contacts=inf,
        min_members=5,
        max_members=inf,
        weight_property="weight",
        resolution=1.0,
        **kwargs
    ):
        """
        Runs model.

        Parameters
        ----------
        min_contacts : int
            Minimum contacts in graph. This parameter
            is useful for determining how many members
            to include in graph. Default is 1.

        max_contacts : int
            Maximum contacts in graph. This determines
            how large a single subscriber's network
            can be. Default is math.inf.

        min_members : int
            Minimum community membership. The final
            graph will only contain those subscribers
            from which community sizes have at least
            this many members. Default is 5.

        max_members : int
            Maximum community membership. The final
            graph will only contain those subscribers
            from which community sizes have at most
            this many members. Default is math.inf.

        **kwargs :
            Eventually passed to community.best_partition().

        Returns
        -------
        A pandas dataframe object. For format see Louvain()
        docstring.

        """

        node_degrees = nx.degree(self.graph)
        number_input_nodes = nx.number_of_nodes(self.graph)
        number_input_edges = nx.number_of_edges(self.graph)

        #
        #  Filters the graph based on the
        #  number of minimum contacts.
        #
        minimum_acceptance_nodes = [
            n for n, v in node_degrees if v >= min_contacts and v <= max_contacts
        ]

        self.graph_louvain = self.graph.subgraph(minimum_acceptance_nodes)

        #
        #  Compute the Louvain partitions and
        #  classifies nodes according to
        #  community membership.
        #
        if min_members > 0:
            communities = self.__get_partitions(
                weight=weight_property, resolution=resolution, **kwargs
            )

            community_subset = communities.groupby("community").size()
            community_subset = community_subset[
                (community_subset >= min_members) & (community_subset <= max_members)
            ]

            m_subset = communities[
                communities["community"].isin(
                    community_subset.reset_index()["community"]
                )
            ]

            self.graph_louvain = self.graph_louvain.subgraph(m_subset["subscriber"])

        communities = self.__get_partitions(
            weight=weight_property, resolution=resolution, **kwargs
        )

        #
        #  Calculates reduction statistics.
        #
        self.reduction_statistics = {
            "input": {"nodes": number_input_nodes, "edges": number_input_edges},
            "output": {
                "communities": communities["community"].nunique(),
                "total_members": len(communities),
                "nodes": nx.number_of_nodes(self.graph_louvain),
                "edges": nx.number_of_edges(self.graph_louvain),
                "reduction_nodes": round(
                    1 - (nx.number_of_nodes(self.graph_louvain) / number_input_nodes), 3
                )
                * 100,
                "reduction_edges": round(
                    1 - (nx.number_of_edges(self.graph_louvain) / number_input_edges), 3
                )
                * 100,
            },
        }
        if (
            self.reduction_statistics["input"]["edges"]
            != self.reduction_statistics["output"]["edges"]
        ):
            logger.warning(
                "\n" + "\n" + "REDUCTION STATISTICS"
                "\n"
                + "-------------------------------"
                + "\n"
                + "Original graph size:"
                + "\n"
                + "\n"
                + "* {} nodes".format(number_input_nodes)
                + "\n"
                + "* {} edges".format(number_input_edges)
                + "\n"
                + "-------------------------------"
                + "\n"
                + "Reduced graph size:"
                + "\n"
                + "\n"
                + "* {} ({}% reduction) nodes".format(
                    self.reduction_statistics["output"]["nodes"],
                    self.reduction_statistics["output"]["reduction_nodes"],
                )
                + "\n"
                + "* {} ({}% reduction) edges.".format(
                    self.reduction_statistics["output"]["edges"],
                    self.reduction_statistics["output"]["reduction_edges"],
                )
                + "\n"
                + "-------------------------------"
                + "\n"
                + "Communities identified: {}".format(
                    self.reduction_statistics["output"]["communities"]
                )
                + "\n"
                + "Total graph members: {}".format(
                    self.reduction_statistics["output"]["total_members"]
                )
            )
        return communities
