# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Mixin providing utility methods for graph type queries.


"""
import warnings
import networkx as nx


class GraphMixin:
    """
    Supplies utility functions for graph-like queries, specifically
    the to_networkx function which returns a networkx graph which
    can then be analysed further using standard libraries.
    """

    def to_networkx(self, source=None, target=None, directed_graph=True):
        """
        By default, the leftmost column will be used as the source, the next
        leftmost as the target, and any other columns will become edge attributes.
        Both or neither of source and target should be provided. 
        
        The default is to return a directed graph.

        Parameters
        ----------
        source : str, optional
            Optionally specify the column name for the source nodes.
        target : str, optional
            Optionally specify the column name for the target nodes.
        directed_graph : bool, default True
            Set to false to return an undirected graph.

        Returns
        -------
        networkx.Graph
            This query as a networkx graph object.

        """

        if bool(source) != bool(target):
            raise ValueError(
                "Both source and target must be specified, " + "or neither should."
            )

        if not source:
            source, target = self.column_names[:2]

        df = self.get_dataframe()
        if directed_graph:
            if (df.groupby([source, target])[source].count() - 1).any():
                warnings.warn(
                    " Duplicate edges in directed graph. Edge "
                    "information will be lost.",
                    stacklevel=2,
                )

            g_type = nx.DiGraph()

        else:
            df["links"] = [tuple(sorted(x)) for x in zip(df[source], df[target])]
            if (df.groupby(["links"])[source].count() - 1).any():
                warnings.warn(
                    " Duplicate edges in undirected graph. Edge "
                    "information will be lost.",
                    stacklevel=2,
                )

            del df["links"]
            g_type = nx.Graph()

        return nx.from_pandas_edgelist(
            df, source, target, edge_attr=True, create_using=g_type
        )
