# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for networkx export.
"""
from typing import List

import pytest

from flowmachine_core.query_bases.query import Query
from flowmachine_core.core.mixins import GraphMixin


class GraphQuery(GraphMixin, Query):
    @property
    def column_names(self) -> List[str]:
        return ["source", "target", "value"]

    def _make_query(self):
        return "SELECT * FROM (VALUES (1, 2, 'a'), (1, 2, 'a'), (3, 4, 'b')) as t(source, target, value)"


def test_nx_object():
    """
    to_networkx() creates a networkx.Graph() object.
    """
    graph = GraphQuery().to_networkx()
    assert graph.has_node(1)
    assert 2 in graph.neighbors(1)
    assert 2 not in graph.neighbors(3)


def test_undirected():
    """
    to_networkx() raises a warning if using duplicate edges are detected.
    """
    with pytest.warns(UserWarning):
        graph = GraphQuery().to_networkx(directed_graph=False)
    assert 2 in graph.neighbors(1)


def test_directed_dupe():
    """
    to_networkx() raises a warning if using duplicate edges are detected in digraph.
    """

    with pytest.warns(UserWarning):
        GraphQuery().to_networkx(directed_graph=True)


@pytest.mark.parametrize("side", ["source", "target"])
def test_errors_with_one_param(side):
    """
    to_networkx() raises error if only one side of parameters is passed.
    """
    with pytest.raises(ValueError):
        GraphQuery().to_networkx(**{side: "source"})
