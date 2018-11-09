# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for networkx export.
"""
import pytest

from flowmachine.core import Query
from flowmachine.core.mixins import GraphMixin

from flowmachine.features import daily_location, Flows


def test_nx_object():
    """
    to_networkx() creates a networkx.Graph() object.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)
    graph = flow.to_networkx()
    assert graph.has_node("Arghakhanchi")
    assert "Dadeldhura" in graph.neighbors("Arghakhanchi")
    assert "Sankhuwasabha" not in graph.neighbors("Arghakhanchi")


def test_undirected():
    """
    to_networkx() raises a warning if using duplicate edges are detected.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)
    with pytest.warns(UserWarning):
        graph = flow.to_networkx(directed_graph=False)
    assert "Sankhuwasabha" in graph.neighbors("Arghakhanchi")


def test_directed_dupe():
    """
    to_networkx() raises a warning if using duplicate edges are detected in digraph.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)

    class DupeFlow(GraphMixin, Query):
        def __init__(self, flow):
            self.union = flow.union(flow)
            super().__init__()

        def _make_query(self):
            return self.union.get_query()

    with pytest.warns(UserWarning):
        graph = DupeFlow(flow).to_networkx(directed_graph=True)


def test_errors_with_one_param():
    """
    to_networkx() raises error if only one side of parameters is passed.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)
    with pytest.raises(ValueError):
        flow.to_networkx(source="name_from")

    with pytest.raises(ValueError):
        flow.to_networkx(target="name_from")
