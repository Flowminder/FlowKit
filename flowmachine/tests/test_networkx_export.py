# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for networkx export.
"""
from typing import List

import pytest

from flowmachine.core import Query
from flowmachine.core.mixins import GraphMixin
from flowmachine.features import Flows, daily_location


def test_nx_object():
    """
    to_networkx() creates a networkx.Graph() object.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)
    graph = flow.to_networkx()
    assert graph.has_node("524 3 09 50")
    assert "524 5 14 73" in graph.neighbors("524 3 09 50")
    assert "524 1 02 09" not in graph.neighbors("524 3 09 50")


def test_undirected():
    """
    to_networkx() raises a warning if using duplicate edges are detected.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)
    with pytest.warns(UserWarning):
        graph = flow.to_networkx(directed_graph=False)
    assert "524 1 02 09" in graph.neighbors("524 3 09 50")


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

        @property
        def column_names(self) -> List[str]:
            return self.union.column_names

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
        flow.to_networkx(source="pcod_from")

    with pytest.raises(ValueError):
        flow.to_networkx(target="pcod_from")
