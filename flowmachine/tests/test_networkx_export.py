# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for networkx export.
"""

from unittest import TestCase

from flowmachine.core import Query
from flowmachine.core.mixins import GraphMixin

from flowmachine.features import daily_location, Flows


class FlowGraphTestCase(TestCase):
    """
    Tests for the to_networkx() method.
    """

    def setUp(self):
        dl1 = daily_location("2016-01-01")
        dl2 = daily_location("2016-01-02")
        self.flow = Flows(dl1, dl2)

    def test_nx_object(self):
        """
        to_networkx() creates a networkx.Graph() object.
        """
        graph = self.flow.to_networkx()
        self.assertTrue(graph.has_node("Arghakhanchi"))
        self.assertIn("Dadeldhura", graph.neighbors("Arghakhanchi"))
        self.assertNotIn("Sankhuwasabha", graph.neighbors("Arghakhanchi"))

    def test_undirected(self):
        """
        to_networkx() raises a warning if using duplicate edges are detected.
        """
        with self.assertWarns(UserWarning):
            graph = self.flow.to_networkx(directed_graph=False)
            self.assertIn("Sankhuwasabha", graph.neighbors("Arghakhanchi"))

    def test_directed_dupe(self):
        """
        to_networkx() raises a warning if using duplicate edges are detected in digraph.
        """

        class DupeFlow(GraphMixin, Query):
            def __init__(self, flow):
                self.union = flow.union(flow)
                super().__init__()

            def _make_query(self):
                return self.union.get_query()

        with self.assertWarns(UserWarning):
            graph = DupeFlow(self.flow).to_networkx(directed_graph=True)

    def test_errors_with_one_param(self):
        """
        to_networkx() raises error if only one side of parameters is passed.
        """
        with self.assertRaises(ValueError):
            self.flow.to_networkx(source="name_from")

        with self.assertRaises(ValueError):
            self.flow.to_networkx(target="name_from")
