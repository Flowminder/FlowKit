# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for flowmachine dependency graph functions
"""
import pytest
import re
import textwrap
import IPython
from io import StringIO

from flowmachine.core import CustomQuery, Query
from flowmachine.core.dummy_query import DummyQuery
from flowmachine.core.subscriber_subsetter import make_subscriber_subsetter
from flowmachine.features import daily_location, EventTableSubset

from flowmachine.core.dependency_graph import (
    print_dependency_tree,
    calculate_dependency_graph,
    unstored_dependencies_graph,
    plot_dependency_graph,
    store_queries_in_order,
    storable_dependencies,
    stored_dependencies,
    stored_dependencies_ratio,
)


def test_print_dependency_tree():
    """
    Test that the expected dependency tree is printed for a daily location query (with an explicit subset).
    """
    subscriber_subsetter = make_subscriber_subsetter(
        CustomQuery(
            "SELECT duration, msisdn as subscriber FROM events.calls WHERE duration < 10",
            ["duration", "subscriber"],
        )
    )
    q = daily_location(
        date="2016-01-02", method="most-common", subscriber_subset=subscriber_subsetter
    )

    expected_output = textwrap.dedent(
        """\
        <Query of type: MostFrequentLocation, query_id: 'xxxxx'>
          - <Query of type: PolygonSpatialUnit, query_id: 'xxxxx'>
             - <Table: 'geography.admin3', query_id: 'xxxxx'>
          - <Query of type: SubscriberLocations, query_id: 'xxxxx'>
             - <Query of type: JoinToLocation, query_id: 'xxxxx'>
                - <Query of type: PolygonSpatialUnit, query_id: 'xxxxx'>
                   - <Table: 'geography.admin3', query_id: 'xxxxx'>
                - <Query of type: EventsTablesUnion, query_id: 'xxxxx'>
                   - <Query of type: EventTableSubset, query_id: 'xxxxx'>
                      - <Query of type: CustomQuery, query_id: 'xxxxx'>
                      - <Table: 'events.sms', query_id: 'xxxxx'>
                         - <Table: 'events.sms', query_id: 'xxxxx'>
                   - <Query of type: EventTableSubset, query_id: 'xxxxx'>
                      - <Query of type: CustomQuery, query_id: 'xxxxx'>
                      - <Table: 'events.calls', query_id: 'xxxxx'>
                         - <Table: 'events.calls', query_id: 'xxxxx'>
             - <Query of type: PolygonSpatialUnit, query_id: 'xxxxx'>
                - <Table: 'geography.admin3', query_id: 'xxxxx'>
        """
    )

    s = StringIO()
    print_dependency_tree(q, stream=s)
    output = s.getvalue()
    output_with_query_ids_replaced = re.sub(r"\b[0-9a-f]+\b", "xxxxx", output)

    assert expected_output == output_with_query_ids_replaced


def test_calculate_dependency_graph():
    """
    Test that calculate_dependency_graph() runs and the returned graph has some correct entries.
    """
    query = daily_location("2016-01-01")
    G = calculate_dependency_graph(query, analyse=True)
    sd = EventTableSubset(
        start="2016-01-01",
        stop="2016-01-02",
        columns=["msisdn", "datetime", "location_id"],
    )
    assert f"x{sd.query_id}" in G.nodes()
    assert G.nodes[f"x{sd.query_id}"]["query_object"].query_id == sd.query_id


def test_unstored_dependencies_graph():
    """
    Test that unstored_dependencies_graph returns the correct graph in an example case.
    """
    # Create dummy queries with dependency structure
    #
    #           5:unstored
    #            /       \
    #       3:stored    4:unstored
    #      /       \     /
    # 1:unstored   2:unstored
    #
    # Note: we add a string parameter to each query so that they have different query IDs
    dummy1 = DummyQuery(dummy_param=["dummy1"])
    dummy2 = DummyQuery(dummy_param=["dummy2"])
    dummy3 = DummyQuery(dummy_param=["dummy3", dummy1, dummy2])
    dummy4 = DummyQuery(dummy_param=["dummy4", dummy2])
    dummy5 = DummyQuery(dummy_param=["dummy5", dummy3, dummy4])
    dummy3.store()

    expected_query_nodes = [dummy2, dummy4]
    graph = unstored_dependencies_graph(dummy5)
    assert not any(dict(graph.nodes(data="stored")).values())
    assert len(graph) == len(expected_query_nodes)
    for query in expected_query_nodes:
        assert f"x{query.query_id}" in graph.nodes()
        assert (
            graph.nodes[f"x{query.query_id}"]["query_object"].query_id == query.query_id
        )


def test_unstored_dependencies_graph_for_stored_query():
    """
    Test that the unstored dependencies graph for a stored query is empty.
    """
    dummy1 = DummyQuery(dummy_param=["dummy1"])
    dummy2 = DummyQuery(dummy_param=["dummy2"])
    dummy3 = DummyQuery(dummy_param=["dummy3", dummy1, dummy2])
    dummy3.store()

    graph = unstored_dependencies_graph(dummy3)
    assert len(graph) == 0


def test_plot_dependency_graph():
    """
    Test that plot_dependency_graph() runs and returns the expected IPython.display objects.
    """
    query = daily_location(date="2016-01-02")
    output_svg = plot_dependency_graph(query, format="svg")
    output_png = plot_dependency_graph(query, format="png", width=600, height=200)

    assert isinstance(output_svg, IPython.display.SVG)
    assert isinstance(output_png, IPython.display.Image)
    assert output_png.width == 600
    assert output_png.height == 200

    with pytest.raises(ValueError, match="Unsupported output format: 'foobar'"):
        plot_dependency_graph(query, format="foobar")


def test_store_queries_in_order():
    """
    Test that store_queries_in_order() stores each query's dependencies before storing that query itself.
    """

    class QueryWithStoreAssertions(DummyQuery):
        def store(self):
            for query in self.dependencies:
                assert query.is_stored
            super().store()

    dummy1 = QueryWithStoreAssertions(dummy_param=["dummy1"])
    dummy2 = QueryWithStoreAssertions(dummy_param=["dummy2"])
    dummy3 = QueryWithStoreAssertions(dummy_param=["dummy3", dummy1, dummy2])
    dummy4 = QueryWithStoreAssertions(dummy_param=["dummy4", dummy2])
    dummy5 = QueryWithStoreAssertions(dummy_param=["dummy5", dummy3, dummy4])
    graph = calculate_dependency_graph(dummy5)
    store_queries_in_order(graph)


def test_storable_dependencies():
    """
    Test that the set of only storeable dependencies is returned.
    """

    class UnStoreableQuery(DummyQuery):
        @property
        def table_name(self):
            raise NotImplementedError("This dummy cannot be stored.")

    dummy = DummyQuery(dummy_param="DUMMY")
    unstoreable_dummy = UnStoreableQuery(dummy_param="UNSTOREABLE_DUMMY")

    nested = DummyQuery(dummy_param=[dummy, unstoreable_dummy])
    assert storable_dependencies(nested) == {dummy, nested}


def test_stored_dependencies(dummy_redis):
    """
    Test that the set of already stored dependencies is returned.
    """
    dummy = DummyQuery(dummy_param="DUMMY")
    stored_dummy = DummyQuery(dummy_param="STORED_DUMMY")
    stored_dummy.store()

    nested = DummyQuery(dummy_param=[dummy, stored_dummy])
    assert stored_dependencies(nested) == {stored_dummy}


def test_stored_dependencies_ratio(dummy_redis):
    """
    Test correct ratio of stored to unstored dependencies is returned.
    """
    dummy = DummyQuery(dummy_param="DUMMY")
    stored_dummy = DummyQuery(dummy_param="STORED_DUMMY")
    stored_dummy.store()

    nested = DummyQuery(dummy_param=[dummy, stored_dummy])
    assert stored_dependencies_ratio(nested) == (1, 3)
    nested.store()
    assert stored_dependencies_ratio(nested) == (1, 1)
