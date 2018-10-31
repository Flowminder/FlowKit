# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the Louvain() class.
"""


import pytest

from flowmachine.models import Louvain
from flowmachine.features import ContactBalance


def test_returns_correct_results():
    """
    Louvain().run() doesn't put everybody in the same community.
    """
    test_graph = ContactBalance("2016-01-01", "2016-01-07")
    l = Louvain(test_graph.to_networkx(directed_graph=False))
    set_df = l.run().get_dataframe().set_index("subscriber")
    assert len(set(set_df.community)) != 1


def test_error_raised_if_graph_not_passed():
    """
    Louvain().run() raises error if graph passed is not nx.Graph()
    """
    with pytest.raises(ValueError):
        Louvain(graph="foo")


def test_louvain_takes_reduction_parameters():
    """
    Louvain().run() takes reduction parameters.
    """
    test_graph = ContactBalance("2016-01-01", "2016-01-07")
    l = Louvain(test_graph.to_networkx(directed_graph=False))
    result = l.run(
        resolution=0.7, min_members=10, weight_property="events"
    ).get_dataframe()
    set_df = result.set_index("subscriber")
    assert (
        set_df.loc["nMvpK39bowVXYN9G"]["community"]
        != set_df.loc["qPAbaED3vDYkjZ0n"]["community"]
    )

    assert (
        set_df.loc["qRlQo7ly3Zg9GVN2"]["community"]
        != set_df.loc["YeqxBmVgL1EnONv8"]["community"]
    )
    assert (
        set_df.loc["E0LZAa7AyNd34Djq"]["community"]
        == set_df.loc["8dpPLR15XwR7jQyN"]["community"]
    )
    assert (
        set_df.loc["APj9roe8jKOwEDZl"]["community"]
        == set_df.loc["4dqenN2oQZExwEK2"]["community"]
    )
