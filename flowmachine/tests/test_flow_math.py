# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for Flows general arithmetic functions.
"""
from operator import add, sub, truediv, mul, pow

import pytest

from flowmachine.features import daily_location, Flows
from flowmachine.features.location.flows import EdgeList


@pytest.mark.parametrize("op", [add, sub, truediv, mul, pow], ids=lambda x: str(x))
def test_flow_math_store(op, exemplar_level_param, flowmachine_connect):
    """
    Storing works for flows added together at all levels
    """
    dl1 = daily_location("2016-01-01", **exemplar_level_param)
    dl2 = daily_location("2016-01-02", **exemplar_level_param)
    fl = op(Flows(dl1, dl2), Flows(dl1, dl2))
    fl.store().result()
    assert flowmachine_connect.has_table(
        *fl.fully_qualified_table_name.split(".")[::-1]
    )


def test_average_self(get_dataframe):
    """
    Adding a Flows() to itself and dividing is the same as the original Flows().
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    avged = get_dataframe((flowA + flowA) / 2)
    orig = get_dataframe(flowA)
    compare = (
        avged.set_index(["pcod_from", "pcod_to"]).sort_index()
        == orig.set_index(["pcod_from", "pcod_to"]).sort_index()
    )
    assert compare.all().values[0]


def test_stdev(get_dataframe):
    """
    Standard deviation of Flows() calculated in DB matches a known value.
    """

    def mean(xs):
        return sum(xs) / len(xs)

    def stdev(xs):
        return (sum((flow - mean(xs)) ** 2 for flow in xs) / (len(xs) - 1)) ** 0.5

    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)

    flow_list = [flowA, flowB]
    mean_fl = mean(flow_list)
    std_fl = stdev(flow_list)
    std_fl = get_dataframe(std_fl).set_index(["pcod_from", "pcod_to"])
    assert std_fl.loc["524 4 12 65", "524 3 09 48"][0] == pytest.approx(
        1.4142135623730951
    )


def test_average_self_v_sites(get_dataframe):
    """
    Adding a versioned-sites level Flows() to itself and dividing is the same as the original Flows().
    """
    dl1 = daily_location("2016-01-01", level="versioned-site")
    dl2 = daily_location("2016-01-02", level="versioned-site")
    flowA = Flows(dl1, dl2)
    avged = get_dataframe((flowA + flowA) / 2)
    orig = get_dataframe(flowA)
    avged.set_index(
        [
            "site_id_from",
            "version_from",
            "lat_from",
            "lon_from",
            "site_id_to",
            "version_to",
            "lat_to",
            "lon_to",
        ],
        inplace=True,
    )
    avged.sort_index(inplace=True)
    orig.set_index(
        [
            "site_id_from",
            "version_from",
            "lat_from",
            "lon_from",
            "site_id_to",
            "version_to",
            "lat_to",
            "lon_to",
        ],
        inplace=True,
    )
    orig.sort_index(inplace=True)
    compare = avged == orig
    assert compare.all().values[0]


def test_stdev_v_sites(get_dataframe):
    """
    Standard deviation of versioned-site level Flows() calculated in DB matches a known value.
    """

    def mean(xs):
        return sum(xs) / len(xs)

    def stdev(xs):
        return (sum((flow - mean(xs)) ** 2 for flow in xs) / (len(xs) - 1)) ** 0.5

    dl1 = daily_location("2016-01-01", level="versioned-site")
    dl2 = daily_location("2016-01-02", level="versioned-site")
    dl3 = daily_location("2016-01-03", level="versioned-site")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl2, dl3)
    flow_list = [flowA, flowB]
    mean_fl = mean(flow_list)
    std_fl = stdev(flow_list)
    std_fl = get_dataframe(std_fl).set_index(
        ["site_id_from", "version_from", "site_id_to", "version_to"]
    )
    assert std_fl.loc["0xqNDj", 0, "B8OaG5", 0][4] == pytest.approx(0.7071067811865480)


def test_edgelist(get_dataframe):
    """Test that an EdgeList object can be created."""
    query = daily_location("2016-01-01").aggregate()
    wrapped = EdgeList(query)
    df = get_dataframe(wrapped)
    assert all(df.set_index(["pcod_from"]).loc["524 2 05 29"]["count"] == 11)
    query = daily_location("2016-01-01").aggregate()
    wrapped = EdgeList(query, left_handed=False)
    df = get_dataframe(wrapped)
    assert all(df.set_index(["pcod_to"]).loc["524 2 05 29"]["count"] == 11)


def test_edgelist_mul(get_dataframe):
    """Test that an edgelist object can be multiplied by a flows"""
    # Basic idea here is that we should get back to the query originally
    # wrapped by dividing the multiplied flow by the original flow
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    query = daily_location("2016-01-01").aggregate()
    wrapped = EdgeList(query)
    mulled = flowA * wrapped
    fl_df = get_dataframe(flowA).set_index(["pcod_from", "pcod_to"])
    qur_df = get_dataframe(query).set_index("pcod")
    mulled_df = get_dataframe(mulled).set_index(["pcod_from", "pcod_to"])
    dived = mulled_df / fl_df
    grped = dived.groupby(level="pcod_from").mean()
    relabled = qur_df.rename_axis("pcod_from").rename(columns={"total": "count"})
    assert all(relabled.sort_index() == grped)
