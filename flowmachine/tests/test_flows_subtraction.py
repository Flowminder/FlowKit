# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.features import daily_location, Flows


@pytest.fixture
def flows(flowmachine_connect):
    """Fixture providing two flows."""
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flow_a = Flows(dl1, dl2)
    flow_b = Flows(dl1, dl3)
    yield flow_a, flow_b


def test_flows_raise_error():
    """
    Flows() raises error if location levels are different.
    """
    dl1 = daily_location("2016-01-01", level="admin3")
    dl2 = daily_location("2016-01-01", level="admin2")
    with pytest.raises(ValueError):
        Flows(dl1, dl2)


def test_a_sub_b(flows, get_dataframe):
    """
    Flows() between two locations returns expected positive value.
    """
    flow_a, flow_b = flows
    relfl = flow_a - flow_b
    df_rel = get_dataframe(relfl)
    diff = df_rel[
        (df_rel.pcod_from == "524 5 13 67") & (df_rel.pcod_to == "524 5 14 73")
    ]["count"].values[0]
    assert 1 == diff


def test_a_sub_b_negative(flows, get_dataframe):
    """
    Flows() between two locations returns expected negattive value.
    """
    flow_a, flow_b = flows
    relfl = flow_a - flow_b
    df_rel = get_dataframe(relfl)
    # 524 5 13 67    524 3 08 44  3   4.0
    diff = df_rel[
        (df_rel.pcod_from == "524 5 13 67") & (df_rel.pcod_to == "524 3 08 44")
    ]["count"].values[0]
    assert -1 == diff


def test_sub_commutative(flows, get_dataframe):
    """
    Flows() subtraction is a noncommutative operation.
    """
    flow_a, flow_b = flows
    diff = get_dataframe(flow_a - (flow_a - flow_b))
    diff = diff[(diff.pcod_from == "524 5 13 67") & (diff.pcod_to == "524 3 08 44")][
        "count"
    ].values[0]
    assert 4 == diff


def test_sub_scalar(flows, get_dataframe):
    """
    Flows() subtraction of a scalar gives a known value.
    """
    flow_a, _ = flows
    diff = get_dataframe(flow_a - 3)
    diff = diff[(diff.pcod_from == "524 5 13 67") & (diff.pcod_to == "524 3 08 44")][
        "count"
    ].values[0]
    assert 0 == diff


def test_a_sub_b_no_b_flow(flows, get_dataframe):
    """
    Flows() between two locations where one location is NA should return the flow of the non NA location.
    """
    flow_a, flow_b = flows
    relfl = flow_a - flow_b
    df_rel = get_dataframe(relfl)
    # 524 4 12 66  524 3 09 49  2   NaN
    diff = df_rel[
        (df_rel.pcod_from == "524 4 12 66") & (df_rel.pcod_to == "524 3 09 49")
    ]["count"].values[0]
    assert 2 == diff


def test_abs_diff_equal(flows, get_dataframe):
    """
    The absolute difference between flows A and B should be equal for A - B and B - A.
    """
    flow_a, flow_b = flows
    relfl = flow_a - flow_b
    df_rel = get_dataframe(relfl)
    relfl_reverse = get_dataframe(flow_b - flow_a)
    compare = abs(
        relfl_reverse.set_index(["pcod_from", "pcod_to"]).sort_index()
    ) == abs(df_rel.set_index(["pcod_from", "pcod_to"]).sort_index())
    assert compare.all().values[0]
