# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.features import daily_location, Flows


def test_flows_raise_error():
    """
    Flows() raises error if location levels are different.
    """
    dl1 = daily_location("2016-01-01", level="admin3")
    dl2 = daily_location("2016-01-01", level="admin2")
    with pytest.raises(ValueError):
        Flows(dl1, dl2)


def test_a_sub_b(get_dataframe):
    """
    Flows() between two locations returns expected positive value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    relfl = flowA - flowB
    df_rel = get_dataframe(relfl)
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Dadeldhura")][
        "count"
    ].values[0]
    assert 1 == diff


def test_a_sub_b_negative(get_dataframe):
    """
    Flows() between two locations returns expected negattive value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    relfl = flowA - flowB
    df_rel = get_dataframe(relfl)
    # Bajhang    Myagdi  3   4.0
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert -1 == diff


def test_sub_commutative(get_dataframe):
    """
    Flows() subtraction is a noncommutative operation.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    diff = get_dataframe(flowA - (flowA - flowB))
    diff = diff[(diff.name_from == "Bajhang") & (diff.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert 4 == diff


def test_sub_scalar(get_dataframe):
    """
    Flows() subtraction of a scalar gives a known value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)

    diff = get_dataframe(flowA - 3)
    diff = diff[(diff.name_from == "Bajhang") & (diff.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert 0 == diff


def test_a_sub_b_no_b_flow(get_dataframe):
    """
    Flows() between two locations where one location is NA should return the flow of the non NA location.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    relfl = flowA - flowB
    df_rel = get_dataframe(relfl)
    # Humla  Kapilbastu  2   NaN
    diff = df_rel[(df_rel.name_from == "Humla") & (df_rel.name_to == "Kapilbastu")][
        "count"
    ].values[0]
    assert 2 == diff


def test_abs_diff_equal(get_dataframe):
    """
    The absolute difference between flows A and B should be equal for A - B and B - A.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    relfl = flowA - flowB
    df_rel = get_dataframe(relfl)
    relfl_reverse = get_dataframe(flowB - flowA)
    compare = abs(
        relfl_reverse.set_index(["name_from", "name_to"]).sort_index()
    ) == abs(df_rel.set_index(["name_from", "name_to"]).sort_index())
    assert compare.all().values[0]
