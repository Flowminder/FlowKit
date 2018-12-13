# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for Flows() additions.
"""

from flowmachine.features import daily_location, Flows


def test_a_plus_b(get_dataframe):
    """
    Adding two Flows() together produces a known value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    df_rel = get_dataframe(flowA + flowB)
    diff = df_rel[
        (df_rel.pcod_from == "524 5 13 67") & (df_rel.pcod_to == "524 3 08 44")
    ]["count"].values[0]
    assert 7 == diff


def test_a_plus_b_no_b_flow(get_dataframe):
    """
    Adding a Flows() where it does not exist on the other side returns the original Flows() count.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    df_rel = get_dataframe(flowA + flowB)
    diff = df_rel[
        (df_rel.pcod_from == "524 4 12 66") & (df_rel.pcod_to == "524 3 09 49")
    ]["count"].values[0]
    assert 2 == diff


def test_a_plus_one(get_dataframe):
    """
    Adding a scalar to a Flows() works.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    df_rel = get_dataframe(1 + flowA)
    diff = df_rel[
        (df_rel.pcod_from == "524 4 12 66") & (df_rel.pcod_to == "524 3 09 49")
    ]["count"].values[0]
    assert 3 == diff


def test_summation(get_dataframe):
    """
    Summing a list of Flows() yields a known value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    flows = [flowA, flowB, flowA]
    summed = sum(flows)
    res = get_dataframe(summed)
    result = res[(res.pcod_from == "524 5 13 67") & (res.pcod_to == "524 3 08 44")][
        "count"
    ].values[0]
    assert 10 == result


def test_flow_scalar_addition_store(flowmachine_connect):
    """
    Adding a scalar to a Flows() can be stored.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    fl = Flows(dl1, dl2)
    rel = 1 + fl
    rel.store().result()

    assert flowmachine_connect.has_table(*rel.table_name.split(".")[::-1])
