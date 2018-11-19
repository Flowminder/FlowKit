# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for Flows() division operations.
"""

import pytest
from flowmachine.features import Flows, daily_location


def test_a_div_b(get_dataframe):
    """
    Dividing one Flows() by another gives an expected value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    relfl = flowA / flowB
    df_rel = get_dataframe(relfl)
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert 3 / 4 == diff


def test_a_div_int(get_dataframe):
    """
    Dividing one Flows() by an integer gives an expected value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    relfl = flowA / 2
    df_rel = get_dataframe(relfl)
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert 3 / 2 == diff


def test_a_div_zero(get_dataframe):
    """
    Dividing Flows() by zero is a NaN.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    relfl = flowA / 0
    df_rel = get_dataframe(relfl)
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert diff is None


def test_a_div_float(get_dataframe):
    """
    Dividing one Flows() by a float gives an expected value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    relfl = flowA / 6.5
    df_rel = get_dataframe(relfl)
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert pytest.approx(3 / 6.5) == diff


def test_bad_divisor():
    """
    Dividing by something which isn't a Flows() or scalar raises a ValueError.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    with pytest.raises(TypeError):
        relfl = flowA / "A"


def test_a_div_b_no_b_flow(get_dataframe):
    """
    Rows where there is not an exact match are not returned.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    relfl = flowA / flowB
    df_rel = get_dataframe(relfl)
    with pytest.raises(IndexError):
        diff = df_rel[(df_rel.name_from == "Humla") & (df_rel.name_to == "Kapilbastu")][
            "count"
        ].values[0]
