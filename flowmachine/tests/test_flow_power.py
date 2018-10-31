# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for exponentiation operations.
"""


import pytest

from flowmachine.features import daily_location, Flows


def test_a_squared(get_dataframe):
    """
    Raising a Flows() to 2 squares it.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    relfl = flowA ** 2
    df_rel = get_dataframe(relfl)
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert 9 == diff


def test_a_negative_exp(get_dataframe):
    """
    Raising to a negative power is the same as x / Flows().
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    relfl = flowA ** -1
    df_rel = get_dataframe(relfl)
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert pytest.approx(1 / 3) == diff


def test_a_float_exp(get_dataframe):
    """
    Raising Flows() to 0.5 gives square root.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    relfl = flowA ** 0.5
    df_rel = get_dataframe(relfl)
    diff = df_rel[(df_rel.name_from == "Bajhang") & (df_rel.name_to == "Myagdi")][
        "count"
    ].values[0]
    assert 3 ** 0.5 == diff


def test_bad_exp():
    """
    Raising to something which isn't a float or an int raises a ValueError.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    with pytest.raises(TypeError):
        relfl = flowA ** "A"
