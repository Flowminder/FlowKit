# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for Flows() multiplication operations.
"""

import pytest

from flowmachine.features import daily_location, Flows


def test_a_mul_b(get_dataframe):
    """
    Multiplying one Flows() by another gives an expected value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    relfl = flowA * flowB
    df_rel = get_dataframe(relfl)
    diff = df_rel[
        (df_rel.pcod_from == "524 5 13 67") & (df_rel.pcod_to == "524 3 08 44")
    ]["count"].values[0]
    assert 12 == diff


def test_a_mul_int(get_dataframe):
    """
    Multiplying one Flows() by an integer gives an expected value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    relfl = flowA * 2
    df_rel = get_dataframe(relfl)
    diff = df_rel[
        (df_rel.pcod_from == "524 5 13 67") & (df_rel.pcod_to == "524 3 08 44")
    ]["count"].values[0]
    assert 6 == diff


def test_a_mul_float(get_dataframe):
    """
    Multiplying one Flows() by a float gives an expected value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    relfl = flowA * 6.5
    df_rel = get_dataframe(relfl)
    diff = df_rel[
        (df_rel.pcod_from == "524 5 13 67") & (df_rel.pcod_to == "524 3 08 44")
    ]["count"].values[0]
    assert 3 * 6.5 == diff


def test_a_mul_fraction(get_dataframe):
    """
    Multiplying one Flows() by a float gives an expected value.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")

    flowA = Flows(dl1, dl2)
    relfl = flowA * 0.5
    df_rel = get_dataframe(relfl)
    diff = df_rel[
        (df_rel.pcod_from == "524 5 13 67") & (df_rel.pcod_to == "524 3 08 44")
    ]["count"].values[0]
    assert pytest.approx(3 / 2) == diff


def test_bad_multiplier():
    """
    Multiplying by something which isn't a Flows() or scalar raises a value error.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flowA = Flows(dl1, dl2)
    with pytest.raises(TypeError):
        relfl = flowA * "A"


def test_a_mul_b_no_b_flow(get_dataframe):
    """
    No row is returned if the Flows() is not in both A and B.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    dl3 = daily_location("2016-01-07")
    flowA = Flows(dl1, dl2)
    flowB = Flows(dl1, dl3)
    relfl = flowA * flowB
    df_rel = get_dataframe(relfl)
    with pytest.raises(IndexError):
        diff = df_rel[
            (df_rel.pcod_from == "524 4 12 66") & (df_rel.pcod_to == "524 3 09 49")
        ]["count"].values[0]
