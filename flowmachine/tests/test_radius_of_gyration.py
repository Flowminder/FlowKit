# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.features.subscriber.daily_location import locate_subscribers
from flowmachine.features.subscriber import *

import pandas as pd


def test_has_right_columns():
    """
    RadiusOfGyration() dataframe returns the right columns.
    """
    RoG = RadiusOfGyration("2016-01-01", "2016-01-02")
    expected_columns = ["subscriber", "rog"]
    assert RoG.column_names == expected_columns


def test_values(get_dataframe):
    """
    RadiusOfGyration() correct values are returned for a few hand picked values.
    """
    RoG = RadiusOfGyration("2016-01-01", "2016-01-02")
    df = get_dataframe(RoG)
    df.set_index("subscriber", inplace=True)
    assert int(df.rog["dM4aLP8N97eYABwR"] * 100.0) == 26506
    assert int(df.rog["GNLM7eW5J5wmlwRa"] * 100.0) == 16882


def test_can_return_in_meters(get_dataframe):
    """
    RadiusOfGyration() can be calculated in meters as well as kilometers.
    """
    RoG = RadiusOfGyration("2016-01-01", "2016-01-02")
    RoG_meters = RadiusOfGyration("2016-01-01", "2016-01-02", unit="m")
    df_m = get_dataframe(RoG_meters)
    df_km = get_dataframe(RoG)
    ratio = df_km["rog"] / df_m["rog"]
    ratio_clean = ratio.dropna()
    ration_clean_round = ratio_clean.round(3)
    assert list(ration_clean_round.dropna().unique()) == [0.001]


def test_can_be_joined(get_dataframe):
    """
    RadiusOfGyration() can be joined with a location type metric.
    """
    RoG = RadiusOfGyration("2016-01-01", "2016-01-02")
    dl = locate_subscribers("2016-01-01", "2016-01-02", level="admin3")
    rog_JA = RoG.join_aggregate(dl)
    df = get_dataframe(rog_JA)
    assert isinstance(df, pd.DataFrame)
    assert rog_JA.column_names == ["name", "rog"]
