# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the inout flows methods
"""


from flowmachine.features import Flows, daily_location


def test_inoutflow_with_double_column_location():
    """
    Test that flowmachine.Inflow can handle a location with
    more than one column.
    """

    dl1 = daily_location("2016-01-01", level="versioned-site")
    dl2 = daily_location("2016-01-02", level="versioned-site")

    flow = Flows(dl1, dl2)
    expected_columns = ["site_id_to", "version_to", "lon_to", "lat_to", "total"]
    assert flow.inflow().column_names == expected_columns

    expected_columns = ["site_id_from", "version_from", "lon_from", "lat_from", "total"]
    assert flow.outflow().column_names == expected_columns


def test_outflow_value(get_dataframe):
    """
    One of the values for the outflows.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)
    outflow = flow.outflow()
    df = get_dataframe(outflow)
    assert df.set_index("pcod_from").loc["524 1 02 09"][0] == 24


def test_inflow_value(get_dataframe):
    """
    One of the values for the outflows.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)
    inflow = flow.inflow()
    df = get_dataframe(inflow)
    assert df.set_index("pcod_to").loc["524 1 03 13"][0] == 20
