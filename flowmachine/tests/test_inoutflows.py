# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the inout flows methods
"""


from flowmachine.core import make_spatial_unit
from flowmachine.features import Flows, daily_location


def test_inoutflow_with_double_column_location():
    """
    Test that flowmachine.Inflow can handle a location with
    more than one column.
    """

    dl1 = daily_location("2016-01-01", spatial_unit=make_spatial_unit("versioned-site"))
    dl2 = daily_location("2016-01-02", spatial_unit=make_spatial_unit("versioned-site"))

    flow = Flows(dl1, dl2)
    expected_columns = ["site_id", "version", "lon", "lat", "value"]
    assert flow.inflow().column_names == expected_columns

    expected_columns = ["site_id", "version", "lon", "lat", "value"]
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
    assert df.set_index("pcod").loc["524 1 02 09"][0] == 24


def test_inflow_value(get_dataframe):
    """
    One of the values for the outflows.
    """
    dl1 = daily_location("2016-01-01")
    dl2 = daily_location("2016-01-02")
    flow = Flows(dl1, dl2)
    inflow = flow.inflow()
    df = get_dataframe(inflow)
    assert df.set_index("pcod").loc["524 1 03 13"][0] == 20
