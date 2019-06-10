# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import LastLocation


def test_last_location_column_names(exemplar_spatial_unit_param, get_dataframe):
    """
    LastLocation() is able to return a dataframe.
    """

    last_loc = LastLocation(
        "2016-01-01", "2016-01-02", spatial_unit=exemplar_spatial_unit_param
    )
    df = get_dataframe(last_loc)
    assert df.columns.tolist() == last_loc.column_names


def test_last_loc_vsite(get_dataframe):
    """
    LastLocation() returns correct last location.
    """

    last_loc = LastLocation(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("versioned-site")
    )

    df = get_dataframe(last_loc)
    df.set_index("subscriber", inplace=True)
    assert "QeBRM8" == df.loc["038OVABN11Ak4W5P"].site_id
    assert "dJb0Wd" == df.loc["zGWn8opVmOQAD6xY"].site_id


def test_last_loc_lon_lat(get_dataframe):
    """
    LastLocation() can make queries at the lon-lat level.
    """

    last_loc = LastLocation(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
    )
    df = get_dataframe(last_loc)
    df.set_index("subscriber", inplace=True)
    assert pytest.approx(83.09669810947962) == float(df.loc["yqw50eNyEwOxNDGL"].lon)
    assert pytest.approx(29.135638957790576) == float(df.loc["yqw50eNyEwOxNDGL"].lat)
