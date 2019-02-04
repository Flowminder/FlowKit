# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pytest

from flowmachine.features import NocturnalCalls


def test_has_right_columns():
    """
    NocturnalCalls() dataframe returns the right columns.
    """
    noc = NocturnalCalls("2016-01-01", "2016-01-02")
    expected_columns = ["subscriber", "percentage_nocturnal"]
    assert noc.column_names == expected_columns


def test_values(get_dataframe):
    """
    NocturnalCalls() returns correct values for a few hand picked values.
    """
    noc = NocturnalCalls("2016-01-01", "2016-01-02")
    df = get_dataframe(noc)
    df.set_index("subscriber", inplace=True)
    assert df.percentage_nocturnal["BKMy1nYEZpnoEA7G"] == pytest.approx(57.142857)
    assert df.percentage_nocturnal["dM4aLP8N97eYABwR"] == pytest.approx(33.333333)
